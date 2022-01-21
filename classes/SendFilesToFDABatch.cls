/*
* @author Smarteeva
* @date 2022-01-17
* @name: SendFilesToFDABatch
* @description: This class is responsible to send the MDR XML Files to FDA.
* @Change History
*
*   Tag                Author              Date         Incident No.   Requested-BY      Description
* *****************************************************************************************************************
*   Version1.0.0       Smarteeva           2022-01-17                                   Initial Version
* *****************************************************************************************************************/
public with sharing class SendFilesToFDABatch Implements Database.Batchable<SObject>, Database.AllowsCallouts{

    public Database.querylocator start(Database.BatchableContext con){
        return Database.getQueryLocator('SELECT (SELECT ID FROM Attachments WHERE name LIKE \'%.xml\' ORDER BY createddate DESC LIMIT 1) FROM smrtat__MDR__c WHERE smrtat__Status__c = \'Submitted to FDA\' AND smrtat__Acknowledgment_Status__c = null LIMIT 95');
    } 

    public void execute(Database.BatchableContext con, List<smrtat__MDR__c> mdrs){
        Set<Id> attIds = new Set<Id>();
        for (smrtat__MDR__c m : mdrs) {
            if(m.Attachments.size() > 0)
            	attIds.add(m.Attachments[0].Id);
        }
        List<Attachment> attachmentsToSubmit = [SELECT ID, name, Body, ParentId, ContentType FROM Attachment WHERE Id IN: attIds];
        if (attachmentsToSubmit.size() > 0) {
            List<smrtat__MDR__c> mdrsToUpdate = new List<smrtat__MDR__c>();

            smrtat__S3_Integration_Info__c allCodes = [SELECT id,setEndpoint_Submit__c,Station__c, Partner__c FROM smrtat__S3_Integration_Info__c LIMIT 1];
            HttpRequest req = new HttpRequest();
            req.setHeader('Content-Type', 'application/json');
            req.setMethod('POST');
            if (Test.isRunningTest())
                req.setEndpoint('https://api.smarteevagateway.com/authorize');
            else
                req.setEndpoint('callout:MFT_Authentication');
           
            Auth au = new Auth();
            au.username = '{!$Credential.UserName}';
            au.password = '{!$Credential.Password}';
            req.setBody(JSON.serialize(au));
            Http http = new Http();
            HTTPResponse res = http.send(req);
            System.debug(res.getBody());
            Token c = (Token)JSON.deserializeStrict(res.getBody(),Token.class);

            for (Attachment att : attachmentsToSubmit) {
                if (att.Name.containsIgnoreCase('.xml')){
                    try{
                        HttpRequest req1 = new HttpRequest();
                        req1.setMethod('POST');
                        req1.setEndpoint(allCodes.setEndpoint_Submit__c);
                        req1.setHeader('Authorization', c.api_token);
                        req1.setHeader('AS2-From',allCodes.Station__c );
                        req1.setHeader('AS2-To', allCodes.Partner__c);
                        req1.setHeader('Attachment-Name', att.Name);
                        req1.setHeader('Content-Type', att.ContentType);
                        req1.setBodyAsBlob(att.Body);
                        Http http1 = new Http();
                        HTTPResponse res1 = http1.send(req1);

                        SendResponse resp = (SendResponse)JSON.deserializeStrict(res1.getBody(),SendResponse.class);

                        smrtat__MDR__c mdr = [SELECT id,smrtat__Message_ID__c FROM smrtat__MDR__c WHERE Id =: att.ParentId];
                        mdr.smrtat__Message_ID__c = resp.as2MessageId;
                        mdr.smrtat__Status__c = 'ACK1 Received';
                        mdrsToUpdate.add(mdr);
                    }catch(Exception e){
                        AuditTrailUtility.logException(e, 'getTxtFile()', 'SendFilesToFDABatch', false);
                    }
                }
            }

            if(mdrsToUpdate.size()  > 0){
                Database.update(mdrsToUpdate, false);
            }
        }
    }
    
    public void finish(Database.BatchableContext con){

    }

    public class Auth{
        public String username;
        public String password;
    }
    public class Token{
        public String api_token;
        public String refresh_token;
    }
    public class SendResponse{
        public String as2MessageId;
        public String message;
    }
}