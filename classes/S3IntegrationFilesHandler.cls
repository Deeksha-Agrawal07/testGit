global class S3IntegrationFilesHandler implements Database.Batchable<SObject>, Database.AllowsCallouts{ 

    public String fileType;

    public S3IntegrationFilesHandler(String fileType){
        this.fileType = fileType;
    }

    public Database.querylocator start(Database.BatchableContext con){
        //fetching all the filelogs that are not downloaded yet.
        String query = 'SELECT Id, File_name__c, Is_Downloaded__c FROM File_Log__c WHERE Is_Downloaded__c = false';
        if(this.fileType == 'text'){
            query = query + ' AND File_name__c LIKE \'%.txt%\'';
        }else if(this.fileType == 'html'){
            query = query + ' AND File_name__c LIKE \'%.html%\'';
        }
        return Database.getQueryLocator(query);
    } 

    public void execute(Database.BatchableContext con, List<File_Log__c> fileLogs){
        List<String> fileNamesToUpdate = new List<String>();
        List<Attachment> attachmentsToCreate = new List<Attachment>();
        //creating map for MDRs to update (creating map instead of list , just to avoid duplicate id issue).
        Map<Id, smrtat__MDR__c> mdrsToUpdateMap = new Map<Id, smrtat__MDR__c>();

        smrtat__S3_Integration_Info__c allCodes = [SELECT id,bucketname__c ,host__c , key__c ,secret__c FROM smrtat__S3_Integration_Info__c LIMIT 1];
        String formattedDateString = Datetime.now().formatGMT('EEE, dd MMM yyyy HH:mm:ss z');
        String key = allCodes.key__c; //AWS key
        String secret = allCodes.secret__c; //AWS Secret key
        String bucketname = allCodes.bucketname__c; //AWS bucket name
        String host = allCodes.host__c;
        string auth;
        String method = 'GET'; 
        
        for (File_Log__c f : fileLogs) {
            String filename = f.File_name__c;
            try{
                String stringToSign = 'GET\n\n\n'+formattedDateString+'\n/'+bucketname +'/'+filename;
                Blob mac = Crypto.generateMac('HMACSHA1', blob.valueof(stringToSign),blob.valueof(secret));
                String sig = EncodingUtil.base64Encode(mac);
                auth = 'AWS' + ' ' + key + ':' + sig;
                HttpRequest req = new HttpRequest();
                req.setMethod(method);
                req.setHeader('Date', formattedDateString);
                req.setHeader('Authorization',auth);
                Set<String> getParams = new Set<String> {
                    'prefix=' + EncodingUtil.urlEncode(filename+'/','UTF-8'),
                        'delimiter=' + EncodingUtil.urlEncode('/', 'UTF-8')
                        };
                            
                String endpointBase = 'https://' + bucketname + '.' + host + '/' + filename;
                
                req.setEndpoint(endpointBase + '?' + 'prefix=' + EncodingUtil.urlEncode(filename+'/','UTF-8')+'&delimiter=' + EncodingUtil.urlEncode('/', 'UTF-8'));
                
                Http http = new Http();
                String msgId, coreId;
                HTTPResponse res = http.send(req);
                //processing file logs with .txt files.
                if(filename.containsIgnoreCase('.txt')){
                    //fetching message id and core Id from the text file.
                    for(String line:res.getBody().split('\n')) {
                        if(line.containsIgnoreCase('MessageId'))
                            msgId = (line.replace('MessageId: ', '')).trim();
                        if(line.containsIgnoreCase('CoreId'))
                            coreId = (line.replace('CoreId: ', '')).trim();
                    }

                    if(msgId != null){
                        List<smrtat__MDR__c> mdr = [SELECT id, smrtat__Message_ID__c, smrtat__Core_id__c FROM smrtat__MDR__c WHERE smrtat__Message_ID__c =: msgId];
                    
                        if(mdr.size() > 0){
                            //creating attachment of the file log from the response recieved using file log from S3 and attaching the file to the mDR.
                            Attachment a = new Attachment();
                            a.Body = res.getBodyAsBlob();
                            a.Name = mdr[0].ID+'.txt';
                            a.ParentId = mdr[0].Id;
                            attachmentsToCreate.add(a);
                            
                            //updating core Id in the Mdr received in the text file.
                            mdr[0].smrtat__Core_id__c = coreId;
                            mdrsToUpdateMap.put(mdr[0].Id, mdr[0]);
                            
                            //collecting file names to update the file logs.
                            fileNamesToUpdate.add(filename);
                        }
                    }else{
                        AuditTrailUtility.logException('ACK2 text file cannot be associated with an existing MDR in the system', 'execute() : text files processing', 'S3IntegrationFilesHandler', false);
                    }
                    
                }else if(filename.containsIgnoreCase('.html')){//processing file logs with .html files.
                    String coreIdhtml;
                    List<String> fileLines = res.getBody().split('\n');
                
                    //fetching core Id from the html file's content.
                    for (Integer i=0; i<fileLines.size(); i++) {
                        if(fileLines[i].containsIgnoreCase('Core ID')){
                            coreIdhtml = (fileLines[i+3]).trim();
                            break;
                        }
                    }

                    List<smrtat__MDR__c> mdrhtml = [SELECT id,smrtat__Message_ID__c,smrtat__Core_id__c FROM smrtat__MDR__c WHERE smrtat__Core_id__c=:coreIdhtml];
                    if(mdrhtml.size() > 0){
                        //creating attachment of the file log from the response recieved using file log from S3 and attaching the file to the mDR.
                        Attachment ahtml= new Attachment();
                        ahtml.Body = res.getBodyAsBlob();
                        ahtml.Name = mdrhtml[0].Id+'.html';
                        ahtml.ParentId = mdrhtml[0].Id;
                        attachmentsToCreate.add(ahtml);

                        //collecting file names to update the file logs.
                        fileNamesToUpdate.add(filename);
                    }else{
                        AuditTrailUtility.logException('ACK2 text file cannot be associated with an existing MDR in the system', 'getTxtFile()', 'S3IntegrationFilesHandler', false);
                    }
                }
            }catch(Exception e){
                AuditTrailUtility.logException(e, 'getTxtFile()', 'S3IntegrationFilesHandler', false);
            }
        }

        if(attachmentsToCreate.size() > 0){
            insert attachmentsToCreate;
        }

        if(mdrsToUpdateMap.size() > 0){
            update mdrsToUpdateMap.values();
        }

        if(fileNamesToUpdate.size() > 0){
            List<File_Log__c> logToDelete = new List<File_Log__c>();
            List<File_Log__c> logToUpdate = new List<File_Log__c>();
            List<File_Log__c> filesList = [SELECT id, File_name__c, Is_Synced_with_Libraries__c, Is_Downloaded__c FROM File_Log__c WHERE File_name__c IN: fileNamesToUpdate];
            for(File_Log__c fl : filesList){
                if(fl.Is_Synced_with_Libraries__c){//collecting logs to delete if they are already synced with the library.
                    logToDelete.add(fl);
                }else{
                    //collecting file logs to update as downloaded if they are not synced with library.
                    fl.Is_Downloaded__c = true;
                    logToUpdate.add(fl);
                }
                fl.Is_Downloaded__c = true;
            }
            if(logToDelete.size() > 0){
                delete logToDelete;
            } 
            if(logToUpdate.size() > 0){
                update logToUpdate;
            }
        }
    }

    public void finish(Database.BatchableContext con){
    }
    
    /*public class Auth{
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
    }*/
    
    //Method to Send the file to S3 via MFT Gateway
    //@future(callout=true)
    global static void sendFile(String attName,  Blob attBody, String attParentId, String contentType){
        /*try{
            smrtat__S3_Integration_Info__c allCodes = [SELECT id, smrtat__SetEndpoint_Submit__c, smrtat__Station__c, smrtat__Partner__c 
                                                        FROM smrtat__S3_Integration_Info__c LIMIT 1];
            
            //perform authentication
            HttpRequest req = new HttpRequest();
            req.setHeader('Content-Type', 'application/json');
            req.setMethod('POST');
            req.setEndpoint('callout:MFT_Authentication');
            Auth au = new Auth();
            if(!Test.isRunningTest()){
                
                au.username = '{!$Credential.UserName}';
                au.password = '{!$Credential.Password}';
            }           

            req.setBody(JSON.serialize(au));
            
            Http http = new Http();
            
            HTTPResponse res = http.send(req);
            //authorization response
            Token responseBody = (Token)JSON.deserializeStrict(res.getBody(),Token.class);
            
            //request creation for sending file.
            HttpRequest sendRequest = new HttpRequest();
            sendRequest.setMethod('POST');
            sendRequest.setEndpoint(allCodes.smrtat__SetEndpoint_Submit__c);
            sendRequest.setHeader('Authorization', responseBody.api_token);
            sendRequest.setHeader('AS2-From',allCodes.Station__c );
            sendRequest.setHeader('AS2-To', allCodes.Partner__c);
            sendRequest.setHeader('Attachment-Name', attName);
            sendRequest.setHeader('Content-Type', contentType);
            sendRequest.setBodyAsBlob(attBody);

            Http http1 = new Http();
            //response after sending file.
            SendResponse resp = (SendResponse) JSON.deserializeStrict(http1.send(sendRequest).getBody(), SendResponse.class);
            
            smrtat__MDR__c mdr = [SELECT id FROM smrtat__MDR__c WHERE id =: attParentId];
            mdr.Message_ID__c = resp.as2MessageId;
            Database.SaveResult updateResult = Database.update(mdr, false);
        }catch(Exception e){
            AuditTrailUtility.logException(e, 'sendFile()', 'S3IntegrationFilesHandler', false);
        }*/
    }
    
    //Method to get the Txt file and store in MDR
    //@future(callout=true)
    global static void getTxtFile(String filename){
       /* try{
            smrtat__S3_Integration_Info__c allCodes = [SELECT id, smrtat__BucketName__c, smrtat__Host__c , smrtat__Key__c, smrtat__Secret__c  FROM smrtat__S3_Integration_Info__c LIMIT 1];

            String formattedDateString = Datetime.now().formatGMT('EEE, dd MMM yyyy HH:mm:ss z');
            String bucketname = allCodes.bucketname__c; //AWS bucket name
            String stringToSign = 'GET\n\n\n'+formattedDateString+'\n/'+bucketname +'/'+filename;
            Blob mac = Crypto.generateMac('HMACSHA1', Blob.valueof(stringToSign), Blob.valueof(allCodes.smrtat__Secret__c));

            string auth = 'AWS' + ' ' + allCodes.smrtat__Key__c + ':' + EncodingUtil.base64Encode(mac);// key__c is AWS key
            String msgId,coreId;

            HttpRequest req = new HttpRequest();
            req.setMethod('GET');
            req.setHeader('Date', formattedDateString);
            req.setHeader('Authorization',auth);
                        
            String endpointBase = 'https://' + bucketname + '.' + allCodes.smrtat__Host__c + '/' + filename;
            
            req.setEndpoint(endpointBase + '?' + 'prefix=' + EncodingUtil.urlEncode(filename+'/','UTF-8')+'&delimiter=' + EncodingUtil.urlEncode('/', 'UTF-8'));            
            Http http = new Http();
            HTTPResponse res = http.send(req);

            for(String line : res.getBody().split('\n')) {
                if(line.containsIgnoreCase('MessageId')){
                    msgId = (line.replace('MessageId: ', '')).trim();
                }                    
                if(line.containsIgnoreCase('CoreId')){
                    coreId = (line.replace('CoreId: ', '')).trim();
                }
            }
            
            smrtat__MDR__c mdr = [SELECT id FROM smrtat__MDR__c 
                                    WHERE Message_ID__c =: msgId];
            
            Attachment a = new Attachment();
            a.Name = mdr.ID + '.txt';
            a.Body = res.getBodyAsBlob();
            a.ParentId = mdr.Id;
            insert a;
            
            mdr.Core_ID__c = coreId;
            update mdr;
            
            List<smrtat__File_Log__c> filesList = [SELECT id FROM smrtat__File_Log__c WHERE smrtat__File_Name__c =: filename LIMIT 1];
             //If the file is already synced with the library, delete the file else update the Is_Downloaded__c to true
             if(filesList[0].Is_Synced_with_Libraries__c){
                delete filesList;
            }else{
                filesList[0].Is_Downloaded__c = true;
                update filesList;
            }

        }catch(Exception e){
            AuditTrailUtility.logException(e, 'getTxtFile()', 'S3IntegrationFilesHandler', false);
        }*/
    }
    
    //Method to get HTML file and store in MDR
    //@future(callout=true)
    global static void getHtmlFile(String filename){
        /*try{
            smrtat__S3_Integration_Info__c allCodes = [SELECT id, smrtat__BucketName__c, smrtat__Host__c , smrtat__Key__c, smrtat__Secret__c  FROM smrtat__S3_Integration_Info__c LIMIT 1];

            String formattedDateString = Datetime.now().formatGMT('EEE, dd MMM yyyy HH:mm:ss z');
            String bucketname = allCodes.smrtat__BucketName__c; //AWS bucket name
            String stringToSign = 'GET\n\n\n' + formattedDateString + '\n/' + bucketname + '/' + filename;
            Blob mac = Crypto.generateMac('HMACSHA1', Blob.valueof(stringToSign), Blob.valueof(allCodes.smrtat__Secret__c));
            String auth = 'AWS' + ' ' + allCodes.smrtat__Key__c + ':' + EncodingUtil.base64Encode(mac);

            HttpRequest req = new HttpRequest();
            req.setMethod('GET');
            req.setHeader('Date', formattedDateString);
            req.setHeader('Authorization',auth);
                        
            String endpointBase = 'https://' + bucketname + '.' + allCodes.smrtat__Host__c + '/' + filename;
            
            req.setEndpoint(endpointBase + '?' + 'prefix=' + EncodingUtil.urlEncode(filename+'/','UTF-8')+'&delimiter=' + EncodingUtil.urlEncode('/', 'UTF-8'));
            
            Http http = new Http();
            HTTPResponse res = http.send(req);

            String coreIdhtml;
            List<String> fileLines = res.getBody().split('\n');
            
            for (Integer i=0; i < fileLines.size(); i++) {
                if(fileLines[i].containsIgnoreCase('Core ID')){
                    coreIdhtml = (fileLines[i+3]).trim();
                    break;
                }
            }
            
            smrtat__MDR__c mdrhtml = [SELECT id FROM smrtat__MDR__c WHERE smrtat__Core_ID__c =: coreIdhtml];
            if(mdrhtml != null){
                Attachment ahtml= new Attachment();
                ahtml.Name = mdrhtml.Id + '.html';
                ahtml.Body = res.getBodyAsBlob();
                ahtml.ParentId = mdrhtml.Id;
                insert ahtml;

                List<smrtat__File_Log__c> filesList = [SELECT id FROM smrtat__File_Log__c WHERE smrtat__File_Name__c =: filename LIMIT 1];
                 //If the file is already synced with the library, delete the file else update the Is_Downloaded__c to true
                if(filesList[0].Is_Synced_with_Libraries__c){
                    delete filesList;
                }else{
                    filesList[0].Is_Downloaded__c = true;
                    update filesList;
                }
            }
        }catch(Exception e){
            AuditTrailUtility.logException(e, 'getHtmlFile()', 'S3IntegrationFilesHandler', false);
        }*/
    }
    
}