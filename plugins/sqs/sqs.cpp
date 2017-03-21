/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */ 


#include <aws/core/Aws.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/AddPermissionRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/ListQueuesRequest.h>
#include <aws/sqs/model/ListQueuesResult.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/model/SendMessageResult.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/ReceiveMessageResult.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/GetQueueUrlResult.h>
#include "platform.h"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include <iostream>
#include <string>
#include <algorithm>
#include <cstring>
#include <cctype>
#include "sqs.h"

using namespace std;

static const char * compatibleVersions[] = {
    "SQS AWS Amazon based on SDK AWS",
    NULL };

static const char *version = "SQS Version depends on AWS";

extern "C" DECL_EXPORT bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlock))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
   	pb->magicVersion = PLUGIN_VERSION;
    	pb->version = version;
    	pb->moduleName = "SQS";
    	pb->ECL = NULL;
    	pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    	pb->description = "SQS based on SDK AWS";
      return true;
    }
    else
  	{
        return false;
	}	
}

const char *const NOTFOUND="NFOUND";

/**
 * This method creates an instance of SQSHPCC
 * @param _queueName this name gave to queue
 *
 */

SQSHPCCPlugin::SQSHPCC::SQSHPCC(const string& _queuename) 
{

  if(_queuename.empty())
    {
      throw runtime_error("QueueName is required");
    }
  this->queueName=_queuename;
  cout << "The queue is " << this->queueName << endl;
  
}


/**
*
*
*/
SQSHPCCPlugin::SQSHPCC::~SQSHPCC()
 {

// Aws::ShutdownAPI(this->options);
  this->queueName="";
  delete(this->sqsClient);
}

/**
 *  
 *
 *
 **/

SQSHPCCPlugin::Response  SQSHPCCPlugin::SQSHPCC::sendMessage(const char* message) 
{

  SQSHPCCPlugin::Response ref = {};

  cout << "SendMessage is " << this->queueUrl << endl; 

  try {
      Aws::SQS::Model::SendMessageRequest* sendMessageRequest=new Aws::SQS::Model::SendMessageRequest();
      sendMessageRequest->SetQueueUrl(this->queueUrl);
      sendMessageRequest->SetMessageBody(message);
   // char *reg = new char[this->queueUrl.length() +1];
  //strcpy(reg,this->queueUrl.c_str());
 
      //ref.test=reg;
 
      Aws::SQS::Model::SendMessageOutcome sendMessageOutcome = this->sqsClient->SendMessage(*sendMessageRequest);
     
     if(!sendMessageOutcome.IsSuccess() || sendMessageOutcome.GetResult().GetMessageId().length() == 0 )
        {
	  cout << "Error occurs during the sending " << endl;
        //  ref.code=-1;
         // ref.body="Error occurs during the sending of message";
	}
      else 
	{
         //  ref.code=3;
	   //ref.body=convertAwsStringToCharPtr(sendMessageOutcome.GetResult().GetMessageId());
	}	
     
     delete(sendMessageRequest);
    } catch (const char* message) {
       cout<< "Error occurs during sending a message [ " << message << " ]" << endl;
  }

  return ref;
}



bool SQSHPCCPlugin::SQSHPCC::disconnect() 
{

	try 
	{
	Aws::ShutdownAPI(this->options);
    
         return true;
	}
	catch(...)
	{
          return false;
	}	
}


/**
*
*
*/
bool SQSHPCCPlugin::SQSHPCC::isQueueExist() 
{
     Aws::SQS::Model::GetQueueUrlRequest* gqu_req = new Aws::SQS::Model::GetQueueUrlRequest();
     gqu_req->SetQueueName(this->queueName.c_str());
 	bool isExist=false;
	try {
   		Aws::SQS::Model::GetQueueUrlOutcome gqu_out = this->sqsClient->GetQueueUrl(*gqu_req);
   		isExist =gqu_out.IsSuccess(); 

 	} catch(const char* message) {
     		cout << "Error occurs " << message << endl;
	}
   
   delete gqu_req;   
   return isExist;
}

/**


 **/
SQSHPCCPlugin::Response SQSHPCCPlugin::SQSHPCC::createQueue()
{

  SQSHPCCPlugin::Response ref = {};

  Aws::SQS::Model::CreateQueueRequest* cq_req = new Aws::SQS::Model::CreateQueueRequest();
  cq_req->SetQueueName(this->queueName.c_str());

  Aws::SQS::Model::CreateQueueOutcome  cq_out = this->sqsClient->CreateQueue(*cq_req);
  if (cq_out.IsSuccess()) {
         cout << "Successfully created queue " << this->queueName << std::endl;
      //  this->setQueueUrlFromQueueName();
 } else {
       cout << "Error creating queue " << this->queueName << ": " <<
          cq_out.GetError().GetMessage() << std::endl;
     }

 delete cq_req;
return ref;
}


/**
*
 **/
SQSHPCCPlugin::Response SQSHPCCPlugin::SQSHPCC::deleteQueue()
{

  SQSHPCCPlugin::Response ref = {};

  Aws::SQS::Model::DeleteQueueRequest* cq_req = new Aws::SQS::Model::DeleteQueueRequest();
  cq_req->SetQueueUrl(this->queueUrl);

  auto  cq_out = this->sqsClient->DeleteQueue(*cq_req);
  if (cq_out.IsSuccess()) {
         cout << "Successfully created queue " << this->queueName << std::endl;
      //  this->setQueueUrlFromQueueName();
 } else {
       cout << "Error creating queue " << this->queueName << ": " <<
          cq_out.GetError().GetMessage() << std::endl;
     }

delete cq_req;
return ref;
}

/**



 **/
SQSHPCCPlugin::Response SQSHPCCPlugin::SQSHPCC::receiveMessage() 
{

   SQSHPCCPlugin::Response ref = {};

try{
  
  Aws::SQS::Model::ReceiveMessageRequest receiveMessageRequest;
  receiveMessageRequest.SetQueueUrl(this->queueUrl);
  receiveMessageRequest.SetMaxNumberOfMessages(1);
  receiveMessageRequest.AddMessageAttributeNames("All");

  Aws::SQS::Model::ReceiveMessageOutcome receiveMessageOutcome = this->sqsClient->ReceiveMessage(receiveMessageRequest);
 if(!receiveMessageOutcome.IsSuccess() || receiveMessageOutcome.GetResult().GetMessages().size() == 0) {
       std::cout << "Error on receive: " << receiveMessageOutcome.GetError().GetMessage() << std::endl;
       ref.code=2;
       return ref;
 }
 
 Aws::SQS::Model::Message msg = receiveMessageOutcome.GetResult().GetMessages()[0];
 cout << msg.GetBody() << endl;
} catch(const char* message) {

  cout << "Error occurs " << message << endl; 
}

 return ref;
}


/**
 *
 *
 *
 *
 **/
void SQSHPCCPlugin::SQSHPCC::setQueueUrlFromQueueName() 
{

   Aws::SQS::Model::GetQueueUrlRequest* gqu_req = new Aws::SQS::Model::GetQueueUrlRequest();
   gqu_req->SetQueueName(this->queueName.c_str());

try {
   Aws::SQS::Model::GetQueueUrlOutcome gqu_out = this->sqsClient->GetQueueUrl(*gqu_req);
//const char* bl = gqu_out.IsSuccess() ? "true":"false";
//Aws::String strBl(bl);
//this->queueUrl=strBl;
  if(gqu_out.IsSuccess()) {
        std::cout << "Queue " << this->queueName.c_str() << " has url " << std::endl;
    this->queueUrl=gqu_out.GetResult().GetQueueUrl();
   } else {
        std::cout << "Error getting url for queue " << this->queueName.c_str() << ": " << std::endl;
      throw runtime_error(gqu_out.GetError().GetMessage().c_str()) ;
    // this->queueUrl = gqu_out.GetError().GetMessage();
   }

 } catch(const char* message) {
     cout << "Error occurs " << message << endl;
}

 delete(gqu_req);
}


/**


 **/
void SQSHPCCPlugin::SQSHPCC::setSQSConfiguration(const string& protocol, const string& region)
 {

  Aws::InitAPI(this->options);
  Aws::Client::ClientConfiguration* config= new Aws::Client::ClientConfiguration();
 if(!protocol.empty())
  {
    config->scheme = Aws::Http::Scheme::HTTPS;
  }

  if(region.empty()) throw string("Region mustn't be empty");
  if(isRegionExist(region))
    {
      config->region = getRegion(region);
    }

  this->sqsClient= new Aws::SQS::SQSClient(*config);

 //this->queueUrl=getRegion(region);
   cout << "Queue URL is "  << this->queueUrl << endl;
// this->setQueueUrlFromQueueName();
 
 delete(config);
}



/**


 **/
bool SQSHPCCPlugin::SQSHPCC::isRegionExist(const string& region)
{
  
  if(region.empty())
    {
      return false;
    }

  char *reg = new char[region.length() +1];
  strcpy(reg,region.c_str());
  upstr(reg);
 
  if((strncmp(reg,"US_EAST_1",9)==0)|| (strncmp(reg,"US_WEST_1",9)==0) || (strncmp(reg,"EU_WEST_1",9)==0)
      || (strncmp(reg,"EU_CENTRAL_1",12)==0) || (strncmp(reg,"AP_SOUTHEAST_1",14)==0) || (strncmp(reg,"AP_SOUTHEAST_2",14)==0)
     ) return true;
  
  return false;
  
}


/**
*
*
*/
const char *const SQSHPCCPlugin::SQSHPCC::getRegion(const string& region) 
{

  char *reg = new char[region.length() +1];
  strcpy(reg,region.c_str());
  upstr(reg);
  puts(reg);
    
  if(strncmp(reg,"US_EAST_1",9)==0) return Aws::Region::US_EAST_1;
  if(strncmp(reg,"US_WEST_1",9)==0) return Aws::Region::US_WEST_1;
  if(strncmp(reg,"EU_WEST_1",9)==0) return Aws::Region::EU_WEST_1;
  if(strncmp(reg,"EU_CENTRAL_1",12)==0) return Aws::Region::EU_CENTRAL_1;
  if(strncmp(reg,"AP_SOUTHEAST_1",14)==0) return Aws::Region::AP_SOUTHEAST_1;
  if(strncmp(reg,"AP_SOUTHEAST_2",14)==0)  return Aws::Region::AP_SOUTHEAST_2;

  return NOTFOUND;
}


 void SQSHPCCPlugin::SQSHPCC::upstr(char *s)
{
  char  *p;

  for (p = s; *p != '\0'; p++) 
    *p = (char) toupper(*p);
}



string SQSHPCCPlugin::SQSHPCC::convertAwsStringToCharPtr(Aws::String str)
{

  //char* chr = new char[str.length()+1];
 // strcpy(chr,str.c_str());
  string res(str.c_str());
  return res;
}

/**
*
*
*/

namespace SQSHPCCPlugin
{
//ECL_SQS_API bool ECL_SQS_CALL publishMessage(ICodeContext* ctx, const char* region, const char* queueName, const char* message)
ECL_SQS_API bool ECL_SQS_CALL publishMessage(ICodeContext * ctx,const char* region, const char* queueName, const char* message)
{

   if(strlen(queueName) == 0) 
	{
	    cout << "QueueName is Empty" << endl;
            throw runtime_error("The queueName mustn't be empty!!!");
	}
   
    try 
	{ 
          SQSHPCCPlugin::SQSHPCC* hpcc= new SQSHPCCPlugin::SQSHPCC(queueName);
         hpcc->setSQSConfiguration("HTTPS",region);
     	 hpcc->setQueueUrlFromQueueName();
         SQSHPCCPlugin::Response response = hpcc->sendMessage(message);
          
//         cout << "The result is " << ref.body << endl; 
         //return (res.code == 3);
       //  hpcc->~SQSHPCC();
         delete(hpcc);
         return true;
	}	
     catch(...)
	{
   	 //  this->queueName=NULL;
	   throw;
	}
  return false;

}



ECL_SQS_API bool ECL_SQS_CALL isQueueExist(ICodeContext* ctx,const char* region, const char* queueName)
{

     SQSHPCCPlugin::SQSHPCC* hpcc= new SQSHPCCPlugin::SQSHPCC(queueName);
     hpcc->setSQSConfiguration("HTPPS",region);
     hpcc->setQueueUrlFromQueueName();
     bool isExist = hpcc->isQueueExist();
   //  hpcc->~SQSHPCC();
     delete(hpcc);

     return isExist;
}



ECL_SQS_API bool createQueue(ICodeContext* ctx,const char* region, const char* queueName)
{
    SQSHPCCPlugin::SQSHPCC* hpcc= new SQSHPCCPlugin::SQSHPCC(queueName);
    hpcc->setSQSConfiguration("HTTPS",region);
	try
 	{
          SQSHPCCPlugin::Response response = hpcc->createQueue();
     //     hpcc->~SQSHPCC();
          delete(hpcc);
          return true;
	}
        catch(...)
        {
	//  hpcc->~SQSHPCC();
	delete(hpcc);
	  return false;
	}	
    
}


ECL_SQS_API bool ECL_SQS_CALL deleteQueue(ICodeContext * ctx,const char* region, const char* queueName)
{

 SQSHPCCPlugin::SQSHPCC* hpcc= new SQSHPCCPlugin::SQSHPCC(queueName);
    hpcc->setSQSConfiguration("HTTPS",region);
  hpcc->setQueueUrlFromQueueName();
        try
        {
          SQSHPCCPlugin::Response response = hpcc->deleteQueue();
//	hpcc->~SQSHPCC(); 
	delete(hpcc);
          return true;
        }
        catch(...)
        {
         // hpcc->~SQSHPCC();
	delete(hpcc);
          return false;
        }
}

}










/**
 * Creates an sqs queue based on command line input
 */
int main(int argc, char** argv)
{
    if (argc != 2) {
        cout << "Usage: create_queue <queue_name>" << endl;
        return 1;
    }

    
    SQSHPCCPlugin::SQSHPCC hpcc{argv[1]};
    hpcc.setSQSConfiguration("","eu_west_1");
    hpcc.createQueue();
    SQSHPCCPlugin::Response res = hpcc.sendMessage("Test");
    


      //hpcc.receiveMessage();
      // hpcc.createQueue();
   
    return 0;
    
}
