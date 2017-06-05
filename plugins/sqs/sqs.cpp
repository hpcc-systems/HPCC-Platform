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
  "AWS Amazon SQS based on SDK AWS",
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
      pb->description = "SQS based on AWS SDK";
      return true;
    }
  else
    {
      return false;
    }
}


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
 * Destructor
 * Release the SQS connection
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
    Aws::SQS::Model::SendMessageRequest sendMessageRequest;
    sendMessageRequest.SetQueueUrl(this->queueUrl);
    sendMessageRequest.SetMessageBody(message);

    Aws::SQS::Model::SendMessageOutcome sendMessageOutcome = this->sqsClient->SendMessage(sendMessageRequest);

    if(!sendMessageOutcome.IsSuccess() || sendMessageOutcome.GetResult().GetMessageId().length() == 0 )
      {
        cout << "Error occurred during the sending " << endl;
        ref.code=-1;
        ref.body="Error occurred during message sending";
      }
    else
      {
        ref.code=3;
        ref.body=sendMessageOutcome.GetResult().GetMessageId().c_str();
      }

  } catch (const char* message) {
    cout<< "Error occurred during sending message [ " << message << " ]" << endl;
  }

  return ref;
}


/**
 *
 *  This function allows to disconnect the link with AWS SQS
 *
 **/

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
bool SQSHPCCPlugin::SQSHPCC::QueueExists()
{
  Aws::SQS::Model::GetQueueUrlRequest gqu_req;
  gqu_req.SetQueueName(this->queueName.c_str());
  bool exists=false;
  try
    {
      Aws::SQS::Model::GetQueueUrlOutcome gqu_out = this->sqsClient->GetQueueUrl(gqu_req);
      exists =gqu_out.IsSuccess();
    }
  catch(const char* message)
    {
      cout << "Error: " << message << endl;
    }

  return exists;
}

SQSHPCCPlugin::Response SQSHPCCPlugin::SQSHPCC::createQueue()
{

  SQSHPCCPlugin::Response ref = {};

  Aws::SQS::Model::CreateQueueRequest cq_req;
  cq_req.SetQueueName(this->queueName.c_str());

  Aws::SQS::Model::CreateQueueOutcome  cq_out = this->sqsClient->CreateQueue(cq_req);
  if (cq_out.IsSuccess())
    {
      cout << "Successfully created queue " << this->queueName << std::endl;
    }
  else
    {
      cout << "Error creating queue " << this->queueName << ": " <<
        cq_out.GetError().GetMessage() << std::endl;
    }

  return ref;
}


SQSHPCCPlugin::Response SQSHPCCPlugin::SQSHPCC::deleteQueue()
{

  SQSHPCCPlugin::Response ref = {};

  Aws::SQS::Model::DeleteQueueRequest cq_req;
  cq_req.SetQueueUrl(this->queueUrl);

  auto  cq_out = this->sqsClient->DeleteQueue(cq_req);
  if (cq_out.IsSuccess())
    {
      cout << "Successfully deleted queue " << this->queueName << std::endl;
    }
  else
    {
      cout << "Error deleting queue " << this->queueName << ": " <<
        cq_out.GetError().GetMessage() << std::endl;
    }

  return ref;
}


SQSHPCCPlugin::Response SQSHPCCPlugin::SQSHPCC::receiveMessage()
{

  SQSHPCCPlugin::Response ref = {};
  try
    {
      Aws::SQS::Model::ReceiveMessageRequest receiveMessageRequest;
      receiveMessageRequest.SetQueueUrl(this->queueUrl);
      receiveMessageRequest.SetMaxNumberOfMessages(1);
      receiveMessageRequest.AddMessageAttributeNames("All");

      Aws::SQS::Model::ReceiveMessageOutcome receiveMessageOutcome = this->sqsClient->ReceiveMessage(receiveMessageRequest);
      if(!receiveMessageOutcome.IsSuccess() || receiveMessageOutcome.GetResult().GetMessages().size() == 0)
        {
          std::cout << "Error on receive: " << receiveMessageOutcome.GetError().GetMessage() << std::endl;
          ref.code=2;
          return ref;
        }

      Aws::SQS::Model::Message msg = receiveMessageOutcome.GetResult().GetMessages()[0];
      cout << msg.GetBody() << endl;
    }
  catch(const char* message)
    {
      cout << "Error: " << message << endl;
    }

  return ref;
}


void SQSHPCCPlugin::SQSHPCC::setQueueUrlFromQueueName()
{

  Aws::SQS::Model::GetQueueUrlRequest gqu_req;
  gqu_req.SetQueueName(this->queueName.c_str());

  try
    {
      Aws::SQS::Model::GetQueueUrlOutcome gqu_out = this->sqsClient->GetQueueUrl(gqu_req);
      if(gqu_out.IsSuccess())
        {
          std::cout << "Queue " << this->queueName.c_str() << " has url " << std::endl;
          this->queueUrl=gqu_out.GetResult().GetQueueUrl();
        }
      else
        {
          std::cout << "Error getting url for queue " << this->queueName.c_str() << ": " << std::endl;
          throw runtime_error(gqu_out.GetError().GetMessage().c_str()) ;
        }

    } catch(const char* message) {
    cout << "Error: " << message << endl;
  }

}

void SQSHPCCPlugin::SQSHPCC::setSQSConfiguration(const string& protocol, const string& region)
{

  Aws::InitAPI(this->options);
  Aws::Client::ClientConfiguration config;

  if(!protocol.empty())
    {
      config.scheme = Aws::Http::Scheme::HTTPS;
    }

  if(region.empty()) throw string("Region mustn't be empty");

  if(RegionExists(region))
    {
      config.region = getRegion(region);
    }

  this->sqsClient= new Aws::SQS::SQSClient(config);

  cout << "Queue URL is "  << this->queueUrl << endl;
}


bool SQSHPCCPlugin::SQSHPCC::RegionExists(const string& region)
{
  if(region.empty())
    {
      return false;
    }

  const char *reg = region.c_str();

  return (
          strieq(reg,"US_EAST_1") ||
              strieq(reg,"US_WEST_1") ||
              strieq(reg,"EU_WEST_1") ||
              strieq(reg,"EU_CENTRAL_1") ||
              strieq(reg,"AP_SOUTHEAST_1") ||
               strieq(reg,"AP_SOUTHEAST_2")
          );
}


const char *const SQSHPCCPlugin::SQSHPCC::getRegion(const string& region)
{

  const char *reg = region.c_str();

  if(strieq(reg,"US_EAST_1"))  return Aws::Region::US_EAST_1;
  if(strieq(reg,"US_WEST_1")) return Aws::Region::US_WEST_1;
  if(strieq(reg,"EU_WEST_1")) return Aws::Region::EU_WEST_1;
  if(strieq(reg,"EU_CENTRAL_1")) return Aws::Region::EU_CENTRAL_1;
  if(strieq(reg,"AP_SOUTHEAST_1")) return Aws::Region::AP_SOUTHEAST_1;
  if(strieq(reg,"AP_SOUTHEAST_2"))  return Aws::Region::AP_SOUTHEAST_2;

  throw string("Your region must be among these regions [ US_EAST_1, US_WEST_1, EU_WEST_1, EU_CENTRAL_1, AP_SOUTHEAST_1, AP_SOUTHEAST_2 ], please check your region");
}

/**
 *  These function expose the contract for ECL
 *
 */

namespace SQSHPCCPlugin
{

  ECL_SQS_API bool ECL_SQS_CALL publishMessage(ICodeContext * ctx,const char* region, const char* queueName, const char* message)
  {

    if(strlen(queueName) == 0)
      {
        cout << "QueueName is Empty" << endl;
        throw runtime_error("The queueName mustn't be empty!!!");
      }
    try
      {
        SQSHPCCPlugin::SQSHPCC hpcc(queueName);
        hpcc.setSQSConfiguration("HTTPS",region);
        hpcc.setQueueUrlFromQueueName();
        SQSHPCCPlugin::Response response = hpcc.sendMessage(message);
        return true;
      }
    catch(...)
      {
        throw;
      }

    return false;
  }

  ECL_SQS_API bool ECL_SQS_CALL QueueExists(ICodeContext* ctx,const char* region, const char* queueName)
  {

    SQSHPCCPlugin::SQSHPCC hpcc(queueName);
    hpcc.setSQSConfiguration("HTPPS",region);
    hpcc.setQueueUrlFromQueueName();
    bool exists = hpcc.QueueExists();
    return exists;
  }

  ECL_SQS_API bool createQueue(ICodeContext* ctx,const char* region, const char* queueName)
  {
    SQSHPCCPlugin::SQSHPCC hpcc(queueName);
    hpcc.setSQSConfiguration("HTTPS",region);
    try
      {
        SQSHPCCPlugin::Response response = hpcc.createQueue();
        return true;
      }
    catch(...)
      {
        return false;
      }

  }

  ECL_SQS_API bool ECL_SQS_CALL deleteQueue(ICodeContext * ctx,const char* region, const char* queueName)
  {

    SQSHPCCPlugin::SQSHPCC hpcc(queueName);
    hpcc.setSQSConfiguration("HTTPS",region);
    hpcc.setQueueUrlFromQueueName();
    try
      {
        hpcc.deleteQueue();
        return true;
      }
    catch(...)
      {
        return false;
      }
  }
}


