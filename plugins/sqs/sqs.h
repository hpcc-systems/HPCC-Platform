
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

#ifndef SQSHPCC_H
#define SQSHPCC_H



#ifdef _WIN32
#define ECL_SQS_CALL _cdecl
#else
#define ECL_SQS_CALL
#endif

#ifdef ECL_SQS_EXPORTS
#define ECL_SQS_API DECL_EXPORT
#else
#define ECL_SQS_API DECL_IMPORT
#endif

#include "platform.h"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "eclhelper.hpp"
#include <aws/core/Aws.h>
#include <aws/sqs/SQSClient.h>
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <libgen.h>
#include <atomic>

#ifdef ECL_SQS_EXPORTS

extern  "C" 
{
  ECL_SQS_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
  ECL_SQS_API void setPluginContext(IPluginContext * _ctx);
}
#endif

extern "C++" 
{
  namespace SQSHPCCPlugin
  {
    using namespace std;
    typedef struct 
    {
      int code;
      std::string body;
      bool success;
    } Response;

    struct AtomicCounter 
    {
      std::atomic<int> counter;
	
      void increment() {
	++counter;
      } 
      
      void decrement() {
        --counter;
      }

      int get() {
        return counter.load();
      } 

    };

  
    class SQSHPCC
    {

    public:
      explicit SQSHPCC(const std::string& _queueName);
      ~SQSHPCC();
 
      Response sendMessage(const char* message,const char* messagecount);
      Response createQueue();
      Response deleteQueue();
      Response deleteMessage(const std::string& message);
      Response receiveMessage();

      void setSQSConfiguration(const std::string& protocol,const std::string& region,const bool useProxy, const std::string& proxyHost,const unsigned proxyPort, const std::string& proxyUsername, const std::string& proxyPassword);
      void setAwsCredentials(const char* accessKeyId, 
			     const char* secretKey); 
      bool disconnect();
      bool QueueExists();

      void setQueueUrlFromQueueName();
    protected:

    private:
      std::string queueName;
      Aws::String queueUrl;
      std::ofstream handlelog;
      AtomicCounter counter;
      Aws::SQS::SQSClient* sqsClient = nullptr;
      Aws::SDKOptions options;
      Aws::Auth::AWSCredentials* credentials=nullptr;
      bool RegionExists(const std::string& region);
      const char *const getRegion(const std::string& region);
      void upstr(char* s);
      std::string convertAwsStringToCharPtr(Aws::String str);
      char*  convertStringToChar(const string& str);
    };


    //----------------------------------------------------------------------

    /**
     * Queues the message for publishing to aws queue
     * 
     * @param   Reqion          
     * @param   QueueName
     * @param   message            The message to send
     *
     * @return  true if the message was cached successfully
     */
    ECL_SQS_API bool ECL_SQS_CALL publishMessage(ICodeContext * ctx,const char* region, const char* queueName, const char* message, bool useProxy, const char* proxyHost, __int32 proxyPort, const char* proxyUsername, const char* proxyPassword);

    //---------------------------------------------------------------------

    ECL_SQS_API bool ECL_SQS_CALL publishOrderedMessage(ICodeContext * ctx,const char* region, const char* queueName, const char* message, const char* messageCount,const bool useProxy, const char* proxyHost, __int32 proxyPort, const char* proxyUsername, const char* proxyPassword);
    /** 
     *
     *
     */
    ECL_SQS_API bool ECL_SQS_CALL createQueue(ICodeContext * ctx,const char* region, const char* queueName);

    ECL_SQS_API bool ECL_SQS_CALL QueueExists(ICodeContext* ctx,const char* region, const char* queueName);
     
    ECL_SQS_API bool ECL_SQS_CALL deleteQueue(ICodeContext * ctx,const char* region, const char* queueName);      
	
  }
}
#endif
