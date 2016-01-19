/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#include "jlib.hpp"
#include "jfile.hpp"
#include "jprop.hpp"
#include "loggingmanager.h"

Owned<IPropertyTree> loggingInput;
IPropertyTree* loggingConfig = NULL;
IPropertyTree* testData = NULL;
Owned<ILoggingManager> loggingManager;

ILoggingManager* loadLogggingManager()
{
    StringBuffer realName;
    realName.append(SharedObjectPrefix).append(LOGGINGMANAGERLIB).append(SharedObjectExtension);
    HINSTANCE loggingManagerLib = LoadSharedObject(realName.str(), true, false);
    if(loggingManagerLib == NULL)
    {
        printf("can't load library %s\n", realName.str());
        return NULL;
    }

    newLoggingManager_t_ xproc = NULL;
    xproc = (newLoggingManager_t_)GetSharedProcedure(loggingManagerLib, "newLoggingManager");

    if (!xproc)
    {
        printf("procedure newLogggingManager of %s can't be loaded\n", realName.str());
        return NULL;
    }

    return (ILoggingManager*) xproc();
}

ILoggingManager* init(const char* inputFileName)
{
    if(!inputFileName || !*inputFileName)
    {
        printf("Input file not defined.\n");
        return NULL;
    }

    printf("Input file: %s\n", inputFileName);
    loggingInput.setown(createPTreeFromXMLFile(inputFileName, ipt_caseInsensitive));
    if (!loggingInput)
    {
        printf("can't read input file.\n");
        return NULL;
    }

    loggingConfig = loggingInput->queryPropTree("config/LoggingManager");
    testData = loggingInput->queryPropTree("test");
    if (!loggingConfig || !testData)
    {
        printf("can't read input file.\n");
        return NULL;
    }

    loggingManager.setown(loadLogggingManager());
    if (!loggingManager)
    {
        printf("No logging manager\n");
        return NULL;
    }
    loggingManager->init(loggingConfig, "test_service");

    return loggingManager;
}

void sendRequest()
{
    if (!loggingManager)
    {
        printf("No logging manager.\n");
        return;
    }

    StringBuffer action, option, status;
    testData->getProp("action", action);
    testData->getProp("option", option);
    if (action.length() && strieq(action.str(), "getTransactionSeed"))
    {
        StringBuffer transactionSeed;
        loggingManager->getTransactionSeed(transactionSeed, status);
        if (transactionSeed.length())
            printf("Got transactionSeed: <%s>\n", transactionSeed.str());
    }
    else if (action.length() && strieq(action.str(), "UpdateLog"))
    {
        IPropertyTree* logContentTree = testData->queryPropTree("LogContent");
        if (!logContentTree)
        {
            printf("can't read log content.\n");
            return;
        }
        StringBuffer logContentXML;
        toXML(logContentTree, logContentXML);
        printf("log content: <%s>.\n", logContentXML.str());

        Owned<IEspContext> espContext =  createEspContext();
        const char* userName = logContentTree->queryProp("ESPContext/UserName");
        const char* sourceIP = logContentTree->queryProp("ESPContext/SourceIP");
        short servPort = logContentTree->getPropInt("ESPContext/Port");
        espContext->setUserID(userName);
        espContext->setServAddress(sourceIP, servPort);

        const char* backEndResp = logContentTree->queryProp("BackEndResponse");
        IPropertyTree* userContextTree = logContentTree->queryPropTree("MyUserContext");
        IPropertyTree* userRequestTree = logContentTree->queryPropTree("MyUserRequest");
        IPropertyTree* userRespTree = logContentTree->queryPropTree("MyUserResponseEx");
        StringBuffer userContextXML, userRequestXML, userRespXML;
        toXML(userRespTree, userRespXML);

        toXML(userContextTree, userContextXML);
        toXML(userRequestTree, userRequestXML);
        printf("userContextXML: <%s>.\n", userContextXML.str());
        printf("userRequestXML: <%s>.\n", userRequestXML.str());
        printf("userRespXML: <%s>.\n", userRespXML.str());
        printf("backEndResp: <%s>.\n", backEndResp);

        //Sleep(5000); //Waiting for loggingManager to start
        loggingManager->updateLog(option.str(), *espContext, userContextTree, userRequestTree, backEndResp, userRespXML.str(), status);
    }
    else if (action.length() && strieq(action.str(), "UpdateLog1"))
    {
        IPropertyTree* logContentTree = testData->queryPropTree("LogContent");
        if (!logContentTree)
        {
            printf("can't read log content.\n");
            return;
        }
        StringBuffer logContentXML;
        toXML(logContentTree, logContentXML);
        printf("log content: <%s>.\n", logContentXML.str());
        //Sleep(5000); //Waiting for loggingManager to start
        loggingManager->updateLog(option.str(), logContentXML.str(), status);
    }
    else
        printf("Invalid action.\n");
}

void doWork(const char* inputFileName)
{
    printf("Enter dowork()\n");
    init(inputFileName);
    sendRequest();
    printf("Waiting for log queue to be read.\n");
    Sleep(10000); //Waiting for loggingManager to process queue
    printf("Finish dowork()\n");
}
