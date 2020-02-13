/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#ifndef AGENTEXEC_SERVER_HPP
#define AGENTEXEC_SERVER_HPP

//---------------------------------------------------------------------------------
//  Reads various customizeable properties from agentexec.xml configuration file,
//  then listens to the dali queue specifed in that file. Reads workunit IDs from 
//  that queue, and executes eclagent.exe to process those workunits
//---------------------------------------------------------------------------------
class CEclAgentExecutionServer : public Thread
{
public:
    CEclAgentExecutionServer(IPropertyTree *config);
    ~CEclAgentExecutionServer();

    void start();
    void stop();

private:
    int run();
    int executeWorkunit(const char * wuid);

    //attributes
    bool started;
    StringAttr agentName;
    Owned<IJobQueue> queue;
    StringBuffer daliServers;
    Linked<IPropertyTree> config;
};

#endif
