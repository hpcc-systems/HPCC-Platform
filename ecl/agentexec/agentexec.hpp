/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    CEclAgentExecutionServer();
    ~CEclAgentExecutionServer();

    void start(StringBuffer & codeDir);
    void stop();

private:
    int run();
    int executeWorkunit(const char * wuid);
    void rebuildLogfileName();

    //attributes
    bool started;
    StringBuffer codeDirectory;
    StringBuffer logDir;
    StringBuffer logfilespec;
    SCMStringBuffer queueNames;
    StringAttr agentName;
    Owned<IJobQueue> queue;
    StringBuffer daliServers;
};

#endif
