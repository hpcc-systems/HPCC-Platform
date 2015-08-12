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

#pragma warning(disable:4786)
#include <list>
#include <iostream>
#include <fstream>
#include "jlib.hpp"
#include "winremote.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include <process.h>

using namespace std;

void usage()
{
    printf("Usage: psexec (\\\\computer | -mFile) -uUser -pPassword [-cfd] [-h dir] cmd [arguments]\n");
    printf("     -m         Specifies file with machine addresses (one per line).\n");
    printf("     -u         Specifies user name for login to remote computer.\n");
    printf("     -p         Specifies password for user name.\n");
    printf("     -c         Copy the specified program to the remote system for execution.\n");
    printf("     -f         Always copy the specified program to the remote system.\n");
    printf("     -d         Do not wait for the process.\n");
    printf("     -n         Do not send user/password, the command will not be able to access network.\n");
    printf("     -h         Home directory for the process on the remote system.\n");
    printf("     cmd        Name of application to execute.\n");
    printf("     arguments  Arguments to pass .\n");
    exit(-2);
}

Owned<IRemoteAgent> ragent(0);

bool ControlHandler() 
{
    if (ragent)
    {
        ragent->stop();
    }
    return false; 
} 

int main(int argc, char** argv)
{
    try
    {
        ragent.setown(createRemoteAgent());
        std::list<std::string> remote;
        StringAttr user,password;

        Owned<IRemoteCommand> rcmd=createRemoteCommand();
        bool nowait=false, network=true;
        
        if (argc<2)
            usage();

        addAbortHandler(ControlHandler);
        int i=1;

        if(argv[i][0]=='\\' && argv[i][1]=='\\')
        {
            remote.push_back(argv[1]+2);
            i++;
        }

        for(;i<argc;i++)
        {
            if(argv[i][0]=='-' || argv[i][0]=='/')
            {
                const char* arg=argv[i]+1;
                switch(tolower(*arg))
                {
                case 'u':
                    user.set(arg+1);
                    break;

                case 'p':
                    password.set(arg+1);
                    break;

                case 'c':
                    rcmd->setCopyFile(false);
                    break;

                case 'f':
                    rcmd->setCopyFile(true);
                    break;

                case 'd':
                    nowait=true;
                    break;

                case 's':
                    rcmd->setSession(atoi(arg+1));
                    break;

                case 'h':
                    rcmd->setWorkDirectory(arg+1);
                    break;

                case 'n':
                    network=false;
                    break;


                case 'm':
                {
                    
                    char buf[256];
                    for(ifstream ips(arg+1);ips && ips.getline(buf,sizeof(buf));)
                    {
                        remote.push_back(buf);
                    }

                    break;
                }


                default:
                    usage();
                }
            }
            else
                break;
        }

        StringBuffer cmd;
        for(;i<argc;i++)
        {
            cmd.append(argv[i]).append(" ");
        }
        rcmd->setCommand(cmd.str());
        rcmd->setNetworkLogon(network);

        if(!nowait)
        {
            rcmd->setOutputStream(queryStdout());
        }
        rcmd->setErrorStream(queryStderr());

        for(std::list<std::string>::iterator it=remote.begin();it!=remote.end();it++)
        {
            ragent->addMachine(it->c_str(),user,password);
        }

        ragent->startCommand(rcmd);
        ragent->wait();
        if(nowait)
        {
            printf("Started process %d",rcmd->getPid());
        }

    }
    catch(IException* e)
    {
        StringBuffer buf;
        printf("%s",e->errorMessage(buf).str());
        e->Release();
        return -1;
    }
    catch(...)
    {
        printf("An unknown exception occurred!");
        return -1;
    }

    return 0;
}
