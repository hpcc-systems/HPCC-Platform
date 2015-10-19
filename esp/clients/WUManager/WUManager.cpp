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

// WUManager.cpp : Defines the entry point for the console application.
//

//#include "stdafx.h"
#include <stdio.h>
#include "jlib.hpp"
#include "ws_workunits.hpp"
#include "ws_workunits_esp.ipp"

void diaplaySyntax()
{
    printf("Usage:\n");
    printf("    WUManager EXPORT <URL> <dest-file-path>\n");

    printf("Security: /UserName:name       -- UserName to authenticate access to service\n");
    printf("          /Password:password   -- Password to authenticate access to service\n\n");

    printf("Filters: /Cluster:name      -- WUs run on this cluster\n");
    printf("     /Owner:name        -- WUs with this owner\n");
    printf("     /StartDate:yyyymmdd    -- WUs created on or after this date\n");
    printf("     /EndDate:yyyymmdd  -- WUs created on or before this date\n");

    printf("     /State:flag        -- WUs with this state\n");

    printf("State Flags:\n");
    printf("            unknown\n");
    printf("            compiled\n");
    printf("            running\n");
    printf("            completed\n");
    printf("            failed\n");
    printf("            archived\n");
    printf("            aborting\n");
    printf("            aborted\n");
    printf("            blocked\n");
    printf("Example:\n");
    printf("    WUManager EXPORT http://server:8010/WsWorkunits Mylocalfile.xml /UserName:jsmith /Password:12345 /StartDate:20030701");
}

int main(int argc, char* argv[])
{
    InitModuleObjects();
    if (argc < 3 || (argv[1][1]=='?') || (argv[1][2]=='?') )
    {
        diaplaySyntax();
        return  0;
    }
    StringBuffer file,url,action,username,password,cluster,owner,start,end,state;
    if(argc >= 2 && argv[1])
    {
        action.append(argv[1]);
    }
    else
    {
        printf("You must define an action");
        return 0;
    }


    if(argc >= 3 && argv[2])
        url.append(argv[2]);
    else
    {
        printf("You must define an SMC server to query");
        return 0 ;
    }
    if(argc >= 4 && argv[3])
    {
        file.append(argv[3]);
    }
    else
    {
        printf("You must specify an output file to write the WU information to.");
        return 0 ;
    }

    for (int i=4;i<argc;i++) {
        char* delim = strstr(argv[i],":");
        if (delim)
        {
            if (strstr(argv[i],"UserName")!=0) 
                username.append( delim+1);
            else if (strstr(argv[i],"Password")!=0)
                password.append(delim+1);
            else if (strstr(argv[i],"Cluster")!=0)
                cluster.append(delim+1);
            else if (strstr(argv[i],"Owner")!=0)
                owner.append(delim+1);
            else if (strstr(argv[i],"StartDate")!=0)
                start.append(delim+1);
            else if (strstr(argv[i],"EndDate")!=0)
                end.append(delim+1);
            else if (strstr(argv[i],"State")!=0)
                state.append(delim+1);
        }
    }

    if (stricmp(action.str(),"EXPORT") == 0)
    {
        try
        {
            Owned<IFile> _file = createIFile(file.str());
            if (_file->exists() == true)
            {
                printf("A file exists at %s. Do you wish to replace it (y/n)?",file.str());
                int ch = getchar();
                if (ch=='n' || ch=='N')
                    return 0;
            }

            Owned<IFileIO> _fileio = _file->open(IFOcreate);

            Owned<IClientWsWorkunits> pServer = new CClientWsWorkunits();
            pServer->addServiceUrl(url.str());
            pServer->setUsernameToken(username.str(),password.str(),"");

            Owned<IClientWUExportRequest> req =  pServer->createWUExportRequest();
            req->setCluster(cluster.str());
            req->setEndDate(end.str());
            req->setOwner(owner.str());
            req->setStartDate(start.str());
            req->setState(state.str());

            printf("Processing request....");
            Owned<IClientWUExportResponse> resp =  pServer->WUExport(req);
        
            __int64 _bytesWritten = _fileio->write(0, resp->getExportData().length(), resp->getExportData().toByteArray());
            printf("Write to file %s is complete.", file.str() );

        }
        catch(IException* e)
        {
            StringBuffer errorStr;
            e->errorMessage(errorStr);
            printf("Exception %s thrown while generate export file",errorStr.str());
            return 0;
        }
        catch(...)
        {
            printf("Unknown exception thrown while generate export file");
            return 0;
        }
    }
    else
        printf("Unknown action %s",action.str());

    releaseAtoms();
    return 0;
}
