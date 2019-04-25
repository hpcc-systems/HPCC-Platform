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

// LogicFileWrapper.cpp: implementation of the LogicFileWrapper class.
//
//////////////////////////////////////////////////////////////////////

#include "LogicFileWrapper.hpp"
#include "dautils.hpp"

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

#define REMOVE_FILE_SDS_CONNECT_TIMEOUT (1000*15)  // 15 seconds

LogicFileWrapper::LogicFileWrapper()
{

}

LogicFileWrapper::~LogicFileWrapper()
{

}

void LogicFileWrapper::FindClusterName(const char* logicalName, StringBuffer& returnCluster, IUserDescriptor* udesc)
{
    try {
        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName, udesc) ;
        if(!df)
            throw MakeStringException(-1,"Could not find logical file");
        df->getClusterName(0,returnCluster);    // ** TBD other cluster
    }
    catch(IException* e){   
      StringBuffer msg;
      e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        IWARNLOG("Unknown Exception thrown within LogicFileWrapper::FindClusterName");
    }
}

bool LogicFileWrapper::doDeleteFile(const char* logicalName,const char *cluster, StringBuffer& returnStr, IUserDescriptor* udesc)
{
    CDfsLogicalFileName lfn;
    lfn.set(logicalName);
    StringBuffer cname;
    lfn.getCluster(cname);
    if (0 == cname.length()) // if has no cluster, use supplied cluster
        lfn.setCluster(cluster);
    lfn.get(cname.clear(), false, true); // get file@cluster form;

    try
    {
        IDistributedFileDirectory &fdir = queryDistributedFileDirectory();
        {
            Owned<IDistributedFile> df = fdir.lookup(cname.str(), udesc, true) ;
            if(!df)
            {
                returnStr.appendf("<Message><Value>File %s not found</Value></Message>", cname.str());
                return false;
            }
        }

        fdir.removeEntry(cname.str(), udesc, NULL, REMOVE_FILE_SDS_CONNECT_TIMEOUT, true);
        returnStr.appendf("<Message><Value>Deleted File %s</Value></Message>", cname.str());
        return true;
    }
    catch (IException *e)
    {
        StringBuffer errorMsg;
        e->errorMessage(returnStr);
        e->Release();
        PROGLOG("%s", errorMsg.str());
        returnStr.appendf("<Message><Value>Failed to delete File %s, error: %s</Value></Message>", cname.str(), errorMsg.str());
    }
    return false;
}

bool LogicFileWrapper::doCompressFile(const char* name,StringBuffer& returnStr, IUserDescriptor* udesc)
{
    try
    {
        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(name, udesc) ;
        if(!df)
            return false;

        ErrorReceiver err;
        TaskQueue tq(200,&err);

        Owned<IDistributedFilePartIterator> pi = df->getIterator();
        ForEach(*pi)
        {
            tq.put(new CompressTask(&pi->query()));
        }

        tq.join();
        err.getErrors(returnStr);
    }
    catch(IException* e){   
      StringBuffer msg;
      e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        IWARNLOG("Unknown Exception caught within doCompressFile");
    }

    return true; 
}
