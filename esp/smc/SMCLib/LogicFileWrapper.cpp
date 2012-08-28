/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
        WARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        WARNLOG("Unknown Exception thrown within LogicFileWrapper::FindClusterName");
    }
}

bool LogicFileWrapper::doDeleteFile(const char* LogicalFileName,const char *cluster, bool nodelete,StringBuffer& returnStr, IUserDescriptor* udesc)
{
    CDfsLogicalFileName lfn;
    StringBuffer cname(cluster);
    lfn.set(LogicalFileName);
    if (!cname.length())
        lfn.getCluster(cname);  // see if cluster part of LogicalFileName

    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(LogicalFileName, udesc, true) ;
    if(!df)
    {
        returnStr.appendf("<Message><Value>File %s not found</Value></Message>", LogicalFileName);
        return false;
    }

    bool deleted;
    if(!nodelete && !df->querySuperFile())
    {
        Owned<IMultiException> pExceptionHandler = MakeMultiException();
        deleted = queryDistributedFileSystem().remove(df,cname.length()?cname.str():NULL,pExceptionHandler);
        StringBuffer errorStr;
        pExceptionHandler->errorMessage(errorStr);
        if (errorStr.length() > 0)
        {
            returnStr.appendf("<Message><Value>%s</Value></Message>",errorStr.str());
            DBGLOG("%s", errorStr.str());
        }
        else {
            PrintLog("Deleted Logical File: %s\n",LogicalFileName);
            returnStr.appendf("<Message><Value>Deleted File %s</Value></Message>",LogicalFileName);
        }

    }
    else
    {
        df.clear(); 
        deleted = queryDistributedFileDirectory().removeEntry(LogicalFileName,udesc); // this can remove clusters also
        if (deleted)
            returnStr.appendf("<Message><Value>Detached File %s</Value></Message>", LogicalFileName);
    }


    return deleted; 
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
        WARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        WARNLOG("Unknown Exception caught within doCompressFile");
    }

    return true; 
}
