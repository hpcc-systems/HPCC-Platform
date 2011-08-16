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
