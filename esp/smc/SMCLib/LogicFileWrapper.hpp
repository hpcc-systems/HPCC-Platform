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

// LogicFileWrapper.h: interface for the LogicFileWrapper class.
//
//////////////////////////////////////////////////////////////////////
#ifndef __DFUWrapper_HPP__
#define __DFUWrapper_HPP__


#ifdef _WIN32
#ifdef SMCLIB_EXPORTS
#define LFWRAPPER_API __declspec(dllexport)
#else
#define LFWRAPPER_API __declspec(dllimport)
#endif
#else
#define LFWRAPPER_API
#endif



#include "jiface.hpp"
#include "jstring.hpp"
#include "dadfs.hpp"
#include "daft.hpp"
#include "dalienv.hpp"
#include "jpqueue.hpp"

class LFWRAPPER_API LogicFileWrapper : public CInterface  
{
public:
    IMPLEMENT_IINTERFACE;
    LogicFileWrapper();
    virtual ~LogicFileWrapper();
    bool doDeleteFile(const char* name, const char *cluster, bool nodelete, StringBuffer& returnStr, IUserDescriptor* udesc = NULL);
    bool doCompressFile(const char* name,StringBuffer& returnStr, IUserDescriptor* udesc = 0);
    void FindClusterName(const char* logicalName,StringBuffer& returnCluster, IUserDescriptor* udesc = 0);

};

struct ErrorReceiver: public CInterface, implements IErrorListener 
{
    IMPLEMENT_IINTERFACE;    
    
    virtual void reportError(const char* err,...)
    {
        va_list args;
        va_start(args, err);
        buf.valist_appendf(err, args);
        va_end(args);
        
    }

    bool hasErrors()
    {
        return buf.length()!=0;
    }

    StringBuffer& getErrors(StringBuffer& err)
    {
        err.append(buf);
        return buf;
    }

    StringBuffer buf;
};
struct DeleteTask: public CInterface, implements ITask
{
    IMPLEMENT_IINTERFACE;    
    DeleteTask(IDistributedFilePart* _part): part(_part)
    {
    }

    virtual int run()
    {
        unsigned copies = part->numCopies();
        StringBuffer errs;
        for (unsigned copy = 0; copy < copies; copy++)
        {
            try
            {
                RemoteFilename r;
                OwnedIFile file = createIFile(part->getFilename(r, copy));
                if (!file->remove())
                {
                    StringBuffer e;
                    e.appendf("Failed to remove file part %s\n",file->queryFilename());
                    LOG(MCerror, unknownJob, e.str());
                    errs.append(e);
                }
            }
            catch(IException* e)
            {
                StringBuffer str;
                e->errorMessage(str);
                PrintLog(str.str());
                e->Release();
            }
            catch(...)
            {
                PrintLog("Unknown Exception thrown while deleteing file part\n");
            }
        }
        if(errs.length())
            throw MakeStringException(0,errs.str());

        return 0;
    }

    virtual bool stop()
    {
        return false;
    }

    Linked<IDistributedFilePart> part;
};
struct CompressTask: public CInterface, implements ITask
{
    IMPLEMENT_IINTERFACE;    
    CompressTask(IDistributedFilePart* _part): part(_part)
    {
    }

    virtual int run()
    {
        try
        {
            queryDistributedFileSystem().compress(part);
        }
        catch(IException* e)
        {
            StringBuffer err;
            e->errorMessage(err);
            LOG(MCerror, unknownJob, err.str());
            e->Release();
        }
        catch(...)
        {
            PrintLog("Unknown Exception thrown\n");
        }
        return 0;
    }

    virtual bool stop()
    {
        return false;
    }

    Linked<IDistributedFilePart> part;
};

#endif //__DFUWrapper_HPP__
