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

// LogicFileWrapper.h: interface for the LogicFileWrapper class.
//
//////////////////////////////////////////////////////////////////////
#ifndef __DFUWrapper_HPP__
#define __DFUWrapper_HPP__


#ifdef SMCLIB_EXPORTS
#define LFWRAPPER_API DECL_EXPORT
#else
#define LFWRAPPER_API DECL_IMPORT
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
    bool doDeleteFile(const char* name, const char *cluster, StringBuffer& returnStr, IUserDescriptor* udesc = NULL);
    bool doCompressFile(const char* name,StringBuffer& returnStr, IUserDescriptor* udesc = 0);
    void FindClusterName(const char* logicalName,StringBuffer& returnCluster, IUserDescriptor* udesc = 0);

};

struct ErrorReceiver: public CInterface, implements IErrorListener 
{
    IMPLEMENT_IINTERFACE;    
    
    virtual void reportError(const char* err,...) __attribute__((format(printf,2,3)))
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
                    LOG(MCuserError, unknownJob, "%s", e.str());
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
            throw MakeStringException(0, "%s", errs.str());

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
            LOG(MCuserError, unknownJob, "%s", err.str());
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
