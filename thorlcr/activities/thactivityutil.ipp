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


#ifndef _thactivityutil_ipp
#define _thactivityutil_ipp

#include "jlib.hpp"
#include "jlzw.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jbuff.hpp"

#include "thormisc.hpp"
#include "thmem.hpp"
#include "thbuf.hpp"
#include "thgraphslave.hpp"
#include "eclhelper.hpp"
#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.ipp"
#include "commonext.hpp"

#define OUTPUT_RECORDSIZE


class CPartHandler : implements IRowStream, public CSimpleInterface
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    virtual ~CPartHandler() { }
    virtual void setPart(IPartDescriptor *partDesc) = 0;
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info, IPartDescriptor *partDesc) const { }
    virtual void stop() = 0;
};

IRowStream *createSequentialPartHandler(CPartHandler *partHandler, IArrayOf<IPartDescriptor> &partDescs, bool grouped);

#define CATCH_NEXTROWX_CATCH \
        catch (IException *_e) \
        { \
            this->processAndThrowOwnedException(_e); \
        }

#define CATCH_NEXTROW() \
    virtual const void *nextRow() override \
    { \
        try \
        { \
            return nextRowNoCatch(); \
        } \
        CATCH_NEXTROWX_CATCH \
    } \
    inline const void *nextRowNoCatch() __attribute__((always_inline))

void initMetaInfo(ThorDataLinkMetaInfo &info);
void calcMetaInfoSize(ThorDataLinkMetaInfo &info, IThorDataLink *link);
void calcMetaInfoSize(ThorDataLinkMetaInfo &info, const CThorInputArray &inputs);
void calcMetaInfoSize(ThorDataLinkMetaInfo &info, const ThorDataLinkMetaInfo *infos, unsigned num);

interface ILookAheadStopNotify
{
    virtual void onInputFinished(rowcount_t count) = 0;
};
IStartableEngineRowStream *createRowStreamLookAhead(CSlaveActivity *activity, IEngineRowStream *inputStream, IThorRowInterfaces *rowIf, size32_t bufsize, bool spillenabled, bool preserveGrouping=true, rowcount_t maxcount=RCUNBOUND, ILookAheadStopNotify *notify=NULL); //maxcount is maximum rows to read set to RCUNBOUND for all


StringBuffer &locateFilePartPath(CActivityBase *activity, const char *logicalFilename, IPartDescriptor &partDesc, StringBuffer &filePath);
void doReplicate(CActivityBase *activity, IPartDescriptor &partDesc, ICopyFileProgress *iProgress=NULL);
void cancelReplicates(CActivityBase *activity, IPartDescriptor &partDesc);

#define TW_Extend 0x01
#define TW_Direct 0x02
#define TW_External 0x04
#define TW_RenameToPrimary 0x08
#define TW_Temporary 0x10
interface IPartDescriptor;
IFileIO *createMultipleWrite(CActivityBase *activity, IPartDescriptor &partDesc, unsigned recordSize, unsigned twFlags, bool &compress, ICompressor *ecomp, ICopyFileProgress *iProgress, bool *aborted, StringBuffer *_locationName=NULL);

class CAsyncCall : implements IThreaded
{
    CThreaded threaded;
    std::function<void()> func;
public:
    CAsyncCall(std::function<void()> _func) : threaded("CAsyncCall", this), func(_func) { }
    void start() { threaded.start(); }
    void wait() { threaded.join(); }
// IThreaded
    virtual void threadmain() override { func(); }
};

class CAsyncCallStart : public CAsyncCall
{
public:
    CAsyncCallStart(std::function<void()> func) : CAsyncCall(func)
    {
        start();
    }
};

#endif
