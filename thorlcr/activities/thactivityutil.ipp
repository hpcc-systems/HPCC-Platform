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

#define OUTPUT_RECORDSIZE



class CThorTransferGroup : public CSimpleInterface, implements IThreaded
{
    unsigned short rcvPort;
    ISocket * acceptListener;
    unsigned count;

    void _receive(ISocket * rcv);
    CriticalSection sect;
    
protected:
    CThreaded threaded;
    Linked<IEngineRowAllocator> allocator;
    Linked<IOutputRowSerializer> serializer;
    Linked<IOutputRowDeserializer> deserializer;
    CGraphElementBase *owner;
    bool aborted;
    void abort();

public:
    CThorTransferGroup(CGraphElementBase *_owner, IEngineRowAllocator *_allocator,IOutputRowSerializer *_serializer,IOutputRowDeserializer *_deserializer,unsigned short _rcvPort,unsigned _count=1) 
        : threaded("CThorTransferGroup"), owner(_owner), allocator(_allocator), serializer(_serializer), deserializer(_deserializer)
    { 
        aborted = false; 
        rcvPort = _rcvPort; 
        acceptListener = NULL; 
        count = _count; 
    } 
    ~CThorTransferGroup()
    {
        if (acceptListener)
            acceptListener->cancel_accept();
    }

// IThreaded
    virtual void main();

    void send(SocketEndpoint &ep,CThorRowArray & group);
    virtual void receive(CThorRowArray *group) = 0; // called function must free group
};


class CGroupTransfer : public CThorTransferGroup
{
    CThorRowArray  *receiveArray;
    Semaphore nextRowSem;
    unsigned next;
    bool firstGet;

// CThorTransferGroup methods
    virtual void receive(CThorRowArray * group);

public:
    CGroupTransfer(CGraphElementBase *_container, IEngineRowAllocator *_allocator,IOutputRowSerializer *_serializer,IOutputRowDeserializer *_deserializer, unsigned short receivePort);
    ~CGroupTransfer();

    const void * nextRow();
    void abort();
};


//void startInput(CActivityBase *activity, IThorDataLink * i, const char *extra=NULL);
//void stopInput(IThorDataLink * i, const char * activityName = NULL, activity_id activitiyId = 0);

class CPartHandler : public CSimpleInterface, implements IRowStream
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    virtual ~CPartHandler() { }
    virtual void setPart(IPartDescriptor *partDesc, unsigned partNoSerialized) = 0;
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info, IPartDescriptor *partDesc) { }
    virtual void stop() = 0;
};

IRowStream *createSequentialPartHandler(CPartHandler *partHandler, IArrayOf<IPartDescriptor> &partDescs, bool grouped);

#define CATCH_NEXTROWX_CATCH \
        catch (IException *_e) \
        { \
            IThorException *e = QUERYINTERFACE(_e, IThorException); \
            if (e) \
            { \
                if (!e->queryActivityId()) \
                    setExceptionActivityInfo(container, e); \
            } \
            else \
            {  \
                e = MakeActivityException(this, _e); \
                _e->Release(); \
            }  \
            if (!e->queryNotified()) \
            { \
                fireException(e); \
                e->setNotified(); \
            } \
            throw e; \
        }

#define CATCH_NEXTROW() \
    const void *nextRow() \
    { \
        try \
        { \
            return nextRowNoCatch(); \
        } \
        CATCH_NEXTROWX_CATCH \
    } \
    const void *nextRowNoCatch()

class CThorDataLink : implements IThorDataLink
{
    CActivityBase *owner;
    rowcount_t count, icount;
    char *activityName;
    activity_id activityId;
    unsigned outputId;
    unsigned limit;

protected:
    inline void dataLinkStart()
    {
        dataLinkStart(NULL, 0);
    }

    inline void dataLinkStart(const char * _activityName, activity_id _activityId, unsigned _outputId = 0)
    {
        if(_activityName) 
        {
            StringBuffer x(_activityName);
            activityName = x.toUpperCase().detach();
            activityId = _activityId;
            outputId = _outputId;
        }
#ifdef _TESTING
        ActPrintLog(owner, "ITDL starting for output %d", outputId);
#endif
#ifdef _TESTING
        assertex(!started() || stopped());      // ITDL started twice
#endif
        icount = 0;
//      count = THORDATALINK_STARTED;
        rowcount_t prevCount = count & THORDATALINK_COUNT_MASK;
        count = prevCount | THORDATALINK_STARTED;
    }

    inline void dataLinkStop()
    {
#ifdef _TESTING
        assertex(started());        // ITDL stopped without being started
#endif
        count |= THORDATALINK_STOPPED;
#ifdef _TESTING
        ActPrintLog(owner, "ITDL output %d stopped, count was %"RCPF"d", outputId, getDataLinkCount());
#endif
        if(activityName) 
        {
            free(activityName);
            activityName = NULL;
        }
    }

    inline void dataLinkIncrement()
    {
        dataLinkIncrement(1);
    }

    inline void dataLinkIncrement(rowcount_t v)
    {
#ifdef _TESTING
        assertex(started());
#ifdef OUTPUT_RECORDSIZE
        if (count==THORDATALINK_STARTED) {
            size32_t rsz = queryRowMetaData(this)->getRecordSize(NULL);
            ActPrintLog(owner, "Record size %s= %d", queryRowMetaData(this)->isVariableSize()?"(max) ":"",rsz);
        }   
#endif
#endif
        icount += v;
        count += v; 
    }

    inline bool started()
    {
        return (count & THORDATALINK_STARTED) ? true : false; 
    }

    inline bool stopped()
    {
        return (count & THORDATALINK_STOPPED) ? true : false;
    }


public:
    CThorDataLink(CActivityBase *_owner) : owner(_owner)
    {
        icount = count = 0;
        activityName = NULL;
        activityId = 0;
    }
#ifdef _TESTING
    ~CThorDataLink()
    { 
        if(started()&&!stopped())
        {
            ActPrintLog(owner, "ERROR: ITDL was not stopped before destruction");
            dataLinkStop(); // get some info (even though failed)       
        }
    }           
#endif

    void dataLinkSerialize(MemoryBuffer &mb)
    {
        mb.append(count);
    }

    unsigned __int64 queryTotalCycles() const { return ((CSlaveActivity *)owner)->queryTotalCycles(); }

    inline rowcount_t getDataLinkGlobalCount()
    {
        return (count & THORDATALINK_COUNT_MASK); 
    } 
    inline rowcount_t getDataLinkCount()
    {
        return icount; 
    } 

    CActivityBase *queryFromActivity() { return owner; }

    void initMetaInfo(ThorDataLinkMetaInfo &info); // for derived children to call from getMetaInfo
    static void calcMetaInfoSize(ThorDataLinkMetaInfo &info,IThorDataLink *input); // for derived children to call from getMetaInfo
    static void calcMetaInfoSize(ThorDataLinkMetaInfo &info,IThorDataLink **link,unsigned ninputs);
    static void calcMetaInfoSize(ThorDataLinkMetaInfo &info, ThorDataLinkMetaInfo *infos,unsigned num);
};

interface ISmartBufferNotify
{
    virtual bool startAsync() =0;                       // return true if need to start asynchronously
    virtual void onInputStarted(IException *e) =0;      // e==NULL if start suceeded, NB only called with exception if Async
    virtual void onInputFinished(rowcount_t count) =0;
};

class CThorRowAggregator : public RowAggregator
{
    bool grow;
    memsize_t maxMem;
    CActivityBase &activity;
    
public:
    CThorRowAggregator(CActivityBase &_activity, IHThorHashAggregateExtra &extra, IHThorRowAggregator &helper, memsize_t _maxMem, bool _grow) : RowAggregator(extra, helper), activity(_activity), maxMem(_maxMem), grow(_grow)
    {
    }

    void checkMem();
    
// overloaded
    AggregateRowBuilder &addRow(const void *row);
    void mergeElement(const void *otherElement);
};

interface IDiskUsage;
IThorDataLink *createDataLinkSmartBuffer(CActivityBase *activity,IThorDataLink *in,size32_t bufsize,bool spillenabled,bool preserveLhsGrouping=true,rowcount_t maxcount=RCUNBOUND,ISmartBufferNotify *notify=NULL, bool inputstarted=false, IDiskUsage *_diskUsage=NULL); //maxcount is maximum rows to read set to RCUNBOUND for all

bool isSmartBufferSpillNeeded(CActivityBase *act);

StringBuffer &locateFilePartPath(CActivityBase *activity, const char *logicalFilename, IPartDescriptor &partDesc, StringBuffer &filePath);
void doReplicate(CActivityBase *activity, IPartDescriptor &partDesc, ICopyFileProgress *iProgress=NULL);
void cancelReplicates(CActivityBase *activity, IPartDescriptor &partDesc);

interface IPartDescriptor;
IFileIO *createMultipleWrite(CActivityBase *activity, IPartDescriptor &partDesc, unsigned recordSize, bool &compress, bool extend, ICompressor *ecomp, ICopyFileProgress *iProgress, bool direct, bool renameToPrimary, bool *aborted, StringBuffer *_locationName=NULL);


#endif
