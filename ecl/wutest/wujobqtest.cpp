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
#include "jlib.hpp"
#include "jfile.hpp"
#include "jprop.hpp"
#include "jsocket.hpp"
#include "wujobq.hpp"
#include "mpbase.hpp"
#include "dllserver.hpp"
#include "daclient.hpp"
#include "dasds.hpp"


#define DEFAULT_PORT 7171

static void usage()
{
    printf("Usage: WUJOBQTEST <dali-ip> <action> <jobq> <params>\n");
    printf("Actions:\n");
    printf("       list <jobq>                         -- prints queue contents\n");
    printf("       stats <jobq>                        -- prints queue contents\n");
    printf("       tofront <jobq> <WUID>               -- puts queue item to front\n");
    printf("       toback <jobq> <WUID>                -- puts queue item to back\n");
    printf("       setprio <jobq> <WUID> <priority)    -- changes priority\n");
    printf("       movebefore <jobq> <WUID> <WUID2>    -- moves before\n");
    printf("       moveafter <jobq> <WUID> <WUID2>     -- moves after\n");
    printf("       remove <jobq> <WUID>                -- removes item from queue\n");
    printf("       pause <jobq>                        -- paused queue\n");
    printf("       stop <jobq>                         -- stops queue\n");
    printf("       resume <jobq>                       -- resumes paused/stopped queue\n");
    printf("       clear <jobq>                        -- clears queue\n");
    printf("       prior <jobq>                        -- prints last item to be dequeued\n");
    printf("       switch <WUID> <cluster>             -- switches WU to new cluster\n");
    printf("       usage <cluster>                     -- monitors job queue for cluster\n");
    printf("Testing:\n");
    printf("       add <jobq> <WUID> <PRIORITY> <EP>\n");
    printf("       dequeue <jobq>\n");
    printf("       accept <jobq>\n");
    printf("       initiate <jobq> <WUID> <PRIORITY>\n");
    printf("       xremove <jobq> <WUID>                    -- removes queue item (doesn't cancel conv)\n");
    exit(2);
}

extern bool switchWorkunitQueue(const char *wuid, const char *cluster);


#if 0

void testEnqueue(unsigned nthreads,const char *qname)
{
    class casyncfor: public CAsyncFor
    {
    public:
        bool ok;
        casyncfor()
        {
        }
        void Do(unsigned i)
        {
            try {
                Owned<IJobQueue> jq = createJobQueue(loadGroup.str());
                IJobQueueItem* item = createJobQueueItem(wuid.str());
                item->setOwner(owner.str());
                item->setPriority(priority);        

                class cPollThread: public Thread
                {
                    Semaphore sem;
                    bool stopped;
                    unsigned starttime;
                    IJobQueue *jq;
                    IAgentContext *realagent;
                public:
                    bool timedout;
                    CTimeMon tm;
                    cPollThread(IJobQueue *_jq,IAgentContext *_realagent,unsigned timelimit)
                        : tm(timelimit)
                    {
                        stopped = false;
                        jq = _jq;
                        realagent = _realagent;
                        timedout = false;
                    }
                    ~cPollThread()
                    {
                        stop();
                    }   
                    int run()
                    {
                        while (!stopped) {
                            sem.wait(ABORT_POLL_PERIOD);
                            if (stopped)
                                break;
                            if (tm.timedout()) {
                                timedout = true;
                                stopped = true;
                                jq->cancelInitiateConversation();
                            }
                            else if (realagent->queryWorkUnit()->aborting()) {
                                stopped = true;
                                jq->cancelInitiateConversation();
                            }

                        }
                        return 0;
                    }
                    void stop()
                    {
                        stopped = true;
                        sem.signal();
                    }
                } pollthread(jq,realAgent,timelimit);
                pollthread.start();












            for (unsigned copy = 0; copy < 2; copy++)
            {
                unsigned idx=copy?((i+width/2)%width):i;
                Owned<IDistributedFilePart> part = file->getPart(idx);
                if (copy&&(copy>=part->numCopies()))
                    continue;
                RemoteFilename rfn;
                part->getFilename(rfn,copy,false);
                if (grpfilter&&(grpfilter->rank(rfn.queryEndpoint())==RANK_NULL))
                    continue;
                if (port)
                    rfn.setPort(port); // if daliservix
                Owned<IFile> partfile = createIFile(rfn);
                StringBuffer eps;
                try
                {
                    unsigned start = msTick();
                    if (!partfile->remove()&&(copy==0)) // only warn about missing primary files
                        LOG(MCuserWarning, unknownJob, "Failed to remove file part %s from %s", partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                    else {
                        unsigned t = msTick()-start;
                        if (t>5*1000) 
                            LOG(MCuserWarning, unknownJob, "Removing %s from %s took %ds", partfile->queryFilename(), rfn.queryEndpoint().getUrlStr(eps).str(), t/1000);
                    }

                }
                catch (IException *e)
                {
                    CriticalBlock block(errcrit);
                    if (mexcept) 
                        mexcept->append(*e);
                    else {
                        StringBuffer s("Failed to remove file part ");
                        s.append(partfile->queryFilename()).append(" from ");
                        rfn.queryEndpoint().getUrlStr(s);
                        EXCLOG(e, s.str());
                        e->Release();
                    }
                    ok = false;
                }
            }
        }
    } afor(this,width,port,grpfilter,mexcept,errcrit);
    afor.For(width,10,false,true);
}



    class cPollThread: public Thread
    {
        Semaphore sem;
        bool stopped;
        unsigned starttime;
        IJobQueue *jq;
        IAgentContext *realagent;
    public:
        
        bool timedout;
        CTimeMon tm;
        cPollThread(IJobQueue *_jq,IAgentContext *_realagent,unsigned timelimit)
            : tm(timelimit)
        {
            stopped = false;
            jq = _jq;
            realagent = _realagent;
            timedout = false;
        }
        ~cPollThread()
        {
            stop();
        }
        int run()
        {
            while (!stopped) {
                sem.wait(ABORT_POLL_PERIOD);
                if (stopped)
                    break;
                if (tm.timedout()) {
                    timedout = true;
                    stopped = true;
                    jq->cancelInitiateConversation();
                }
                else if (realagent->queryWorkUnit()->aborting()) {
                    stopped = true;
                    jq->cancelInitiateConversation();
                }

            }
            return 0;
        }
        void stop()
        {
            stopped = true;
            sem.signal();
        }
    } pollthread(jq,realAgent,timelimit);
    pollthread.start();
    PROGLOG("Enqueuing on %s to run wuid=%s, graph=%s, timelimit=%d, priority=%d", loadGroup.str(), wuid.str(), graphName, timelimit, priority);
    Owned<IConversation> conversation = jq->initiateConversation(item);
    bool got = conversation.get()!=NULL;
    pollthread.stop();
    pollthread.join();
    if (!got) {
        if (pollthread.timedout)
            throw MakeStringException(0, "Query %s failed to start within specified timelimit (%d)", wuid.str(), timelimit);
        throw MakeStringException(0, "Query %s cancelled (1)",wuid.str());
    }
    // get the thor ep from whoever picked up

    SocketEndpoint thorMaster;
    MemoryBuffer msg;
    if (!conversation->recv(msg,1000*60)) {
        throw MakeStringException(0, "Query %s cancelled (2)",wuid.str());
    }
    thorMaster.deserialize(msg);
    msg.clear().append(graphName);
    SocketEndpoint myep;
    myep.setLocalHost(0);
    myep.serialize(msg);  // only used for tracing
    if (!conversation->send(msg)) {
        StringBuffer s("Failed to send query to Thor on ");
        thorMaster.getUrlStr(s);
        throw MakeStringException(-1, s.str()); // maybe retry?
    }

#endif

static void cmd_list(IJobQueue *queue)                            
{
    CJobQueueContents contents;
    queue->copyItems(contents);
    Owned<IJobQueueIterator> iter = contents.getIterator();
    unsigned n=0;
    ForEach(*iter) {
        n++;
        IJobQueueItem &item = iter->query();
        StringBuffer eps;
        StringBuffer dts;
        printf("%3d: %s owner=%s priority=%d session=%" I64F "x ep=%s port=%d enqueuedt=%s\n",n,item.queryWUID(),item.queryOwner(),item.getPriority(),item.getSessionId(),item.queryEndpoint().getUrlStr(eps).str(),item.getPort(),item.queryEnqueuedTime().getString(dts).str());
    }
}

static void cmd_prior(IJobQueue *queue)                            
{
    const char *qn = queue->nextQueueName(NULL);
    while (qn) {
        StringAttr wuid;
        CDateTime enqueuedt;
        int prio;
        queue->setActiveQueue(qn);
        if (queue->getLastDequeuedInfo(wuid,enqueuedt,prio)) {
            StringBuffer dts;
            enqueuedt.getString(dts);
            printf("%s: wuid=%s enqueuedt=%s priority=%d \n",qn,wuid.get(),dts.str(),prio);
        }
        else
            printf("%s: No prior item recorded\n");
        qn = queue->nextQueueName(qn);
    }
}


static void cmd_xremove(IJobQueue *queue,const char *wuid)
{
    if (queue->remove(wuid))
        printf("%s removed\n",wuid);
    else
        printf("%s not removed\n",wuid);

}

static void cmd_tofront(IJobQueue *queue,const char *wuid) 
{
    if (queue->moveToHead(wuid))
        printf("%s moved to front\n",wuid);
    else
        printf("%s not moved\n",wuid);
}

static void cmd_toback(IJobQueue *queue,const char *wuid) 
{
    if (queue->moveToTail(wuid))
        printf("%s moved to back\n",wuid);
    else
        printf("%s not moved\n",wuid);
}

static void cmd_setprio(IJobQueue *queue,const char *wuid,int prio) 
{
    if (queue->changePriority(wuid,prio))
        printf("%s changed priority to %d\n",wuid,prio);
    else
        printf("%s could not change priority\n",wuid);
}

static void cmd_movebefore(IJobQueue *queue,const char *wuid,const char *wuid2) 
{
    if (queue->moveBefore(wuid,wuid2))
        printf("%s moved before %s\n",wuid,wuid2);
    else
        printf("%s not moved\n",wuid);
}

static void cmd_moveafter(IJobQueue *queue,const char *wuid,const char *wuid2) 
{
    if (queue->moveAfter(wuid,wuid))
        printf("%s moved after %s\n",wuid,wuid2);
    else
        printf("%s not moved\n",wuid);
}

static void cmd_pause(IJobQueue *queue) 
{
    queue->pause();
}

static void cmd_stop(IJobQueue *queue) 
{
    queue->stop();
    for (;;) {
        unsigned enqueued=0;
        unsigned connected=0;
        unsigned waiting=0;
        queue->getStats(connected,waiting,enqueued);
        printf("%d connected, waiting...\n",connected);
        if (connected==0) {
            queue->resume();    // auto resume
            break;
        }
        if (!queue->waitStatsChange(unsigned()-1))
            break;
    }
}

static void cmd_resume(IJobQueue *queue) 
{
    queue->resume();
}

static void cmd_clear(IJobQueue *queue) 
{
    queue->clear();
}

static void cmd_add(IJobQueue *queue,const char *wuid,int prio,const char *eps) 
{
    IJobQueueItem *item = createJobQueueItem(wuid);
    item->setPriority(prio);
    if (eps&&*eps) {
        SocketEndpoint ep(eps);
        item->setEndpoint(ep);
    }
    item->setOwner("testUser");
    queue->enqueue(item);
}

static void cmd_accept(IJobQueue *queue) 
{
    queue->connect();
    IJobQueueItem *item;
    Owned<IConversation> conv = queue->acceptConversation(item);
    if (!conv.get()) {
        printf("Initiate failed\n");
        queue->disconnect();
        return;
    }
    MemoryBuffer mb;
    mb.append("hello"); 
    conv->send(mb);
    printf("sent '%s'\n","hello");
    queue->disconnect();

}

static void cmd_dequeue(IJobQueue *queue) 
{
    queue->connect(false);
    Owned<IJobQueueItem> item = queue->dequeue(1000*60);
    if (!item) {
        printf("Timed out\n");
        return;
    }
    StringBuffer eps;
    printf("%s owner=%s priority=%d session=%" I64F "x ep=%s port=%d\n",item->queryWUID(),item->queryOwner(),item->getPriority(),item->getSessionId(),item->queryEndpoint().getUrlStr(eps).str(),item->getPort());
    queue->disconnect();
}

static void cmd_initiate(IJobQueue *queue,const char *wuid,int prio) 
{
    IJobQueueItem *item = createJobQueueItem(wuid);
    item->setPriority(prio);
    item->setPort(DEFAULT_PORT);    
    Owned<IConversation> conv= queue->initiateConversation(item);
    if (!conv.get()) {
        printf("Initiate failed\n");
        return;
    }
    MemoryBuffer mb;
    if (!conv->recv(mb, 3*60*1000)) {
        printf("initial message not received\n");
        return;
    }
    StringAttr s;
    mb.read(s);
    printf("acceptor sent '%s'\n",s.get());
}

static void cmd_remove(IJobQueue *queue,const char *wuid)
{
    if (queue->cancelInitiateConversation(wuid))
        printf("removed %s\n",wuid);
    else
        printf("failed to remove %s\n",wuid);

}


static void cmd_stats(IJobQueue *queue,bool wait) 
{
    for (;;) {
        unsigned enqueued=0;
        unsigned connected=0;
        unsigned waiting=0;
        queue->getStats(connected,waiting,enqueued);
        printf("%d item(s) on queue, %d client(s) connected, %d client(s) waiting\n",enqueued,connected,waiting);
        if (!wait)
            break;
        if (!queue->waitStatsChange(unsigned()-1))
            break;
    }
}

static void cmd_activity(IJobQueue *queue,const char *qname)
{
    StringBuffer xpath;
    xpath.appendf("Server[@queue=\"%s\"]/WorkUnit",qname);
    Owned<IRemoteConnection> conn = querySDS().connect("Status/Servers", myProcessSession(), 0, 100000);
    if (!conn) {
        ERRLOG("cannot connect to Status/Servers");
        return;
    }
    for (;;) {
        conn->reload();
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements(xpath.str());
        StringArray wuids;
        ForEach(*iter) {
            IPropertyTree &wu = iter->query();
            wuids.append(wu.queryProp(NULL));
        }
        unsigned enqueued=0;
        unsigned connected=0;
        unsigned waiting=0;
        queue->getStats(connected,waiting,enqueued);
        StringBuffer times;
        CDateTime time;
        time.setNow();
        time.getString(times);
        fprintf(stdout,"%s,%d,%d,%d,%d,%s,%s\n",times.str(),wuids.ordinality(),enqueued,waiting,connected,wuids.ordinality()>0?wuids.item(0):"---",wuids.ordinality()>1?wuids.item(1):"---");
        fflush(stdout);
        Sleep(60*1000);
    }
}


int main(int argc, const char *argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    if (argc<4)
        usage();
    Owned<IGroup> serverGroup = createIGroup(argv[1], DALI_SERVER_PORT);
    initClientProcess(serverGroup,DCR_Other);
    try
    {
        const char *action = argv[2];       
        const char *qname = argv[3];        
        const char *wuid = (argc>4)?argv[4]:"";     
        int prio = (argc>5)?atoi(argv[5]):0;        
        const char *wuid2 = (argc>5)?argv[5]:"";        
        Owned<IJobQueue> queue = (stricmp(action,"switch")!=0)?createJobQueue(qname):NULL;

        if (stricmp(action,"list")==0)
            cmd_list(queue);                            
        else if (stricmp(action,"xremove")==0)
            cmd_xremove(queue,wuid);
        else if (stricmp(action,"remove")==0)
            cmd_remove(queue,wuid);
        else if (stricmp(action,"tofront")==0)
            cmd_tofront(queue,wuid); 
        else if (stricmp(action,"toback")==0)
            cmd_toback(queue,wuid); 
        else if (stricmp(action,"setprio")==0)
            cmd_setprio(queue,wuid,prio); 
        else if (stricmp(action,"movebefore")==0)
            cmd_movebefore(queue,wuid,wuid2); 
        else if (stricmp(action,"moveafter")==0)
            cmd_moveafter(queue,wuid,wuid2); 
        else if (stricmp(action,"pause")==0)
            cmd_pause(queue); 
        else if (stricmp(action,"stop")==0)
            cmd_stop(queue); 
        else if (stricmp(action,"resume")==0)
            cmd_resume(queue); 
        else if (stricmp(action,"clear")==0)
            cmd_clear(queue); 
        else if (stricmp(action,"add")==0)
            cmd_add(queue,wuid,prio,(argc>6)?argv[6]:NULL); 
        else if (stricmp(action,"switch")==0) {
//          switchWorkunitQueue(argv[3],argv[4]); 
        }
        else if (stricmp(action,"accept")==0)
            cmd_accept(queue); 
        else if (stricmp(action,"dequeue")==0)
            cmd_dequeue(queue); 
        else if (stricmp(action,"initiate")==0)
            cmd_initiate(queue,wuid,prio); 
        else if (stricmp(action,"stats")==0)
            cmd_stats(queue,(argc>4)?(stricmp(argv[4],"loop")==0):false); 
        else if (stricmp(action,"activity")==0)
            cmd_activity(queue,qname); 
        else if (stricmp(action,"prior")==0)
            cmd_prior(queue); 
        else
            usage();
        queue.clear();

    }
    catch (IException *e)
    {
        StringBuffer m;
        printf("Error: %s\n", e->errorMessage(m).str());
        e->Release();
    }
    closedownClientProcess();   // dali client closedown
    releaseAtoms();
    return 0;
}




