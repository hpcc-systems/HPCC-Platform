#include "platform.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jhash.hpp"
#include "jregexp.hpp"

#include "dasds.hpp"
#include "daaudit.hpp"
#include "saserver.hpp"
#include "workunit.hpp"
#include "wujobq.hpp"
#include "environment.hpp"

//#define TESTING

#define DEFAULT_QMONITOR_INTERVAL       1  // minutes
#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite
#define SDS_LOCK_TIMEOUT 300000


class CSashaQMonitorServer: public ISashaServer, public Thread
{  

    bool stopped;
    bool qinitdone;
    Semaphore stopsem;
    StringArray qnames;
    StringArray cnames;
    IArrayOf<IJobQueue> queues;
public:
    IMPLEMENT_IINTERFACE;

    CSashaQMonitorServer()
        : Thread("CSashaQMonitorServer")
    {
        stopped = false;
        qinitdone = false;
    }

    ~CSashaQMonitorServer()
    {
    }

    void start()
    {
        Thread::start();
    }

    void ready()
    {
    }
    
    void stop()
    {
        if (!stopped) {
            stopped = true;
            stopsem.signal();
        }
        if (!join(1000*60*3))
            IERRLOG("CSashaQMonitorServer aborted");
    }


    bool initQueueNames(const char *qlist)
    {
        if (!qlist||!*qlist)
            return false;
        if (!qinitdone) {
            qinitdone = true;
            StringArray qs;
            qs.appendListUniq(qlist, ",");
            if (!qs.ordinality())
                return false;
            StringArray cna;
            StringArray gna;
            StringArray tna;
            StringArray qna;
            if (getEnvironmentThorClusterNames(cna,gna,tna,qna)==0)
                return false;
            ForEachItemIn(i1,tna) {
                const char *qname = tna.item(i1); // JCSMORE - ThorQMon/@queues is actually matching targets, rename property to @targets ?
                bool ok = false;
                ForEachItemIn(i2,qs) {
                    if (WildMatch(qname,qs.item(i2),true)) {
                        ok = true;
                        break;
                    }
                }
                if (ok) {
                // see if already done
                    ForEachItemIn(i2,qnames) {
                        if (strcmp(qname,qnames.item(i2))==0) {
                            ok = false;
                            break;
                        }
                    }
                    if (ok) {
                        qnames.append(qname);
                        cnames.append(tna.item(i1));
                    }
                }
            }
        }
        return qnames.ordinality()!=0;
    }


    bool doSwitch(const char *wuid,const char *cluster)
    {
        class cQswitcher: public CInterface, implements IQueueSwitcher
        {
            const StringArray &qnames;
            const IArrayOf<IJobQueue> &queues;
            IJobQueue *findQueue(const char *qname) // this is a bit OTT as we should know the queues involved
            {
                // remove .thor
                if (!qname)
                    return NULL;
                size32_t l = strlen(qname);
                if (l<6)
                    return NULL;
                StringBuffer test(l-5,qname);
                qname = test.str();
                ForEachItemIn(i,qnames) {
                    if (strcmp(qname,qnames.item(i))==0)
                        return &queues.item(i);
                }
                return NULL;
            }
        public:
            IMPLEMENT_IINTERFACE;
            cQswitcher(const StringArray &_qnames,const IArrayOf<IJobQueue> &_queues)
                : qnames(_qnames), queues(_queues)
            {
            }
            void * getQ(const char * qname, const char * wuid)
            {
                IJobQueue *q = findQueue(qname);
                if (!q)
                    return NULL;
                return q->take(wuid);
            }
            void putQ(const char * qname, const char * wuid, void * qitem)
            {
                IJobQueue *q = findQueue(qname);
                if (q)
                    q->enqueue((IJobQueueItem *)qitem);
                else
                    IERRLOG("cQswitcher cannot match queue %s",qname); // I don't think this can ever really happen
            }
            bool isAuto()
            {
                return true;
            }
        } switcher(qnames,queues);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
        if (wu)
            return wu->switchThorQueue(cluster, &switcher);
        return false;
    }

    bool switchQueues(unsigned qi)
    {
        // don't swap to it if stopped or paused!
        IJobQueue &dest = queues.item(qi);
        if (dest.paused()||dest.stopped())
            return false;

        // see if can find candidate on another queue
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        ForEachItemIn(i1,queues) {
            if (i1!=qi) {
                IJobQueue &srcq = queues.item(i1);
                CJobQueueContents qc;
                srcq.copyItems(qc);
                Owned<IJobQueueIterator> iter = qc.getIterator();
                ForEach(*iter) {
                    const char *wuid = iter->query().queryWUID();
                    if (wuid&&*wuid) {
                        Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid);
                        if (wu) {
                            SCMStringBuffer allowedClusters;
                            if (wu->getAllowedClusters(allowedClusters).length()) {
                                StringArray acs;
                                acs.appendListUniq(allowedClusters.str(), ",");
                                bool found = true;
                                ForEachItemIn(i,acs) {
                                    if (strcmp(cnames.item(qi),acs.item(i))==0) 
                                        return doSwitch(wuid,acs.item(i));
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    int run()
    {
        Owned<IPropertyTree> qmonprops = serverConfig->getPropTree("ThorQMon");
        if (!qmonprops)
            qmonprops.setown(createPTree("ThorQMon"));
        unsigned interval = qmonprops->getPropInt("@interval",DEFAULT_QMONITOR_INTERVAL); // probably always 1
        unsigned autoswitch = qmonprops->getPropInt("@switchMinTime",0);
        if (!interval)
            return 0;
        if (!initQueueNames(qmonprops->queryProp("@queues")))
            return 0;
        Owned<IRemoteConnection> conn = querySDS().connect("Status/Servers", myProcessSession(), 0, 100000);
        if (!conn) {
            OERRLOG("cannot connect to Status/Servers");
            return -1;
        }
        unsigned *qidlecount = new unsigned[qnames.ordinality()];
        ForEachItemIn(i1,qnames) {
            StringBuffer qname(qnames.item(i1));
            qname.append(".thor");
            queues.append(*createJobQueue(qname.str()));
            qidlecount[i1] = 0;
        }
        unsigned sleeptime = autoswitch?1:interval;
        unsigned moninter = interval;
        while (!stopped) {
            stopsem.wait(60*1000*sleeptime);
            if (stopped)
                break;
            moninter-=sleeptime;
            if (autoswitch||(moninter==0)) { // always true at moment
                try {
                    conn->reload();
                    ForEachItemIn(qi,qnames) {
                        const char *qname = qnames.item(qi);
                        StringBuffer xpath;
                        xpath.appendf("Server[@queue=\"%s.thor\"]/WorkUnit",qname);
                        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements(xpath.str());
                        StringArray wuids;
                        ForEach(*iter) {
                            IPropertyTree &wu = iter->query();
                            wuids.append(wu.queryProp(NULL));
                        }
                        unsigned enqueued=0;
                        unsigned connected=0;
                        unsigned waiting=0;
                        queues.item(qi).getStats(connected,waiting,enqueued);
                        if (moninter==0)
                            LOG(MCauditInfo,",ThorQueueMonitor,%s,%d,%d,%d,%d,%d,%s,%s",qname,wuids.ordinality(),enqueued,waiting,connected,qidlecount[qi],wuids.ordinality()>0?wuids.item(0):"---",wuids.ordinality()>1?wuids.item(1):"---");
                        if (waiting>0) 
                            qidlecount[qi]++;
                        else
                            qidlecount[qi] = 0;
                    }
                }
                catch (IException *e) {
                    StringBuffer s;
                    EXCLOG(e, "QMONITOR");
                    e->Release();
                }
                if (autoswitch) {
                    ForEachItemIn(qi2,qnames) {
                        if (qidlecount[qi2]>autoswitch) {   // > not >= to get conservative estimate of how long idle
                            if (switchQueues(qi2))
                                break; // only switch one per cycle (bit of cop-out)
                        }
                    }
                }
            }
            if (moninter==0)
                moninter = interval;
        }
        delete [] qidlecount;
        return 0;
    }


} *sashaQMonitorServer = NULL;


ISashaServer *createSashaQMonitorServer()
{
    assertex(!sashaQMonitorServer); // initialization problem
    sashaQMonitorServer = new CSashaQMonitorServer();
    return sashaQMonitorServer;
}



