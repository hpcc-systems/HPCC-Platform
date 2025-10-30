//TBD check min time from when *finished*

#include "jptree.hpp"
#include "jregexp.hpp"

#include "mpcomm.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"

#include "sautil.hpp"
#include "sacoalescer.hpp"
#include "sacmd.hpp"

#include "xref.hpp"

static constexpr float maxMemPercentage = 0.9; // In containerized, leave some headroom for the pod
class CSashaXRefServer: public ISashaServer, public Thread
{
    bool stopped;
    Semaphore stopsem;
    Mutex runmutex;
    bool ignorelazylost, suspendCoalescer;
    Owned<IPropertyTree> props;
    std::unordered_map<std::string, Linked<IPropertyTree>> storagePlanes;

    class cRunThread: public Thread
    {
        CSashaXRefServer &parent;
        StringAttr servers;
    public:
        cRunThread(CSashaXRefServer &_parent,const char *_servers)
            : parent(_parent), servers(_servers)
        {
        }
        int run()
        {
            parent.runXRef(servers,false,false);
            return 0;
        }
    };


public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaXRefServer()
        : Thread("CSashaXRefServer")
    {
        if (!isContainerized())
            suspendCoalescer = true; // can be overridden by configuration setting
        else
            suspendCoalescer = false;
        stopped = false;
    }

    ~CSashaXRefServer()
    {
    }

    void start()
    {
        Thread::start(false);
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
        synchronized block(runmutex);   // hopefully stopped should stop
        if (!join(1000*60*3))
            OERRLOG("CSashaXRefServer aborted");
    }

    void runXRef(const char *clustcsl,bool updateeclwatch,bool byscheduler)
    {
        if (stopped||!clustcsl||!*clustcsl)
            return;
        class CSuspendResume : public CSimpleInterface
        {
        public:
            CSuspendResume()
            {
                PROGLOG(LOGPFX "suspending coalesce");
                suspendCoalescingServer();
            }
            ~CSuspendResume()
            {
                PROGLOG(LOGPFX "resuming coalesce");
                resumeCoalescingServer();
            }
        };
        if (!isContainerized()) {
            Owned<CSimpleInterface> suspendresume;
            if (suspendCoalescer)
                suspendresume.setown(new CSuspendResume());
        }
        synchronized block(runmutex);
        if (stopped)
            return;
        CSuspendAutoStop suspendstop;
        PROGLOG(LOGPFX "Started %s",clustcsl);
        StringArray list;
        getFileGroups(clustcsl, list);
        bool checksuperfiles=false;
        ForEachItemInRev(i0,list) {
            if (strcmp(list.item(i0),"SuperFiles")==0) {
                checksuperfiles = true;
                list.remove(i0);
            }
        }
        // Revisit: XREF should really be plane centric only
        StringArray groups;
        StringArray cnames;
        // NB: must be a list of planes only
        ForEachItemIn(i1, list) {
            const char *planeName = list.item(i1);
            Owned<IPropertyTreeIterator> planesIter = getPlanesIterator("data", planeName);
            ForEach(*planesIter) {
                IPropertyTree &plane = planesIter->query();
                bool isNotCopy = !plane.getPropBool("@copy", false);
                bool isNotHthorPlane = !plane.getPropBool("@hthorplane", false);
                if (isNotCopy && isNotHthorPlane) {
                    planeName = plane.queryProp("@name");
                    if (isContainerized()) {
                        groups.append(planeName);
                        cnames.append(planeName);
                    }
                    storagePlanes[planeName].set(&plane);
                }
            }
        }
        if (!isContainerized()) {
            Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
            if (!conn) {
                OERRLOG("Could not connect to /Environment/Software");
                return;
            }
            clustersToGroups(conn->queryRoot(),list,cnames,groups,NULL);
        }
        IArrayOf<IGroup> groupsdone;
        StringArray dirsdone;
        ForEachItemIn(i,groups) {
#ifdef TESTINGSUPERFILELINKAGE
            continue;
#endif
            const char *gname = groups.item(i);
            unsigned maxMb;
            if (isContainerized()) {
                const char *resourcedMemory = props->queryProp("resources/@memory");
                if (!isEmptyString(resourcedMemory)) {
                    offset_t sizeBytes = friendlyStringToSize(resourcedMemory);
                    maxMb = (unsigned)(sizeBytes / 0x100000);
                }
                else
                    maxMb = DEFAULT_MAXMEMORY;
                maxMb *= maxMemPercentage;
            }
            else
                maxMb = props->getPropInt("@memoryLimit", DEFAULT_MAXMEMORY);
            CNewXRefManager manager(storagePlanes[gname],maxMb,serverConfig);
            if (!manager.setGroup(cnames.item(i),gname,groupsdone,dirsdone))
                continue;
            manager.start(updateeclwatch);
            manager.updateStatus(true);
            if (stopped)
                break;
            unsigned numThreads = props->getPropInt("@numThreads", DEFAULT_MAXDIRTHREADS);
            unsigned int recentCutoffDays = props->getPropInt("@cutoff", DEFAULT_RECENT_CUTOFF_DAYS);
            if (manager.scanDirectories(stopped,numThreads)) {
                manager.updateStatus(true);
                manager.scanLogicalFiles(stopped);
                manager.updateStatus(true);
                manager.listLost(stopped,ignorelazylost,recentCutoffDays);
                manager.updateStatus(true);
                manager.listOrphans(stopped,recentCutoffDays);
                manager.updateStatus(true);
                manager.saveToEclWatch(stopped,byscheduler);
                manager.updateStatus(true);
            }
            manager.finish(stopped);
            manager.updateStatus(true);
            if (stopped)
                break;
        }
        if (checksuperfiles&&!stopped) {
            CSuperfileCheckManager scmanager;
            scmanager.start(updateeclwatch);
            scmanager.updateStatus(true);
            if (stopped)
                return;
            scmanager.checkSuperFileLinkage();
            scmanager.updateStatus(true);
            scmanager.saveToEclWatch(stopped,byscheduler);
            scmanager.updateStatus(true);
        }
        PROGLOG(LOGPFX "%s %s",clustcsl,stopped?"Stopped":"Done");
    }

    void xrefRequest(const char *servers)
    {
        //MORE: This could still be running when the server terminates which will likely cause the thread to core
        cRunThread *thread = new cRunThread(*this,servers);
        thread->startRelease();
    }

    bool checkClusterSubmitted(StringBuffer &cname)
    {
        cname.clear();
        Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_LOCK_WRITE ,INFINITE);
        Owned<IPropertyTreeIterator> clusters= conn->queryRoot()->getElements("Cluster");
        ForEach(*clusters) {
            IPropertyTree &cluster = clusters->query();
            const char *status = cluster.queryProp("@status");
            if (status&&(stricmp(status,"Submitted")==0)) {
                cluster.setProp("@status","Not Found"); // prevent cycling
                const char *name = cluster.queryProp("@name");
                if (name) {
                    if (cname.length())
                        cname.append(',');
                    cname.append(name);
                }
            }
        }
        return cname.length()!=0;
    }

    void setSubmittedOk(bool on)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE ,INFINITE);
        if (conn->queryRoot()->getPropBool("@useSasha")!=on)
            conn->queryRoot()->setPropBool("@useSasha",on);
    }

    int run()
    {
        if (isContainerized())
            props.setown(getComponentConfig());
        else
        {
            props.setown(serverConfig->getPropTree("DfuXRef"));
            if (!props)
                props.setown(createPTree("DfuXRef"));
        }

        bool eclwatchprovider = true;
        if (!isContainerized()) // NB: containerized does not support xref any other way.
        {
            // eclwatchProvider sets useSasha in call to setSubmittedOk
            eclwatchprovider = props->getPropBool("@eclwatchProvider");
        }

        unsigned interval = props->getPropInt("@interval",DEFAULT_XREF_INTERVAL);
        const char *clusters = props->queryProp(isContainerized() ? "@planes" : "@clusterlist");
        StringBuffer clusttmp;
        // TODO: xref should support checking superfiles in containerized
        if (props->getPropBool("@checkSuperFiles",isContainerized()?false:true))
        {
            if (!clusters||!*clusters)
                clusters = "SuperFiles";
            else
                clusters = clusttmp.append(clusters).append(',').append("SuperFiles").str();
        }
        if (!interval)
            stopped = !eclwatchprovider;
        setSubmittedOk(eclwatchprovider);
        if (!isContainerized())
            suspendCoalescer = props->getPropBool("@suspendCoalescerDuringXref", true);
        ignorelazylost = props->getPropBool("@ignoreLazyLost",true);
        PROGLOG(LOGPFX "min interval = %d hr", interval);
        unsigned initinterval = (interval-1)/2+1;  // wait a bit til dali has started
        CSashaSchedule schedule;
        if (interval)
            schedule.init(props,interval,initinterval);
        initinterval *= 60*60*1000; // ms
        unsigned started = msTick();
        while (!stopped)
        {
            stopsem.wait(1000*60);
            if (stopped)
                break;
            StringBuffer cname;
            bool byscheduler=false;
            if (!eclwatchprovider||!checkClusterSubmitted(cname.clear()))
            {
                if (!interval||((started!=(unsigned)-1)&&(msTick()-started<initinterval)))
                    continue;
                started = (unsigned)-1;
                if (!schedule.ready())
                    continue;
                byscheduler = true;
            }
            try
            {
                runXRef(cname.length()?cname.str():clusters,true,byscheduler);
                cname.clear();
            }
            catch (IException *e)
            {
                StringBuffer s;
                EXCLOG(e, LOGPFX);
                e->Release();
            }
        }
        PROGLOG(LOGPFX "Exit");
        return 0;
    }


} *sashaXRefServer = NULL;


ISashaServer *createSashaXrefServer()
{
    assertex(!sashaXRefServer); // initialization problem
    sashaXRefServer = new CSashaXRefServer();
    return sashaXRefServer;
}

void processXRefRequest(ISashaCommand *cmd)
{
    if (sashaXRefServer) {
        StringBuffer clusterlist(cmd->queryCluster());
        // only support single cluster for the moment
        if (clusterlist.length())
            sashaXRefServer->xrefRequest(clusterlist);
    }
}



// File Expiry monitor

class CSashaExpiryServer: public ISashaServer, public Thread
{
    bool stopped;
    Semaphore stopsem;
    Mutex runmutex;
    Owned<IUserDescriptor> udesc;
    Linked<IPropertyTree> props;

public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaExpiryServer(IPropertyTree *_config)
        : Thread("CSashaExpiryServer"), props(_config)
    {
        stopped = false;

        StringBuffer userName;
#ifdef _CONTAINERIZED
        props->getProp("@user", userName);
#else
        serverConfig->getProp("@sashaUser", userName);
#endif
        udesc.setown(createUserDescriptor());
        udesc->set(userName.str(), nullptr);
    }

    ~CSashaExpiryServer()
    {
    }

    void start()
    {
        Thread::start(false);
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
        synchronized block(runmutex);   // hopefully stopped should stop
        if (!join(1000*60*3))
            OERRLOG("CSashaExpiryServer aborted");
    }

    void runExpiry()
    {
        synchronized block(runmutex);
        if (stopped)
            return;
        PROGLOG(LOGPFX2 "Started");
        unsigned defaultExpireDays = props->getPropInt("@expiryDefault", DEFAULT_EXPIRYDAYS);
        unsigned defaultPersistExpireDays = props->getPropInt("@persistExpiryDefault", DEFAULT_PERSISTEXPIRYDAYS);
        StringArray expirylist;

        StringBuffer filterBuf;
        // all non-superfiles
        filterBuf.append(DFUQFTspecial).append(DFUQFilterSeparator).append(DFUQSFFileType).append(DFUQFilterSeparator).append(DFUQFFTnonsuperfileonly).append(DFUQFilterSeparator);
        // hasProp,SuperOwner,"false" - meaning not owned by a superfile
        filterBuf.append(DFUQFThasProp).append(DFUQFilterSeparator).append(getDFUQFilterFieldName(DFUQFFsuperowner)).append(DFUQFilterSeparator).append("false").append(DFUQFilterSeparator);
        // hasProp,Attr/@expireDays,"true" - meaning file has @expireDays attribute
        filterBuf.append(DFUQFThasProp).append(DFUQFilterSeparator).append(getDFUQFilterFieldName(DFUQFFexpiredays)).append(DFUQFilterSeparator).append("true").append(DFUQFilterSeparator);

        bool allMatchingFilesReceived;
        Owned<IPropertyTreeIterator> iter = queryDistributedFileDirectory().getDFAttributesTreeIterator(filterBuf,
            nullptr, nullptr, udesc, true, allMatchingFilesReceived);
        ForEach(*iter)
        {
            IPropertyTree &attr=iter->query();
            if (attr.hasProp("@expireDays"))
            {
                unsigned expireDays = attr.getPropInt("@expireDays");
                const char * name = attr.queryProp("@name");
                const char *lastAccessed = attr.queryProp("@accessed");
                if (lastAccessed && name&&*name)
                {
                    if (0 == expireDays)
                    {
                        bool isPersist = attr.getPropBool("@persistent");
                        expireDays = isPersist ? defaultPersistExpireDays : defaultExpireDays;
                    }
                    CDateTime now;
                    now.setNow();
                    CDateTime expires;
                    try
                    {
                        expires.setString(lastAccessed);
                        expires.adjustTime(60*24*expireDays);
                        if (now.compare(expires,false)>0)
                        {
                            expirylist.append(name);
                            StringBuffer expiresStr;
                            expires.getString(expiresStr);
                            PROGLOG(LOGPFX2 "%s expired on %s", name, expiresStr.str());
                        }
                    }
                    catch (IException *e)
                    {
                        StringBuffer s;
                        EXCLOG(e, LOGPFX2 "setdate");
                        e->Release();
                    }
                }
            }
        }
        iter.clear();
        ForEachItemIn(i,expirylist)
        {
            if (stopped)
                break;
            const char *lfn = expirylist.item(i);
            try
            {
                /* NB: 0 timeout, meaning fail and skip, if there is any locking contention.
                 * If the file is locked, it implies it is being accessed.
                 */
                queryDistributedFileDirectory().removeEntry(lfn, udesc, NULL, 0, true);
                PROGLOG(LOGPFX2 "Deleted %s",lfn);
            }
            catch (IException *e) // may want to just detach if fails
            {
                OWARNLOG(e, LOGPFX2 "remove");
                e->Release();
            }
        }
        PROGLOG(LOGPFX2 "%s",stopped?"Stopped":"Done");
    }

    int run()
    {
        unsigned interval = props->getPropInt("@interval",DEFAULT_EXPIRY_INTERVAL);
        if (!interval)
            stopped = true;
        PROGLOG(LOGPFX2 "min interval = %d hr", interval);
        unsigned initinterval = (interval-1)/2;  // wait a bit til dali has started
        CSashaSchedule schedule;
        if (interval)
            schedule.init(props,interval,initinterval);
        initinterval *= 60*60*1000; // ms
        unsigned started = msTick();
        while (!stopped)
        {
            stopsem.wait(1000*60);
            if (stopped)
                break;
            if (!interval||((started!=(unsigned)-1)&&(msTick()-started<initinterval)))
                continue;
            started = (unsigned)-1;
            if (!schedule.ready())
                continue;
            try {
                runExpiry();
            }
            catch (IException *e) {
                StringBuffer s;
                EXCLOG(e, LOGPFX2);
                e->Release();
            }
        }
        PROGLOG(LOGPFX2 "Exit");
        return 0;
    }


} *sashaExpiryServer = NULL;


ISashaServer *createSashaFileExpiryServer()
{
    assertex(!sashaExpiryServer); // initialization problem
#ifdef _CONTAINERIZED
    Linked<IPropertyTree> config = serverConfig;
#else
    Owned<IPropertyTree> config = serverConfig->getPropTree("DfuExpiry");
    if (!config)
        config.setown(createPTree("DfuExpiry"));
#endif
    sashaExpiryServer = new CSashaExpiryServer(config);
    return sashaExpiryServer;
}

void runExpiryCLI()
{
    Owned<IPropertyTree> config = serverConfig->getPropTree("DfuExpiry");
    if (!config)
        config.setown(createPTree("DfuExpiry"));
    Owned<CSashaExpiryServer> sashaExpiryServer = new CSashaExpiryServer(config);
    sashaExpiryServer->runExpiry();
}
