#include "platform.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jhash.hpp"
#include "jregexp.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "rmtfile.hpp"
#include "dasds.hpp"
#include "dadfs.hpp"
#include "saserver.hpp"
#include "saverify.hpp"
#include "sautil.hpp"
#include "dautils.hpp"
#include "rmtfile.hpp"
#include "rmtclient.hpp"

//#define TESTING

#define DEFAULT_VERIFY_INTERVAL     7 // hours
#define DEFAULT_VERIFY_CUTOFF       7 // days 
#define DEFAULT_VERIFY_MIN_CUTOFF   1 // days
#define DEFAULT_DAFSMONITOR_INTERVAL 1 // hour

#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite
#define SDS_LOCK_TIMEOUT 300000

struct CMachineEntry: public CInterface
{
    CMachineEntry(const char *_mname,SocketEndpoint _ep,bool _avail)
        : mname(_mname),ep(_ep)
    {
        avail = _avail;
    }
    StringAttr mname;
    SocketEndpoint ep;
    bool avail;
};

typedef CMachineEntry *CMachineEntryPtr;

typedef MapStringTo<CMachineEntryPtr> CMachineEntryMap;
static  CMachineEntryMap machinemap;
static  CIArrayOf<CMachineEntry> machinelist;


static void loadMachineMap()
{
    if (machinelist.ordinality())
        return;
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Hardware", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return;
    IPropertyTree* root = conn->queryRoot();
    Owned<IPropertyTreeIterator> machines= root->getElements("Computer");
    if (machines->first()) {
        do {
            IPropertyTree &machine = machines->query();
            SocketEndpoint ep(machine.queryProp("@netAddress"));
            ep.port = getDaliServixPort();
            const char *name = machine.queryProp("@name");
            const char *state=machine.queryProp("@state");
            CMachineEntry *entry = new CMachineEntry(name,ep,!state||stricmp(state,"Available")==0);
            machinemap.setValue(name, entry);
            machinelist.append(*entry);
        } while (machines->next());
    }
}

static bool getCluster(const char *clustername,SocketEndpointArray &eps)
{
    Owned<IGroup> grp = queryNamedGroupStore().lookup(clustername);
    if (grp.get()==NULL)
        return false;
    unsigned n = grp->ordinality();
    unsigned short p = getDaliServixPort();
    for (unsigned i=0;i<n;i++) {
        SocketEndpoint ep(p,grp->queryNode(i).endpoint());
        eps.append(ep);
    }
    return eps.ordinality()!=0;
}

static void appendNodeEndpoint(IPropertyTree& node,SocketEndpointArray &eps)
{
    const char *computer = node.queryProp("@computer");
    CMachineEntryPtr *m = machinemap.getValue(computer);
    if (!m) {
        OERRLOG("Computer name %s not found",computer);
        return;
    }
    SocketEndpoint ep = (*m)->ep;
    if (ep.port==0)
        ep.port = getDaliServixPort();
    eps.append(ep);
}


static void appendClusterEndpoints(IPropertyTree& cluster,SocketEndpointArray &eps)
{
    SocketEndpoint ep;
    Owned<IPropertyTree> master = cluster.getPropTree("ThorMasterProcess");
    if (master)
        appendNodeEndpoint(*master,eps);
    Owned<IPropertyTreeIterator> nodes= cluster.getElements("ThorSlaveProcess");
    if (nodes&&nodes->first()) {
        do {
            appendNodeEndpoint(nodes->query(),eps);
        } while (nodes->next());
    }
}


class CIpItem: public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    IpAddress ip;
    bool ok;
};


class CIpTable: public SuperHashTableOf<CIpItem,IpAddress>
{


public:
    ~CIpTable()
    {
        _releaseAll();
    }

    void onAdd(void *)
    {
        // not used
    }

    void onRemove(void *e)
    {
        CIpItem &elem=*(CIpItem *)e;        
        elem.Release();
    }

    unsigned getHashFromElement(const void *e) const
    {
        const CIpItem &elem=*(const CIpItem *)e;        
        return elem.ip.iphash();
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const IpAddress *)fp)->iphash();
    }

    const void * getFindParam(const void *p) const
    {
        const CIpItem &elem=*(const CIpItem *)p;        
        return &elem.ip;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned fphash) const
    {
        return ((CIpItem *)et)->ip.ipequals(*(IpAddress *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CIpItem,IpAddress);

    bool verifyDaliFileServer(IpAddress &ip)
    {
        CIpItem *item=find(ip);
        if (!item) {
            item = new CIpItem;
            item->ip.ipset(ip);
            item->ok = testDaliServixPresent(ip);
            add(*item);
        }
        return item->ok;
    }

};


class CFileCrcItem: public CInterface
{
public:
    RemoteFilename filename;
    unsigned requiredcrc;
    unsigned crc;
    unsigned partno;
    unsigned copy;
    IMPLEMENT_IINTERFACE;
    IpAddress ip;
    bool ok;
};

class CFileCrcList
{
    CIpTable dafilesrvips;
    Owned<IUserDescriptor> udesc;
public:
    bool &stopped;
    CIArrayOf<CFileCrcItem> list;

    CFileCrcList(bool &_stopped)
        : stopped(_stopped)
    {
        StringBuffer userName;
        serverConfig->getProp("@sashaUser", userName);
        udesc.setown(createUserDescriptor());
        udesc->set(userName.str(), nullptr);
    }

    void add(RemoteFilename &filename,unsigned partno,unsigned copy,unsigned crc)
    {
        CFileCrcItem *item = new CFileCrcItem();
        item->filename.set(filename);
        item->partno = partno;
        item->copy = copy;
        item->crc = crc;
        item->requiredcrc = crc;
        list.append(*item);
    }

    void verifyFile(const char *name,CDateTime *cutoff)
    {
        Owned<IDistributedFile> file=queryDistributedFileDirectory().lookup(name,udesc,false,false,false,nullptr,defaultPrivilegedUser);
        if (!file)
            return;
        IPropertyTree &fileprops = file->queryAttributes();
        bool blocked = false;
        if (file->isCompressed(&blocked)&&!blocked)
            return;
        if (stopped)
            return;
        StringBuffer dtstr;
        if (fileprops.getProp("@verified",dtstr)) {
            if (!cutoff)
                return;
            CDateTime dt;
            dt.setString(dtstr.str());
            if (dt.compare(*cutoff)<=0)
                return;
        }
        list.kill();
        unsigned width = file->numParts();
        unsigned short port = getDaliServixPort();
        try {
            Owned<IDistributedFilePart> testpart = file->getPart(0);
            SocketEndpoint ep(testpart->queryNode()->endpoint()); 
            if (!dafilesrvips.verifyDaliFileServer(ep)) {
                StringBuffer ips;
                ep.getIpText(ips);
                PROGLOG("VERIFY: file %s, cannot run DAFILESRV on %s",name,ips.str());
                return;
            }
        }
        catch (IException *e)
        {
            StringBuffer s;
            s.appendf("VERIFY: file %s",name);
            EXCLOG(e, s.str());
            e->Release();
            return;
        }
        for (unsigned idx=0;idx<width;idx++) {
            Owned<IDistributedFilePart> part = file->getPart(idx);
            for (unsigned copy = 0; copy < part->numCopies(); copy++) {
                if (stopped)
                    return;
                unsigned reqcrc;
                if (!part->getCrc(reqcrc))
                    continue;
                SocketEndpoint ep(part->queryNode()->endpoint());
                if (!dafilesrvips.verifyDaliFileServer(ep)) {
                    StringBuffer ips;
                    ep.getIpText(ips);
                    PROGLOG("VERIFY: file %s, cannot run DAFILESRV on %s",name,ips.str());
                    continue;
                }
                RemoteFilename rfn;
                part->getFilename(rfn,copy);
                rfn.setPort(port); 
                add(rfn,idx,copy,reqcrc);
            }
        }
        if (list.ordinality()==0)
            return;
        PROGLOG("VERIFY: file %s started",name);
        file.clear();
        CriticalSection crit;
        class casyncfor: public CAsyncFor
        {
            CFileCrcList *parent;
            CriticalSection &crit;
        public:
            bool ok;
            casyncfor(CFileCrcList *_parent, CriticalSection &_crit)
                : crit(_crit)
            {
                parent = _parent;
                ok = true;
            }
            void Do(unsigned i)
            {
                CriticalBlock block(crit);
                if (parent->stopped)
                    return;
                CFileCrcItem &item = parent->list.item(i);
                RemoteFilename &rfn = item.filename;
                Owned<IFile> partfile;
                StringBuffer eps;
                try
                {
                    partfile.setown(createIFile(rfn));
                    // PROGLOG("VERIFY: part %s on %s",partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                    if (partfile) {
                        if (parent->stopped)
                            return;
                        CriticalUnblock unblock(crit);
                        item.crc = partfile->getCRC();
                    }
                    else
                        ok = false;

                }
                catch (IException *e)
                {
                    StringBuffer s;
                    s.appendf("VERIFY: part %s on %s",partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                    EXCLOG(e, s.str());
                    e->Release();
                    ok = false;
                }
            }
        } afor(this,crit);
        afor.For(list.ordinality(),50,false,true);
        ForEachItemIn(j,list) {
            CFileCrcItem &item = list.item(j);
            if (item.crc!=item.requiredcrc) {
                if (stopped)
                    return;
                StringBuffer rfs;
                PROGLOG("VERIFY: FAILED %s (%x,%x) file %s",name,item.crc,item.requiredcrc,item.filename.getRemotePath(rfs).str());
                afor.ok = false;
            }
        }
        if (!stopped) {
            file.setown(queryDistributedFileDirectory().lookup(name,udesc,false,false,false,nullptr,defaultPrivilegedUser));
            if (!file)
                return;
            if (afor.ok) {
                CDateTime dt;
                dt.setNow();
                DistributedFilePropertyLock lock(file);
                StringBuffer str;
                lock.queryAttributes().setProp("@verified",dt.getString(str).str());
            }
            PROGLOG("VERIFY: file %s %s",name,afor.ok?"OK":"FAILED");
        }
    }

};


class CSashaVerifierServer: public ISashaServer, public Thread
{  

    bool stopped;
    Semaphore stopsem;
    Owned<IUserDescriptor> udesc;
public:
    IMPLEMENT_IINTERFACE;

    CSashaVerifierServer()
        : Thread("CSashaVerifierServer")
    {
        stopped = false;

        StringBuffer userName;
        serverConfig->getProp("@sashaUser", userName);
        udesc.setown(createUserDescriptor());
        udesc->set(userName.str(), nullptr);
    }

    ~CSashaVerifierServer()
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
            IERRLOG("CSashaVerifierServer aborted");
    }



    int run()
    {
        Owned<IPropertyTree> verifprops = serverConfig->getPropTree("Verifier");
        if (!verifprops)
            verifprops.setown(createPTree("Verifier"));
        unsigned interval = verifprops->getPropInt("@interval",DEFAULT_VERIFY_INTERVAL);
        if (!interval)
            stopped = true;
        while (!stopped) {
            try {
                PROGLOG("VERIFIER: Started");
                CFileCrcList filelist(stopped);
                Owned<IDFAttributesIterator> iter = queryDistributedFileDirectory().getDFAttributesIterator("*",udesc,true,false);
                if (iter) {
                    CDateTime mincutoff;
                    mincutoff.setNow();
                    mincutoff.adjustTime(-60*24*verifprops->getPropInt("@mincutoff",DEFAULT_VERIFY_MIN_CUTOFF));
                    if (iter->first()) {
                        do {
                            IPropertyTree &attr=iter->query();
                            CDateTime moddt;
                            moddt.setString(attr.queryProp("@modified"));
                            if (moddt.compare(mincutoff)>=0)
                                continue;
                            filelist.verifyFile(attr.queryProp("@name"),NULL);
                        } while (!stopped&&iter->next());
                        if (!stopped&&iter->first()) {
                            CDateTime cutoff;
                            cutoff.setNow();
                            cutoff.adjustTime(-60*24*verifprops->getPropInt("@cutoff",DEFAULT_VERIFY_CUTOFF));
                            do {
                                IPropertyTree &attr=iter->query();
                                CDateTime moddt;
                                moddt.setString(attr.queryProp("@modified"));
                                if (moddt.compare(mincutoff)>=0)
                                    continue;
                                filelist.verifyFile(attr.queryProp("@name"),&cutoff);
                            } while (!stopped&&iter->next());
                        }
                    }
                }
                PROGLOG("VERIFIER: Done");
            }
            catch (IException *e)
            {
                StringBuffer s;
                EXCLOG(e, "VERIFIER");
                e->Release();
            }
            
            stopsem.wait(60*60*1000*interval);
        }
        return 0;
    }


} *sashaVerifierServer = NULL;

class CSashaDaFSMonitorServer: public ISashaServer, public Thread
{  

    bool stopped;
    Semaphore stopsem;
public:
    IMPLEMENT_IINTERFACE;

    CSashaDaFSMonitorServer()
        : Thread("CSashaDaFSMonitorServer")
    {
        stopped = false;
    }

    ~CSashaDaFSMonitorServer()
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
            IERRLOG("CSashaDaFSMonitorServer aborted");
    }

    void checkCluster(SocketEndpointArray &eps,const char *clustername)
    {
        if (eps.ordinality()==0)
            return;
        IPointerArrayOf<ISocket> sockets;
        multiConnect(eps,sockets,60*1000);
        CriticalSection sect;
        unsigned failurelimit = 10;             // only report 10 from each cluster
        class casyncfor: public CAsyncFor
        {
            SocketEndpointArray &eps;
            IPointerArrayOf<ISocket> &sockets;
            CriticalSection &sect;
            unsigned &failurelimit;
            const char *clustername;
        public:
            casyncfor(SocketEndpointArray &_eps,IPointerArrayOf<ISocket> &_sockets, CriticalSection &_sect, unsigned &_failurelimit,const char *_clustername) 
                : eps(_eps), sockets(_sockets), sect(_sect), failurelimit(_failurelimit)
            { 
                clustername = _clustername;
            }
            void Do(unsigned i)
            {
                {
                    CriticalBlock block(sect);
                    if (failurelimit==0)
                        return;
                }
                ISocket *sock = sockets.item(i);
                try {
                    StringBuffer verstr;
                    unsigned rver = sock?getRemoteVersion(sock, verstr):0;
                    if (rver==0) {
                        StringBuffer epstr;
                        SocketEndpoint ep = eps.item(i);
                        ep.getUrlStr(epstr);
                        CriticalBlock block(sect);
                        if (failurelimit) {
                            LOG(MCoperatorError, unknownJob,"DAFSMON: dafilesrv on %s cannot be contacted",epstr.str());
                            if (--failurelimit==0) 
                                LOG(MCoperatorError, unknownJob,"DAFSMON: monitoring suspended for cluster %s (too many failures)",clustername);
                        }
                    }
                }
                catch (IException *e)
                {
                    CriticalBlock block(sect);
                    StringBuffer epstr;
                    SocketEndpoint ep = eps.item(i);
                    ep.getUrlStr(epstr);
                    StringBuffer s;
                    s.appendf("DAFSMON: dafilesrv %s",epstr.str());
                    LOG(MCoperatorError, unknownJob, e, s.str());
                    e->Release();
                    return;
                }
            }
        } afor(eps,sockets,sect,failurelimit,clustername);
        afor.For(eps.ordinality(), 10, false, true);
    }

    void checkClusters(const char *clustlist)
    {
        if (!clustlist)
            return;
        loadMachineMap();
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn) {
            OERRLOG("Could not connect to /Environment/Software");
            return;
        }
        StringArray list;
        getFileGroups(clustlist, list);
        StringArray groups;
        bool *done = (bool *)calloc(list.ordinality(),sizeof(bool));
        clustersToGroups(conn->queryRoot(),list,groups,done);
        ForEachItemIn(j,groups) {
            const char *gname = groups.item(j);
            PROGLOG("DAFSMON: Scanning %s",gname);
            SocketEndpointArray eps;
            getCluster(gname,eps);
            checkCluster(eps,gname);
            PROGLOG("DAFSMON: Done scanning %s",gname);
        }
        // now single IPs
        StringBuffer trc;
        SocketEndpointArray eps;
        ForEachItemIn(i,list) {
            const char *s = list.item(i);
            if (!done[i]) {
                SocketEndpoint ep(s);
                if (!ep.isNull()) {
                    if (trc.length())
                        trc.append(", ");
                    ep.getUrlStr(trc);
                    if (ep.port==0)
                        ep.port = getDaliServixPort();
                    eps.append(ep);
                }
                else 
                    OWARNLOG("DAFSMON: Cannot resolve %s in cluster list",s);
            }
        }
        if (eps.ordinality()) {
            PROGLOG("DAFSMON: Scanning IPs %s",trc.str());
            checkCluster(eps,"IP");
            PROGLOG("DAFSMON: Done scanning IPs");
        }
        free(done);
    
    }


    int run()
    {
        Owned<IPropertyTree> monprops = serverConfig->getPropTree("DaFileSrvMonitor");
        if (!monprops)
            monprops.setown(createPTree("DaFSMonitorServer"));
        unsigned interval = monprops->getPropInt("@interval",DEFAULT_DAFSMONITOR_INTERVAL);
        if (!interval)
            stopped = true;
        StringBuffer s;
        PROGLOG("DAFSMON: interval = %d hr", interval);
        CSashaSchedule schedule;
        schedule.init(monprops,DEFAULT_DAFSMONITOR_INTERVAL);
        while (!stopped) {
            stopsem.wait(1000*60);
            if (!stopped && schedule.ready()) {
                try {
                    PROGLOG("DAFSMON: Started");
                    checkClusters(monprops->queryProp("@clusterlist"));
                    PROGLOG("DAFSMON: Done");
                }
                catch (IException *e)
                {
                    StringBuffer s;
                    EXCLOG(e, "DaFSMonitorServer");
                    e->Release();
                }
            }
        }
        return 0;
    }


} *sashaDaFSMonitorServer = NULL;

ISashaServer *createSashaVerifierServer()
{
    assertex(!sashaVerifierServer); // initialization problem
    sashaVerifierServer = new CSashaVerifierServer();
    return sashaVerifierServer;
}

ISashaServer *createSashaDaFSMonitorServer()
{
    assertex(!sashaDaFSMonitorServer); // initialization problem
    sashaDaFSMonitorServer = new CSashaDaFSMonitorServer();
    return sashaDaFSMonitorServer;
}


