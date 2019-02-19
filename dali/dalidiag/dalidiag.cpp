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

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"

#include "daclient.hpp"
#include "dadiags.hpp"
#include "danqs.hpp"
#include "dasds.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "jptree.hpp"
#include "jlzw.hpp"

static const char *cmds[] = { "locks", "sdsstats", "sdssubscribers", "connections", "threads", "mpqueue", "clients", "mpverify", "timeq", "cleanq",  "timesds", "build", "sdsfetch", "dirparts", "sdssize", "nodeinfo", "slavenode", "backuplist", "save", NULL };

void usage(const char *exe)
{
    printf("USAGE: dalidiag <dali-ip> -command\n");
    printf("Commands:\n");
    printf("-locks              -- list active SDS locks\n");
    printf("-sdsstats           -- SDS statistics\n");
    printf("-sdssubscribers     -- list active SDS subscribers\n");
    printf("-connections        -- list SDS connections\n");
    printf("-threads            -- running threads\n");
    printf("-mpqueue            -- list waiting MP queue items\n");
    printf("-clients            -- list connected Dali clients\n");
    printf("-mpverify           -- test MP connections and remove stale\n");
    printf("                       (NB should not do on busy system!)\n");
    printf("-timeq              -- time Dali named queue speed\n");
    printf("-cleanq             -- remove empty queue names\n");
    printf("-timesds            -- time SDS subscriptions\n");
    printf("-build              -- list current build info\n");
    printf("-sdsfetch <xpath>   -- get a SDS branch\n");
    printf("                       (no externals, better to use daliadmin export)\n");
    printf("-sdssize            -- calculate size of SDS branch\n");
    printf("                       (NB don't do on large branch - may run out of memory!)\n");
    printf("-nodeinfo <ip>      -- information about the given node (i.e cluster and part)\n");
    printf("-slavenode <cluster> <partno>   -- lists IP of given part (0 for master)\n");
    printf("-backuplist <cluster>           -- list of nodes and backup nodes in cluster\n");
    printf("-partlist <filename> [0|1]      -- lists the part info for a file\n"); 
    printf("                                   the optional 0 or 1 is the copy\n");
    printf("-perf               -- performance info\n");
    printf("-disconnect <ip>:<port> -- forcably disconnect a clients connection\n");
    printf("-permissions <logicalname> <user> <password> -- get file permissions\n");
    printf("-unlock <connection_id> [close] -- forcibly disconnect an sds lock\n"); 
    printf("                                   (use id's given by '-locks'\n");
    printf("-settracetransactions    -- trace dali transactions\n");
    printf("-settraceslowtransactions <millisecond-threshold> -- trace slow dali transactions\n");
    printf("-cleartracetransactions  -- stop tracing dali transactions\n");
    printf("-setldapflags <val>      -- set LDAP flags\n");
    printf("-getldapflags            -- get LDAP flags\n");

}


class CTestSDSSubscription : public CInterface, implements ISDSSubscription
{
public:
    Semaphore notifysem;
    IMPLEMENT_IINTERFACE;
    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        notifysem.signal();
    }
};

#define MAXHISTORY 1000
#define MININC (0.001)

void timeQorSDS(bool timeq)
{
    double *res = (double *)calloc(MAXHISTORY,sizeof(double));
    unsigned in=0;
    unsigned sz=0;
    double max=0;
    double av=0;
    char inc=' ';
    double last100av=0;
    double last100max=0;
    char last100inc=' ';
    double last10av=0;
    double last10max=0;
    char last10inc=' ';
    unsigned i=0;

    SocketEndpoint ep;
    ep.setLocalHost(0);
    MemoryBuffer mb;
    Owned<INamedQueueConnection> qconn;
    Owned<IRemoteConnection> sdsconn;
    IPropertyTree *sdsroot = NULL;
    Owned<IQueueChannel> channel;
    CTestSDSSubscription testsdssub;
    SubscriptionId sdssubid=0;
    StringBuffer subname;

    if (timeq) {
        StringBuffer qname("TESTINGQ_");
        ep.getUrlStr(qname);
        qconn.setown(createNamedQueueConnection(0));
        channel.setown(qconn->open(qname.str()));
        while (channel->probe()) {
            mb.clear();
            channel->get(mb);
        }
        mb.clear().append("Hello").append(i);       // prime queue to always keep one item on it
        ep.serialize(mb);
        channel->put(mb);
    }
    else {
        subname.append("TESTSDS_");
        ep.getUrlStr(subname);
        StringBuffer fullname("TESTING/");
        fullname.append(subname);
        sdssubid = querySDS().subscribe(fullname.str(), testsdssub);
        sdsconn.setown(querySDS().connect("/TESTING", myProcessSession(), RTM_CREATE_QUERY, 1000000));
        sdsroot = sdsconn->queryRoot();
    }
    HiresTimer hrt;
    unsigned last = msTick();
    PrintLog("last 10   last 100 last 1000 change");
    for (;;) {
        i++;
        hrt.reset();
        if (timeq) {
            mb.clear().append("Hello").append(i);
            ep.serialize(mb);
            channel->put(mb);
            channel->get(mb);
        }
        else {
            sdsroot->setPropInt(subname.str(),i++);
            sdsconn->commit();
            if (!testsdssub.notifysem.wait(1000*60))
                PrintLog("Notify Timeout!");
        }
        res[in] = hrt.get();
        if (sz<MAXHISTORY)
            sz++;
        in++;
        if (in>=MAXHISTORY) 
            in = 0;
        Sleep(500);                 // stop getting too busy
        if (msTick()-last>5000) {
            unsigned p=in;
            // first total ac
            unsigned j=0;
            double m=0;
            double a=0;
            for (j=0;j<sz;j++) {
                if (p==0)
                    p = MAXHISTORY;
                p--;
                a+=res[p];
                if (res[p]>m)
                    m = res[p];
            }   
            a /= sz;
            if (av>0.0) {
                if (a>av+MININC) inc='+'; else if (a<av-MININC) inc='-'; else inc=' ';
            }
            av = a;
            max = m;
            unsigned s=(sz<10)?sz:10;
            p = in;

            j=0;
            m=0;
            a=0;
            for (j=0;j<s;j++) {
                if (p==0)
                    p = MAXHISTORY;
                p--;
                a+=res[p];
                if (res[p]>m)
                    m = res[p];
            }   
            a /= s;
            if (last10av>0.0) {
                if (a>last10av+MININC) last10inc='+'; else if (a<last10av-MININC) last10inc='-'; else last10inc=' ';
            }
            last10av = a;
            last10max = m;
            s=(sz<100)?sz:100;
            p = in;
            j=0;
            m=0;
            a=0;
            for (j=0;j<s;j++) {
                if (p==0)
                    p = MAXHISTORY;
                p--;
                a+=res[p];
                if (res[p]>m)
                    m = res[p];
            }   
            a /= s;
            if (last100av>0.0) {
                if (a>last100av+MININC) last100inc='+'; else if (a<last100av-MININC) last100inc='-'; else last100inc=' ';
            }
            last100av = a;
            last100max = m;
            PrintLog("%.6f, %.6f, %.6f, (%c%c%c)",
                      last10av,last100av,av,last10inc,last100inc,inc);
            last = msTick();
        }
    }
    free(res);
    if (sdssubid!=0)
       querySDS().unsubscribe(sdssubid);

}
    
void cleanq()
{
    StringBuffer path("/Queues");
    Owned<IRemoteConnection> conn = querySDS().connect(path.str(),myProcessSession(),RTM_LOCK_WRITE, INFINITE);
    if (!conn) {
        PrintLog("Could not connect to %s",path.str());
        return;
    }
    Owned<IPropertyTree> root = conn->getRoot();
    Owned<IPropertyTreeIterator> elems=root->getElements("Queue");
    ICopyArrayOf<IPropertyTree> toremove;
    ForEach(*elems.get()) {
        IPropertyTree& elem = elems->query();
        if (!elem.hasProp("Item[1]")) { // empty, lets delete
            toremove.append(elem);
        }
    }
    ForEachItemIn(i,toremove) {
        IPropertyTree &item=toremove.item(i);
        root->removeTree(&item);
    }
    conn->commit();
    elems.clear();
    root.clear();
}

void dirParts(const char *ip,const char *dir)
{
    PROGLOG("No longer supported");
/*
    SocketEndpoint ep(ip);
    Owned<IDirectoryPartIterator> iter = queryDistributedFileDirectory().getDirectoryPartIterator(dir, ep);
    ForEach(*iter) {
        StringBuffer tmp(iter->queryFileName());
        tmp.append('[').append(iter->partNum());
        if (iter->isReplicate())
            tmp.append('R');
        tmp.append(']');
        printf("%-30s %-48s\n",iter->queryPartName(),tmp.str());
    }
*/
}

void partInfo(const char *name,unsigned copy)
{
    Owned<IDistributedFile> f = queryDistributedFileDirectory().lookup(name,UNKNOWN_USER);
    if (f) {
        Owned<IDistributedFilePartIterator> parts = f->getIterator();
        unsigned partno = 0;
        ForEach(*parts) {
            partno++;
            IDistributedFilePart &part = parts->query();
            RemoteFilename fn;
            part.getFilename(fn,copy);
            StringBuffer buf;
            SocketEndpoint ep = fn.queryEndpoint();
            printf("%3d %10" I64F "d %5s\n",partno,part.queryAttributes().getPropInt64("@size", -1),fn.getRemotePath(buf).str());
        }
    }
    else
        OERRLOG("ERROR: %s not found", name);
}




void nodeInfo(const char *ip)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), 0, INFINITE);
    IPropertyTree* root = conn->queryRoot();
    StringBuffer query("Hardware/Computer[@netAddress=\"");
    query.append(ip).append("\"]");
    Owned<IPropertyTree> machine = root->getPropTree(query.str());
    if (machine) {
        printf("Node:       %s\n",ip);
        const char *name=machine->queryProp("@name");
        printf("Name:       %s\n",name);
        printf("State:      %s\n",machine->queryProp("@state"));
        Owned<IPropertyTreeIterator> clusters= root->getElements("Software/ThorCluster");
        ForEach(*clusters) {
            query.clear().append("ThorSlaveProcess[@computer=\"").append(name).append("\"]");
            IPropertyTree &cluster = clusters->query();
            Owned<IPropertyTree> slave = cluster.getPropTree(query.str());
            if (slave) {
                printf("Cluster:    %s\n",cluster.queryProp("@name"));
                printf("Id:         %s\n",slave->queryProp("@name"));
            }
        }
    }
    else
        printf("ERROR: cannot find '%s' in Dali Environment\n",ip);
}

void slaveNode(const char *thor,unsigned n)
{
    StringBuffer tag;
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), 0, INFINITE);
    IPropertyTree* root = conn->queryRoot();
    StringBuffer query("Software/ThorCluster[@name=\"");
    query.append(thor).append("\"]");
    Owned<IPropertyTree> cluster = root->getPropTree(query.str());
    if (n==0) {
        tag.append("m1");
        printf("%s Master ",thor);
    }
    else {
        tag.append('s').append(n);
        printf("%s Slave %d ",thor,n);
    }
    if (cluster) {
        query.clear().append("ThorSlaveProcess[@name=\"").append(tag.str()).append("\"]");
        Owned<IPropertyTree> process = cluster->getPropTree(query.str());
        if (process) {
            const char *cname = process->queryProp("@computer");
            query.clear().append("Hardware/Computer[@name=\"").append(cname).append("\"]");
            Owned<IPropertyTree> machine = root->getPropTree(query.str());
            if (machine) {
                printf("on %s\n",machine->queryProp("@netAddress"));
            }
            else
                printf("%s  IP not found in Environment(Error)\n",cname);
        }
        else
            printf("'%s' not found in %s Environment\n",tag.str(),thor);
    }
    else
        printf("cluster name not found in Environment\n");
}

void backupList(const char *cluster)
{
    Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
    if (group) {
        unsigned n = group->ordinality();
        rank_t r;
        StringBuffer str;
        for (r=0;r<n;r++) {
            group->queryNode(r).endpoint().getUrlStr(str.clear());
            str.append(' ');
            group->queryNode((r+1)%n).endpoint().getUrlStr(str);
            printf("%s\n",str.str());
        }
    }
    else 
        OERRLOG("Cluster %s not found", cluster);
}

void filePermissions(const char *lname,const char *username,const char *password)
{
    Owned<IUserDescriptor> user = createUserDescriptor();
    user->set(username,password);
    SecAccessFlags perm=queryDistributedFileDirectory().getFilePermissions(lname,user);
    printf("Permissions for %s = %d\n",lname,perm);
}

void nqPingPong(const char *q,const char *q2)   
{
    if (q2) {
        Owned<INamedQueueConnection> qconn = createNamedQueueConnection(0);
        Owned<IQueueChannel> channel1=qconn->open(q);
        Owned<IQueueChannel> channel2=qconn->open(q2);
        MemoryBuffer mb;
        while (channel2->probe())
            channel2->get(mb.clear());
        for (;;) {
            Sleep(getRandom()%500);
            PROGLOG("queue put to %s",q);
            mb.clear().append(q2);
            channel1->put(mb);
            Sleep(getRandom()%500);
            PROGLOG("queue got from %s",q2);
            for (;;) {
                channel2->get(mb.clear(),60*1000);
                if (mb.length())
                    break;
                PROGLOG("queue cycle");
            }
        }
    }
    class cThread: public Thread
    {
    public:
        const char *q;
        int num;
        int run() {
            MemoryBuffer mb;
            Owned<INamedQueueConnection> qconn = createNamedQueueConnection(0);
            Owned<IQueueChannel> channel1=qconn->open(q);
            while (channel1->probe())
                channel1->get(mb.clear());
            for (;;) {
                Sleep(getRandom()%1000);
                channel1->get(mb.clear(),60*1000);
                StringAttr replyq;
                if (mb.length()!=0) {
                    mb.read(replyq);
                    Owned<IQueueChannel> channel2=qconn->open(replyq);
                    PROGLOG("queue %d got",num);
                    Sleep(getRandom()%1000);
                    mb.clear().append("Hello");
                    PROGLOG("queue %d put to %s",num,replyq.get());
                    channel2->put(mb);
                }
                else
                    PROGLOG("queue %d cycle",num);
            }
            return 0;
        }
    } threads[2];
    threads[0].q = q;
    threads[0].num = 1;
    threads[1].q = q;
    threads[1].num = 2;
    threads[0].start();
    threads[1].start();
    threads[0].join();
    threads[1].join();
}

static bool begins(const char *&ln,const char *pat)
{
    size32_t sz = strlen(pat);
    if (memicmp(ln,pat,sz)==0) {
        ln += sz;
        return true;
    }
    return false;
}

// NB: there's strtoll under Linux
static unsigned __int64 hextoll(const char *str, bool *error=NULL)
{
    unsigned len = strlen(str);
    if (!len) return 0;

    unsigned __int64 factor = 1;
    unsigned __int64 rolling = 0;
    char *ptr = (char *)str+len-1;
    for (;;)
    {
        char c = *ptr;
        unsigned v;
        if (isdigit(c))
            v = c-'0';
        else if (c>='A' && c<='F')
            v = 10+(c-'A');
        else if (c>='a' && c<='f')
            v = 10+(c-'a');
        else
        {
            if (error)
                *error = true;
            return 0;
        }
        rolling += v * factor;
        factor <<= 4;
        if (ptr == str)
            break;
        --ptr;
    }
    if (error)
        *error = false;
    return rolling;
}

int main(int _argc, char* argv[])
{
    unsigned argc = _argc;
    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    SocketEndpoint ep;
    SocketEndpointArray epa;
    unsigned i;
    for (i=0;i<argc-1;i++) {
        if (argv[i+1][0]!='-') {
            ep.set(argv[i+1],DALI_SERVER_PORT);
            epa.append(ep);
        }
        else
            break;
    }

    if (!epa.ordinality())
    {
        usage(argv[0]);
        return 0;
    }
    Owned<IGroup> group = createIGroup(epa); 
    assertex(group);
    //CSystemCapability capability(DCR_Diagnostic, "DALIDIAG");
    //capability.secure((byte *)CLIENT_ENCRYPT_KEY, strlen(CLIENT_ENCRYPT_KEY));
    assertex(group);
    initClientProcess(group, DCR_Diagnostic, 0, NULL, NULL, MP_WAIT_FOREVER);

    if (argc<2)
    {
        usage(argv[0]);
        return 0;
    }


    try {
        i++;
        for (; i <argc; i++)
        {
            const char *arg = argv[i];
            if ('-' == *arg)
            {
                ++arg;
                if (stricmp(arg,"timeq")==0) {
                    timeQorSDS(true);
                    break;
                }
                if (stricmp(arg,"timesds")==0) {
                    timeQorSDS(false);
                    break;
                }
                if (stricmp(arg,"cleanq")==0) {
                    cleanq();
                    break;
                }
                StringBuffer buf;
                if ((i+1<argc)&&(stricmp(arg,"sdsfetch")==0)) {
                    MemoryBuffer mb;
                    mb.append("sdsfetch").append(argv[++i]);
                    getDaliDiagnosticValue(mb);
                    Owned<IPropertyTree> pt = createPTree(mb);
                    if (pt) 
                        toXML(pt,buf,2);
                    printf("%s",buf.str());
                    break;
                }
                if ((i+1<argc)&&(stricmp(arg,"sdssize")==0)) {
                    MemoryBuffer mb;
                    mb.append("sdssize").append(argv[++i]);
                    getDaliDiagnosticValue(mb);
                    size32_t ret;
                    mb.read(ret);
                    printf("Size %s = %d\n",argv[i],ret);
                    break;
                }
                else if ((i+2<argc)&&(stricmp(arg,"dirparts")==0)) {
                    dirParts(argv[i+1],argv[i+2]);
                    break;
                }
                else if ((i+1<argc)&&(stricmp(arg,"nodeinfo")==0)) {
                    nodeInfo(argv[i+1]);
                    break;
                }
                else if ((i+2<argc)&&(stricmp(arg,"slavenode")==0)) {
                    slaveNode(argv[i+1],atoi(argv[i+2]));
                    break;
                }
                else if ((i+1<argc)&&(stricmp(arg,"backuplist")==0)) {
                    backupList(argv[i+1]);
                    break;
                }
                else if ((i+1<argc)&&(stricmp(arg,"partlist")==0)) {
                    partInfo(argv[i+1],(i+2<argc)?atoi(argv[i+2]):0);
                    break;
                }
                if ((i+1<argc)&&(stricmp(arg,"disconnect")==0)) {
                    MemoryBuffer mb;
                    mb.append("disconnect").append(argv[++i]);
                    getDaliDiagnosticValue(mb);
                    break;
                }
                if ((i+3<argc)&&(stricmp(arg,"permissions")==0)) {
                    filePermissions(argv[i+1],argv[i+2],argv[i+3]);
                    break;
                }
                if ((i+1<argc)&&(stricmp(arg,"nqpingpong")==0)) {
                    nqPingPong(argv[i+1],i+2<argc?argv[i+2]:NULL);
                    break;
                }
                if ((i+1<argc)&&(stricmp(arg,"unlock")==0)) {
                    MemoryBuffer mb;
                    __int64 connectionId;
                    connectionId = hextoll(argv[i+1]);
                    bool disconnect = (i+2<argc && 0==stricmp("close", argv[i+2]));
                    mb.append("unlock").append(connectionId).append(disconnect);
                    getDaliDiagnosticValue(mb);
                    bool success;
                    mb.read(success);
                    StringBuffer connectionInfo;
                    if (!success)
                        PROGLOG("Lock not found");
                    else
                    {
                        mb.read(connectionInfo);
                        PROGLOG("Lock successfully removed: %s", connectionInfo.str());
                    }
                    break;
                }
                if ((i+1<argc)&&(stricmp(arg,"setldapflags")==0)) {
                    MemoryBuffer mb;
                    mb.append("setldapflags");
                    unsigned f = (unsigned)atoi(argv[++i]);
                    mb.append(f);
                    getDaliDiagnosticValue(mb);
                    printf("Dali LDAP flags set to %d\n",f);
                    break;
                }
                if ((stricmp(arg,"getldapflags")==0)) {
                    MemoryBuffer mb;
                    mb.append("getldapflags");
                    getDaliDiagnosticValue(mb);
                    unsigned f;
                    mb.read(f);
                    printf("Dali LDAP flags = %d\n",f);
                    break;
                }
                if ((stricmp(arg,"setsdsdebug")==0)) {
                    MemoryBuffer mb;
                    mb.append("setsdsdebug");
                    unsigned n=argc-(i+1);
                    mb.append(n);
                    bool success = false;
                    StringAttr reply;
                    if (n)
                    {
                        while (n--)
                            mb.append(argv[++i]);
                        getDaliDiagnosticValue(mb);
                        mb.read(success);
                        mb.read(reply);
                    }
                    StringBuffer s("Dali setsdsdebug() call ");
                    s.append(success?"successful":"failed");
                    if (reply.length())
                        s.append(" - ").append(reply);
                    s.newline();
                    printf("%s", s.str());
                    break;
                }
                if (0 == stricmp(arg,"save")) {
                    PROGLOG("Requesting SDS save");
                    MemoryBuffer mb;
                    mb.append("save");
                    getDaliDiagnosticValue(mb);
                    PROGLOG("SDS store saved");
                    break;
                }
                if (0 == stricmp(arg, "locks")) {
                    Owned<ILockInfoCollection> lockInfoCollection = querySDS().getLocks();
                    lockInfoCollection->toString(buf);
                    printf("\n%s:\n%s",arg,buf.str());
                    break;
                }
                if (0 == stricmp(arg,"sdsstats")) {
                    querySDS().getUsageStats(buf);
                    printf("\n%s:\n%s",arg,buf.str());
                    break;
                }
                if (0 == stricmp(arg, "connections")) {
                    querySDS().getConnections(buf);
                    printf("\n%s:\n%s",arg,buf.str());
                    break;
                }
                if (0 == stricmp(arg, "sdssubscribers")) {
                    querySDS().getSubscribers(buf);
                    printf("\n%s:\n%s",arg,buf.str());
                    break;
                }
                if ((stricmp(arg,"settraceslowtransactions")==0)) {
                    MemoryBuffer mb;
                    mb.append("settraceslowtransactions");
                    unsigned slowThresholdMs = atoi(argv[++i]);
                    mb.append(slowThresholdMs);
                    getDaliDiagnosticValue(mb);
                    StringAttr response;
                    mb.read(response);
                    printf("\nsettraceslowtransactions:\n%s", response.get());
                    break;
                }
                else {
                    for (;;) {
                        getDaliDiagnosticValue(arg,buf.clear());
                        if (stricmp(arg,"build")==0) {
                            if (strcmp(buf.str(),"$I""d$")==0)
                                buf.clear().append("Development");
                            else if (strcmp(buf.str(),"UNKNOWN OPTION: build")==0)
                                buf.clear().append("<=190a");
                        }
                        printf("\n%s:\n%s",arg,buf.str());
                        if ((i+1<argc)&&(stricmp(arg,"perf")==0)) 
                            Sleep(1000*atoi(argv[i+1]));
                        else
                            break;
                    }
                }
            }
            else
            {
                usage(argv[0]);
                break;
            }
        }

    }
    catch (IException *e) {
        pexception("Exception",e);
        e->Release();
    }

    closedownClientProcess();

    releaseAtoms();
    return 0;
}

