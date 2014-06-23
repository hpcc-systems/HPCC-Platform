/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jio.hpp"
#include "jmisc.hpp"
#include "jsuperhash.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "rmtfile.hpp"
#include "sockfile.hpp"

#include "dalienv.hpp"

//#define TRACE

#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite

void usage()
{
    printf("DAFSCONTROL usage:\n");
    printf("  dafscontrol [<dali-ip>] STOP  <ip-or-cluster>\n");
    printf("  dafscontrol [<dali-ip>] CHECK <ip-or-cluster>\n");
    printf("  dafscontrol [<dali-ip>] VER <ip-or-cluster>\n");
    printf("  dafscontrol [<dali-ip>] CHECKVER <ip-or-cluster>\n");
    printf("  dafscontrol [<dali-ip>] STOPVER  <ip-or-cluster>\n");
    printf("  dafscontrol [<dali-ip>] CHECKVERMAJOR <ip-or-cluster>\n");
    printf("  dafscontrol [<dali-ip>] TRACE <ip> <num>\n");
    printf("  dafscontrol [<dali-ip>] CHKDSK <ip> <num>\n");
    printf("  dafscontrol MYVER\n");
    exit(1);
}

const char * queryDaFileSrvExecutable(const IpAddress &ip, StringBuffer &ret)
{
    StringBuffer dir; // not currently used
    return querySlaveExecutable("DaFileSrvProcess", "dafilesrv", NULL, ip, ret, dir);
}


//extern REMOTE_API unsigned getDaliServixVersion(IpAddress &ip,StringBuffer &ver);

enum ApplyMode
{
    AMcheckver,         // check version failures to result and resultstr version
    AMcheckvermajor,    // only check major version
    AMcheck,    // 
    AMstop,     // unconditional stop - stopped to results and resultstr version
    AMstopver,  // stop if not current version
    AMver,      // return versions of each
    AMmax
};

bool getCluster(const char *clustername,SocketEndpointArray &eps)
{
    Owned<IGroup> grp = queryNamedGroupStore().lookup(clustername);
    if (grp.get()==NULL)
        return false;
    unsigned n = grp->ordinality();
    unsigned p = getDaliServixPort();
    for (unsigned i=0;i<n;i++) {
        SocketEndpoint ep(p,grp->queryNode(i).endpoint());
        eps.append(ep);
    }
    return eps.ordinality()!=0;
}

unsigned applyNodes(const char *grpip, ApplyMode mode, unsigned ver, bool isdali, bool quiet)
{
    SocketEndpointArray eps;
    if (isdali&&(stricmp(grpip,"all")==0)) {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn) 
            return 0;
        IPropertyTree* root = conn->queryRoot();
        Owned<IPropertyTreeIterator> clusters= root->getElements("ThorCluster");
        unsigned ret = 0;
        if (clusters->first()) {
            do {
                IPropertyTree &cluster = clusters->query();
                ret += applyNodes(cluster.queryProp("@name"),mode,ver,true,quiet);
            } while (clusters->next());
        }
        return ret;
    }
    SocketEndpointArray result;
    StringAttrArray resultstr;
    if (!isdali||!getCluster(grpip,eps)) {
        SocketEndpoint ep(grpip);
        if (ep.isNull()) {
            ERRLOG("%s is not a group name or ip",grpip);
            return 0;
        }
        if (ep.port==0)
            ep.port = getDaliServixPort();
        eps.append(ep);
    }
    PointerIArrayOf<ISocket> sockets;
    unsigned to=10*1000;
    unsigned n=eps.ordinality();    // use approx log scale (timeout is long but only for failure situation)
    while (n>1) {
        n/=2;
        to+=10*1000;
    }
    if (!quiet&&(n>1))
        PROGLOG("Scanning %s...",grpip);
    multiConnect(eps,sockets,to);
    CriticalSection sect;
    class casyncfor: public CAsyncFor
    {
        SocketEndpointArray &eps;
        PointerIArrayOf<ISocket> &sockets;
        ApplyMode mode;
        unsigned ver;
        SocketEndpointArray &result;
        StringAttrArray &resultstr;
        CriticalSection &sect;
    public:
        casyncfor(ApplyMode _mode, unsigned _ver,SocketEndpointArray &_eps,PointerIArrayOf<ISocket> &_sockets,SocketEndpointArray &_result, StringAttrArray &_resultstr,CriticalSection &_sect) 
            : eps(_eps), sockets(_sockets), result(_result), resultstr(_resultstr), sect(_sect)
        { 
            mode = _mode;
            ver = _ver;
        }
        void Do(unsigned i)
        {
            ISocket *sock = sockets.item(i);
            StringBuffer epstr;
            SocketEndpoint ep = eps.item(i);
            ep.getUrlStr(epstr);
//          PROGLOG("T.1 %s %x",epstr.str(),(unsigned)sock);
            StringBuffer verstr;
            unsigned rver=0;
            if (sock) {
                rver = getRemoteVersion(sock, verstr);
                switch (mode) {
                case AMcheck:
                    if (rver!=0)
                        return;
                case AMver: {
                        CriticalBlock block(sect);
                        result.append(ep);
                        StringBuffer ln;
                        ln.append(rver).append(",\"").append(verstr).append('"');
                        resultstr.append(* new StringAttrItem(ln.str()));
                    }
                    return;
                case AMstopver:
                case AMcheckver: 
                case AMcheckvermajor: {
                    // compares versions up to the '-'
                        const char *rv = verstr.str();
                        const char *v = remoteServerVersionString();
                        if (mode!=AMcheckvermajor) {
                            while (*v&&(*v!='-')&&(*v==*rv)) {
                                v++;
                                rv++;
                            }
                        }
                        if ((*rv==*v)&&(rver==ver))
                            return;
                        while (*rv&&(*rv!='-'))
                            rv++;
                        verstr.setLength(rv-verstr.str());
                        if ((mode==AMcheckver)||(mode==AMcheckvermajor))
                            break;

                    }
                    // fall through
                case AMstop:
                    {
                        unsigned err = stopRemoteServer(sock);
                        if (err!=0) {
                            ERRLOG("Could not stop server on %s, %d returned",epstr.str(),err);
                            if (mode!=AMstopver)
                                return;     // even though failed to stop - still return code
                        }
                        else 
                            Sleep(1000); // let stop
                    }
                    break;
                default:
                    return;
                }
            }
            CriticalBlock block(sect);
            result.append(ep);
            if ((mode!=AMver)&&(mode!=AMcheckver)&&(mode!=AMcheckvermajor)&&(mode!=AMstopver))
                resultstr.append(* new StringAttrItem(""));
            else
                resultstr.append(* new StringAttrItem(verstr.str()));
        }
    } afor(mode,ver,eps,sockets,result,resultstr,sect);
    afor.For(eps.ordinality(), 10, false, true);
    if (result.ordinality()==0)
        return 0;
    switch (mode) {
    case AMstopver: 
    case AMcheckver: 
    case AMcheckvermajor: 
        if (!quiet) {
            StringBuffer epstr;
            ForEachItemIn(i,result) {
                result.item(i).getUrlStr(epstr.clear());
                StringAttrItem &attr = resultstr.item(i);
                if (attr.text.length()==0) 
                    ERRLOG("%s: %s not running DAFILESRV",grpip,epstr.str());
                else 
                    ERRLOG("%s: %s %s running DAFILESRV version %s",grpip,(mode==AMstopver)?"was":"is",epstr.str(),attr.text.get());
            }
            unsigned numok = eps.ordinality()-result.ordinality();
            if (mode==AMcheckvermajor)
                PROGLOG("%s: %d node%s running version %.1f of DAFILESRV",grpip,numok,(numok!=1)?"s":"",((double)FILESRV_VERSION)/10.0);
            else {
                StringBuffer vs;
                const char *v = remoteServerVersionString();
                while (*v&&(*v!='-'))
                    vs.append(*(v++));
                PROGLOG("%s: %d node%s running version %s of DAFILESRV",grpip,numok,(numok!=1)?"s":"",vs.str());
            }
        }
        break;
    case AMver: {
            StringBuffer epstr;
            unsigned failed=0;
            ForEachItemIn(i,result) {
                result.item(i).getUrlStr(epstr.clear());
                StringAttrItem &attr = resultstr.item(i);
                if (attr.text.length()!=0) 
                    PROGLOG("%s,%s,%s",grpip,epstr.str(),attr.text.get());
                else
                    failed++;
            }
            if (failed&&!quiet) 
                PROGLOG("%s: %d node%s not running DAFILESRV",grpip,failed,(failed!=1)?"s":"");
        }
        break;
    case AMcheck: 
        if (!quiet) {
            StringBuffer epstr;
            ForEachItemIn(i,result) {
                result.item(i).getUrlStr(epstr.clear());
                ERRLOG("%s: %s not running DAFILESRV",grpip,epstr.str());
            }
            unsigned numok = eps.ordinality()-result.ordinality();
            PROGLOG("%s: %d node%s running DAFILESRV",grpip,numok,(numok!=1)?"s":"");
        }
        break;
    case AMstop: {
            if (!quiet)
                PROGLOG("%s: %d stopped",grpip, result.ordinality());
        }
        break;
    }
    return result.ordinality();
}


struct ReleaseAtomBlock { ~ReleaseAtomBlock() { releaseAtoms(); } };
int main(int argc, char* argv[])
{   
    ReleaseAtomBlock rABlock;
    InitModuleObjects();
    if (argc<2) {
        usage();
        return 0;
    }

    EnableSEHtoExceptionMapping();
    attachStandardFileLogMsgMonitor("dafscontrol.log", NULL, MSGFIELD_STANDARD, MSGAUD_all, MSGCLS_all, TopDetail, false);
    queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);
    int ret = 0;
    try {
        unsigned ai=1;
        unsigned ac=argc;
        bool isdali=false;
        bool quiet = false;
        if ((ac>2)&&(stricmp(argv[ac-1],"quiet")==0)) {
            quiet = true;
            ac--;
        }
        loop {
            if (ai>=ac) {
                usage();
                break;
            }
            if (stricmp(argv[ai],"myver")==0) {
                const char *v = remoteServerVersionString();
                StringBuffer vs;
                if (memicmp(v,"DS V",4)==0)
                    v += 4;
                while (*v&&(*v!=' ')) {
                    vs.append(*v);
                    v++;
                }
                printf("%s\n",vs.str());
                break;

            }
            if (stricmp(argv[ai],"stop")==0) {
                if (ai+1>=ac) 
                    usage(); 
                else
                    applyNodes(argv[ai+1], AMstop, FILESRV_VERSION,isdali,quiet);
                break;
            }
            if (stricmp(argv[ai],"stopver")==0) {
                if (ai+1>=ac) 
                    usage(); 
                else
                    applyNodes(argv[ai+1], AMstopver, FILESRV_VERSION,isdali,quiet);
                break;
            }
            if (stricmp(argv[ai],"check")==0) {
                if (ai+1>=ac) 
                    usage(); 
                else
                    if (applyNodes(argv[ai+1], AMcheck, FILESRV_VERSION,isdali,quiet)>0)
                        ret = 1;
                break;
            }
            if (stricmp(argv[ai],"checkver")==0) {
                if (ai+1>=ac) 
                    usage();
                else
                    if (applyNodes(argv[ai+1], AMcheckver, FILESRV_VERSION,isdali,quiet)>0)
                        ret = 1;
                break;
            }
            if (stricmp(argv[ai],"checkvermajor")==0) {
                if (ai+1>=ac) 
                    usage();
                else
                    if (applyNodes(argv[ai+1], AMcheckvermajor, FILESRV_VERSION,isdali,quiet)>0)
                        ret = 1;
                break;
            }
            if (stricmp(argv[ai],"ver")==0) {
                if (ai+1>=ac) 
                    usage(); 
                else
                    applyNodes(argv[ai+1], AMver, FILESRV_VERSION,isdali,quiet);
                break;
            }
            if (stricmp(argv[ai],"trace")==0) {
                if (ai+2>=ac) 
                    usage(); 
                else {
                    SocketEndpointArray eps;
                    if (!isdali||!getCluster(argv[ai+1],eps)) {
                        SocketEndpoint ep(argv[ai+1]);
                        int ret = setDafileSvrTraceFlags(ep,(byte)atoi(argv[ai+2]));
                        if (ret!=0)
                            ERRLOG("setDafileSvrTraceFlags returned %d",ret);
                    }
                    else {
                        ForEachItemIn(ni,eps) {
                            SocketEndpoint ep = eps.item(ni);
                            int ret = setDafileSvrTraceFlags(ep,(byte)atoi(argv[ai+2]));
                            if (ret!=0)
                                ERRLOG("setDafileSvrTraceFlags returned %d",ret);
                            StringBuffer s("done ");
                            ep.getUrlStr(s);
                            PROGLOG("%s",s.str());
                        }
                    }
                }
                break;
            }
            SocketEndpoint ep;
            SocketEndpointArray epa;
            ep.set(argv[ai],DALI_SERVER_PORT);
            epa.append(ep);
            if (ep.isNull()) {
                usage();
                break;
            }
            Owned<IGroup> daligroup = createIGroup(epa);
            initClientProcess(daligroup, DCR_Util );
            isdali = true;
            ai++;
        }
        closeEnvironment();
        if (isdali)
            closedownClientProcess();
    }
    catch (IException *e) {
        EXCLOG(e, "DAFSCONTROL");
        e->Release();
    }
    return ret;
}

