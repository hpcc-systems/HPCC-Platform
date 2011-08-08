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

#include "platform.h"
#include "thirdparty.h"

#include "jlib.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "jprop.hpp"
#include "jmisc.hpp"

#include "mpbase.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "dalienv.hpp"
#include "rmtfile.hpp"
#include "rmtsmtp.hpp"

#ifndef _ESP
#include "dautils.hpp" 
#include "workunit.hpp"
#else
#include "swapnodemain.hpp"
#endif




#define SDS_LOCK_TIMEOUT 30000
#define SWAPNODE_RETRY_TIME (1000*60*60*1) // 1hr


#ifdef _DEBUG
#ifdef NIGEL_TESTING
#define FILES_WRITE_PREFIX  "test_"
#else
#define FILES_WRITE_PREFIX  ""
#endif
#else
#define FILES_WRITE_PREFIX  ""
#endif

#ifndef _ESP
static void doAutoSwapNode(IRemoteConnection *connEnv,IRemoteConnection *connFiles,IPropertyTree *options,bool doswap);
static const LogMsgJobInfo swapnodeJob(UnknownJob, UnknownUser);
static void autoRestart(IPropertyTree *options);
#endif

static IRemoteConnection* GetRemoteLock(const char* path, unsigned int mode, bool nonfatalinuse)
{
    // this code not nice - could do with rewrite when time permits!

    IRemoteConnection* pRemoteConnection = NULL;

    try
    {
        PROGLOG("Getting a lock on %s ...", path);
        pRemoteConnection = querySDS().connect(path, myProcessSession(), mode,  SDS_LOCK_TIMEOUT);
    }
    catch (IException* e)
    {
       StringBuffer sErrMsg;
       e->errorMessage(sErrMsg);
       e->Release();

       /*typical error message when lock fails is as follows:
       SDS: Lock timeout
       SDS Reply Error  : SDS: Lock timeout
       Failed to establish lock to NewEnvironment/
       Existing lock status: Locks on path: /NewEnvironment/
       Endpoint            |SessionId       |ConnectionId    |mode

       172.16.48.175:7254  |c00000038       |c0000003b       |653
       */

       const char* pattern = "Failed to establish lock to ";
       const char* match = strstr(sErrMsg.str(), pattern);
       if (match)
       {
           match += strlen(pattern);
           const char* eol = strchr(match, '\n');
           StringBuffer path;
           path.append(eol - match - 1, match);

           //if we can extract IP address of computer holding the lock then
           //show a customized message.
           //
           //Retrieve IP address of computer holding the lock...
           char achHost[128] = "";
           const char* p = strstr(sErrMsg.str(), "\n\n");
           if (p && *(p+=2))
           {
              const char* q = strchr(p, ':');
              if (q)
              {
                 const int len = q-p;
                 strncpy(achHost, p, len);
                 achHost[len] = '\0';
              }
           }

           StringBuffer sMsg;
           sMsg.appendf("Failed to get a lock on /%s", path.str());

           if (achHost[0])
              sMsg.appendf(" because it is locked by computer %s.", achHost);
           else
              sMsg.append(":\n\n").append(sErrMsg);
           if (nonfatalinuse) {
               WARNLOG("%s",sMsg.str());
               return NULL;
           }
           throw ::MakeStringException(-1, "%s", sMsg.str());
       }
       else
           throw ::MakeStringException(-1, "%s", sErrMsg.str());
    }

    return pRemoteConnection;
}

static bool ensureThorIsDown(const char* cluster, bool nofail, bool wait)
{
    bool retry = false;
    do {
        Owned<IRemoteConnection> pStatus = querySDS().connect("/Status/Servers", myProcessSession(), RTM_NONE,  SDS_LOCK_TIMEOUT);
        Owned<IPropertyTreeIterator> it = pStatus->queryRoot()->getElements("Server[@name='ThorMaster']");
        retry = false;
        ForEach(*it) {
            IPropertyTree* pServer = &it->query();
            if (pServer->hasProp("@cluster") && !strcmp(pServer->queryProp("@cluster"), cluster)) {
                if (nofail) {
                    WARNLOG("A Thor on cluster %s is still active", cluster);
                    if (!wait)
                        return false;
                    Sleep(1000*10);
                    PROGLOG("Retrying...");
                    retry = true;
                    break;
                }
                throw MakeStringException(-1, "A Thor cluster node swap requires the cluster to be offline.  Please stop the Thor cluster '%s' and try again.", cluster);
            }
        }
    } while (retry);
    return true;
}


static bool doEnvironment(IPropertyTree* root, const char* clustername, const char* oldip, const char* newip, unsigned nodenum, bool& bNewMachineIsLinux)
{
    IPropertyTree* pHardware = root->queryPropTree("Hardware");

    IPropertyTree* oldmachine = pHardware->queryPropTree(StringBuffer("Computer[@netAddress=\"").append(oldip).append("\"]").str());
    if(!oldmachine) {
        ERRLOG("Could not find computer with ip=%s", oldip);
        return false;
    }

    IPropertyTree* newmachine = pHardware->queryPropTree(StringBuffer("Computer[@netAddress=\"").append(newip).append("\"]").str());
    if(!newmachine) {
        ERRLOG("Could not find computer with ip=%s", newip);
        return false;
    }

    StringBuffer xpath;

    //determine if the new machine is linux so we can properly update its slaves file
    const char* newMachineType = newmachine->queryProp("@computerType");
    xpath.clear().appendf("ComputerType[@name='%s']", newMachineType);
    IPropertyTree* newMachineTypeNode = pHardware->queryPropTree( xpath.str() );
    if (!newMachineTypeNode) {
        ERRLOG("computer type '%s' of the new slave is not defined!", newMachineType);
        return false;
    }

    const char* os = newMachineTypeNode->queryProp("@opSys");
    bNewMachineIsLinux = os && !strcmp(os, "linux");

    DBGLOG("NewMachine=%s (%s)",newmachine->queryProp("@name"),bNewMachineIsLinux?"linux":"windows");
    DBGLOG("OldMachine=%s",oldmachine->queryProp("@name"));

    IPropertyTree* pSoftware = root->queryPropTree("Software");
    if(!pSoftware) {
        ERRLOG("Could not find /Software!");
        return false;
    }

    // look for all ThorCluster entries with correct nodegroup (needed for multithor)

    Owned<IPropertyTreeIterator> clusters = pSoftware->getElements("ThorCluster");
    ForEach(*clusters) {
        IPropertyTree &cluster = clusters->query();
        const char *groupname = cluster.queryProp("@nodeGroup");
        if (!groupname||!*groupname)
            groupname = cluster.queryProp("@name");
        if (strcmp(groupname,clustername)!=0)
            continue;

        xpath.clear().appendf("ThorSlaveProcess[@computer='%s']", oldmachine->queryProp("@name"));
        IPropertyTree* slave = cluster.queryPropTree(xpath.str());
        if(!slave) {
            ERRLOG("Could not find slave %s in thor  %s", oldmachine->queryProp("@name"), cluster.queryProp("@name"));
            return false;
        }

        xpath.clear().appendf("ThorSlaveProcess[@computer='%s']", newmachine->queryProp("@name"));
        if (cluster.queryPropTree(xpath.str())) {
            ERRLOG("This would duplicate slave %s in thor %s", oldmachine->queryProp("@name"), cluster.queryProp("@name"));
            return false;
        }


        StringBuffer sn;
        if (nodenum!=0) {
            sn.append('s').append(nodenum);
            if (strcmp(slave->queryProp("@name"),sn.str())!=0) {
                ERRLOG("Incorrect slave number %d for slave %s(%s) in thor %s",
                         nodenum,oldmachine->queryProp("@name"), slave->queryProp("@name"), cluster.queryProp("@name"));
                return false;
            }
        }


        xpath.clear().appendf("ThorSpareProcess[@computer='%s']", newmachine->queryProp("@name"));
        IPropertyTree* spare = cluster.queryPropTree(xpath.str());
        if(spare) {
            const char *state = slave->queryProp("@state"); // not sure if anyone actuall sets this but bwd compat.
            DBGLOG("Removing Spare:%s%s%s",slave->queryProp("@name"),state?" with status: ":"",state?state:"");
            cluster.removeTree(spare);
        }

        slave->setProp("@computer",newmachine->queryProp("@name"));
        newmachine->setProp("@state","Unavailable");


    }
    return true;
}

static bool resolveComputerName(IPropertyTree *rootEnv,const char *name,IpAddress &ip)
{
    StringBuffer query;
    query.appendf("Hardware/Computer[@name=\"%s\"]",name);
    Owned<IPropertyTree> machine = rootEnv->getPropTree(query.str());
    const char *node = machine?machine->queryProp("@netAddress"):NULL;
    if (!node||!*node)
        false;
    ip.ipset(node);
    return true;
}

class CfixDaliDFS
{

    void writeSlavesFile(IPropertyTree* rootEnv, const char *newep, IPropertyTree& cluster, bool bNewMachineIsLinux)
    {
        Owned<INode> newnode = createINode(newep);
        const char *groupname = cluster.queryProp("@nodeGroup");
        if (!groupname||!*groupname)
            groupname = cluster.queryProp("@name");

        Owned<IGroup> grp = queryNamedGroupStore().lookup(groupname);
        if (!grp) {
            ERRLOG("writeSlavesFile: group not found for cluster %s",groupname);
            return;
        }
        if (!grp->isMember(newnode))
            return;
        PROGLOG("Writing slaves file for cluster %s",groupname);
        const char *computer = cluster.queryProp("@computer");
        if (!computer||!*computer) {
            ERRLOG("writeSlavesFile: cluster has no computer specified");
            return;
        }
        const char *dir = cluster.queryProp("@directory");
        if (!dir||!*dir) {
            ERRLOG("writeSlavesFile: cluster has no directory specified");
            return;
        }
        IpAddress masterip;
        if (!resolveComputerName(rootEnv,computer,masterip)) {
            ERRLOG("writeSlavesFile: cannot resolve thor master at %s",computer);
            return;
        }
        char sep = bNewMachineIsLinux?'/':'\\';
        StringBuffer filename;
        filename.append(sep).append(sep);
        masterip.getIpText(filename);
        if (dir&&*dir&&!isPathSepChar(*dir))
            filename.append(sep);
        while (dir&&*dir) {
            if (isPathSepChar(*dir))
                filename.append(sep);
            else
                filename.append(*dir);
            dir++;
        }
        addPathSepChar(filename,sep);
        size32_t dirsz = filename.length();
        filename.append(FILES_WRITE_PREFIX "slaves");
        StringBuffer str;
        ForEachNodeInGroup(r,*grp) {
            grp->queryNode(r).endpoint().getUrlStr(str);
            if (!bNewMachineIsLinux)
                str.append('\r');       // not sure a good idea but consistent with deploy engine
            str.append('\n');
        }
        PROGLOG("Writing slaves to %s",filename.str());
        Owned<IFile> outfile = createIFile(filename.str());
        Owned<IFileIO> outfileio = outfile->open(IFOcreate);
        if (!outfileio)
            throw MakeStringException (-1,"Cannot create slaves file %s",filename.str());
        outfileio->write(0,str.length(),str.str());
        outfileio.clear();
        outfile.clear();

        str.clear();
        Owned<IPropertyTreeIterator> spares = cluster.getElements("ThorSpareProcess");
        ForEach(*spares) {
            computer = spares->query().queryProp("@computer");
            if (!computer||!*computer) {
                WARNLOG("writeSlavesFile: spare has no computer specified");
                continue;
            }
            IpAddress nodeip;
            if (!resolveComputerName(rootEnv,computer,nodeip)) {
                WARNLOG("writeSlavesFile: cannot resolve spare at %s",computer);
                str.append(computer);
            }
            else
                nodeip.getIpText(str);
            if (!bNewMachineIsLinux)
                str.append('\r');       // not sure a good idea but consistent with deploy engine
            str.append('\n');
        }
        filename.setLength(dirsz);
        filename.append(FILES_WRITE_PREFIX "spares");
        PROGLOG("Writing spares to %s",filename.str());
        outfile.setown(createIFile(filename.str()));
        outfileio.setown(outfile->open(IFOcreate));
        if (!outfileio)
            throw MakeStringException (-1,"Cannot create spares file %s",filename.str());
        outfileio->write(0,str.length(),str.str());
    }


public:

    void doThorSlavesFiles(const char *newip, IPropertyTree* rootEnv, bool bNewMachineIsLinux)
    {   // recreates DFS Groups (bit over the top for this usage, but effective)
        Owned<IPropertyTreeIterator> clusters= rootEnv->getElements("Software/ThorCluster");
        ForEach(*clusters) {
            IPropertyTree &cluster = clusters->query();
            writeSlavesFile(rootEnv, newip, cluster, bNewMachineIsLinux);
        }
    }



    bool doFiles(IPropertyTree* filesRoot, const char* thor, const char* oldip, const char* newip,unsigned partno)
    {
        class cfilescan
        {

            void processScopes(IPropertyTree &root,StringBuffer &name)
            {
                size32_t ns = name.length();
                if (ns)
                    name.append("::");
                size32_t ns2 = name.length();

                Owned<IPropertyTreeIterator> iter = root.getElements("Scope");
                if (iter->first()) {
                    do {
                        IPropertyTree &scope = iter->query();
                        name.append(scope.queryProp("@name"));
                        processScopes(scope,name);
                        name.setLength(ns2);
                    } while (iter->next());
                }
                processFiles(root,name);
                name.setLength(ns);
            }

            void processFiles(IPropertyTree &root,StringBuffer &name)
            {
                size32_t ns = name.length();
                Owned<IPropertyTreeIterator> iter = root.getElements("File");
                if (iter->first()) {
                    do {
                        IPropertyTree &file = iter->query();
                        name.append(file.queryProp("@name"));
                        processFile(file,name);
                        name.setLength(ns);
                    } while (iter->next());
                }
            }

            void processFile(IPropertyTree &file,StringBuffer &name)
            {
                Owned<IPropertyTreeIterator> iter = file.getElements(frompart.str());
                if (iter->first())
                {
                    loop
                    {
                        IPropertyTree &item = iter->query();
                        if (!partno || item.getPropInt("@num",0)==partno) {
                            PROGLOG("Processing file %s",name.str());
                            item.setProp("@node",to);
                        }
                        else
                            WARNLOG("ignoring node on file %s parts don't match (%d,%d)",name.str(),item.getPropInt("@num",0),partno);
                        if (!iter->next())
                            break;
                    }
                }
            }
        public:



            void scan(IPropertyTree *sroot)
            {
                StringBuffer name;
                processScopes(*sroot,name);
            }

            StringBuffer frompart;
            const char* to;
            unsigned partno;

        } filescan;

        filescan.frompart.append("Part[@node=\"").append(oldip).append("\"]");
        filescan.to = newip;
        filescan.partno = partno;
        filescan.scan(filesRoot);

        return true;
    }



};


static bool doSingleSwapNode(IRemoteConnection *connEnv,IRemoteConnection *connFiles,const char* cluster,const char* oldip,const char* newip,unsigned nodenum,IPropertyTree *info,const char *timechecked)
{
    IPropertyTree* rootEnv = connEnv->queryRoot();
    IPropertyTree* rootFiles = connFiles->queryRoot();
    bool bNewMachineIsLinux;
    if (doEnvironment(rootEnv, cluster,oldip,newip,nodenum, bNewMachineIsLinux)) {
        CfixDaliDFS fixdfs;
        fixdfs.doFiles(rootFiles, cluster,oldip,newip,nodenum);
        // no turning back now
        connEnv->commit();
        connFiles->commit();
        SocketEndpoint ipfrom(oldip);
        SocketEndpoint ipto(newip);
        queryNamedGroupStore().swapNode(ipfrom,ipto);
        fixdfs.doThorSlavesFiles(newip,connEnv->queryRoot(), bNewMachineIsLinux);   // must be done after doEnvironment
        if (info) {
            StringBuffer times(timechecked);
            if (times.length()==0) {
                CDateTime dt;
                dt.setNow();
                dt.getString(times);
            }
            StringBuffer xpath;

            // TBD tie up with bad node in auto?

            IPropertyTree *swap = info->addPropTree("Swap",createPTree("Swap"));
            swap->setProp("@inNetAddress",newip);
            swap->setProp("@outNetAddress",oldip);
            swap->setProp("@time",times.str());
            swap->setPropInt("@rank",nodenum-1);
        }

        return true;

    }
    return false;
}




static bool doSwapNode(IPropertyTree *options,bool doswap,const char* cluster,const char* oldip,const char* newip,unsigned nodenum, bool nofail)
{
    Owned<IRemoteConnection> connNewEnv;    // only used as lock (apparently)
    Owned<IRemoteConnection> connEnv;
    Owned<IRemoteConnection> connFiles;
    try {
        const unsigned int mode = RTM_CREATE | RTM_CREATE_QUERY | RTM_LOCK_READ | RTM_LOCK_WRITE | RTM_DELETE_ON_DISCONNECT;
        const unsigned int mode2 = RTM_LOCK_READ;  // only lock for read as NewEnvironment will protect against configenv


        connNewEnv.setown(GetRemoteLock("/NewEnvironment",mode,nofail));
        if (!connNewEnv)
            return false;
        connEnv.setown(GetRemoteLock("/Environment",mode2,nofail));
        if (!connEnv)
            return false;

        if (doswap) {
            connFiles.setown(GetRemoteLock("/Files",mode2, nofail));
            if (!connFiles)
                return false;
        }
#ifndef _ESP
        if (options) {
            doAutoSwapNode(connEnv,connFiles,options,doswap);
            autoRestart(options);
        }
        else
#endif
        {
            ensureThorIsDown(cluster,false,false);
            Owned<IPropertyTree> info;
#ifndef _ESP
            Owned<IPropertyTree> opt = createPTree(ipt_caseInsensitive);
            opt->setProp("@nodeGroup",cluster);
            Owned<IGroup> grp;
            Owned<IRemoteConnection> connSwapNode;
            StringAttr grpname;
            getSwapNodeInfo(opt,grpname,grp,connSwapNode,info,true);
#endif
            doSingleSwapNode(connEnv,connFiles,cluster,oldip,newip,nodenum,info,NULL);
        }

    }
    catch (IException *) {
        if (connEnv)
            connEnv->rollback();
        if (connFiles)
            connFiles->rollback();
        throw;
    }
    PROGLOG("SwapNode finished");
    return true;
}


void SwapNode(const char* cluster,const char* oldip,const char* newip,unsigned nodenum)
{
    PROGLOG("SWAPNODE(%s,%s,%s,%d) starting",cluster,oldip,newip,nodenum);
    doSwapNode(NULL,true,cluster,oldip,newip,nodenum,false);
}

#ifndef _ESP

bool WuResubmit(const char *wuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
    if (!wu) {
        ERRLOG("WuResubmit(%s): could not find workunit",wuid);
        return false;
    }
    if (wu->getState()!=WUStateFailed) {
        SCMStringBuffer state;
        wu->getStateDesc(state);
        ERRLOG("WuResubmit(%s): could not resubmit as workunit state is '%s'",wuid,state.str());
        return false;
    }
    SCMStringBuffer token;
    wu->getSecurityToken(token);
    SCMStringBuffer user;
    SCMStringBuffer password;
    extractToken(token.str(), wuid, user, password);
    wu->resetWorkflow();
    wu->setState(WUStateSubmitted);
    wu->commit();
    wu.clear();
    submitWorkUnit(wuid,user.str(),password.str());

    PROGLOG("WuResubmit(%s): resubmitted",wuid);
    return true;
}

void swapNodeHistory(IPropertyTree *options,unsigned days,StringBuffer *out)
{
    Owned<IGroup> grp;
    Owned<IRemoteConnection> connSwapNode;
    Owned<IPropertyTree> info;
    StringAttr grpname;
    if (!getSwapNodeInfo(options,grpname,grp,connSwapNode,info,true)) { 
        if (out)
            out->append("No swapnode info\n");
        else
            ERRLOG("No swapnode info");
        return;
    }
    StringBuffer line;
    CDateTime tt;
    CDateTime cutoff;
    if (days) {
        cutoff.setNow();
        cutoff.adjustTime(-60*24*(int)days);
    }
    unsigned i=0;
    if (out) 
        out->append("Failure, Time, NodeNum, NodeIp, ErrCode, Error Message\n------------------------------------------------------\n");
    else {
        PROGLOG("Failure, Time, NodeNum, NodeIp, ErrCode, Error Message");
        PROGLOG("------------------------------------------------------");
    }
    Owned<IPropertyTreeIterator> it1 = info->getElements("BadNode");
    ForEach(*it1) {
        IPropertyTree &badnode = it1->query();
        const char *ts = badnode.queryProp("@time");
        if (!ts)
            continue;
        if (days) {
            tt.setString(ts);
            if (cutoff.compare(tt)>0)
                continue;
        }
        line.clear().append(++i).append(", ");
        line.append(ts).append(", ").append(badnode.getPropInt("@rank",-1)+1).append(", ");
        badnode.getProp("@netAddress",line);
        line.append(", ").append(badnode.getPropInt("@code")).append(", \"");
        badnode.getProp(NULL,line);
        line.append('\"');
        if (out)
            out->append(line).append('\n');
        else
            PROGLOG("%s",line.str());
    }
    if (out) 
        out->append("\nSwapped, Time, NodeNum, OutIp, InIp\n-----------------------------------\n");
    else {
        PROGLOG("%s", "");
        PROGLOG("Swapped, Time, NodeNum, OutIp, InIp");
        PROGLOG("-----------------------------------");
    }
    i = 0;
    Owned<IPropertyTreeIterator> it2 = info->getElements("Swap");
    ForEach(*it2) {
        IPropertyTree &swappednode = it2->query();
        const char *ts = swappednode.queryProp("@time");
        if (!ts)
            continue;
        if (days) {
            tt.setString(ts);
            if (cutoff.compare(tt)>0)
                continue;
        }
        line.clear().append(++i).append(", ");
        swappednode.getProp("@time",line);
        line.append(", ").append(swappednode.getPropInt("@rank",-1)+1).append(", ");
        swappednode.getProp("@outNetAddress",line);
        line.append(", ");
        swappednode.getProp("@inNetAddress",line);
        if (out) 
            out->append(line.str()).append('\n');
        else
            PROGLOG("%s",line.str());
    }
}

bool checkIfNodeInUse(IPropertyTree *root, IpAddress &ip, bool includespares, StringBuffer &clustname)
{
    SocketEndpoint ep(0,ip);
    IPropertyTree* pSoftware = root->queryPropTree("Software");
    if(!pSoftware)
        throw MakeStringException(-1,"Could not find /Environment/Software!");

    // look for all ThorCluster entries with correct nodegroup (needed for multithor)
    StringBuffer endpoint;
    Owned<IPropertyTreeIterator> clusters = pSoftware->getElements("ThorCluster");
    StringBuffer xpath;
    ForEach(*clusters) {
        IPropertyTree &cluster = clusters->query();
        const char *groupname = cluster.queryProp("@nodeGroup");
        if (!groupname||!*groupname)
            groupname = cluster.queryProp("@name");
        Owned<IGroup> grp = queryNamedGroupStore().lookup(groupname);
        if (!grp) {
            ERRLOG("writeSlavesFile: group not found for cluster %s",groupname);
            continue;
        }
        if ((int)grp->rank(ep)>=0) {
            clustname.append(groupname);
            return true;
        }
        if (!includespares)
            continue;
        Owned<IPropertyTreeIterator> spares = cluster.getElements("ThorSpareProcess");
        ForEach(*spares) {
            const char *computer = spares->query().queryProp("@computer");
            if (!computer||!*computer) {
                WARNLOG("checkIfNodeInUse: spare has no computer specified");
                continue;
            }
            IpAddress nodeip;
            if (!resolveComputerName(root,computer,nodeip)) {
                WARNLOG("checkIfNodeInUse: cannot resolve spare at %s",computer);
                continue;
            }
            if (nodeip.ipequals(ip)) {
                clustname.append(groupname).append(" spares");
                return true;
            }
        }
    }
    return false;
}

void swappedList(IPropertyTree *options,unsigned days, StringBuffer *out)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), 0,  SDS_LOCK_TIMEOUT);
    if (!conn)
        return;
    Owned<IGroup> grp;
    Owned<IRemoteConnection> connSwapNode;
    Owned<IPropertyTree> info;
    StringAttr grpname;
    if (!getSwapNodeInfo(options,grpname,grp,connSwapNode,info,true)) { // should put out error if returns false
        return;
    }
    CDateTime tt;
    CDateTime cutoff;
    if (days) {
        cutoff.setNow();
        cutoff.adjustTime(-60*24*(int)days);
    }
    Owned<IPropertyTreeIterator> it2 = info->getElements("Swap");
    ForEach(*it2) {
        IPropertyTree &swappednode = it2->query();
        const char *ts = swappednode.queryProp("@time");
        if (!ts)
            continue;
        if (days) {
            tt.setString(ts);
            if (cutoff.compare(tt)>0)
                continue;
        }
        const char *ips = swappednode.queryProp("@outNetAddress");
        if (!ips||!*ips)
            continue;
        IpAddress ip(ips);
        StringBuffer clustname;
        if (checkIfNodeInUse(conn->queryRoot(),ip,true,clustname))
            continue; // ignore
        if (out) 
            out->append(ips).append('\n');
        else
            PROGLOG("%s",ips);

    }
}

void EmailSwap(IPropertyTree *options, const char *msg, bool warn=false, bool sendswapped=false, bool sendhistory=false)
{
    StringBuffer emailtarget;
    StringBuffer smtpserver;
    if (options->getProp("SwapNode/@EmailAddress",emailtarget)&&emailtarget.length()&&options->getProp("SwapNode/@EmailSMTPServer",smtpserver)&&smtpserver.length()) {
        const char * subject = options->queryProp("SwapNode/@EmailSubject");
        if (!subject)
            subject = "SWAPNODE automated email";
        StringBuffer msgs;
        if (!msg) {
            StringAttr grpname;
            grpname.set(options->queryProp("@nodeGroup"));
            if (grpname.isEmpty())
                grpname.set(options->queryProp("@name"));
            msgs.append("Swapnode command line, Cluster: ");
            msg = msgs.append(grpname).append('\n').str();
        }
        CDateTime dt;
        dt.setNow();
        StringBuffer out;
        dt.getString(out,true).append(": ").append(msg).append("\n\n");
        if (options->getPropBool("SwapNode/@EmailSwappedList")||sendswapped) {
            out.append("Currently swapped out nodes:\n");
            swappedList(options,0,&out);
            out.append('\n');
        }
        if (options->getPropBool("SwapNode/@EmailHistory")||sendhistory) {
            out.append("Swap history:\n");
            swapNodeHistory(options,0,&out);
            out.append('\n');
        }
        SocketEndpoint ep(smtpserver.str(),25);
        StringBuffer sender("swapnode@");
        queryHostIP().getIpText(sender);
        // add tbd
        StringBuffer ips;
        StringArray warnings;
        sendEmail(emailtarget.str(),subject,out.str(),ep.getIpText(ips).str(),ep.port,sender.str(),&warnings);
        ForEachItemIn(i,warnings) 
            WARNLOG("SWAPNODE: %s",warnings.item(i));
    }
    else if (warn)
        WARNLOG("Either SwapNode/@EmailAddress or SwapNode/@EmailSMTPServer not set in thor.xml");
}


// SwapNode info
//
//  SwapNode/
//    Thor [ @group, @timeChecked ]
//      BadNode [ @netAddress, @timeChecked, @time, @numTimes, @code, @rank, @ (msg)
//      Swap [ @inNetAddress, @outNetAddress, @time, @rank]
//      WorkUnit [ @id @time @resubmitted ]

//time,nodenum,ip,code,errmsg
//time,nodenum,swapout,swapin

static void autoRestart(IPropertyTree *options)
{
    // restarts any workunits that failed near to swap
    // let see if need resubmit any nodes
    StringArray toresubmit;
    if (options->getPropBool("SwapNode/@swapNodeRestartJob")) {
        Owned<IGroup> grp;
        Owned<IRemoteConnection> connSwapNode;
        Owned<IPropertyTree> info;
        StringAttr grpname;
        if (!getSwapNodeInfo(options,grpname,grp,connSwapNode,info,false)) {    // should put out error if returns false
            PROGLOG("SWAPNODE(autoRestart) exiting");
            return;
        }
        CDateTime recent;
        recent.setNow();
        recent.adjustTime(-SWAPNODE_RETRY_TIME/(1000*60));
        Owned<IPropertyTreeIterator> it = info->getElements("WorkUnit");
        ForEach(*it) {
            IPropertyTree &wu = it->query();
            const char *wuid = wu.queryProp("@id");
            if (!wuid)
                continue;
            if (!wu.getPropBool("@resubmitted")) {
                // see if any swaps recently done
                const char *dt1s = wu.queryProp("@time");
                if (!dt1s||!*dt1s)
                    continue;
                CDateTime dt1;
                dt1.setString(dt1s);
                dt1.adjustTime(SWAPNODE_RETRY_TIME/(1000*60));
                Owned<IPropertyTreeIterator> swit = info->getElements("Swap");
                ForEach(*swit) {
                    IPropertyTree &swap = swit->query();
                    const char *dt2s = swap.queryProp("@time");
                    if (!dt2s||!*dt2s)
                        continue;
                    CDateTime dt2;
                    dt2.setString(dt2s);
                    if ((dt2.compare(recent)>0)&&(dt1.compare(dt2)>0)) {
                        wu.setPropBool("@resubmitted",true); // only one attempt
                        toresubmit.append(wuid);
                        break;
                    }
                }
            }
        }
    }
    ForEachItemIn(ir,toresubmit) {
        WuResubmit(toresubmit.item(ir));
    }

}

static void doAutoSwapNode(IRemoteConnection *connEnv,IRemoteConnection *connFiles,IPropertyTree *options,bool doswap)
{
    if (!checkThorNodeSwap(options,NULL,doswap?5:0)) {
        PROGLOG("No bad nodes detected");
        PROGLOG("SWAPNODE(auto) exiting");
        return;
    }
    Owned<IGroup> grp;
    Owned<IRemoteConnection> connSwapNode;
    Owned<IPropertyTree> info;
    StringAttr grpname;
    if (!getSwapNodeInfo(options,grpname,grp,connSwapNode,info,false)) {    // should put out error if returns false
        PROGLOG("SWAPNODE(auto) exiting");
        return;
    }
    StringBuffer ts;
    if (!info->getProp("@timeChecked",ts)) {
        PROGLOG("SWAPNODE(auto): no check information generated");
        return;
    }

    // enumerate bad nodes
    StringBuffer xpath;
    xpath.appendf("BadNode[@time=\"%s\"]",ts.str());
    Owned<IPropertyTreeIterator> it = info->getElements(xpath.str());
    SocketEndpointArray epa1;
    ForEach(*it) {
        IPropertyTree &badnode = it->query();
        const char *ip = badnode.queryProp("@netAddress");
        if (!ip)
            continue;
        SocketEndpoint ep(ip);
        ep.port = getDaliServixPort();
        epa1.append(ep);
    }
    // recheck
    SocketEndpointArray badepa;
    UnsignedArray failedcodes;
    StringArray failedmessages;
    unsigned start = msTick();
    validateNodes(epa1,options->getPropBool("SwapNode/@swapNodeCheckC",true),options->getPropBool("SwapNode/@swapNodeCheckD",false),false,options->queryProp("SwapNode/@swapNodeCheckScript"),options->getPropInt("SwapNode/@swapNodeCheckScriptTimeout")*1000,badepa,failedcodes,failedmessages);
    if (!badepa.ordinality()) {
        PROGLOG("SWAPNODE: on recheck all bad nodes passed (%s,%s)",grpname.get(),ts.str());
        return;
    }
    CDateTime dt;
    dt.setNow();
    dt.getString(ts.clear());
    bool abort=false;
    UnsignedArray badrank;
    ForEachItemIn(i1,badepa) {
        SocketEndpoint ep(badepa.item(i1));
        ep.port = 0;    // should be no ports in group
        StringBuffer ips;
        ep.getIpText(ips);
        xpath.clear().appendf("BadNode[@netAddress=\"%s\"]",ips.str());
        IPropertyTree *bnt = info->queryPropTree(xpath.str());
        if (!bnt) {
            ERRLOG("SWAPNODE node %s not found in swapnode info!",ips.str());
            return;
        }
        bnt->setProp("@time",ts.str());
        int r = bnt->getPropInt("@rank",-1);
        if ((int)r<0) { // shouldn't occur
            ERRLOG("SWAPNODE node %s rank not found in group %s",ips.str(),grpname.get());
            return;
        }
        badrank.append((unsigned)r);
        for (unsigned j1=0;j1<i1;j1++) {
            SocketEndpoint ep1(badepa.item(j1));
            ep1.port = 0;   // should be no ports in group
            int r1 = (int)badrank.item(j1);
            if ((r==(r1+1)%grp->ordinality())||
                (r1==(r+1)%grp->ordinality())) {
                StringBuffer ips1;
                ep1.getIpText(ips1);
                ERRLOG("SWAPNODE adjacent nodes %d (%s) and %d (%s) are bad!",r+1,ips.str(),r1+1,ips1.str());
                abort = true;
            }
        }
    }
    // now see if any of bad nodes have been swapped out recently
    CDateTime recent = dt;
    int snint = options->getPropInt("SwapNode/@swapNodeInterval",24);
    recent.adjustTime(-60*snint);
    it.setown(info->getElements("Swap"));
    ForEach(*it) {
        IPropertyTree &swappednode = it->query();
        CDateTime dt1;
        const char *dt1s = swappednode.queryProp("@time");
        if (!dt1s||!*dt1s)
            continue;
        dt1.setString(dt1s);
        if (dt1.compare(recent)<0)
            continue;
        const char *ips = swappednode.queryProp("@outNetAddress");
        if (!ips||!*ips)
            continue;
        int r1 = swappednode.getPropInt("@rank",-1);
        SocketEndpoint swappedep(ips);
        swappedep.port = 0;
        ForEachItemIn(i2,badepa) {
            SocketEndpoint badep(badepa.item(i2));
            int badr = (int)badrank.item(i2);
            badep.port = 0;
            if (swappedep.equals(badep)) {
                // not sure if *really* want this
                ERRLOG("Node %d (%s) was swapped out on %s (too recent)",badr+1,ips,dt1s);
                abort = true;
            }
            else if ((badr==(r1+1)%grp->ordinality())||
                (r1==(badr+1)%grp->ordinality())) {
                StringBuffer bs;
                ERRLOG("SWAPNODE adjacent node to bad node %d (%s), %d (%s) was swapped on %s (too recent) !",badr+1,badep.getIpText(bs).str(),r1+1,ips,dt1s);
                abort = true;
            }
        }
    }
    const char *intent = doswap?"will":"would";
    // find spares
    IPropertyTree* rootEnv = connEnv->queryRoot();
    SocketEndpointArray spareepa;
    StringArray swapfrom;
    StringArray swapto;
    if (!abort) {
        Owned<IPropertyTreeIterator> clusters = connEnv->queryRoot()->getElements("Software/ThorCluster");
        ForEach(*clusters) {
            IPropertyTree &cluster = clusters->query();
            const char *cname = cluster.queryProp("@nodeGroup");
            if (!cname||!*cname)
                cname = cluster.queryProp("@name");
            if (strcmp(grpname.get(),cname)!=0)
                continue;
            Owned<IPropertyTreeIterator> spares = cluster.getElements("ThorSpareProcess");
            ForEach(*spares) {
                const char *computer = spares->query().queryProp("@computer");
                if (!computer||!*computer) {
                    WARNLOG("SWAPNODE: spare has no computer specified");
                    continue;
                }
                SocketEndpoint nodeep;
                if (!resolveComputerName(rootEnv,computer,nodeep)) {
                    WARNLOG("SWAPNODE: cannot resolve spare at %s",computer);
                    continue;
                }
                nodeep.port = 0;
                bool found = false;
                ForEachItemIn(j1,spareepa) {
                    if (spareepa.item(j1).ipequals(nodeep)) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    spareepa.append(nodeep);
            }
        }
        ForEachItemIn(i3,badepa) {
            StringBuffer from;
            badepa.item(i3).getIpText(from);
            if (i3<spareepa.ordinality()) {
                StringBuffer to;
                spareepa.item(i3).getIpText(to);
                PROGLOG("SWAPNODE %s swap node %d from %s to %s",intent,badrank.item(i3)+1,from.str(),to.str());
            }
            else {
                abort = true;
                ERRLOG("SWAPNODE no spare available to swap for node %d (%s)",badrank.item(i3)+1,from.str());
            }
        }
    }
    // now list what can do
    if (abort) {
        ERRLOG("SWAPNODE: problems found (listed above), no swap %s be attempted",intent);
        return;
    }
    if (!doswap)
        return;
    // need to release swapnode lock for multi thor not to get deadlocked
    connSwapNode.clear();
    ensureThorIsDown(grpname,true,true);
    ForEachItemIn(i4,badepa) {
        StringBuffer from;
        badepa.item(i4).getIpText(from);
        StringBuffer to;
        spareepa.item(i4).getIpText(to);
        if (doSingleSwapNode(connEnv,connFiles,grpname,from.str(),to.str(),badrank.item(i4)+1,info,ts.str())) {
            StringBuffer msg;
            msg.appendf("AUTOSWAPNODE: cluster %s node %d: swapped out %s, swapped in %s",grpname.get(),badrank.item(i4)+1,from.str(),to.str());
            EmailSwap(options,msg.str());
            FLLOG(MCoperatorError, swapnodeJob, "%s", msg.str());
        }
    }
    return;
}

void autoSwapNode(IPropertyTree *options,bool doswap)
{
    PROGLOG("SWAPNODE(auto%s) starting",doswap?",swap":"");
    unsigned start = msTick();
    loop {
        if (doSwapNode(options,doswap,NULL,NULL,NULL,0,true))
            break;
        if (msTick()-start>SWAPNODE_RETRY_TIME) {
            ERRLOG("Retry time exceeded, exiting");
            break;
        }
        WARNLOG("Swapnode pausing before retry");
        Sleep(60+(getRandom()%60));
    }
}


struct DaliClient
{
    DaliClient(const char* daliserver): serverGroup(createIGroup(daliserver, DALI_SERVER_PORT))
    {
        if (!serverGroup)
            throw MakeStringException(0, "Could not instantiate IGroup");

        if (!initClientProcess(serverGroup,DCR_Util))
            throw MakeStringException(0, "Could not initializing client process");
        setPasswordsFromSDS();
        closeEnvironment();
    }

    ~DaliClient()
    {
        clearPasswordsFromSDS();
        closedownClientProcess();
    }

    Owned<IGroup> serverGroup;
};

void suppressStdOut(bool suppress=true)
{
    static HANDLE out;
    static HANDLE saveout;
#ifdef WIN32
    if (suppress) {
        saveout = GetStdHandle(STD_OUTPUT_HANDLE);
        out = ::CreateFile("nul",GENERIC_WRITE,0,NULL,CREATE_ALWAYS,FILE_FLAG_WRITE_THROUGH,NULL);
        SetStdHandle(STD_OUTPUT_HANDLE,out);
    }
    else {
        SetStdHandle(STD_OUTPUT_HANDLE,saveout);
        CloseHandle(out);
    }
#else
    saveout = fileno(stdout);
#endif

}


int main(int argc,char** argv)
{
    InitModuleObjects();

    int ret = 0;

    bool isauto = (argc>=2)&&(stricmp(argv[1],"auto")==0);
    bool ishistory = (argc>=2)&&(stricmp(argv[1],"history")==0);
    bool isswapped = (argc>=2)&&(stricmp(argv[1],"swapped")==0);
    bool isemail = (argc>=2)&&(stricmp(argv[1],"email")==0);
    if ((argc<5)&&!isauto&&!ishistory&&!isswapped&&!isemail) {
        fprintf(stderr,"Usage: swapnode <daliserver> <thor-cluster> <oldip> <newip> <nodenum>\n");
        fprintf(stderr,"   or: swapnode history [<days>]                -- list swap history \n");
        fprintf(stderr,"   or: swapnode history [<days>] 2> outfile.csv -- save swap history \n");
        fprintf(stderr,"   or: swapnode swapped [<days>]      -- list currently swapped nodes\n");
        fprintf(stderr,"   or: swapnode email                           -- tests email\n");
        fprintf(stderr,"   or: swapnode auto [swap]\n");
        fprintf(stderr,"NB auto,history,swapped and email must be run in a thor deploy directory \n");
        fprintf(stderr,"   (e.g. /c$/thor) or in a directory with copy of thor.xml\n");
        fprintf(stderr,"if 'swap' not specified after 'auto' then only displays what *would* be swapped\n");
        ret = 2;
    }
    else {
        try {
            const char* daliserver;
            Owned<IPropertyTree> options;
            if (isauto||ishistory|isswapped|isemail) {
                options.setown(createPTreeFromXMLFile("thor.xml", ipt_caseInsensitive));
                daliserver = options?options->queryProp("@daliServers"):NULL;
                if (!daliserver||!*daliserver)
                    throw MakeStringException(-1,"Either thor.xml not found or DALISERVERS not found in thor.xml");
            }
            else {
                options.setown(createPTree(ipt_caseInsensitive)); // don't use thor.xml
                daliserver = argv[1];
            }

            DaliClient dclient(daliserver);
            StringBuffer logname;
            splitFilename(argv[0], NULL, NULL, &logname, NULL);
            addFileTimestamp(logname, true);
            logname.append(".log");
            appendLogFile(logname.str(),0,false);
            queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);
            if (options&&options->getPropBool("@enableSysLog",true))
                UseSysLogForOperatorMessages();

            if (argc>=5) {
                //DOM- Moved logic to swapnodemain.cpp so I use it within the management console...
                const char* thor=argv[2];
                const char* oldip=argv[3];
                const char* newip=argv[4];
                unsigned nodenum=(argc>5)?atoi(argv[5]):0;
                SwapNode(thor,oldip,newip,nodenum);
            }
            else if (isauto)
                autoSwapNode(options,(argc>2)&&(stricmp(argv[2],"swap")==0));
            else if (ishistory)
                swapNodeHistory(options,(argc>2)?atoi(argv[2]):0,NULL);
            else if (isswapped)
                swappedList(options,(argc>2)?atoi(argv[2]):0,NULL);
            else if (isemail) {
                bool sendswapped = (argc>=3)&&(stricmp(argv[2],"swapped")==0);
                bool sendhistory = (argc>=3)&&(stricmp(argv[2],"history")==0);
                EmailSwap(options, NULL,true, sendswapped,sendhistory);
            }

        }
        catch (IException *e) {
            EXCLOG(e,"SWAPNODE");
            e->Release();
            ret = -1;
        }
    }
    UseSysLogForOperatorMessages(false);

    ExitModuleObjects();
    return ret;
}

#endif
