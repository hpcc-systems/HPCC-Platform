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

#include "dautils.hpp"
#include "workunit.hpp"

#include "swapnodelib.hpp"

#define SDS_LOCK_TIMEOUT 30000
#define SWAPNODE_RETRY_TIME (1000*60*60*1) // 1hr

static const LogMsgJobInfo swapnodeJob(UnknownJob, UnknownUser);

static bool ensureThorIsDown(const char *cluster, bool nofail, bool wait)
{
    bool retry = false;
    do {
        Owned<IRemoteConnection> pStatus = querySDS().connect("/Status/Servers", myProcessSession(), RTM_NONE, SDS_LOCK_TIMEOUT);
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


// SwapNode info
//
//  SwapNode/
//    Thor [ @group, @timeChecked ]
//      BadNode [ @netAddress, @timeChecked, @time, @numTimes, @code, @rank, @ (msg)
//      Swap [ @inNetAddress, @outNetAddress, @time, @rank]
//      WorkUnit [ @id @time @resubmitted ]

//time,nodenum,ip,code,errmsg
//time,nodenum,swapout,swapin


class CSwapNode
{
protected:
    Linked<IPropertyTree> environment;
    StringAttr clusterName;
    StringAttr groupName, spareGroupName;
    IPropertyTree *options;
    Owned<IGroup> group, spareGroup;

    bool checkIfNodeInUse(IpAddress &ip, bool includespares, StringBuffer &clustname)
    {
        SocketEndpoint ep(0,ip);
        if (RANK_NULL != group->rank(ep)) {
            clustname.append(groupName);
            return true;
        }
        else if (includespares) {
            if (RANK_NULL != spareGroup->rank(ep)) {
                clustname.append(groupName).append(" spares");
                return true;
            }
        }
        return false;
    }
    IPropertyTree *getSwapNodeInfo(bool create)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/SwapNode", myProcessSession(), RTM_LOCK_WRITE|(create?RTM_CREATE_QUERY:0), 1000*60*5);
        if (!conn) {
            ERRLOG("SWAPNODE: could not connect to /SwapNode branch");
            return NULL;
        }
        StringBuffer xpath;
        xpath.appendf("Thor[@group=\"%s\"]",groupName.get());
        Owned<IPropertyTree> info = conn->queryRoot()->getPropTree(xpath.str());
        if (!info) {
            if (!create) {
                PROGLOG("SWAPNODE: no information for group %s",groupName.get());
                return NULL;
            }
            info.set(conn->queryRoot()->addPropTree("Thor",createPTree("Thor")));
            info->setProp("@group",groupName.get());
        }
        return info.getClear();
    }
    bool doSwap(const char *oldip, const char *newip)
    {
        Owned<INode> newNode = createINode(newip);
        Owned<INode> oldNode = createINode(oldip);
        if (!group->isMember(oldNode)) {
            ERRLOG("Node %s is not part of group %s", oldip, groupName.get());
            return false;
        }
        if (group->isMember(newNode)) {
            ERRLOG("Node %s is already part of group %s", newip, groupName.get());
            return false;
        }
        queryNamedGroupStore().swapNode(oldNode->endpoint(),newNode->endpoint());
        return true;
    }
    bool doSingleSwapNode(const char *oldip,const char *newip,unsigned nodenum,IPropertyTree *info,const char *timechecked)
    {
        if (doSwap(oldip,newip)) {
            if (info) {
                StringBuffer times(timechecked);
                if (times.length()==0) {
                    CDateTime dt;
                    dt.setNow();
                    dt.getString(times);
                }
                // TBD tie up with bad node in auto?

                IPropertyTree *swap = info->addPropTree("Swap",createPTree("Swap"));
                swap->setProp("@inNetAddress",newip);
                swap->setProp("@outNetAddress",oldip);
                swap->setProp("@time",times.str());
                if (UINT_MAX != nodenum)
                    swap->setPropInt("@rank",nodenum-1);
            }
            return true;
        }
        return false;
    }

    void init()
    {
        StringBuffer xpath("Software/ThorCluster[@name=\"");
        xpath.append(clusterName).append("\"]");
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        environment.setown(createPTreeFromIPT(conn->queryRoot()));
        options = environment->queryPropTree(xpath.str());
        if (!options)
            throwUnexpected();
        groupName.set(options->queryProp("@nodeGroup"));
        if (groupName.isEmpty())
            groupName.set(options->queryProp("@name"));
        VStringBuffer spareS("%s_spares", groupName.get());
        spareGroupName.set(spareS);
        group.setown(queryNamedGroupStore().lookup(groupName));
        spareGroup.setown(queryNamedGroupStore().lookup(spareGroupName));
    }
public:
    CSwapNode(const char *_clusterName) :clusterName(_clusterName)
    {
        init();
    }
    void swappedList(unsigned days, StringBuffer *out)
    {
        Owned<IPropertyTree> info = getSwapNodeInfo(true); // should put out error if returns false
        if (!info.get())
            return;
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
            if (checkIfNodeInUse(ip,true,clustname))
                continue; // ignore
            if (out)
                out->append(ips).append('\n');
            else
                PROGLOG("%s",ips);
        }
    }
    void emailSwap(const char *msg, bool warn=false, bool sendswapped=false, bool sendhistory=false)
    {
        StringBuffer emailtarget;
        StringBuffer smtpserver;
        if (options->getProp("SwapNode/@EmailAddress",emailtarget)&&emailtarget.length()&&options->getProp("SwapNode/@EmailSMTPServer",smtpserver)&&smtpserver.length()) {
            const char * subject = options->queryProp("SwapNode/@EmailSubject");
            if (!subject)
                subject = "SWAPNODE automated email";
            StringBuffer msgs;
            if (!msg) {
                msgs.append("Swapnode command line, Cluster: ");
                msg = msgs.append(groupName).append('\n').str();
            }
            CDateTime dt;
            dt.setNow();
            StringBuffer out;
            dt.getString(out,true).append(": ").append(msg).append("\n\n");
            if (options->getPropBool("SwapNode/@EmailSwappedList")||sendswapped) {
                out.append("Currently swapped out nodes:\n");
                swappedList(0,&out);
                out.append('\n');
            }
            if (options->getPropBool("SwapNode/@EmailHistory")||sendhistory) {
                out.append("Swap history:\n");
                swapNodeHistory(0,&out);
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
    void swapNodeHistory(unsigned days,StringBuffer *out)
    {
        Owned<IPropertyTree> info = getSwapNodeInfo(true);
        if (!info.get()) {
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
    bool checkThorNodeSwap(const char *failedwuid, unsigned mininterval)
    {
        bool ret = false;
        if (mininterval==(unsigned)-1) { // called by thor
            mininterval = 0;
            if (!options||!options->getPropBool("SwapNode/@autoSwapNode"))
                return false;
            if ((!failedwuid||!*failedwuid)&&!options->getPropBool("SwapNode/@checkAfterEveryJob"))
                return false;
        }

        try {
            Owned<IPropertyTree> info = getSwapNodeInfo(true);
            if (info.get()) {
                PROGLOG("checkNodeSwap started");
                StringBuffer xpath;
                CDateTime dt;
                StringBuffer ts;
                // see if done less than mininterval ago
                if (mininterval) {
                    dt.setNow();
                    dt.adjustTime(-((int)mininterval));
                    if (info->getProp("@timeChecked",ts)) {
                        CDateTime dtc;
                        dtc.setString(ts.str());
                        if (dtc.compare(dt,false)>0) {
                            PROGLOG("checkNodeSwap using cached validate from %s",ts.str());
                            xpath.clear().appendf("BadNode[@timeChecked=\"%s\"]",ts.str());
                            return info->hasProp(xpath.str());
                        }
                    }
                }

                Owned<IGroup> grp = queryNamedGroupStore().lookup(groupName);
                if (!grp)
                    PROGLOG("%s group doesn't exist", groupName.get());
                else
                {
                    SocketEndpointArray epa;
                    grp->getSocketEndpoints(epa);
                    ForEachItemIn(i1,epa) {
                        epa.item(i1).port = getDaliServixPort();
                    }
                    SocketEndpointArray failures;
                    UnsignedArray failedcodes;
                    StringArray failedmessages;
                    unsigned start = msTick();

                    const char *thorname = options->queryProp("@name");
                    StringBuffer dataDir, mirrorDir;
                    getConfigurationDirectory(environment->queryPropTree("Software/Directories"),"data","thor",thorname,dataDir); // if not defined can't check
                    getConfigurationDirectory(environment->queryPropTree("Software/Directories"),"mirror","thor",thorname,mirrorDir); // if not defined can't check

                    validateNodes(epa,dataDir.str(),mirrorDir.str(),false,options->queryProp("SwapNode/@swapNodeCheckScript"),options->getPropInt("SwapNode/@swapNodeCheckScriptTimeout")*1000,failures,failedcodes,failedmessages);

                    dt.setNow();
                    dt.getString(ts.clear());
                    ForEachItemIn(i,failures) {
                        SocketEndpoint ep(failures.item(i));
                        ep.port = 0;
                        StringBuffer ips;
                        ep.getIpText(ips);
                        int r = (int)grp->rank(ep);
                        if (r<0) {  // shouldn't occur
                            ERRLOG("SWAPNODE node %s not found in group %s",ips.str(),groupName.get());
                            continue;
                        }
                        PROGLOG("CheckSwapNode FAILED(%d) %s : %s",failedcodes.item(i),ips.str(),failedmessages.item(i));
                        // SNMP TBD?

                        ret = true;
                        xpath.clear().appendf("BadNode[@netAddress=\"%s\"]",ips.str());
                        IPropertyTree *bnt = info->queryPropTree(xpath.str());
                        if (!bnt) {
                            bnt = info->addPropTree("BadNode",createPTree("BadNode"));
                            bnt->setProp("@netAddress",ips.str());
                        }
                        bnt->setPropInt("@numTimes",bnt->getPropInt("@numTimes",0)+1);
                        bnt->setProp("@timeChecked",ts.str());
                        bnt->setProp("@time",ts.str());
                        bnt->setPropInt("@code",failedcodes.item(i));
                        bnt->setPropInt("@rank",r);
                        bnt->setProp(NULL,failedmessages.item(i));
                    }
                    if (failedwuid&&*failedwuid) {
                        xpath.clear().appendf("WorkUnit[@id=\"%s\"]",failedwuid);
                        IPropertyTree *wut = info->queryPropTree(xpath.str());
                        if (!wut) {
                            wut = info->addPropTree("WorkUnit",createPTree("WorkUnit"));
                            wut->setProp("@id",failedwuid);
                        }
                        wut->setProp("@time",ts.str());
                    }
                    PROGLOG("checkNodeSwap: Time taken = %dms",msTick()-start);
                    info->setProp("@timeChecked",ts.str());
                }
            }
        }
        catch (IException *e) {
            EXCLOG(e,"checkNodeSwap");
        }
        return ret;
    }
};

void swappedList(const char *clusterName, unsigned days, StringBuffer *out)
{
    CSwapNode swapNode(clusterName);
    swapNode.swappedList(days, out);
}

void emailSwap(const char *clusterName, const char *msg, bool warn, bool sendswapped, bool sendhistory)
{
    CSwapNode swapNode(clusterName);
    swapNode.emailSwap(msg, warn, sendswapped, sendhistory);
}

void swapNodeHistory(const char *clusterName, unsigned days, StringBuffer *out)
{
    CSwapNode swapNode(clusterName);
    swapNode.swapNodeHistory(days, out);
}

bool checkThorNodeSwap(const char *clusterName, const char *failedwuid, unsigned mininterval)
{
    CSwapNode swapNode(clusterName);
    return swapNode.checkThorNodeSwap(failedwuid, mininterval);
}


class CSingleSwapNode : public CSwapNode
{
public:
    CSingleSwapNode(const char *clusterName) : CSwapNode(clusterName)
    {
    }
    bool swap(const char *oldip, const char *newip)
    {
        ensureThorIsDown(clusterName,false,false);

        Owned<IPropertyTree> info = getSwapNodeInfo(true);
        if (!doSingleSwapNode(oldip,newip,UINT_MAX,info,NULL))
            return false;
        // check to see if it was a spare and remove
        SocketEndpoint spareEp(newip);
        rank_t r = spareGroup->rank(spareEp);
        if (RANK_NULL != r)
        {
            PROGLOG("Removing spare : %s", newip);
            spareGroup.setown(spareGroup->remove(r));
            queryNamedGroupStore().add(spareGroupName, spareGroup); // NB: replace
        }

        info.clear();

        PROGLOG("SwapNode finished");

        return true;
    }
};

bool swapNode(const char *cluster, const char *oldip, const char *newip)
{
    PROGLOG("SWAPNODE(%s,%s,%s) starting",cluster,oldip,newip);
    CSingleSwapNode swapNode(cluster);
    return swapNode.swap(oldip, newip);
}


class CAutoSwapNode : public CSwapNode
{
    bool doAutoSwapNode(bool dryRun=false)
    {
        if (!checkThorNodeSwap(NULL,dryRun?0:5)) {
            PROGLOG("No bad nodes detected");
            PROGLOG("SWAPNODE(auto) exiting");
            return false;
        }
        Owned<IPropertyTree> info = getSwapNodeInfo(false);
        if (!info.get()) {    // should put out error if returns false
            PROGLOG("SWAPNODE(auto) exiting");
            return false;
        }
        StringBuffer ts;
        if (!info->getProp("@timeChecked",ts)) {
            PROGLOG("SWAPNODE(auto): no check information generated");
            return false;
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

        const char *thorname = options->queryProp("@name");
        StringBuffer dataDir, mirrorDir;
        if (options->getPropBool("SwapNode/@swapNodeCheckPrimaryDrive",true))
            getConfigurationDirectory(environment->queryPropTree("Software/Directories"),"data","thor",thorname,dataDir); // if not defined can't check
        if (options->getPropBool("SwapNode/@swapNodeCheckMirrorDrive",true))
            getConfigurationDirectory(environment->queryPropTree("Software/Directories"),"mirror","thor",thorname,mirrorDir); // if not defined can't check

        validateNodes(epa1, dataDir.str(), mirrorDir.str(), false, options->queryProp("SwapNode/@swapNodeCheckScript"), options->getPropInt("SwapNode/@swapNodeCheckScriptTimeout")*1000, badepa, failedcodes, failedmessages);
        if (!badepa.ordinality()) {
            PROGLOG("SWAPNODE: on recheck all bad nodes passed (%s,%s)",groupName.get(),ts.str());
            return false;
        }
        Owned<IGroup> grp = queryNamedGroupStore().lookup(groupName);
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
                return false;
            }
            bnt->setProp("@time",ts.str());
            int r = bnt->getPropInt("@rank",-1);
            if ((int)r<0) { // shouldn't occur
                ERRLOG("SWAPNODE node %s rank not found in group %s",ips.str(),groupName.get());
                return false;
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
        const char *intent = dryRun?"would":"will";
        // find spares
        SocketEndpointArray spareepa;
        StringArray swapfrom;
        StringArray swapto;
        Owned<IGroup> spareGroup;
        if (!abort) {
            spareGroup.setown(queryNamedGroupStore().lookup(spareGroupName));
            if (!spareGroup) {
                ERRLOG("SWAPNODE could not find spare group %s", spareGroupName.get());
                abort = true;
            }
            else
            {
                spareGroup->getSocketEndpoints(spareepa);
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
        }
        // now list what can do
        if (abort) {
            ERRLOG("SWAPNODE: problems found (listed above), no swap %s be attempted",intent);
            return false;
        }
        if (dryRun)
            return false;
        // need to release swapnode lock for multi thor not to get deadlocked
        info.clear(); // NB: This clears the connection to SwapNode
        ensureThorIsDown(clusterName,true,true);
        ForEachItemIn(i4,badepa) {
            StringBuffer from;
            badepa.item(i4).getIpText(from);
            SocketEndpoint &spareEp = spareepa.item(i4);
            StringBuffer to;
            spareEp.getIpText(to);
            rank_t r = spareGroup->rank(spareEp);
            spareGroup.setown(spareGroup->remove(r));
            queryNamedGroupStore().add(spareGroupName, spareGroup); // NB: replace
            Owned<IPropertyTree> info = getSwapNodeInfo(false);
            if (doSingleSwapNode(from.str(),to.str(),badrank.item(i4)+1,info,ts.str())) {
                StringBuffer msg;
                msg.appendf("AUTOSWAPNODE: cluster %s node %d: swapped out %s, swapped in %s",groupName.get(),badrank.item(i4)+1,from.str(),to.str());
                emailSwap(msg.str());
                FLLOG(MCoperatorError, swapnodeJob, "%s", msg.str());
            }
        }
        return true;
    }
    void autoRestart()
    {
        // restarts any workunits that failed near to swap
        // let see if need resubmit any nodes
        StringArray toresubmit;
        if (options->getPropBool("SwapNode/@swapNodeRestartJob")) {
            Owned<IPropertyTree> info = getSwapNodeInfo(false); // should put out error if returns false
            if (!info.get())
            {
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
public:
    CAutoSwapNode(const char *clusterpName) : CSwapNode(clusterpName)
    {
    }
public:
    bool swap(bool dryRun)
    {
        PROGLOG("SWAPNODE(auto%s) starting",dryRun?",dryRun":"");

        if (!doAutoSwapNode(dryRun)) // using info in Dali (spares etc.)
            return false;

        autoRestart();

        PROGLOG("AutoSwapNode finished");
        return true;
    }
};

bool autoSwapNode(const char *groupName, bool dryRun)
{
    CAutoSwapNode swapNode(groupName);
    return swapNode.swap(dryRun);
}
