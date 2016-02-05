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
#include "jprop.hpp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "jiter.ipp"
#include "jptree.hpp"
#include "jencrypt.hpp"

#ifndef dfuwu_decl
#define dfuwu_decl __declspec(dllexport)
#endif



#include "mpbase.hpp"
#include "daclient.hpp"
#include "dasds.hpp"
#include "dasess.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "dafdesc.hpp"
#include "wujobq.hpp"
#include "dfuutil.hpp"

#include "dfuwu.hpp"

#define COPY_WAIT_SECONDS 30


#define SDS_LOCK_TIMEOUT 30000

static StringBuffer &getXPathBase(StringBuffer &wuRoot)
{
    return wuRoot.append("/DFU/WorkUnits");
}

static StringBuffer &getXPath(StringBuffer &wuRoot, const char *wuid)
{
    getXPathBase(wuRoot);
    return wuRoot.append('/').append(wuid);
}

static void removeTree(IPropertyTree *root,const char *name)
{
    IPropertyTree *t = root->queryPropTree(name);
    if (t)
        root->removeTree(t);
}

static StringBuffer &newWUID(StringBuffer &wuid)
{
    wuid.append('D');
    char result[32];
    time_t ltime;
    time( &ltime );
    tm *today = localtime( &ltime );   
    strftime(result, sizeof(result), "%Y%m%d-%H%M%S", today);
    wuid.append(result);
    return wuid;
}


struct DFUstateStruct { int val; const char *str; } DFUstates[] = 
{
    {DFUstate_unknown,"unknown"},
    {DFUstate_scheduled, "scheduled"},
    {DFUstate_queued,"queued"},
    {DFUstate_started,"started"},
    {DFUstate_aborted,"aborted"},
    {DFUstate_failed,"failed"},
    {DFUstate_finished,"finished"},
    {DFUstate_monitoring,"monitoring"},
    {DFUstate_aborting,"aborting"},
    {DFUstate_unknown,""}               // must be last
};

struct DFUcmdStruct { int val; const char *str; } DFUcmds[] = 
{
    {DFUcmd_copy,               "copy"},
    {DFUcmd_remove,             "remove"},
    {DFUcmd_move,               "move"},
    {DFUcmd_rename,             "rename"},
    {DFUcmd_replicate,          "replicate"},
    {DFUcmd_import,             "import"},
    {DFUcmd_export,             "export"},
    {DFUcmd_add,                "add"},
    {DFUcmd_transfer,           "transfer"},
    {DFUcmd_savemap,            "savemap"},
    {DFUcmd_addgroup,           "addgroup"},
    {DFUcmd_server,             "server"},
    {DFUcmd_monitor,            "monitor"},
    {DFUcmd_copymerge,          "copymerge"},
    {DFUcmd_supercopy,          "supercopy"},
    {DFUcmd_none,               ""}             // must be last
};


struct DFUsortField { int val; const char *str; } DFUsortfields[] =
{
    {DFUsf_wuid,                    "@"}, //This duplicated item is added for getDFUSortFieldXPath()
    {DFUsf_user,                    "@submitID"},
    {DFUsf_cluster,                 "@cluster"},
    {DFUsf_state,                   "Progress/@state"},
    {DFUsf_command,                 "@command"},
    {DFUsf_job,                     "@jobName"},
    {DFUsf_wuid,                    "@"},
    {DFUsf_pcdone,                  "Progress/@percentdone"},
    {DFUsf_protected,               "@protected"},
    {DFUsf_term,                    ""}
};

const char *getDFUSortFieldXPath(DFUsortfield sortField)
{
    if (sortField < sizeof(DFUsortfields)/sizeof(DFUsortField))
        return DFUsortfields[sortField].str;
    return NULL;
}

DFUcmd decodeDFUcommand(const char * str)
{
    if (!str)
        return DFUcmd_none;
    unsigned i=0;
    loop {
        const char *cmp=DFUcmds[i].str;
        if (!*cmp||(stricmp(str,cmp)==0))
            break;
        i++;
    }
    return (DFUcmd)DFUcmds[i].val;
}

StringBuffer &encodeDFUcommand(DFUcmd cmd,StringBuffer &str)
{
    unsigned i=0;
    loop {
        if (!*DFUcmds[i].str||(DFUcmds[i].val==(int)cmd))
            break;
        i++;
    }
    return str.append(DFUcmds[i].str);
}

DFUstate decodeDFUstate(const char * str)
{
    if (!str)
        return DFUstate_unknown;
    unsigned i=0;
    loop {
        const char *cmp=DFUstates[i].str;
        if (!*cmp||(stricmp(str,cmp)==0))
            break;
        i++;
    }
    return (DFUstate)DFUstates[i].val;
}

StringBuffer &encodeDFUstate(DFUstate state,StringBuffer &str)
{
    unsigned i=0;
    loop {
        if (!*DFUstates[i].str||(DFUstates[i].val==(int)state))
            break;
        i++;
    }
    return str.append(DFUstates[i].str);
}



DFUsortfield decodeDFUsortfield(const char * s)
{
    if (!s)
        return DFUsf_term;
    int mod = 0;
    while (*s) {
        if (*s=='-') 
            mod |= DFUsf_reverse;
        else if (*s=='?') 
            mod |= DFUsf_nocase;
        else if (*s=='#') 
            mod |= DFUsf_numeric;
        else
            break;
        s++;
    }
    unsigned i=0;
    loop {
        const char *cmp=DFUsortfields[i].str;
        if (!*cmp||(DFUsortfields[i].val==(int)DFUsf_term)) 
            return DFUsf_term;
        if (stricmp(s,cmp)==0)
            break;
        i++;
    }
    return (DFUsortfield)(DFUsortfields[i].val+mod);
}

StringBuffer &encodeDFUsortfield(DFUsortfield fmt,StringBuffer &str,bool incmodifier)
{
    if (incmodifier) {
        if (((int)fmt)&DFUsf_reverse) 
            str.append('-');
        if (((int)fmt)&DFUsf_nocase) 
            str.append('?');
        if (((int)fmt)&DFUsf_numeric) 
            str.append('#');
    }
    fmt = (DFUsortfield)(((int)fmt)&0xff);
    unsigned i=0;
    loop {
        if ((DFUsortfields[i].val==(int)DFUsf_term)||(DFUsortfields[i].val==(int)fmt))
            break;
        i++;
    }
    return str.append(DFUsortfields[i].str);
}


class CDFUWorkUnitBase: public CInterface, implements IDFUWorkUnit , implements ISDSSubscription
{
protected: friend class CLinkedDFUWUchild; friend class CDFUprogress; friend class CDFUfileSpec; friend class CDFUoptions; friend class CDFUmonitor;
    Owned<IRemoteConnection> conn;
    Owned<IPropertyTree> root;
    mutable CriticalSection crit;
    virtual ~CDFUWorkUnitBase() 
    {
        root.clear();
        conn.clear();
    }



public:
    IMPLEMENT_IINTERFACE;
};


class CLinkedDFUWUchild : public CInterface
{
    mutable Linked<IPropertyTree> root;
    StringAttr name;
protected:
    CDFUWorkUnitBase  *parent;
public:

    CLinkedDFUWUchild()
    {
        parent = NULL;
    }
    ~CLinkedDFUWUchild()
    {
        root.clear();
        parent = NULL;
    }


    void init(CDFUWorkUnitBase  *_parent,const char *_name, bool lazy)
    {
        name.set(_name);
        parent = _parent;
        if (!lazy)
            queryRoot();
    }

    IPropertyTree *queryRoot() const
    {
        if (!root) {
            root.set(parent->root->queryPropTree(name.get()));
            if (!root)
                root.set(parent->root->setPropTree(name,createPTree(name)));
            assertex(root);
        }
        return root.get();
    }

    void reinit()
    {
        root.set(parent->root->queryPropTree(name.get()));
    }

    virtual void Link(void) const       { parent->Link(); } 
    virtual bool Release(void) const    { return parent->Release(); }



};

#define IMPLEMENT_DFUWUCHILD    virtual void Link(void) const       { CLinkedDFUWUchild::Link(); } \
                                virtual bool Release(void) const    { return CLinkedDFUWUchild::Release(); }


class CDFUprogress: public CLinkedDFUWUchild, implements IDFUprogress
{
public:
    IMPLEMENT_DFUWUCHILD;

    bool getReplicating() const
    {
        CriticalBlock block(parent->crit);
        return queryRoot()->getPropInt("@replicating",0)!=0;
    }

    unsigned getPercentDone() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned)queryRoot()->getPropInt("@percentdone");
    }

    unsigned getSecsLeft() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned)queryRoot()->getPropInt("@secsleft");
    }

    StringBuffer &getTimeLeft(StringBuffer &str) const
    {
        CriticalBlock block(parent->crit);
        queryRoot()->getProp("@timeleft",str);
        return str;
    }
    unsigned __int64 getScaledDone() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned __int64)queryRoot()->getPropInt64("@scaleddone");
    }
    unsigned __int64 getScaledTotal() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned __int64)queryRoot()->getPropInt64("@scaledtotal");
    }
    StringBuffer &getScale(StringBuffer &str) const
    {
        CriticalBlock block(parent->crit);
        queryRoot()->getProp("@scale",str);
        return str;
    }
    unsigned getKbPerSecAve() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned)queryRoot()->getPropInt("@kbpersecave");
    }
    unsigned getKbPerSec() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned)queryRoot()->getPropInt("@kbpersec");
    }
    unsigned getSlavesDone() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned)queryRoot()->getPropInt("@slavesdone");
    }
    unsigned getTotalNodes() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned)queryRoot()->getPropInt("@totalnodes");
    }
    StringBuffer &getTimeTaken(StringBuffer &str) const
    {
        CriticalBlock block(parent->crit);
        queryRoot()->getProp("@timetaken",str);
        return str;
    }
    StringBuffer &formatProgressMessage(StringBuffer &str) const
    {
        CriticalBlock block(parent->crit);
        StringBuffer timeleft;
        StringBuffer scale;
        unsigned pc = getPercentDone();
        str.appendf("%d%% Done",pc);
        bool replicating = getReplicating();
        if (replicating&&(pc<100))
            str.appendf(", Replicating");
        getTimeLeft(timeleft);
        if (timeleft.length()&&((pc!=100)||(stricmp(timeleft.str(),"unknown")!=0)))
            str.appendf(", %s left",timeleft.str());
        if (!replicating) {
            unsigned __int64 sdone = getScaledDone();
            unsigned __int64 stotal = getScaledTotal();
            getScale(scale); 
            getKbPerSecAve();
            unsigned kbs = getKbPerSecAve();
            if ((kbs!=0)||(sdone!=0)||(stotal!=0)) {
                str.append(" (");
                if ((sdone!=0)||(stotal!=0)) {
                   str.appendf("%" I64F "d/%" I64F "d%s",sdone,stotal,scale.str());
                   if (kbs!=0)
                       str.append(' ');
                }
                if (kbs!=0)
                    str.appendf("@%dKB/sec",kbs);
                str.append(')');
            }
            kbs = getKbPerSec();
            if (kbs!=0)
                str.appendf(" current rate=%dKB/sec",kbs);
        }
        unsigned totnodes=getTotalNodes();
        if (totnodes==0) { // print subdone/done
            StringBuffer s;
            if (queryRoot()->getProp("@subinprogress",s)&&s.length())
                str.appendf(" %s in progress",s.str());
            if (queryRoot()->getProp("@subdone",s.clear())&&s.length())
                str.appendf(" %s completed",s.str());
        }
        else
            str.appendf(" [%d/%dnodes]",getSlavesDone(),totnodes);
        return str;
    }
    StringBuffer &formatSummaryMessage(StringBuffer &str) const
    {
        CriticalBlock block(parent->crit);
        Owned<IExceptionIterator> ei = parent->getExceptionIterator();
        if (ei->first()) {
            IException &e = ei->query();
            str.append("Failed: ");
            e.errorMessage(str);
        }
        else {
            StringBuffer timetaken;
            str.appendf("Total time taken %s", getTimeTaken(timetaken).str());
            unsigned kbs = getKbPerSecAve();
            if (kbs!=0)
                str.appendf(", Average transfer %dKB/sec", kbs);
        }
        return str;
    }
    DFUstate getState() const 
    {
        CriticalBlock block(parent->crit);
        return decodeDFUstate(queryRoot()->queryProp("@state"));
    }
    CDateTime &getTimeStarted(CDateTime &val) const 
    {
        CriticalBlock block(parent->crit);
        StringBuffer str;
        queryRoot()->getProp("@timestarted",str);
        val.setString(str.str());
        return val; 
    }
    CDateTime &getTimeStopped(CDateTime &val) const 
    {
        CriticalBlock block(parent->crit);
        StringBuffer str;
        queryRoot()->getProp("@timestopped",str);
        val.setString(str.str());
        return val;
    }
    void setProgress(   unsigned percentDone, unsigned secsLeft, const char * timeLeft,
                        unsigned __int64 scaledDone, unsigned __int64 scaledTotal, const char * scale,
                        unsigned kbPerSecAve, unsigned kbPerSecRate,
                        unsigned slavesDone, bool replicating)
    {
        CriticalBlock block(parent->crit);
        queryRoot()->setPropInt("@percentdone",(int)percentDone);
        queryRoot()->setPropInt("@secsleft",(int)secsLeft);
        queryRoot()->setProp("@timeleft",timeLeft);
        queryRoot()->setPropInt64("@scaleddone",scaledDone);
        queryRoot()->setPropInt64("@scaledtotal",scaledTotal);
        queryRoot()->setProp("@scale",scale);
        queryRoot()->setPropInt("@kbpersecave",(int)kbPerSecAve);
        queryRoot()->setPropInt("@kbpersec",(int)kbPerSecRate);
        queryRoot()->setPropInt("@slavesdone",(int)slavesDone);
        queryRoot()->setPropInt("@replicating",replicating?1:0);
        parent->commit();
    }
    void setPercentDone(unsigned percentDone)
    {
        CriticalBlock block(parent->crit);
        queryRoot()->setPropInt("@percentdone",(int)percentDone);
    }
    void clearProgress()
    {
        CriticalBlock block(parent->crit);
        queryRoot()->removeProp("@percentdone");
        queryRoot()->removeProp("@secsleft");
        queryRoot()->removeProp("@timetaken");
        queryRoot()->removeProp("@timeleft");
        queryRoot()->removeProp("@scaleddone");
        queryRoot()->removeProp("@scaledtotal");
        queryRoot()->removeProp("@scale");
        queryRoot()->removeProp("@kbpersecave");
        queryRoot()->removeProp("@kbpersec");
        queryRoot()->removeProp("@slavesdone");
        parent->commit();
    }
    void setDone(const char * timeTaken, unsigned kbPerSec, bool set100pc)
    {
        CriticalBlock block(parent->crit);
        if (timeTaken) {
            queryRoot()->setProp("@timetaken",timeTaken);
            queryRoot()->setPropInt("@kbpersecave",(int)kbPerSec);
            queryRoot()->setPropInt("@kbpersec",(int)kbPerSec);
        }
        if (set100pc)
            queryRoot()->setPropInt("@percentdone",(int)100);
        queryRoot()->setPropInt("@replicating",0);
        parent->commit();
    }
    void setState(DFUstate state) 
    {
        CriticalBlock block(parent->crit);
        CDateTime dt;
        switch (state) {
        case DFUstate_started:
            dt.setNow();
            setTimeStarted(dt); 
            break;
        case DFUstate_aborting:
            {
                DFUstate oldstate = getState();
                if ((oldstate==DFUstate_aborted)||(oldstate==DFUstate_failed)||(oldstate==DFUstate_finished)) 
                    state = oldstate;
            }
            // fall through
        case DFUstate_aborted:
        case DFUstate_failed:
        case DFUstate_finished:
            if (parent->removeQueue()&&(state==DFUstate_aborting))
                state = DFUstate_aborted;
            dt.setNow();
            setTimeStopped(dt); 
            break;
        }
        StringBuffer s;
        encodeDFUstate(state,s);
        queryRoot()->setProp("@state",s.str());
        parent->commit();
    }
    void setTimeStarted(const CDateTime &val) 
    {
        CriticalBlock block(parent->crit);
        StringBuffer str;
        val.getString(str);
        queryRoot()->setProp("@timestarted",str.str());
    }
    void setTimeStopped(const CDateTime &val) 
    {
        CriticalBlock block(parent->crit);
        StringBuffer str;
        val.getString(str);
        queryRoot()->setProp("@timestopped",str.str());
    }
    void setTotalNodes(unsigned val)
    {
        CriticalBlock block(parent->crit);
        queryRoot()->setPropInt("@totalnodes",(int)val);
    }


    StringBuffer &getSubInProgress(StringBuffer &str) const
    {
        CriticalBlock block(parent->crit);
        queryRoot()->getProp("@subinprogress",str);
        return str;
    }
    
    StringBuffer &getSubDone(StringBuffer &str) const
    {
        CriticalBlock block(parent->crit);
        queryRoot()->getProp("@subdone",str);
        return str;
    }
    
    void setSubInProgress(const char *str)
    {
        CriticalBlock block(parent->crit);
        queryRoot()->setProp("@subinprogress",str);
    }

    void setSubDone(const char *str)
    {
        CriticalBlock block(parent->crit);
        queryRoot()->setProp("@subdone",str);
    }

};

class CDFUmonitor: public CLinkedDFUWUchild, implements IDFUmonitor
{
public:
    IMPLEMENT_DFUWUCHILD;

    unsigned getCycleCount() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned)queryRoot()->getPropInt("@cycles");
    }

    unsigned getShotCount() const
    {
        CriticalBlock block(parent->crit);
        return (unsigned)queryRoot()->getPropInt("@shots");
    }

    bool getHandlerEp(SocketEndpoint &ep) const
    {
        const char *s = queryRoot()->queryProp("@handler");
        if (s&&*s) {
            ep.set(s);
            if (!ep.isNull())
                return true;
        }
        return false;
        
    }

    StringBuffer &getEventName(StringBuffer &str)const 
    {
        queryRoot()->getProp("@eventname",str);
        return str;
    }

    bool getSub()const 
    {
        return queryRoot()->getPropBool("@sub");
    }

    unsigned getShotLimit() const
    {
        return queryRoot()->getPropInt("@shotlimit");
    }

    unsigned getTriggeredList(StringAttrArray &files) const
    {
        MemoryBuffer buf;
        if (!queryRoot()->getPropBin("triggeredList",buf)||(buf.length()<sizeof(unsigned))) 
            return 0;
        unsigned n;
        buf.read(n);
        for (unsigned i=0;i<n;i++) {
            StringAttrItem &item = *new StringAttrItem;
            buf.read(item.text);
            files.append(item);
        }
        return n;
    }

    void setCycleCount(unsigned val)
    {
        CriticalBlock block(parent->crit);
        queryRoot()->setPropInt("@cycles",(int)val);
    }

    void setShotCount(unsigned val)
    {
        CriticalBlock block(parent->crit);
        queryRoot()->setPropInt("@shots",(int)val);
    }

    void setHandlerEp(const SocketEndpoint &ep)
    {
        if (ep.isNull()) 
            queryRoot()->removeProp("@handler");
        else {
            StringBuffer s;
            queryRoot()->setProp("@handler",ep.getUrlStr(s).str());
        }
    }

    void setSub(bool sub) 
    {
        queryRoot()->setPropBool("@sub",sub);

    }

    void setEventName(const char *lfn)
    {
        queryRoot()->setProp("@eventname",lfn);
    }

    void setShotLimit(unsigned limit)
    {
        queryRoot()->setPropInt("@shotlimit",limit);
    }

    void setTriggeredList(const StringAttrArray &files)
    {
        MemoryBuffer buf;
        unsigned n = files.ordinality();
        buf.append(n);
        for (unsigned i=0;i<n;i++) {
            StringAttrItem &item = files.item(i);
            buf.append(item.text.get());
        }
        queryRoot()->setPropBin("triggeredList",buf.length(),buf.toByteArray());
    }


};

static void printDesc(IFileDescriptor *desc)
{
    // ** TBD
    if (desc) {
        StringBuffer tmp1;
        StringBuffer tmp2;
        unsigned n = desc->numParts();
        PROGLOG("  numParts = %d",n);
        PROGLOG("  numCopies(0) = %d",desc->numCopies(0));
//      PROGLOG("  groupWidth = %d",desc->queryClusterGroup(0)->ordinality());
//      PROGLOG("  numSubFiles = %d",desc->getNumSubFiles());
//      Owned<IGroup> group = desc->getGroup(0);
//      PROGLOG("  group(0) = %d,%s,...,%s",group->ordinality(),group->queryNode(0).endpoint().getUrlStr(tmp1.clear()).str(),group->queryNode(group->ordinality()-1).endpoint().getUrlStr(tmp2.clear()).str());
//      group.setown(desc->getGroup(1));
//      PROGLOG("  group(1) = %d,%s,...,%s",group->ordinality(),group->queryNode(0).endpoint().getUrlStr(tmp1.clear()).str(),group->queryNode(group->ordinality()-1).endpoint().getUrlStr(tmp2.clear()).str());
        unsigned copy;
        for (copy = 0;copy<2;copy++) {
            unsigned i;
            for (i=0;i<n;i++) {
                RemoteFilename rfn;
                desc->getFilename(i,copy,rfn);
                PROGLOG("  file (%d,%d) = %s",copy,i,rfn.getRemotePath(tmp1.clear()).str());
            }
        }
    }
}




class CDFUfileSpec: public CLinkedDFUWUchild, implements IDFUfileSpec
{
    unsigned numpartsoverride;
    mutable DFD_OS os;
    mutable Owned<IPropertyTree> nullattr;

public:
    CDFUfileSpec()
    {
        numpartsoverride = 0;
        os = DFD_OSdefault;
    }

    IMPLEMENT_DFUWUCHILD;

    IFileDescriptor *getFileDescriptor(bool iskey,bool ignorerepeats) const 
    {
        unsigned nc = numClusters();
        unsigned n=nc?getNumParts(0,iskey):0;
        if (!n) {
            StringBuffer lname;
            if (getLogicalName(lname).length()) {
                CDfsLogicalFileName lfn;
                lfn.set(lname.str());
                Owned<IUserDescriptor> userdesc = createUserDescriptor();
                SocketEndpoint foreignep;
                bool isforeign = getForeignDali(foreignep);
                if (lfn.isForeign())
                    isforeign = true;
                else if (isforeign)
                    lfn.setForeign(foreignep,false);
                StringBuffer username;
                StringBuffer password;
                if (!isforeign||!getForeignUser(username,password)) {
                    parent->getUser(username);
                    parent->getPassword(password);
                }
                userdesc->set(username.str(),password.str());
                Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lfn,userdesc);
                if (file)
                    return file->getFileDescriptor();
            }
            StringBuffer s;
            SocketEndpoint ep;
            if ((getGroupName(0,s).length()!=0)&&!getForeignDali(ep) ) {
                Owned<IGroup> grp = queryNamedGroupStore().lookup(s.str());
                if (!grp) 
                    throw MakeStringException(-1,"CDFUfileSpec: Cluster %s not found",s.str());
            }
            throw MakeStringException(-1,"CDFUfileSpec: No parts found for file!");
        }
        IPropertyTree *p = createPTreeFromIPT(queryProperties());
        if (iskey) {
            p->removeProp("@blockCompressed");      // can't compress keys
        }
        Owned<IFileDescriptor> ret = createFileDescriptor(p);
        StringBuffer s;
        bool dirgot = false;
        if (getDirectory(s).length()) {   
            dirgot = true;
            ret->setDefaultDir(s.str());
        }
        if (getTitle(s.clear()).length())
            ret->setTraceName(s.str());
        else if (getLogicalName(s).length())
            ret->setTraceName(s.str());
        else if (getFileMask(s).length())
            ret->setTraceName(s.str());
        bool initdone = false;
        StringBuffer partmask;
        if (getFileMask(partmask).length())
            ret->setPartMask(partmask.str());
        for (unsigned clustnum=0;clustnum<nc;clustnum++) {
            if (clustnum)
                n=getNumParts(clustnum,iskey);
            if (!n)
                continue;
            ClusterPartDiskMapSpec mspec;               
            getClusterPartDiskMapSpec(clustnum,mspec);  
            if (ignorerepeats&&(mspec.repeatedPart!=CPDMSRP_notRepeated)) {
                if (mspec.repeatedPart&CPDMSRP_onlyRepeated)
                    continue;  // ignore only repeated cluster
                mspec.repeatedPart = CPDMSRP_notRepeated;
                mspec.flags &= ~CPDMSF_repeatedPart;
            }
            const char *grpname=NULL;
            StringBuffer gs;
            if (getGroupName(clustnum,gs).length()) 
                grpname = gs.str();
            Owned<IGroup> grp(getGroup(clustnum));
            if (dirgot&&grp.get()&&partmask.length()) {  // not sure if need dir here 
                if (!initdone) {
                    ret->setNumParts(n); // NB first cluster determines number of parts
                    initdone = true;
                }
                ret->addCluster(grpname,grp,mspec);
            }
            else if (!initdone) { // don't do if added cluster already
                unsigned i;
                for (i=0;i<n;i++) {
                    RemoteFilename rfn;
                    getPartFilename(clustnum,i,rfn,iskey);
                    ret->setPart(i,rfn,queryPartProperties(i));
                }
                ret->endCluster(mspec);
                break;
            }
            else
                throw MakeStringException(-1,"CDFUfileSpec: getFileDescriptor: Could not find group for cluster %d", clustnum);
        }
        return ret.getClear();
    }
    StringBuffer &getTitle(StringBuffer &str) const 
    {
        queryRoot()->getProp("@title",str);
        return str;
    }

    StringBuffer &getRawDirectory(StringBuffer &str) const 
    {
        queryRoot()->getProp("@directory",str);
        return str;
    }   
    StringBuffer &getDirectory(StringBuffer &str) const 
    {
        if (!queryRoot()->getProp("@directory",str)) {
            StringBuffer tmp;
            if (getRoxiePrefix(tmp).length()) 
                tmp.append("::");
            size32_t tmpl = tmp.length();
            if (getLogicalName(tmp).length()>tmpl) {
                CDfsLogicalFileName lfn;
                if (!lfn.setValidate(tmp.str(),true)) {
                    throw MakeStringException(-1,"DFUWU: Logical name %s invalid(2)",tmp.str());
                }
                StringBuffer baseoverride;
                getClusterPartDefaultBaseDir(NULL,baseoverride);
                bool iswin;
                getWindowsOS(iswin); // sets os
                makePhysicalPartName(lfn.get(),0,0,str,false,os,baseoverride.str());
            }
        }
        return str;
    }
    StringBuffer &getLogicalName(StringBuffer &str)const 
    {
        queryRoot()->getProp("OrigName",str);
        return str;
    }
    StringBuffer &getFileMask(StringBuffer &str) const 
    {
        if (!queryRoot()->getProp("@partmask",str)) {
            StringBuffer tmp;
            getLogicalName(tmp);
            if (tmp.length()) {
                CDfsLogicalFileName lfn;
                if (!lfn.setValidate(tmp.str()))
                    throw MakeStringException(-1,"DFUWU: Logical name %s invalid",tmp.str());
                lfn.getTail(str);
                str.append("._$P$_of_$N$");
            }
        }
        return str;
    }

    StringBuffer &getRawFileMask(StringBuffer &str) const 
    {
        queryRoot()->getProp("@partmask",str);
        return str;
    }
    
    StringBuffer &getGroupName(unsigned clustnum,StringBuffer &str) const 
    {
        // first see if Cluster spec
        if (clustnum<numClusters()) {
            StringBuffer xpath;
            xpath.appendf("Cluster[%d]",clustnum+1);
            IPropertyTree *ct = queryRoot()->queryPropTree(xpath.str()); 
            if (ct&&ct->getProp("@name",str))
                return str;
            StringBuffer s;     // old style
            queryRoot()->getProp("@group",s); 
            StringArray gs;
            getFileGroups(s.str(),gs);
            if (clustnum<gs.ordinality()) 
                str.append(gs.item(clustnum));
        }
        return str;
    }
    IPropertyTree *queryProperties() const 
    {
        IPropertyTree *ret = queryRoot()->queryPropTree("Attr");
        if (!ret) {
            ret = nullattr.get();
            if (!ret) {
                nullattr.setown(createPTree("Attr"));
                ret = nullattr.get();
            }
        }
        return ret;
    }
    IPropertyTree *queryUpdateProperties() 
    {
        IPropertyTree *ret = queryRoot()->queryPropTree("Attr");
        if (!ret)
            ret = setProperties(createPTree("Attr"));
        return ret;
    }
    size32_t getRecordSize() const 
    {
        return queryProperties()->getPropInt("@recordSize");
    }
    bool isCompressed() const 
    {
        bool blocked;
        if (!::isCompressed(*queryProperties(),&blocked))
            return false;
        return blocked; // only block compression supported
    }

    IGroup *getGroup(unsigned clustnum) const
    {
        StringBuffer gs;
        if (getGroupName(clustnum,gs).length()) {
            Owned<IGroup> grp = queryNamedGroupStore().lookup(gs.str());
            if (!grp) 
                throw MakeStringException(-1,"DFUWU: Logical group %s not found",gs.str());
            return grp.getClear();
        }
        return NULL;
    }

    RemoteFilename &getPartFilename(unsigned clustnum,unsigned partidx, RemoteFilename &rfn, bool iskey) const 
    {
        // supports both with and without Part
        StringBuffer tmp;
        StringBuffer tmpmask;
        StringBuffer tmpfn;
        SocketEndpoint ep;
        CDfsLogicalFileName lfn;
        StringBuffer dir;
        ClusterPartDiskMapSpec mspec;
        getClusterPartDiskMapSpec(clustnum,mspec);  
        const char *mask = getFileMask(tmpmask).str();
        const char *fn = NULL;
        // now read part
        Owned<IGroup> grp(getGroup(clustnum));        
        unsigned np = getNumParts(clustnum,iskey);
        unsigned npt = queryRoot()->getPropInt("@numparts",np);
        StringBuffer xpath;
        xpath.append("Part[@num=\"").append((partidx%npt)+1).append("\"]");
        IPropertyTree *part = queryRoot()->queryPropTree(xpath.str());
        if (part) {
            const char *ns=part->queryProp("@node");
            if (ns) 
                ep.set(ns);
            if (!getWrap()&&(partidx<npt)) {    // override
                const char *n=part->queryProp("@name");
                if (n&&*n) {
                    if (findPathSepChar(n)) 
                        fn = splitDirTail(n,dir);
                    else
                        fn = n;
                }
            }
        }
        else { // if Parts specified
            MemoryBuffer mb;
            if (queryRoot()->getPropBin("Parts",mb)) {
                Owned<IPropertyTreeIterator> pi = deserializePartAttrIterator(mb);
                ForEach(*pi) {
                    IPropertyTree &part = pi->get();
                    if (part.getPropInt("@num")==(partidx%npt)+1) {
                        const char *ns=part.queryProp("@node");
                        if (ns) 
                            ep.set(ns);
                        if (!getWrap()&&(partidx<npt)) {    // override
                            const char *n=part.queryProp("@name");
                            if (n&&*n) {
                                if (findPathSepChar(n)) 
                                    fn = splitDirTail(n,dir);
                                else
                                    fn = n;
                            }
                        }
                        break;
                    }
                }
            }
        }
        if (!fn)
            fn = mask?expandMask(tmpfn,mask,partidx,np).str():NULL;
        unsigned nn;
        unsigned dn;
        mspec.calcPartLocation(partidx,np,0,grp.get()?grp->ordinality():np,nn,dn);
        // now we should have tail name and possibly ep and dir
        if (!fn||!*fn)
            throw MakeStringException(-1,"DFUWU: cannot construct part file name");
        if (ep.isNull()) {
            if (!grp)
                throw MakeStringException(-1,"DFUWU: cannot determine endpoint for part file");
            ep = grp->queryNode(nn).endpoint();
        }
        StringBuffer tmpout;
        // now its a bit of a kludge but can be multiple filenames 
        if (strchr(fn,',')) {
            StringArray sub;
            RemoteMultiFilename::expand(fn,sub);
            StringBuffer prevdir;
            ForEachItemIn(i1,sub) {
                const char *subfn = sub.item(i1);
                if (!subfn||!*subfn)
                    continue;
                if (tmpout.length())
                    tmpout.append(',');
                if (!isAbsolutePath(subfn)) {
                    if (!dir.length())
                        getDirectory(dir);
                    if ((dir.length()==0)&&(prevdir.length()==0))
                        throw MakeStringException(-1,"DFUWU: cannot determine file part directory for %s",subfn);
                    if (prevdir.length())
                        tmpout.append(prevdir);
                    else
                        tmpout.append(dir);
                    addPathSepChar(tmpout);
                }
                else
                    splitDirTail(subfn,prevdir.clear());
                tmpout.append(subfn);
            }
            fn = tmpout.str();
        }
        else if (!isAbsolutePath(fn)) { // shouldn't be absolute or tail
            if (!dir.length())
                getDirectory(dir);
            if (dir.length()==0)
                throw MakeStringException(-1,"DFUWU: cannot determine file part directory for %s",fn);
            fn = addPathSepChar(dir).append(fn).str();
        }
        StringBuffer filename;
        if (dn) {
            filename.append(fn);
            setReplicateFilename(filename,dn); 
            fn = filename.str();
        }
        rfn.setPath(ep,fn);
        return rfn;
    }
    StringBuffer &getPartUrl(unsigned clustnum,unsigned partidx, StringBuffer &url, bool iskey) const 
    {   // loses port
        RemoteFilename rfn;
        getPartFilename(clustnum,partidx,rfn,iskey);
        if (rfn.queryIP().isNull())
            rfn.getLocalPath(url);
        else
            rfn.getRemotePath(url);
        return url;
    }
    IPropertyTree *queryPartProperties(unsigned partidx) const 
    {
        StringBuffer path;
        path.append("Part[@num=\"").append(partidx+1).append("\"]");
        return queryRoot()->queryPropTree(path.str());
    }

    IPropertyTree *queryUpdatePartProperties(unsigned partidx)
    {
        StringBuffer path;
        path.append("Part[@num=\"").append(partidx+1).append("\"]");
        IPropertyTree *ret = queryRoot()->queryPropTree(path.str());
        if (!ret) {
            ret = queryRoot()->addPropTree("Part",createPTree("Part"));
            ret->setPropInt("@num",partidx+1);
        }
        return ret;
    }   
    
    unsigned getNumParts(unsigned clustnum,bool iskey) const
    {
        unsigned n = numpartsoverride?numpartsoverride:(unsigned)queryRoot()->getPropInt("@numparts",0);
        if (!n) {
            StringBuffer s;
            SocketEndpoint ep;
            if ((getGroupName(clustnum,s).length()!=0)&&!getForeignDali(ep) ) {
                Owned<IGroup> grp = queryNamedGroupStore().lookup(s.str());
                if (grp)
                    n = grp->ordinality();
                else {
                    ERRLOG("DFUWU: Logical group %s not found",s.str());
                    return 0;
                }
                ClusterPartDiskMapSpec mspec;
                getClusterPartDiskMapSpec(clustnum,mspec); 
                if (mspec.flags&CPDMSF_wrapToNextDrv)
                    n*=mspec.maxDrvs;
                if (iskey)
                    n++;
            }
        }
        return n;
    }

    void setTitle(const char *val) 
    {
        queryRoot()->setProp("@title",val);

    }
    void setDirectory(const char *val) 
    {
        queryRoot()->setProp("@directory",val);

    }
    void setLogicalName(const char *val)
    {
        const char *tail=val;
        loop {
            const char *n = strstr(tail,"::");
            if (!n)
                break;
            tail = n+2;
        }
        queryRoot()->setProp("@name",tail);
        queryRoot()->setProp("OrigName",val);
    }
    void setFileMask(const char *val) 
    {
        queryRoot()->setProp("@partmask",val);
    }
    void setNumParts(unsigned val) 
    {
        queryRoot()->setPropInt("@numparts",val);

    }
    void setGroupName(const char *val) 
    {
        StringArray gs;
        getFileGroups(val,gs);
        ForEachItemIn(i,gs) {
            addCluster(gs.item(i));
        }
    }
    IPropertyTree *setProperties(IPropertyTree *val) 
    {
        return queryRoot()->setPropTree("Attr",val);
    }
    void setRecordSize(size32_t size) 
    {
        if (size)
            queryUpdateProperties()->setPropInt("@recordSize",(int)size);
        else
            queryUpdateProperties()->removeProp("@recordSize");
    }
    void setCompressed(bool set) 
    {
        // only block compressed supported
        if (set)
            queryUpdateProperties()->setPropBool("@blockCompressed",true);
        else
            queryUpdateProperties()->removeProp("@blockCompressed");
    }
    virtual void setFromFileDescriptor(IFileDescriptor &fd)
    {
        // use dfsdesc for hard work 
        queryRoot()->setPropTree(NULL,fd.getFileTree(CPDMSF_packParts));
    }

    StringBuffer &quoteStringIfNecessary(const char *s,StringBuffer &dest)
    {
        // we could add other characters
        if (strchr(s,',')||strchr(s,'"')) {
            dest.append('"');
            while (*s) {
                if (*s=='"')
                    dest.append('"');
                dest.append(*s);
                s++;
            }
            dest.append('"');
        }
        else
            dest.append(s);
        return dest;
    }

    virtual void setMultiFilename(RemoteMultiFilename &rmfn)
    {
        // first check for common directory
        StringBuffer path;
        StringBuffer dir;
        ForEachItemIn(i1,rmfn) {
            const RemoteFilename &rfn = rmfn.item(i1);
            rfn.getLocalPath(path.clear());
            const char *s=path.str();
            size32_t dirlen = 0;
            while (*s) {
                if (isPathSepChar(*s)) { // must support unix/windows from either platform 
                    dirlen = s-path.str();
                    if ((dirlen==0)||((dirlen==2)&&!isPathSepChar(path.charAt(0))&&(path.charAt(1)==':')))
                        dirlen++;
                }
                s++;
            }
            if (i1==0)
                dir.append(dirlen,path.str());
            else {
                size32_t dl = dir.length();
                if (dl>dirlen)
                    dl = dirlen;
                s = path.str();
                const char *t = dir.str();
                size32_t l = 0;
                while ((l<dl)&&(s[l]==t[l]))    // we should probably case insensitive for windows
                    l++;
                while (l&&!isPathSepChar(s[l]))
                    l--;
                if (l<dir.length()) {
                    while (l&&!isPathSepChar(t[l]))
                        l--;
                    dir.setLength(l);
                }
            }
            if (dir.length()==0)
                break;
        }
        if ((dir.length()==2)&&(dir.charAt(1)==':'))
            dir.append('\\');
        setDirectory(dir.str());
        StringBuffer mask;
        ForEachItemIn(i2,rmfn) {                    // now set mask
            const RemoteFilename &rfn = rmfn.item(i2);
            rfn.getLocalPath(path.clear());
            const char *s=path.str()+dir.length();
            if (isPathSepChar(*s)&&dir.length())
                s++;
            if (mask.length())
                mask.append(',');
            quoteStringIfNecessary(s,mask);
        }
        setFileMask(mask.str());
        queryRoot()->setPropInt("@numparts",1);
        IPropertyTree * part = queryRoot()->setPropTree("Part",createPTree("Part"));
        part->setPropInt("@num",1);
        StringBuffer url;
        rmfn.queryEndpoint().getUrlStr(url);
        part->setProp("@node",url.str());
    }

    virtual void setSingleFilename(RemoteFilename &rfn)
    {
        RemoteMultiFilename rmfn;
        rmfn.append(rfn);
        setMultiFilename(rmfn);
    }



    size32_t getMaxRecordSize() const
    {
        size32_t ret = queryProperties()->getPropInt("@maxRecordSize",8192);
        if (ret==0) {
            if (getFormat()==DFUff_fixed)
                ret = getRecordSize();  // if fixed defaults to recordSize
            ret = 8192;
        }
        return ret;
    }

    virtual void setMaxRecordSize(size32_t size)
    {
        if (size)
            queryUpdateProperties()->setPropInt("@maxRecordSize",(int)size);
        else
            queryUpdateProperties()->removeProp("@maxRecordSize");
    }

    virtual void setFormat(DFUfileformat format)
    {
        StringBuffer s;
        CDFUfileformat::encode(format,s);
        queryUpdateProperties()->setProp("@format",s.str());
    }

    virtual DFUfileformat getFormat() const
    {
        return CDFUfileformat::decode(queryProperties()->queryProp("@format"));
    }

    virtual void getCsvOptions(StringBuffer &separate,StringBuffer &terminate,StringBuffer &quote,StringBuffer &escape,bool &quotedTerminator) const
    {
        IPropertyTree *t = queryProperties();
        const char *sep=t->queryProp("@csvSeparate");
        separate.append(sep?sep:"\\,");
        const char *ter=t->queryProp("@csvTerminate");
        terminate.append(ter?ter:"\\n,\\r\\n");
        const char *quo=t->queryProp("@csvQuote");
        quote.append(quo?quo:"\"");
        const char *esc=t->queryProp("@csvEscape");
        if (esc && *esc)
            escape.set(esc);
        quotedTerminator = t->getPropBool("@quotedTerminator", true);
    }

    void setCsvOptions(const char *separate,const char *terminate,const char *quote,const char *escape,bool quotedTerminator)
    {
        IPropertyTree *t = queryUpdateProperties();
        if (separate) //Enable to pass zero string to override default separator
            t->setProp("@csvSeparate",separate);
        if (terminate && *terminate)
            t->setProp("@csvTerminate",terminate);
        if (quote)  //Enable to pass zero string to override default quote
            t->setProp("@csvQuote",quote);
        if (escape && *escape)
            t->setProp("@csvEscape",escape);
        t->setPropBool("@quotedTerminator", quotedTerminator);
    }

    StringBuffer &getRowTag(StringBuffer &str)const 
    {
        queryProperties()->getProp("@rowTag",str);
        return str;
    }

    void setRowTag(const char *str)
    {
        IPropertyTree *t = queryUpdateProperties();
        t->setProp("@rowTag",str);
    }

    void setFromXML(const char *xml)
    {
        // the following is slightly odd: xml->tree->file->tree
        Owned<IPropertyTree> t = createPTreeFromXMLString(xml);
        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(t,&queryNamedGroupStore(),0);
        setFromFileDescriptor(*fdesc);
    }


    void setForeignDali(const SocketEndpoint &ep) 
    {
        // only used for source of copy
        IPropertyTree *t = queryUpdateProperties();
        StringBuffer s;
        t->setProp("@foreignDali",ep.getUrlStr(s).str());
    }
        
    bool getForeignDali(SocketEndpoint &ep) const
    {
        // only used for source of copy
        const char *s = queryProperties()->queryProp("@foreignDali");
        if (!s||!*s)
            return false;
        ep.set(s);
        return true;
    }

    void setForeignUser(const char *user,const char *password)
    {
        IPropertyTree *t = queryUpdateProperties();
        t->setProp("@foreignUser",user);
        StringBuffer pw;            // minimal encryprion to obscure (will need improvement)
        pw.append(parent->queryId());
        pw.append(password);
        StringBuffer buf;
        encrypt(buf,pw.str());
        t->setProp("@foreignPassword",buf.str());
    }
    bool getForeignUser(StringBuffer &user,StringBuffer &password) const
    {
        IPropertyTree *t = queryProperties();
        const char *s = t->queryProp("@foreignUser");
        if (!s||!*s)
            return false;
        user.append(s);
        StringBuffer pw;
        t->getProp("@foreignPassword",pw);
        if (pw.length()) {
            StringBuffer buf;
            decrypt(buf,pw.str());    // minimal encryprion to obscure (will need improvement)
            const char *p = buf.str();
            const char *i = parent->queryId();
            while (*p&&*i&&(*p==*i)) {
                p++;
                i++;
            }
            password.append(p);
        }
        return true;
    }
    

    bool getWrap() const
    {
        return queryRoot()->getPropInt("@wrap")!=0;
    }

    bool getMultiCopy() const
    {
        return queryRoot()->getPropInt("@multiCopy")!=0;
    }

    void setWrap(bool val)
    {
        queryRoot()->setPropInt("@wrap",val?1:0);
    }

    void setMultiCopy(bool val)
    {
        queryRoot()->setPropInt("@multicopy",val?1:0);
    }

    void setNumPartsOverride(unsigned num)
    {
        numpartsoverride = num;
    }

    
    StringBuffer &getDiffKey(StringBuffer &str) const 
    {
        queryRoot()->getProp("@diffKey",str);
        return str;
    }
    void setDiffKey(const char *str)
    {
        queryRoot()->setProp("@diffKey",str);
    }

    void getClusterPartDiskMapSpec(unsigned clusternum, ClusterPartDiskMapSpec &spec) const
    {
        unsigned nc = numClusters();
        StringBuffer xpath;
        xpath.appendf("Cluster[%d]",clusternum+1);
        IPropertyTree *pt = queryRoot()->queryPropTree(xpath.str());
        if (pt) 
            spec.fromProp(pt); 
        else { 
            ClusterPartDiskMapSpec defspec;
            spec = defspec;
        }
    }

    bool getClusterPartDiskMapSpec(const char *clustername, ClusterPartDiskMapSpec &spec) const
    {

        unsigned clusternum;
        if (!findCluster(clustername,clusternum))
            return false;
        getClusterPartDiskMapSpec(clusternum,spec);
        return true;
    }

    unsigned findCluster(const char *clustername, unsigned &clusternum) const
    {
        Owned<IPropertyTreeIterator> iter = queryRoot()->getElements("Cluster");
        if (!clustername) {
            if (!iter->first()) {
                iter.clear();
                IPropertyTree *pt= createPTree("Cluster");
                ClusterPartDiskMapSpec spec;
                spec.toProp(pt);
                StringBuffer grpname; // this shouldn't be set but if it is then use
                if (queryRoot()->getProp("@group",grpname)) {
                    const char * s = grpname.str();
                    const char *e = strchr(s,',');
                    if (e)
                        grpname.setLength(e-s);
                }
                if (grpname.length()) {
                    queryRoot()->setProp("@group",grpname.str());
                    pt->setProp("@name",grpname.str());
                }
                queryRoot()->addPropTree("Cluster",pt);
                queryRoot()->setPropInt("@numclusters",1);
            }
            clusternum = 0;
            return true;
        }
        // done via iterate to catch correct index
        clusternum = 0;
        ForEach(*iter) {
            const char *name = iter->query().queryProp("@name");
            if (name&&(stricmp(name,clustername)==0))
                return true;
            clusternum++;
        }
        return false;
    }


    void setClusterPartDiskMapSpec(unsigned clusternum, ClusterPartDiskMapSpec &spec)
    {
        StringBuffer xpath;
        xpath.appendf("Cluster[%d]",clusternum+1);
        IPropertyTree *pt = queryRoot()->queryPropTree(xpath.str());
        if (pt) 
            spec.toProp(pt);  
    }

    unsigned addCluster(const char *clustername)
    {
        StringBuffer _clustername;
        if (clustername) 
            clustername = _clustername.append(clustername).trim().toLowerCase().str();
        unsigned clusternum;
        if (!findCluster(clustername,clusternum)) {
            IPropertyTree *pt = createPTree("Cluster");
            if (clustername&&*clustername)
                pt->setProp("@name",clustername);
            queryRoot()->addPropTree("Cluster",pt);
            queryRoot()->setPropInt("@numclusters",clusternum+1);
            StringBuffer grps;
            Owned<IPropertyTreeIterator> iter = queryRoot()->getElements("Cluster");
            ForEach(*iter) {
                const char *name = iter->query().queryProp("@name");
                if (name&&*name) {
                    if (grps.length())
                        grps.append(',');
                    grps.append(name);
                }
            }
            queryRoot()->setProp("@group",grps.str());
        }
        return clusternum;
    }

    void setClusterPartDiskMapSpec(const char *clustername, ClusterPartDiskMapSpec &spec)
    {
        setClusterPartDiskMapSpec(addCluster(clustername),spec);
    }

    void setClusterPartDefaultBaseDir(const char *clustername,const char *basedir) 
    {
        unsigned clusternum;
        if (findCluster(clustername,clusternum)) {
            ClusterPartDiskMapSpec spec;
            getClusterPartDiskMapSpec(clusternum, spec);
            spec.setDefaultBaseDir(basedir);
            setClusterPartDiskMapSpec(clusternum, spec);
        }
    }

    void setClusterPartDiskMapping(DFUclusterPartDiskMapping val,const char *basedir, const char *clustername, bool repeatlast, bool onlyrepeated)
    {
        ClusterPartDiskMapSpec spec;
        switch(val) {
        case DFUcpdm_c_replicated_by_d:
            spec.defaultCopies = DFD_DefaultCopies;
            break;
        case DFUcpdm_c_only:
            spec.defaultCopies = DFD_NoCopies;
            break;
        case DFUcpdm_d_only:
            spec.defaultCopies = DFD_NoCopies;
            spec.startDrv = 1;
            break;
        case DFUcpdm_c_then_d:
            spec.defaultCopies = DFD_NoCopies;
            spec.flags = CPDMSF_wrapToNextDrv;
            break;
        }
        if (basedir&&*basedir)
            spec.setDefaultBaseDir(basedir);
        if (repeatlast) 
            spec.setRepeatedCopies(CPDMSRP_lastRepeated,onlyrepeated);
        setClusterPartDiskMapSpec(clustername,spec);

    }


    StringBuffer &getClusterPartDefaultBaseDir(const char *clustername,StringBuffer &str) const
    {
        ClusterPartDiskMapSpec spec;
        if (getClusterPartDiskMapSpec(clustername,spec)&&!spec.defaultBaseDir.isEmpty())
            str.append(spec.defaultBaseDir);
        return str;
    }


    unsigned numClusters() const
    {
        return queryRoot()->getPropInt("@numclusters",1);
    }

    void setReplicateOffset(int val)
    {
        unsigned nc = numClusters();    // sets for all
        for (unsigned i=0;i<nc;i++) {
            ClusterPartDiskMapSpec spec;
            getClusterPartDiskMapSpec(i,spec);
            spec.replicateOffset = val;
            setClusterPartDiskMapSpec(i,spec);
        }
    }

    void setWindowsOS(bool iswin)
    {
        os = iswin?DFD_OSwindows:DFD_OSunix;
    }

    bool getWindowsOS(bool &iswin) const
    {
#ifdef _WIN32
        iswin = true;
#else
        iswin = false;
#endif
        switch (os) {
        case DFD_OSwindows:
            iswin = true;
            return true;
        case DFD_OSunix:
            iswin = true;
            return true;
        }
        StringBuffer dir;
        if (!queryRoot()->getProp("@directory",dir)) 
            getClusterPartDefaultBaseDir(NULL,dir);
        if (!dir.length())
            return false;
        iswin = getPathSepChar(dir.str())=='\\';
        os = iswin?DFD_OSwindows:DFD_OSunix;
        return true;
    }

    void setRoxiePrefix(const char *val)
    {
        queryRoot()->setProp("@roxiePrefix",val);
    }

    StringBuffer &getRoxiePrefix(StringBuffer &str) const
    {
        queryRoot()->getProp("@roxiePrefix",str);
        return str.toLowerCase();
    }

    bool getRemoteGroupOverride() const 
    {
        return queryRoot()->getPropBool("@remoteGroupOverride");
    }
    
    void setRemoteGroupOverride(bool set)
    {
        queryRoot()->setPropBool("@remoteGroupOverride",set);
    }

};

class CDFUoptions: public CLinkedDFUWUchild, implements IDFUoptions
{
public:
    IMPLEMENT_DFUWUCHILD;

    bool getNoSplit() const
    {
        return queryRoot()->getPropInt("@nosplit")!=0;
    }

    bool getReplicate() const
    {
        return (queryRoot()->getPropInt("@replicate")!=0);
    }

    bool getRecover() const
    {
        return queryRoot()->getPropInt("@recover")!=0;
    }


    bool getNoRecover() const
    {
        return queryRoot()->getPropInt("@noRecover")!=0;
    }

    bool getIfNewer() const
    {
        return queryRoot()->getPropInt("@ifNewer")!=0;
    }

    bool getIfModified() const
    {
        return queryRoot()->getPropInt("@ifModified")!=0;
    }

    bool getSuppressNonKeyRepeats() const
    {
        return queryRoot()->getPropInt("@suppressNonKeyRepeats")!=0;
    }

    bool getSlavePathOverride(StringBuffer &path) const 
    {
        return queryRoot()->getProp("@slave",path)&&(path.length()!=0);
    }

    bool getCrcCheck() const
    {
        return queryRoot()->getPropInt("@crcCheck")!=0;
    }

    __int64 getRecover_ID() const
    {
        return queryRoot()->getPropInt64("@recover_ID");
    }

    unsigned getmaxConnections() const
    {
        return (unsigned)queryRoot()->getPropInt("@maxConnections");
    }

    bool getCrc() const
    {
        return queryRoot()->getPropInt("@crc")!=0;
    }

    unsigned getRetry() const
    {
        return queryRoot()->getPropInt("@retry")!=0;
    }

    bool getPush() const
    {
        return queryRoot()->getPropInt("@push")!=0;
    }

    bool getKeepHeader() const
    {
        return queryRoot()->getPropInt("@keepHeader")!=0;
    }

    bool getPull() const
    {
        return queryRoot()->getPropInt("@pull")!=0;
    }

    unsigned getThrottle() const
    {
        return queryRoot()->getPropInt("@throttle");
    }

    size32_t getTransferBufferSize() const
    {
        return(size32_t)queryRoot()->getPropInt("@transferBufferSize");
    }

    bool getVerify() const
    {
        return queryRoot()->getPropInt("@verify")!=0;
    }

    bool getOverwrite() const
    {
        return queryRoot()->getPropInt("@overwrite")!=0;
    }

    DFUreplicateMode getReplicateMode(StringBuffer &cluster, bool &repeatlast,bool &onlyrepeated) const
    {
        repeatlast = false;
        onlyrepeated = false;
        const char *s = queryRoot()->queryProp("@replicatecluster");
        if (s&&*s) {
            cluster.append(s);
            repeatlast = queryRoot()->getPropInt("@repeatlast")!=0;
            onlyrepeated = queryRoot()->getPropInt("@onlyrepeated")!=0;
        }
        return (DFUreplicateMode)queryRoot()->getPropInt("@replicatemode");
    }

    IPropertyTree *queryTree() const
    {
        return queryRoot();
    }

    const char * queryPartFilter() const
    {
        return queryRoot()->queryProp("@partfilter");
    }

    const char * queryFooter() const
    {
        return queryRoot()->queryProp("@footer");
    }

    const char * queryHeader() const
    {
        return queryRoot()->queryProp("@header");
    }

    const char * queryGlue() const
    {
        return queryRoot()->queryProp("@glue");
    }


    const char * queryLengthPrefix() const
    {
        return queryRoot()->queryProp("@prefix");
    }

    const char * querySplitPrefix() const
    {
        return queryRoot()->queryProp("@splitPrefix");
    }


    void setNoDelete(bool val)
    {
        queryRoot()->setPropInt("@nodelete",val?1:0);
    }

    void setNoRecover(bool val)
    {
        queryRoot()->setPropInt("@noRecover",val?1:0);
    }

    void setIfNewer(bool val)
    {
        queryRoot()->setPropInt("@ifNewer",val?1:0);
    }

    void setIfModified(bool val)
    {
        queryRoot()->setPropInt("@ifModified",val?1:0);
    }

    void setSuppressNonKeyRepeats(bool val)
    {
        queryRoot()->setPropInt("@suppressNonKeyRepeats",val?1:0);
    }

    void setSlavePathOverride(const char *path)
    {
        if (path&&*path)
            queryRoot()->setProp("@slave",path);
        else
            queryRoot()->removeProp("@slave");
    }


    void setCrcCheck(bool val)
    {
        queryRoot()->setPropInt("@crcCheck",val?1:0);
    }

    void setNoSplit(bool val=true)
    {
        queryRoot()->setPropInt("@nosplit",val?1:0);
    }

    void setReplicate(bool val=true)
    {
        queryRoot()->setPropInt("@replicate",val?1:0);
    }

    void setRecover(bool val=true)
    {
        queryRoot()->setPropInt("@recover",val?1:0);
    }

    void setRecover_ID(__int64 val)
    {
        queryRoot()->setPropInt64("@recover_ID",val);
    }

    void setmaxConnections(unsigned val)
    {
        queryRoot()->setPropInt64("@maxConnections",(int)val);
    }

    void setCrc(bool val=true)
    {
        queryRoot()->setPropInt("@crc",val?1:0);
    }

    void setRetry(unsigned val)
    {
        queryRoot()->setPropInt("@retry",val?1:0);
    }

    void setPush(bool val=true)
    {
        queryRoot()->setPropInt("@push",val?1:0);
    }

    void setKeepHeader(bool val=true)
    {
        queryRoot()->setPropInt("@keepHeader",val?1:0);
    }

    void setPull(bool val=true)
    {
        queryRoot()->setPropInt("@pull",val?1:0);
    }

    void setThrottle(unsigned val)
    {
        queryRoot()->setPropInt("@throttle",val);
    }

    void setTransferBufferSize(unsigned val)
    {
        queryRoot()->setPropInt("@transferBufferSize",val);
    }

    void setVerify(bool val=true)
    {
        queryRoot()->setPropInt("@verify",val?1:0);
    }

    void setOverwrite(bool val=true)
    {
        queryRoot()->setPropInt("@overwrite",val?1:0);
    }

    void setReplicateMode(DFUreplicateMode val,const char *cluster=NULL,bool repeatlast=false,bool onlyrepeated=false)
    {
        queryRoot()->setPropInt("@replicatemode",(int)val);
        if (cluster) {
            queryRoot()->setProp("@replicatecluster",cluster);
            queryRoot()->setPropInt("@repeatlast",repeatlast?1:0);
            queryRoot()->setPropInt("@onlyrepeated",onlyrepeated?1:0);
        }
    }

    void setPartFilter(const char *filter)
    {
        queryRoot()->setProp("@partfilter",filter);
    }

    void setHeader(const char *str)
    {
        queryRoot()->setProp("@header",str);
    }

    void setGlue(const char *str)
    {
        queryRoot()->setProp("@glue",str);
    }

    void setFooter(const char *str)
    {
        queryRoot()->setProp("@footer",str);
    }

    void setLengthPrefix(const char *str)
    {
        queryRoot()->setProp("@prefix",str);
    }

    void setSplitPrefix(const char *str)
    {
        queryRoot()->setProp("@splitPrefix",str);
    }

    void setSubfileCopy(bool set)
    {
        queryRoot()->setPropBool("@subFileCopy",set);
    }

    bool getSubfileCopy() const 
    {
        return queryRoot()->getPropBool("@subFileCopy");
    }

    void setEncDec(const char *enc,const char *dec)
    {
        assertex(parent);
        const char *wuid = parent->root->queryName();
        assertex(wuid&&*wuid);
        MemoryBuffer mb;
        mb.append(enc);
        mb.append(dec);
        while (mb.length()<1024) // salt
            mb.append((char)getRandom()%255); // 255 deliberate so I can add stuff later
        Csimplecrypt c((const byte *)wuid, strlen(wuid), mb.length());
        c.encrypt((void *)mb.toByteArray());
        queryRoot()->setPropBin("Data",mb.length(),mb.toByteArray());
    }

    virtual bool getEncDec(StringAttr &enc,StringAttr &dec)
    {
        MemoryBuffer mb;
        if (queryRoot()->getPropBin("Data",mb)) {
            assertex(parent);
            const char *wuid = parent->root->queryName();
            assertex(wuid&&*wuid);
            Csimplecrypt c((const byte *)wuid, strlen(wuid), mb.length());
            c.decrypt((void *)mb.toByteArray());
            mb.read(enc).read(dec);
            return true;
        }
        return false;
    }

    bool getFailIfNoSourceFile() const
    {
        return queryRoot()->getPropBool("@failIfNoSourceFile");
    }

    void setFailIfNoSourceFile(bool val)
    {
        queryRoot()->setPropBool("@failIfNoSourceFile",val);
    }

    bool getRecordStructurePresent() const
    {
        return queryRoot()->getPropBool("@recordStructurePresent");
    }

    void setRecordStructurePresent(bool val)
    {
        queryRoot()->setPropBool("@recordStructurePresent",val);
    }

    bool getQuotedTerminator() const
    {
        return queryRoot()->getPropBool("@quotedTerminator");
    }

    void setQuotedTerminator(bool val)
    {
        queryRoot()->setPropBool("@quotedTerminator",val);
    }

    bool getPreserveCompression() const
    {
        return queryRoot()->getPropBool("@preserveCompression");
    }

    void setPreserveCompression(bool val)
    {
        queryRoot()->setPropBool("@preserveCompression",val);
    }
    StringBuffer &getUMask(StringBuffer &str)const
    {
        if (queryRoot()->hasProp("@umask"))
            queryRoot()->getProp("@umask",str);
        return str;
    }
    void setUMask(const char *val)
    {
        queryRoot()->setProp("@umask",val);

    }
};

class CExceptionIterator: public CInterface, implements IExceptionIterator
{
    Linked<IPropertyTree> tree;
    unsigned i;
    Owned<IException> cur;
public:
    IMPLEMENT_IINTERFACE;
    CExceptionIterator(IPropertyTree *_tree) 
        : tree(_tree)
    {
        i = 0;
    }
    bool first()
    {
        i = 0;
        return next();
    }

    bool  next()
    {
        StringBuffer key;
        key.append("Exception[").append(++i).append(']');
        IPropertyTree *et = tree.get()?tree->queryPropTree(key.str()):NULL;
        if (!et) {
            cur.clear();
            return false;
        }
        int code = et->getPropInt("@exceptionCode");
        StringBuffer msg;
        et->getProp("@exceptionMessage",msg);
        cur.setown(MakeStringException(code, "%s", msg.str()));
        return true;
    }

    bool  isValid() 
    {
        return cur.get()!=NULL;
    }

    IException &   query() 
    {
        return *cur.get();
    }
};

class CDFUWorkUnit: public CDFUWorkUnitBase
{
    mutable CDFUprogress progress;
    mutable CDFUfileSpec source;
    mutable CDFUfileSpec destination;
    mutable CDFUoptions  options;
    mutable CDFUmonitor  monitor;
    Mutex    updatelock;
    bool     updating;
    Linked<IDFUprogressSubscriber> subscriber;
    Linked<IDFUabortSubscriber> abortsubscriber;
    SubscriptionId subscriberid;
    Semaphore completed;    
    unsigned localedition;
    Linked<IDFUWorkUnitFactory> parent;
public:

    bool checkconn()
    {
        if (!conn) {
            StringBuffer wuRoot;
            getXPath(wuRoot, queryId());
            conn.setown(querySDS().connect(wuRoot.str(), myProcessSession() , 0, SDS_LOCK_TIMEOUT));
            if (!conn)
                return false;
            root.setown(conn->getRoot());
        }
        return true;
    }

    CDFUWorkUnit(IDFUWorkUnitFactory *_parent,IRemoteConnection *_conn,IPropertyTree *tree,bool _lock=false) 
        : parent(_parent)
    {
        updating = false;
        subscriberid = 0;
        if (_conn) {
            conn.setown(_conn);
            root.setown(conn->getRoot());
        }
        else
            root.set(tree);
        localedition = _lock?(unsigned)root->getPropInt("Progress/Edition",0):0;
        progress.init(this,"Progress",!_lock);
        source.init(this,"Source",!_lock);
        destination.init(this,"Destination",!_lock);
        options.init(this,"Options",!_lock);
        monitor.init(this,"Monitor",!_lock);
        if (_lock) {            
            updatelock.lock();
            assertex(!updating);
            updating = true;
        }
    }

    ~CDFUWorkUnit() 
    {
        CriticalBlock block(crit);
        try {
            subscriber.clear();
            abortsubscriber.clear();

            unsubscribe();

            if (updating) {
                conn.clear();   
                updatelock.unlock();
            }
            else if (conn) {
                conn->rollback(); // prevent writing created branches
                conn.clear();
            }
        }
        catch (IException *e) {
            // destructor should always succeed
            EXCLOG(e,"~CDFUWorkUnit");
            e->Release();
        }
    }

    const char *queryId() const
    {
        return root->queryName();
    }

    
    void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {   // for progress and/or state changed
        Linked<IDFUabortSubscriber> notifyabortsubscriber;
        {
            CriticalBlock block(crit);
            if (subscriber) {
                queryProgress(true);
                subscriber->notify(&progress);
            }
            else if (abortsubscriber)
                queryProgress(true);        // reload progress
            DFUstate state = progress.getState();
            switch (state) {
            case DFUstate_aborting:
                if (abortsubscriber) {
                    notifyabortsubscriber.set(abortsubscriber.getClear());
                    notifyabortsubscriber->notifyAbort();
                }
                return;
            case DFUstate_aborted:
                if (abortsubscriber) {
                    notifyabortsubscriber.set(abortsubscriber.getClear());
                    break;
                }
                // fall through
            case DFUstate_failed:
            case DFUstate_finished:
                completed.signal();
                break;
            }
        }
        if (notifyabortsubscriber) {
            notifyabortsubscriber->notifyAbort();
            completed.signal();
        }
    }

    void requestAbort()
    {
        updatelock.lock();
        progress.setState(DFUstate_aborting);
        updatelock.unlock();
    }

    StringBuffer &getDFUServerName(StringBuffer &str) const
    {
        root->getProp("@dfuserver",str);
        return str;
    }


    StringBuffer &getClusterName(StringBuffer &str) const
    {
        root->getProp("@cluster",str);
        return str;
    }

    StringBuffer &getJobName(StringBuffer &str) const
    {
        root->getProp("@jobName",str);
        return str;
    }

    StringBuffer &getQueue(StringBuffer &str) const
    {
        root->getProp("@queue",str);
        return str;
    }

    StringBuffer &getUser(StringBuffer &str) const
    {
        root->getProp("@submitID",str);
        return str;
        
    }

    StringBuffer &getPassword(StringBuffer &str) const
    {
        StringBuffer pw;
        root->getProp("@password",pw);
        if (pw.length()) {
            StringBuffer buf;
            decrypt(buf,pw.str());    // minimal encryprion to obscure (will need improvement)
            const char *p = buf.str();
            const char *i = queryId();
            while (*p&&*i&&(*p==*i)) {
                p++;
                i++;
            }
            str.append(p);
        }
        return str;
    }


    bool isProtected() const
    {
        return root->getPropInt("@protected",0)!=0;
    }


    IDFUWorkUnit *openUpdate(bool exclusive)
    {
        updatelock.lock();
        assertex(!updating);
        updating = true;
        if (!checkconn())
            return NULL;
        conn->changeMode(exclusive?RTM_LOCK_WRITE:RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        reinit();
        localedition = (unsigned)root->getPropInt("Progress/Edition",0);
        Link();
        return this;

    }

    void closeUpdate()
    {
        assertex(updating);
        conn->changeMode(0, SDS_LOCK_TIMEOUT);
        reinit();
        updating = false;
        updatelock.unlock();
    }


    DFUcmd getCommand() const
    {
        StringBuffer s;
        root->getProp("@command",s);
        return decodeDFUcommand(s.str());
    }

    StringBuffer &getCommandName(StringBuffer &str) const
    {
        root->getProp("@command",str);
        return str;
    }

    CDateTime &getTimeScheduled(CDateTime &val) const 
    {
        StringBuffer str;
        root->getProp("@timescheduled",str);
        val.setString(str.str());
        return val; 
    }

    void setTimeScheduled(const CDateTime &val) 
    {
        StringBuffer str;
        val.getString(str);
        root->setProp("@timescheduled",str.str());
    }

    IConstDFUoptions *queryOptions() const
    {
        return &options;
    }

    IConstDFUfileSpec *querySource() const
    {
        return &source;
    }

    IConstDFUfileSpec *queryDestination() const
    {
        return &destination;
    }

    IConstDFUprogress *queryProgress(bool reload=true)
    {
        CriticalBlock block(crit);
        if (reload) {
            if (!checkconn())
                return NULL;
            conn->commit();
            conn->reload("Progress");
            if (!checkconn())
                return NULL;
            progress.reinit();
        }
        return &progress;
    }

    IConstDFUmonitor *queryMonitor(bool reload=true)
    {
        CriticalBlock block(crit);
        if (reload) {
            if (!checkconn())
                return NULL;
            conn->commit();
            conn->reload("Monitor");
            if (!checkconn())
                return NULL;
            monitor.reinit();
        }
        return &monitor;
    }

    void subscribe()
    {
        // called with crit locked
        if (!subscriberid) {
            StringBuffer xpath;
            getXPath(xpath,queryId()).append("/Progress/Edition");
            if (parent)
                subscriberid = (SubscriptionId)parent->subscribe(xpath.str(),QUERYINTERFACE(this,ISDSSubscription));
        }
    }


    void unsubscribe()
    {
        // NOT called with crit locked (as causes deadlock in notifyAbort)
        if (subscriberid) {
            if (parent)
                parent->subscribe(NULL,QUERYINTERFACE(this,ISDSSubscription));
            subscriberid = 0;
        }
    }

    void subscribeProgress(IDFUprogressSubscriber *sub)
    {

        { 
            CriticalBlock block(crit);
            if (sub) {
                subscriber.set(sub);
                subscribe();
                return;
            }
            else {
                subscriber.clear();
                if (abortsubscriber.get())
                    return;
            }
        }
        unsubscribe();  // call outside crit
    }

    void subscribeAbort(IDFUabortSubscriber *sub)
    {
        { 
            CriticalBlock block(crit);
            if (sub) {
                abortsubscriber.set(sub);
                subscribe();
                return;
            }
            else {
                abortsubscriber.clear();
                if (subscriber.get())
                    return;
            }
        }
        unsubscribe();  // call outside crit
    }

    DFUstate waitForCompletion(int timeout)
    {
        {
            CriticalBlock block(crit);
            subscribe();
        }
        loop {
            DFUstate ret = queryProgress(true)->getState();
            switch (ret) {
            case DFUstate_aborted:
            case DFUstate_failed:
            case DFUstate_finished:
                return ret;
            }
            if (!completed.wait(timeout))       // should only go round loop once
                break;
        }
        return queryProgress(true)->getState();
    }

    void reinit()
    {
        if (checkconn()) {
            root.setown(conn->getRoot());
            progress.reinit();
            source.reinit();
            destination.reinit();
            options.reinit();
            monitor.reinit();
        }
    }
    
    unsigned commit()
    {
        CriticalBlock block(crit);
        if (!conn) 
            return 0;
        localedition++;
        root->setPropInt("Progress/Edition",localedition);
        conn->commit();
        reinit();
        return localedition;
    }

    void rollback()
    {
        CriticalBlock block(crit);
        if (conn) {
            conn->rollback();
            reinit();
        }
    }

    unsigned getEdition(bool local)
    {
        CriticalBlock block(crit);
        if (local)
            return localedition;
        if (!conn) 
            return 0;
        conn->reload("Progress/Edition");                   // this may cause problems TBI
        return root->getPropInt("Progress/Edition",0);
    }


    void protect(bool protectMode)
    {
        root->setPropInt("@protected", protectMode?1:0);
    }

    void setDFUServerName(const char * val)
    {
        root->setProp("@dfuserver",val);
    }

    void setClusterName(const char * val)
    {
        root->setProp("@cluster",val);
    }

    void setJobName(const char * val)
    {
        root->setProp("@jobName",val);
    }

    void setQueue(const char * val)
    {
        root->setProp("@queue",val);
    }

    void setUser(const char * val)
    {
        root->setProp("@submitID",val);
    }

    void setPassword(const char * val)
    {
        if (!val||!*val)
            return;
        StringBuffer pw;            // minimal encryprion to obscure (will need improvement)
        pw.append(queryId());
        pw.append(val);
        StringBuffer buf;
        encrypt(buf,pw.str());
        root->setProp("@password",buf.str());
    }

    void setCommand(DFUcmd cmd)
    {
        StringBuffer s;
        encodeDFUcommand(cmd,s);
        root->setProp("@command",s.str());
    }

    IDFUoptions *queryUpdateOptions()
    {
        return &options;        
    }

    IDFUfileSpec *queryUpdateSource()
    {
        return &source;
    }

    IDFUfileSpec *queryUpdateDestination()
    {
        return &destination;
    }

    void addOptions(IPropertyTree *tree)
    {
        // TBD
    }

    IDFUprogress *queryUpdateProgress()
    {
        return &progress;
    }

    IDFUmonitor *queryUpdateMonitor()
    {
        return &monitor;
    }

    void cleanupAndDelete()
    {
        if (isProtected())
            throw MakeStringException(-1, "DFU Workunit is protected");
        switch (progress.getState()) {
        case DFUstate_unknown:
        case DFUstate_aborted:
        case DFUstate_failed:
        case DFUstate_finished:
            break;
        default:
            throw MakeStringException(-1, "DFU Workunit is active");
            break;
        }
        if (checkconn()) {
            conn->changeMode(RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT); // make sure not locked
            root.clear();
            conn->close(true);  
            conn.clear();
        }
    }
    
    void queryRecoveryStore(IRemoteConnection *& _conn,IPropertyTree *&_tree, StringBuffer &runningpath)
    {
        if (!conn) 
            reinit();
        _conn = conn;
        _tree = root->queryPropTree("Recovery");
        if (!_tree)
            _tree = root->addPropTree("Recovery",createPTree("Recovery"));
        getXPath(runningpath,queryId()).append("/Running");
    }

    void removeRecoveryStore()
    {
        IPropertyTree *tree = root->queryPropTree("Recovery");
        root->removeTree(tree);
    }

    void addException(IException *e)
    {
        IPropertyTree *tree = root->queryPropTree("Exceptions");
        if (!tree)
            tree = root->addPropTree("Exceptions",createPTree("Exceptions"));
        IPropertyTree *et = tree->addPropTree("Exception",createPTree("Exception"));
        et->setPropInt("@exceptionCode", e->errorCode());
        StringBuffer msg;
        et->setProp("@exceptionMessage",e->errorMessage(msg).str());
    }

    IExceptionIterator *getExceptionIterator()
    {
        IPropertyTree *tree = root->queryPropTree("Exceptions");
        return new CExceptionIterator(tree);
    }

    void clearExceptions()
    {
        removeTree(root,"Exceptions");
    }

    StringBuffer& getApplicationValue(const char *app, const char *propname, StringBuffer &str) const
    {
        IPropertyTree *tree = root->queryPropTree("Application");
        if (tree) {
            StringBuffer prop;
            prop.append(app).append('/').append(propname);
            tree->getProp(prop.str(),str); 
        }
        return str;
    }

    int getApplicationValueInt(const char *app, const char *propname, int ret) const
    {
        IPropertyTree *tree = root->queryPropTree("Application");
        if (tree) {
            StringBuffer prop;
            prop.append(app).append('/').append(propname);
            ret = tree->getPropInt(prop.str(),ret); 
        }
        return ret;
    }

    void setApplicationValue(const char *app, const char *propname, const char *value, bool overwrite)
    {
        IPropertyTree *tree = root->queryPropTree("Application");
        if (!tree) 
            tree = root->addPropTree("Application",createPTree("Application"));
        IPropertyTree *sub = tree->queryPropTree(app);
        if (!sub) 
            sub = tree->addPropTree(app,createPTree(app));
        if (overwrite || !sub->hasProp(propname)) 
            sub->setProp(propname, value); 
    }

    void setApplicationValueInt(const char *app, const char *propname, int value, bool overwrite)
    {
        StringBuffer str;
        str.append(value);
        setApplicationValue( app, propname, str.str(), overwrite);
    }

    StringBuffer &getDebugValue(const char *propname, StringBuffer &str) const
    {
        StringBuffer prop("Debug/");
        prop.append(propname);
        const char * val = root->queryProp(prop.str());
        if (!val)
            return str;
        return str.append(val); 
    }

    void setDebugValue(const char *propname, const char *value, bool overwrite)
    {
        IPropertyTree *tree = root->queryPropTree("Debug");
        if (!tree) 
            tree = root->addPropTree("Debug",createPTree("Debug"));
        if (overwrite || !tree->hasProp(propname))
            tree->setProp(propname, value); 
    }

    StringBuffer &toXML(StringBuffer &str)
    {
        if (root)
            ::toXML(root, str, 0, XML_Format|XML_SortTags);
        return str;
    }

    bool removeQueue()
    {
        StringBuffer qname;
        if (getQueue(qname).length()!=0) {
            Owned<IJobQueue> queue = createJobQueue(qname.str());
            if (queue.get()) {
                Owned<IJobQueueItem> item = queue->take(queryId());
                if (item.get())
                    return true;
            }
        }
        return false;
    }

};




class CConstDFUWorkUnitIterator: public CInterface, implements IConstDFUWorkUnitIterator
{
    Linked<IRemoteConnection> conn;
    Linked<IPropertyTreeIterator> iter;
    Linked<IDFUWorkUnitFactory> parent;
public:
    IMPLEMENT_IINTERFACE;
    CConstDFUWorkUnitIterator(IDFUWorkUnitFactory *_parent,IRemoteConnection *_conn,IPropertyTreeIterator *_iter)   // takes ownership of conn and iter
        : parent(_parent), conn(_conn),iter(_iter)
    {
    }
    ~CConstDFUWorkUnitIterator()
    {
        iter.clear();
        conn.clear();
    }

    bool first()
    {
        return iter?iter->first():false;
    }
    bool next()
    {
        return iter?iter->next():false;
    }
    bool isValid()
    {
        return iter&&iter->isValid();
    }
    StringBuffer &getId(StringBuffer &str)
    {
        IPropertyTree &pt=iter->query();
        return pt.getName(str);
    }
    virtual IConstDFUWorkUnit * get()
    {
        if (!isValid())
            return NULL;
        StringBuffer wuid;
        return parent?parent->openWorkUnit(getId(wuid).str(),false):NULL;
    }
};


class CConstDFUWUArrayIterator : public CInterface, implements IConstDFUWorkUnitIterator
{
    IArrayOf<IConstDFUWorkUnit> wua;
    unsigned idx;
public:
    IMPLEMENT_IINTERFACE;
    CConstDFUWUArrayIterator(IDFUWorkUnitFactory *_parent,IRemoteConnection *_conn, IArrayOf<IPropertyTree> &trees) 
    {
        idx = 0;
        ForEachItemIn(i,trees) {
            IPropertyTree &tree = trees.item(i);
            wua.append(*(IConstDFUWorkUnit *) new CDFUWorkUnit(_parent,NULL,&tree));
        }
    }
    bool first() 
    { 
        idx = 0;
        return isValid();
    }
    bool isValid() 
    { 
        return idx<wua.ordinality();
    }
    bool next() 
    { 
        idx++;
        return isValid();
    }
    IConstDFUWorkUnit & query() 
    { 
        return wua.item(idx);
    }
    IConstDFUWorkUnit * get() 
    { 
        if (!isValid())
            return NULL;
        IConstDFUWorkUnit *ret = &wua.item(idx);
        return LINK(ret);
    }
    virtual StringBuffer &getId(StringBuffer &str)
    {
        return str.append(query().queryId());
    }
};

class CDFUWorkUnitFactory : public CInterface, implements IDFUWorkUnitFactory, implements ISDSSubscription
{
    CriticalSection proxylock;
    PointerArray subscribers;
    Int64Array subscriberids;
    Int64Array active;  // active TIDS
    
    void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {   
        __uint64 tid = (__uint64) GetCurrentThreadId();
        Linked<ISDSSubscription> dest;
        {
            CriticalBlock block(proxylock);
            if (active.find(tid)!=NotFound)
                return;
            active.append(tid);
            unsigned i1 = subscriberids.find(id);
            if (i1!=NotFound) 
                dest.set((ISDSSubscription *)subscribers.item(i1));
        }
        try {
            if (dest.get()) 
                dest->notify(id,xpath,flags,valueLen,valueData);
        }
        catch (IException *e) {
            EXCLOG(e,"CDFUWorkUnitFactory:notify");
            e->Release();
        }
        CriticalBlock block(proxylock);
        active.zap(tid);
    }

    __int64 subscribe (const char *xpath,void *iface)
    {
        // idea is to avoid subscribing/unsubscribing while processing a notify
        // *unless* on my own thread
        // this happens when waiting to abort
        CriticalBlock block(proxylock);
        unsigned __int64 tid = (unsigned __int64) GetCurrentThreadId();
        ThreadId atid = 0;
        for (unsigned i=0;i<100;i++) {
            bool ok = true;
            ForEachItemInRev(j,active) {
                if (active.item(j)!=tid) {
                    ok = false;
                    atid = (ThreadId)active.item(j);
                }
            }
            if (ok)
                break;
            if (i%10==9)
                WARNLOG("CDFUWorkUnitFactory: Subscription(%d,%" I64F "d) busy %s",i,(__int64)atid,xpath?xpath:"");
            CriticalUnblock unblock(proxylock);
            Sleep(i*10);
            if (i==99) 
                PrintStackReport();
        }
        SubscriptionId subscriberid = 0;
        ForEachItemInRev(i1,subscribers) {
            if (subscribers.item(i1)==iface) {
                querySDS().unsubscribe(subscriberids.item(i1));
                subscribers.remove(i1);
                subscriberids.remove(i1);
            }
        }
        if (xpath) {
            subscriberid = querySDS().subscribe(xpath, *this, false);
            subscribers.append(iface);
            subscriberids.append(subscriberid);
        }
        return subscriberid;
    }


public:


    IMPLEMENT_IINTERFACE;
    IDFUWorkUnit * createWorkUnit()
    {
        StringBuffer wuid;
        newWUID(wuid);
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid.str());
        IRemoteConnection* conn = querySDS().connect(wuRoot.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_UNIQUE, SDS_LOCK_TIMEOUT);
        conn->queryRoot()->setProp("@xmlns:xsi", "http://www.w3.org/1999/XMLSchema-instance");
        IDFUWorkUnit *ret = new CDFUWorkUnit(this, conn, NULL, true);
        // created time stamp? TBD
        return ret;
    }
    bool deleteWorkUnit(const char * wuid)
    {
        StringBuffer wuids(wuid);
        wuids.trim();
        if (!wuids.length())
            return false;
        StringBuffer wuRoot;
        getXPath(wuRoot, wuids.str());
        IRemoteConnection *conn = querySDS().connect(wuRoot.str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        if (!conn)
            return false;
        Owned<CDFUWorkUnit> cw = new CDFUWorkUnit(this,conn,NULL,true);
        try
        {
            cw->cleanupAndDelete();
            return true;
        }
        catch (IException *e)
        {
            EXCLOG(e, "DFUWU Exception: ");
            e->Release();
            return false;
        }
        return false;
    }
    IConstDFUWorkUnit * openWorkUnit(const char * wuid, bool lock)
    {
        StringBuffer wuids(wuid);
        wuids.trim();
        if (!wuids.length())
            return NULL;
        StringBuffer wuRoot;
        getXPath(wuRoot, wuids.str());
        IRemoteConnection* conn = querySDS().connect(wuRoot.str(), myProcessSession() , lock ? RTM_LOCK_READ : 0, SDS_LOCK_TIMEOUT);
        if (!conn)
            return NULL;
        return new CDFUWorkUnit(this, conn, NULL, false);
    }
    IConstDFUWorkUnitIterator * getWorkUnitsByXPath(const char *xpath)
    {
        StringBuffer wuRoot;
        getXPathBase(wuRoot);
        Owned<IRemoteConnection> conn = querySDS().connect(wuRoot.str(), myProcessSession() , 0, SDS_LOCK_TIMEOUT);
        if (!conn.get())
            return new CConstDFUWorkUnitIterator(this,NULL,NULL);
        CDaliVersion serverVersionNeeded("3.2");
        Owned<IPropertyTreeIterator> iter(queryDaliServerVersion().compare(serverVersionNeeded) < 0 ? 
            conn->queryRoot()->getElements(xpath) : 
            conn->getElements(xpath));
        return new CConstDFUWorkUnitIterator(this,conn,iter);
    }
    IConstDFUWorkUnitIterator * getWorkUnitsByOwner(const char * owner)
    {
        StringBuffer path("*");
        if (owner && *owner)
            path.append("[@submitID=\"").append(owner).append("\"]");
        return getWorkUnitsByXPath(path.str());
    }
    IConstDFUWorkUnitIterator * getWorkUnitsByState(DFUstate state)
    {
        StringBuffer path;
        encodeDFUstate(state,path.append("*[Progress/@state=\"")).append("\"]");
        return getWorkUnitsByXPath(path.str());
    }
    IDFUWorkUnit * updateWorkUnit(const char * wuid, bool exclusive)
    {
        StringBuffer wuids(wuid);
        wuids.trim();
        if (!wuids.length())
            return NULL;
        StringBuffer wuRoot;
        getXPath(wuRoot, wuids.str());
        IRemoteConnection* conn = querySDS().connect(wuRoot.str(), myProcessSession(), exclusive?RTM_LOCK_WRITE:RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        if (!conn)
            return NULL;
        return new CDFUWorkUnit(this, conn, NULL, true);
    }

    IConstDFUWorkUnitIterator* getWorkUnitsSorted(  DFUsortfield *sortorder, // list of fields to sort by (terminated by WUSFterm)
                                                    DFUsortfield *filters, // list of fields to filter by (terminated by WUSFterm)
                                                    const void *filterbuf,     
                                                    unsigned startoffset,
                                                    unsigned maxnum,
                                                    const char *queryowner, 
                                                    __int64 *cachehint,
                                                    unsigned *total)
    {
        class CDFUWorkUnitsPager : public CSimpleInterface, implements IElementsPager
        {
            StringAttr xPath;
            StringAttr sortOrder;
            StringAttr nameFilterLo;
            StringAttr nameFilterHi;
            StringArray unknownAttributes;

        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CDFUWorkUnitsPager(const char* _xPath, const char *_sortOrder, const char* _nameFilterLo, const char* _nameFilterHi, StringArray& _unknownAttributes)
                : xPath(_xPath), sortOrder(_sortOrder), nameFilterLo(_nameFilterLo), nameFilterHi(_nameFilterHi)
            {
                ForEachItemIn(x, _unknownAttributes)
                    unknownAttributes.append(_unknownAttributes.item(x));
            }
            virtual IRemoteConnection* getElements(IArrayOf<IPropertyTree> &elements)
            {
                Owned<IRemoteConnection> conn = querySDS().connect("DFU/WorkUnits", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
                if (!conn)
                    return NULL;
                Owned<IPropertyTreeIterator> iter = conn->getElements(xPath);
                if (!iter)
                    return NULL;
                sortElements(iter, sortOrder.get(), nameFilterLo.get(), nameFilterHi.get(), unknownAttributes, elements);
                return conn.getClear();
            }
            virtual bool allMatchingElementsReceived() { return true; }//For now, dali always returns all of matched WUs.
        };

        StringBuffer query;
        StringAttr namefilter("*");
        StringBuffer so;
        const char *field;
        StringBuffer sf;
        StringAttr namefilterlo;
        StringAttr namefilterhi;
        StringArray unknownAttributes;
        if (filters)
        {
            const char *fv = (const char *)filterbuf;
            for (unsigned i=0;filters[i]!=DFUsf_term;i++)
            {
                DFUsortfield fmt = filters[i];
                if (fmt==DFUsf_wuid) 
                    namefilterlo.set(fv);
                else if (fmt==DFUsf_wuidhigh)
                    namefilterhi.set(fv);
                else if (fmt==DFUsf_wildwuid)
                    namefilter.set(fv);
                else if (!fv || !*fv)
                {
                    const char* attr = getDFUSortFieldXPath(fmt);
                    if (attr && *attr)
                        unknownAttributes.append(attr);
                }
                else
                {
                    field = encodeDFUsortfield(fmt,sf.clear(),false).str();
                    query.append('[').append(field).append('=');
                    if (((int)fmt)&DFUsf_nocase)
                        query.append('?');
                    if (((int)fmt)&DFUsf_wild)
                        query.append('~');
                    query.append('"').append(fv).append("\"]");
                }
                fv += strlen(fv)+1;
            }
        }
        query.insert(0, namefilter.get());
        if (sortorder)
        {
            for (unsigned i=0;sortorder[i]!=DFUsf_term;i++)
            {
                field = encodeDFUsortfield(sortorder[0],sf.clear(),true).str();
                if (so.length())
                    so.append(',');
                so.append(field);
            }
        }
        IArrayOf<IPropertyTree> results;
        Owned<IElementsPager> elementsPager = new CDFUWorkUnitsPager(query.str(), so.length()?so.str():NULL, namefilterlo.get(), namefilterhi.get(), unknownAttributes);
        Owned<IRemoteConnection> conn=getElementsPaged(elementsPager,startoffset,maxnum,NULL,queryowner,cachehint,results,total, NULL);
        return new CConstDFUWUArrayIterator(this,conn,results);
    }

    virtual unsigned numWorkUnits()
    {
        Owned<IRemoteConnection> conn = querySDS().connect("DFU/WorkUnits", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
        if (!conn) 
            return 0;
        IPropertyTree *root = conn->queryRoot();
        return root->numChildren();
    }
};

IDFUWorkUnitFactory * getDFUWorkUnitFactory()
{
    return new CDFUWorkUnitFactory;
}



dfuwu_decl unsigned queuedJobs(const char *queuename,StringAttrArray &wulist)
{
    unsigned ret = 0;
    try{
        Owned<IRemoteConnection> conn = querySDS().connect("/Status/Servers",myProcessSession(),RTM_LOCK_READ,SDS_LOCK_TIMEOUT);
        if (conn) {
            StringBuffer mask;
            mask.appendf("Server[@name=\"DFUserver\"]/Queue[@name=\"%s\"]",queuename);
            Owned<IPropertyTreeIterator> iterq = conn->queryRoot()->getElements(mask.str());
            ForEach(*iterq) {
                Owned<IPropertyTreeIterator> iterj = iterq->query().getElements("Job");
                ForEach(*iterj) {
                    const char *wuid = iterj->query().queryProp("@wuid");
                    if (wuid&&*wuid&&(*wuid!='!')) {        // filter escapes
                        wulist.append(*new StringAttrItem(wuid));
                        ret++;
                    }
                }
            }
        }
    }
    catch(IException* e){   
        StringBuffer msg;
        e->errorMessage(msg);
        ERRLOG("DFUWU runningJobs(%s) %s",queuename,msg.str());
        e->Release();
    }
    try{
        Owned<IJobQueue> queue = createJobQueue(queuename);
        if (queue) {
            CJobQueueContents contents;
            queue->copyItems(contents);
            Owned<IJobQueueIterator> iter = contents.getIterator();
            ForEach(*iter) {
                const char *wuid = iter->query().queryWUID();
                if (wuid&&*wuid&&(*wuid!='!')) {        // filter escapes
                    wulist.append(*new StringAttrItem(wuid));
                }
            }
        }
    }
    catch(IException* e){   
        StringBuffer msg;
        e->errorMessage(msg);
        ERRLOG("DFUWU queuedJobs(%s) %s",queuename,msg.str());
        e->Release();
    }
    return ret;
}


IDfuFileCopier *createRemoteFileCopier(const char *qname,const char *clustername, const char *jobname, bool replicate)
{
    class cCopier: public CInterface, implements IDfuFileCopier
    {
        Owned<IDFUWorkUnitFactory> factory;
        StringAttr qname; 
        StringAttr clustername;
        StringAttr jobname;
//      DFD_OS os;
        StringArray wuids;
        bool replicate;
    public:
        IMPLEMENT_IINTERFACE;
        cCopier(const char *_qname,const char *_clustername, const char *_jobname, bool _replicate)
            : qname(_qname), clustername(_clustername), jobname(_jobname)
        {
            factory.setown(getDFUWorkUnitFactory());
            replicate = _replicate;
        }
        bool copyFile(const char *lfn,SocketEndpoint &srcdali,const char *srclfn,IUserDescriptor *srcuser,IUserDescriptor *user)
        {
            Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
            wu->setClusterName(clustername);
            wu->setJobName(jobname);
            wu->setQueue(qname);
            if (user) {
                StringBuffer uname;
                user->getUserName(uname);
                wu->setUser(uname.str());
                StringBuffer pwd;
                wu->setPassword(user->getPassword(pwd).str());
            }
            IDFUfileSpec *source = wu->queryUpdateSource();
            IDFUfileSpec *destination = wu->queryUpdateDestination();
            IDFUoptions *options = wu->queryUpdateOptions();
            wu->setCommand(DFUcmd_copy);
            source->setLogicalName(srclfn);
            source->setForeignDali(srcdali);
            destination->setLogicalName(lfn);
            destination->setGroupName(clustername);              
            options->setReplicate(true);
            // should be no need for overwrite
            const char *wuid = wu->queryId();
            StringBuffer eps;
            PROGLOG("%s: Copy %s from %s to %s",wuid,srclfn,srcdali.getUrlStr(eps).str(),lfn);
            wuids.append(wuid);
            submitDFUWorkUnit(wu.getClear());
            return true;
        }

        bool wait()
        {
            ForEachItemIn(i,wuids) {
                const char *wuid = wuids.item(i);
                Owned<IConstDFUWorkUnit> dfuwu = factory->openWorkUnit(wuid,false);
                if (!dfuwu) 
                    throw MakeStringException(-1,"DFUWU %s could not be found",wuid);
                IConstDFUprogress *progress = dfuwu->queryProgress();
                PROGLOG("Waiting for %s",wuid);
                DFUstate state = dfuwu->waitForCompletion(1000*60*60*24*4); // big timeout
                switch(state)
                {
                case DFUstate_unknown:
                case DFUstate_scheduled:
                case DFUstate_queued:
                case DFUstate_started:
                case DFUstate_monitoring:
                case DFUstate_aborting:
                    return false;
                case DFUstate_aborted:
                    return false;
                case DFUstate_failed:
                    return false;
                case DFUstate_finished:
                    break;
                }
                Sleep(COPY_WAIT_SECONDS*1000);  
            }
            return true;
        }
    };
    return new cCopier(qname,clustername,jobname,replicate);
}


extern dfuwu_decl void submitDFUWorkUnit(IDFUWorkUnit *workunit)
{
    Owned<IDFUWorkUnit> wu(workunit);
    StringBuffer qname;
    if (wu->getQueue(qname).length()==0) {
        throw MakeStringException(-1, "DFU no queue name specified");
    }
    Owned<IJobQueue> queue = createJobQueue(qname.str());
    if (!queue.get()) {
        throw MakeStringException(-1, "Cound not create queue");
    }
    StringBuffer user;
    wu->getUser(user);
    IDFUprogress *progress = wu->queryUpdateProgress();
    progress->setState(DFUstate_queued);
    progress->clearProgress();
    wu->clearExceptions();
    wu->commit();

    StringAttr wuid(wu->queryId());
    wu.clear();
    IJobQueueItem *item = createJobQueueItem(wuid.get());
    item->setEndpoint(queryMyNode()->endpoint());
    if (user.length()!=0)
        item->setOwner(user.str());
    queue->enqueue(item);
}

extern dfuwu_decl void submitDFUWorkUnit(const char *wuid)
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(wuid);
    if(!wu)
        throw MakeStringException(-1, "DFU workunit %s could not be opened for update", wuid);
    submitDFUWorkUnit(wu.getClear());
}
