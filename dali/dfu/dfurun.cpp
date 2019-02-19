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

/* todo
test multiclusteradd
test multiclusteradd with replicate
*/


#include <platform.h>
#include <stdio.h>
#include <limits.h>
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"
#include "jstring.hpp"
#include "jmisc.hpp"
#include "jprop.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "dasds.hpp"
#include "dasess.hpp"
#include "dalienv.hpp"
#include "daaudit.hpp"
#include "jio.hpp"
#include "daft.hpp"
#include "daftcfg.hpp"
#include "fterror.hpp"
#include "rmtfile.hpp"
#include "daftprogress.hpp"
#include "dfuwu.hpp"
#include "dfurun.hpp"
#include "eventqueue.hpp"
#include "wujobq.hpp"

#define SDS_CONNECT_TIMEOUT (5*60*100)

extern ILogMsgHandler * fileMsgHandler;

extern void doKeyDiff(IFileDescriptor *oldf,IFileDescriptor *newf,IFileDescriptor *patchf); // patchf out
extern void doKeyPatch(IFileDescriptor *oldf,IFileDescriptor *newf,IFileDescriptor *patchf);


static void LOGXML(const char *trc,const IPropertyTree *pt)
{
    StringBuffer s;
    toXML(pt,s);
    PROGLOG("%s:\n%s",trc,s.str());
}



class CDFUengine: public CInterface, implements IDFUengine
{
    StringBuffer dfuServerName;
    size32_t defaultTransferBufferSize;

    void setDefaultTransferBufferSize(size32_t size)
    {
        defaultTransferBufferSize = size;
    }

    void setDFUServerName(const char* name)
    {
        dfuServerName = name;
    }

    void Audit(const char *func,IUserDescriptor *userdesc,const char *lfn1, const char *lfn2)
    {
        SocketEndpoint ep;
        ep.setLocalHost(0);
        StringBuffer aln;
        aln.append(",FileAccess,DfuPlus,").append(func).append(',');
        ep.getUrlStr(aln);
        aln.append(',');
        if (userdesc)
            userdesc->getUserName(aln);
        if (lfn1&&*lfn1) {
            aln.append(',').append(lfn1);
            if (lfn2&&*lfn2) {
                aln.append(',').append(lfn2);
            }
        }
        LOG(daliAuditLogCat,"%s",aln.str());
    }


    class cProgressReporter : public DaftProgress
    {
        IDFUprogress *progress;
        unsigned start;
    public:
        enum { REPnone, REPbefore, REPduring } repmode;
        cProgressReporter(IDFUprogress *_progress)
        {
            progress = _progress;
            repmode = REPnone;
            start = msTick();
        }
        void displayProgress(unsigned percentDone, unsigned secsLeft, const char * timeLeft,
                                unsigned __int64 scaledDone, unsigned __int64 scaledTotal, const char * scale,
                                unsigned kbPerSecondAve, unsigned kbPerSecondRate,
                                unsigned slavesDone)
        {
            if (repmode==REPbefore)
                percentDone /= 2;
            else
                if (repmode==REPduring)
                    percentDone = percentDone/2+50;
            progress->setProgress(percentDone, secsLeft, timeLeft, scaledDone, scaledTotal, scale,
                                 kbPerSecondAve, kbPerSecondRate, slavesDone, repmode==REPduring);
        }
        void displaySummary(const char * timeTaken, unsigned kbPerSecond)
        {
            // ignore time passed through - use own
            char tts[20];
            formatTime(tts, (msTick()-start)/1000);
            progress->setDone(tts, kbPerSecond, (repmode!=REPbefore));
        }

        void setRange(unsigned __int64 sizeReadBefore, unsigned __int64 totalSize, unsigned _totalNodes)
        {
            DaftProgress::setRange(sizeReadBefore,totalSize,_totalNodes);
            progress->setTotalNodes(_totalNodes);
        }

    };

    class cAbortNotify : public CInterface, implements IAbortRequestCallback, implements IDFUabortSubscriber
    {
        bool abort;
        bool last;
    public:
        IMPLEMENT_IINTERFACE;
        cAbortNotify()
        {
            abort = false;
            last = abort;
        }
        virtual bool abortRequested()
        {
            if (abort&&!last)
                PROGLOG("ABORT checked");
            last = abort;
            return abort;
        }
        void notifyAbort()
        {
            if (!abort)
                PROGLOG("ABORT notified");
            abort = true;
        }
    };


    class cDFUlistener: public Thread
    {
        Owned<IJobQueue> queue;
        StringAttr queuename;
        unsigned timeout;
        bool ismon;
        CSDSServerStatus *serverstatus;
    protected:
        bool cancelling;
        CDFUengine *parent;

        void setRunningStatus(const char *qname,const char *wuid,bool set)
        {
            if (ismon||!serverstatus||!wuid||!*wuid)        // monitor not in status
                return;
            StringBuffer mask;
            mask.appendf("Queue[@name=\"%s\"][1]",qname);
            IPropertyTree *t =  serverstatus->queryProperties()->queryPropTree(mask.str());
            if (!t) {
                WARNLOG("DFUWU: setRunningStatus queue %s not set",qname);
                return;
            }
            mask.clear().appendf("Job[@wuid=\"%s\"]",wuid);
            if (set&&!t->hasProp(mask.str())) {

                t->addPropTree("Job",createPTree())->setProp("@wuid",wuid);
            }
            else
                t->removeProp(mask.str());
            serverstatus->commitProperties();
        }


    public:
        cDFUlistener(CDFUengine *_parent,const char *_queuename, bool _ismon, CSDSServerStatus *_serverstatus, unsigned _timeout=WAIT_FOREVER)
            : queuename(_queuename)
        {
            serverstatus = _serverstatus;
            ismon = _ismon;
            timeout = _timeout;
            parent = _parent;
            queue.setown(createJobQueue(queuename));
            cancelling = false;
        }
        int run()
        {
            try {
                queue->connect(false);
            }
            catch (IException *e) {
                EXCLOG(e, "DFURUN Server Connect queue: ");
                e->Release();
                return -1;
            }
            const char *serv = ismon?"Monitor":"Server";
            try {
//              MemoryBuffer mb;
                unsigned start = msTick();
                if (ismon) {
                    try {
                        onCycle();  // first run
                    }
                    catch (IException *e) {
                        EXCLOG(e, "DFURUN Monitor Exception(1): ");
                        e->Release();
                    }
                }
                for (;;) {
                    unsigned char mode;
                    StringAttr wuid;
                    Owned<IJobQueueItem> item = queue->dequeue(timeout);
                    if (item.get()) {
                        wuid.set(item->queryWUID());
                        if ((wuid.length()==0)||(stricmp(wuid,"!stop")==0))
                            mode = DFUservermode_stop;
                        else
                            mode = DFUservermode_run;
                    }
                    else {
                        if (cancelling||(timeout==WAIT_FOREVER)||isAborting())
                            mode = DFUservermode_stop;
                        else
                            mode = DFUservermode_cycle;
                    }
                    try {
                        switch ((DFUservermode)mode) {
                        case DFUservermode_run: {
                                PROGLOG("DFU %s running job: %s",serv,wuid.get());
                                setRunningStatus(queuename.get(),wuid,true);
                                try {
                                    parent->runWU(wuid);
                                }
                                catch (IException *) {
                                    setRunningStatus(queuename.get(),wuid,false);
                                    throw;
                                }
                                if (!ismon) {
                                    setRunningStatus(queuename.get(),wuid,false);
                                    PROGLOG("DFU %s finished job: %s",serv,wuid.get());
                                }
                                PROGLOG("DFU %s waiting on queue %s",serv,queuename.get());
                            }
                            break;
                        case DFUservermode_stop:
                            if (cancelling||(msTick()-start>5000))
                                return 0;
                            start = msTick(); // remove enqueued stops
                            break;
                        case DFUservermode_cycle:
                                onCycle();
                            break;
                        default:
                            OERRLOG("DFURUN Unknown mode");
                            break;
                        }
                    }
                    catch (IException *e) {
                        EXCLOG(e, "DFURUN Server Exception(1): ");
                        e->Release();
                    }

                }
            }
            catch (IException *e) {
                EXCLOG(e, "DFURUN Server Exception(2): ");
                e->Release();
                PROGLOG("Exiting DFU Server");
            }
            try {
                queue->disconnect();
            }
            catch (IException *e) {
                EXCLOG(e, "DFURUN Server queue disconnect: ");
                e->Release();
            }
            return 0;
        }
        void cancel()
        {
            cancelling = true;
            queue->cancelAcceptConversation();
        }
        virtual void onCycle()
        {
        }
    };

    class cDFUmonitor: public cDFUlistener
    {
    public:
        cDFUmonitor(CDFUengine *_parent,const char *_queuename, CSDSServerStatus *serverstatus, unsigned _timeout)
            : cDFUlistener(_parent,_queuename,true,serverstatus,_timeout)
        {
        }
        virtual void onCycle()
        {
            parent->monitorCycle(cancelling);
        }
    };

    IRemoteConnection *setRunning(const char * path)
    {
        unsigned mode = RTM_CREATE_QUERY | RTM_LOCK_READ | RTM_DELETE_ON_DISCONNECT;
        IRemoteConnection *runningconn = querySDS().connect(path, myProcessSession(), mode, SDS_CONNECT_TIMEOUT);
        if (runningconn) {
            runningconn->queryRoot()->setPropBool("", true);
            runningconn->commit();
        }
        return runningconn;
    }

    void setFileRepeatOptions(IDistributedFile &file,const char *cluster,bool repeatlast,bool onlyrepeated)
    {
        if (!cluster||!*cluster)
            return;
        StringBuffer dir;
        GroupType groupType;
        Owned<IGroup> grp = queryNamedGroupStore().lookup(cluster, dir, groupType);
        if (!grp) {
            throw MakeStringException(-1,"setFileRepeatOptions cluster %s not found",cluster);
            return;
        }
        ClusterPartDiskMapSpec spec;
        unsigned cn = file.findCluster(cluster);
        if (cn!=NotFound)
            spec = file.queryPartDiskMapping(cn);
        if (repeatlast)
            spec.setRepeatedCopies(file.numParts()-1,onlyrepeated);
        if (dir.length())
            spec.setDefaultBaseDir(dir.str());
        if (cn==NotFound)
            file.addCluster(cluster,spec);
        else
            file.updatePartDiskMapping(cluster,spec);
    }


    bool testLocalCluster(const char *groupname)
    {
        if (!groupname)
            return false;
        if (isdigit(*groupname)) {  // allow IPs
            const char *s = groupname+1;
            while (*s) {
                if (!isdigit(*s)&&(*s!='.')&&(*s!='-')&&(*s!=',')&&(*s!=':'))
                    break;
                s++;
            }
            if (!*s)
                return true;
        }
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn)
            return false;
        IPropertyTree* root = conn->queryRoot();
        Owned<IPropertyTreeIterator> clusters;
        clusters.setown(root->getElements("ThorCluster"));
        ForEach(*clusters) {
            StringBuffer thorClusterGroupName;
            getClusterGroupName(clusters->query(), thorClusterGroupName);
            if (strcmp(thorClusterGroupName.str(),groupname)==0)
                return true;
        }
        clusters.setown(root->getElements("RoxieCluster"));
        ForEach(*clusters) {
            IPropertyTree& cluster = clusters->query();
            if (strcmp(cluster.queryProp("@name"),groupname)==0)
                return true;
            Owned<IPropertyTreeIterator> farms = cluster.getElements("RoxieFarmProcess");  // probably only one but...
            ForEach(*farms) {
                IPropertyTree& farm = farms->query();
                StringBuffer fgname(cluster.queryProp("@name"));
                fgname.append("__");
                fgname.append(farm.queryProp("@name"));
                if (strcmp(fgname.str(),groupname)==0)
                    return true;
            }
        }
        clusters.setown(root->getElements("EclAgentProcess"));
        ForEach(*clusters) {
            unsigned ins = 0;
            IPropertyTree &pt = clusters->query();
            const char *hgname = pt.queryProp("@name");
            if (hgname&&*hgname) {
                Owned<IPropertyTreeIterator> insts = pt.getElements("Instance");
                ForEach(*insts) {
                    const char *na = insts->query().queryProp("@netAddress");
                    if (na&&*na) {
                        SocketEndpoint ep(na);
                        if (!ep.isNull()) {
                            ins++;
                            StringBuffer gname("hthor__");
                            if (memicmp(groupname,gname.str(),gname.length())==0)
                                gname.append(groupname+gname.length());
                            else
                                gname.append(groupname);
                            if (ins>1)
                                gname.append('_').append(ins);
                            if (strcmp(gname.str(),groupname)==0)
                                return true;
                        }
                    }
                }
            }
        }
        return false;
    }


    // DropZone check
    void checkFilePath(RemoteFilename & filename)
    {
        StringBuffer filePath;
        filename.getLocalPath(filePath);
        const char * pfilePath = filePath.str();

        if (filename.queryIP().isLoopBack())
            throwError1(DFTERR_LocalhostAddressUsed, pfilePath);

    #ifdef _DEBUG
        LOG(MCdebugInfo, unknownJob, "File path is '%s'", filePath.str());
    #endif

        const char pathSep = filename.getPathSeparator();
        const char dotString[]    = {pathSep, '.', pathSep, '\0'};
        const char dotDotString[] = {pathSep, '.', '.', pathSep, '\0'};

        const char * isDotString = strstr(pfilePath, dotString);
        const char * isDotDotString = strstr(pfilePath, dotDotString);
        if ((isDotDotString != nullptr) || (isDotString != nullptr))
            throwError3(DFTERR_InvalidFilePath, pfilePath, dotDotString, dotString);

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> env = factory->openEnvironment();
        StringBuffer netaddress;
        filename.queryIP().getIpText(netaddress);

        Owned<IConstDropZoneInfo> dropZone = env->getDropZoneByAddressPath(netaddress.str(), pfilePath);
        if (!dropZone)
        {
            if (env->isDropZoneRestrictionEnabled())
                throwError2(DFTERR_NoMatchingDropzonePath, netaddress.str(), pfilePath);
            else
                LOG(MCdebugInfo, unknownJob, "No matching drop zone path on '%s' to file path: '%s'", netaddress.str(), pfilePath);
        }
#ifdef _DEBUG
        else
        {
            SCMStringBuffer dropZoneName;
            dropZone->getName(dropZoneName);

            LOG(MCdebugInfo, unknownJob, "Drop zone path '%s' is %svisible in ECLWatch."
                , dropZoneName.str()
                , (dropZone->isECLWatchVisible() ? "" : "not ")
                );
        }
#endif
    }

    // Prepare DropZone check for file(s)
    void checkSourceTarget(IFileDescriptor * file)
    {
        unsigned numParts = file->numParts();
        for (unsigned idx=0; idx < numParts; idx++)
        {
            if (file->isMulti(idx))
            {
                // It expands wildcards and file list
                RemoteMultiFilename multi;
                file->getMultiFilename(idx, 0, multi);
                multi.expandWild();

                ForEachItemIn(i, multi)
                {
                    RemoteFilename rfn2(multi.item(i));
                    checkFilePath(rfn2);
                }
            }
            else
            {
                RemoteFilename filename;
                file->getFilename(idx, 0, filename);
                checkFilePath(filename);
            }
        }
    }

    Owned<IScheduleEventPusher> eventpusher;
    IArrayOf<cDFUlistener> listeners;

    CriticalSection monitorsect;
    CriticalSection subcopysect;
    atomic_t runningflag;

public:
    IMPLEMENT_IINTERFACE;

    CDFUengine()
    {
        defaultTransferBufferSize = 0;
        atomic_set(&runningflag,1);
        eventpusher.setown(getScheduleEventPusher());
    }

    ~CDFUengine()
    {
        abortListeners();
        joinListeners();
    }

    void startListener(const char *queuename,CSDSServerStatus *serverstatus)
    {
        PROGLOG("DFU server waiting on queue %s",queuename);
        cDFUlistener *lt = new cDFUlistener(this,queuename,false,serverstatus);
        listeners.append(*lt);
        lt->start();
    }

    void startMonitor(const char *queuename,CSDSServerStatus *serverstatus,unsigned timeout)
    {
        if (timeout==0)
            return;
        PROGLOG("DFU monitor waiting on queue %s timeout %d",queuename,timeout);
        cDFUlistener *lt = new cDFUmonitor(this,queuename,serverstatus,timeout);
        listeners.append(*lt);
        lt->start();
    }

    void joinListeners()
    {
        unsigned i;
        for (i=0;i<listeners.ordinality();i++)
            listeners.item(i).join();
    }
    void abortListeners()
    {
        unsigned i;
        for (i=0;i<listeners.ordinality();i++)
            listeners.item(i).cancel();
    }

    void checkPhysicalFilePermissions(IFileDescriptor *fd,IUserDescriptor *user,bool write)
    {
        unsigned auditflags = (DALI_LDAP_AUDIT_REPORT|DALI_LDAP_READ_WANTED);
        if (write)
            auditflags |= DALI_LDAP_WRITE_WANTED;
        SecAccessFlags perm = queryDistributedFileDirectory().getFDescPermissions(fd,user,auditflags);
        IDFS_Exception *e = NULL;
        if (!HASREADPERMISSION(perm))
            throw MakeStringException(DFSERR_LookupAccessDenied,"Lookup permission denied for physical file(s)");
        if (write&&!HASWRITEPERMISSION(perm))
            throw MakeStringException(DFSERR_CreateAccessDenied,"Create permission denied for physical file(s)");
    }

    void monitorCycle(bool &cancelling)
    {
        CriticalBlock block(monitorsect);
        // scan all monitoring WUs
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IConstDFUWorkUnitIterator> iter = factory->getWorkUnitsByState(DFUstate_monitoring);
        StringBuffer wuid;
        if (iter) {
            StringAttrArray eventstriggered;
            StringAttrArray eventsfile;
            ForEach(*iter) {
                if (cancelling)
                    break;
                // check for other owner here TBD
                iter->getId(wuid.clear());
                Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(wuid.str(),true);
                if (!wu)
                    continue;
                IDFUmonitor *monitor = wu->queryUpdateMonitor();
                if (!monitor)
                    continue;
                SocketEndpoint handler;
                INode *me = queryMyNode();
                if (monitor->getHandlerEp(handler)) {
                    if (!me->endpoint().equals(handler)) {
                        Owned<IGroup> grp = createIGroup(1,&handler);
                        Owned<ICommunicator> comm = createCommunicator(grp,true);
                        if (comm->verifyConnection(0,1000*60))  // shouldn't take long
                            continue;   // other handler running
                        monitor->setHandlerEp(me->endpoint());
                    }
                }
                else
                    monitor->setHandlerEp(me->endpoint());

                Owned<IUserDescriptor> userdesc = createUserDescriptor();
                {
                    StringBuffer username;
                    StringBuffer password;
                    wu->getUser(username);
                    wu->getPassword(password);
                    userdesc->set(username.str(),password.str());
                }
                if (performMonitor(wu,monitor,wu->querySource(),false,&eventstriggered,&eventsfile,userdesc)) {
                    wu->queryUpdateProgress()->setState(DFUstate_finished);
                }
                wu->commit();
            }
            pushEvents(eventstriggered,eventsfile);
        }
    }

    void pushEvents(StringAttrArray &eventstriggered, StringAttrArray &eventsfile)
    {
        ForEachItemIn(i,eventstriggered) {
            const char *ename = eventstriggered.item(i).text.get();
            const char *fname = eventsfile.item(i).text.get();
            bool dup = false;
            for (unsigned j=i;j>0;j--) { // n^2 but not many hopefully
                if ((stricmp(ename,eventstriggered.item(j-1).text.get())==0)&&
                    (stricmp(fname,eventsfile.item(j-1).text.get())==0)) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                eventpusher->push(ename,fname,NULL);
                PROGLOG("DFUMON Event Pushed: %s, %s",ename,fname);
            }
        }
    }

    bool performMonitor(IDFUWorkUnit *wu,IDFUmonitor *monitor,IConstDFUfileSpec *source, bool raiseexception, StringAttrArray *eventstriggered, StringAttrArray *eventsfile, IUserDescriptor *user)
    {


        bool sub = monitor->getSub();
        StringBuffer lfn;
        source->getLogicalName(lfn);
        StringAttrArray prev;
        StringAttrArray done;
        monitor->getTriggeredList(prev);
        monitor->setCycleCount(monitor->getCycleCount()+1);
        if (lfn.length()) {                                                 // no wild cards so only 0 or 1 prev
            if (queryDistributedFileDirectory().exists(lfn.str(),user)) {
                done.append(*new StringAttrItem(lfn.str()));
                bool isdone = ((prev.ordinality()!=0)&&
                                (stricmp(prev.item(0).text.get(),lfn.str())==0));
                if (!isdone) {
                    if (eventstriggered) {
                        StringBuffer ename;
                        monitor->getEventName(ename);
                        if (!ename.length())
                            ename.append("DfuLogicalFileMonitor");
                        PROGLOG("MONITOR(%s): triggering event: %s, %s",wu->queryId(),ename.str(),lfn.str());
                        eventstriggered->append(*new StringAttrItem(ename.str()));
                        eventsfile->append(*new StringAttrItem(lfn.str()));
                        unsigned shots = monitor->getShotCount()+1;
                        monitor->setShotCount(shots);
                        unsigned limit = monitor->getShotLimit();
                        if (limit<1)
                            limit = 1;
                        unsigned pc = (shots*100)/limit;
                        IDFUprogress *progress = wu->queryUpdateProgress();
                        progress->setPercentDone(pc);
                        if (shots>=limit) {
                            PROGLOG("MONITOR(%s): Complete",wu->queryId());
                            monitor->setTriggeredList(prev);
                            return true;    // all done
                        }
                    }
                }
                // could compare prev and done TBD
                monitor->setTriggeredList(done);
            }
        }
        else {
            Owned<IFileDescriptor> fdesc = source->getFileDescriptor();
            if (!fdesc)
                return false;
            StringBuffer path;
            StringBuffer dir;
            RemoteFilename rfn;
            if (fdesc->numParts()!=1) {
                OERRLOG("MONITOR: monitor file incorrectly specified");
                if (raiseexception)
                    throw MakeStringException(-1,"MONITOR: monitor file incorrectly specified");
                return true;
            }
            fdesc->getFilename(0,0,rfn);
            rfn.getLocalPath(path);
            const char * filemask = splitDirTail(path.str(),dir);
            RemoteFilename dirfn;
            dirfn.setPath(rfn.queryEndpoint(),dir.str());
            Owned<IFile> dirf = createIFile(dirfn);
            if (!dirf||(dirf->isDirectory()!=foundYes)) {
                OERRLOG("MONITOR: %s is not a directory in DFU WUID %s",dir.str(),wu->queryId());
                if (raiseexception)
                    throw MakeStringException(-1,"MONITOR: %s is not a directory in DFU WUID %s",dir.str(),wu->queryId());
                return true;
            }
            Owned<IDirectoryIterator> iter =  dirf->directoryFiles(filemask,sub);
            if (iter) {
                StringBuffer fname;
                StringBuffer fnamedate;
                CDateTime mod;
                ForEach(*iter) {
                    fname.clear().append(iter->query().queryFilename());    // may need to adjust to match input
                    fnamedate.clear().append(fname).append(';');
                    iter->getModifiedTime(mod);
                    mod.getString(fnamedate);
                    done.append(*new StringAttrItem(fnamedate.str()));
                    bool isdone = false;
                    ForEachItemIn(i,prev) {
                        if(strcmp(prev.item(i).text.get(),fnamedate.str())==0) {
                            isdone = true;
                            break;
                        }
                    }
                    if (!isdone&&eventstriggered) {
                        StringBuffer ename;
                        monitor->getEventName(ename);
                        if (!ename.length())
                            ename.append("DfuFileMonitor");
                        PROGLOG("MONITOR(%s): triggering event: %s, %s",wu->queryId(),ename.str(),fname.str());
                        eventstriggered->append(*new StringAttrItem(ename.str()));
                        eventsfile->append(*new StringAttrItem(fname.str()));
                        unsigned shots = monitor->getShotCount()+1;
                        monitor->setShotCount(shots);
                        unsigned limit = monitor->getShotLimit();
                        if (limit<1)
                            limit = 1;
                        unsigned pc = (shots*100)/limit;
                        IDFUprogress *progress = wu->queryUpdateProgress();
                        progress->setPercentDone(pc);
                        if (shots>=limit) {
                            monitor->setTriggeredList(done);
                            PROGLOG("MONITOR(%s): Complete",wu->queryId());
                            return true;    // all done
                        }
                    }
                }
                // could compare prev and done TBD
                monitor->setTriggeredList(done);
            }
        }
        return false;
    }

    INode *getForeignDali(IConstDFUfileSpec *source)
    {
        SocketEndpoint ep;
        if (!source->getForeignDali(ep))
            return NULL;
        if (ep.port==0)
            ep.port= DALI_SERVER_PORT;
        return createINode(ep);
    }

    struct sSuperCopyContext
    {
        IDFUWorkUnitFactory *wufactory;
        IUserDescriptor *srcuser;
        IUserDescriptor *user;
        IConstDFUWorkUnit *superwu;
        IConstDFUfileSpec *superdestination;
        IConstDFUoptions *superoptions;
        IDFUprogress *superprogress;
        cProgressReporter *feedback;
        unsigned level;
    };

    void constructDestinationName(const char *dstlfn, const char *oldprefix, IConstDFUfileSpec *dest, CDfsLogicalFileName &dlfn, StringBuffer &roxieprefix)
    {
        dlfn.set(dstlfn);
        if (dlfn.isForeign())  // trying to confuse me again!
            throw MakeStringException(-1,"Destination cannot be foreign file");
        if (dest->getRoxiePrefix(roxieprefix).length()) {
            StringBuffer tmp;
            dstlfn = dlfn.get();
            tmp.append(roxieprefix).append("::");
            if (oldprefix&&*oldprefix) {
                size32_t l = strlen(oldprefix);
                if ((l+2<strlen(dstlfn))&&
                    (memicmp(oldprefix,dstlfn,l)==0)&&
                    (dstlfn[l]==':') && (dstlfn[l+1]==':'))
                    dstlfn += l+2;
            }
            tmp.append(dstlfn);
            dlfn.set(tmp.str());
        }
    }


    bool doSubFileCopy(sSuperCopyContext &ctx,const char *dstlfn,INode *srcdali,const char *srclfn,StringAttr &wuid, bool iskey, const char *roxieprefix)
    {
        StringBuffer saveinprogress;
        {
            CriticalBlock block(subcopysect);
            Owned<IDFUWorkUnit> wu = ctx.wufactory->createWorkUnit();
            ctx.superprogress->getSubInProgress(saveinprogress);
            ctx.superprogress->setSubInProgress(wu->queryId());

            StringBuffer tmp;
            ctx.superwu->getClusterName(tmp);
            wu->setClusterName(tmp.str());
            ctx.superwu->getJobName(tmp.clear());
            wu->setJobName(tmp.str());
            ctx.superwu->getQueue(tmp.clear());
            wu->setQueue(tmp.str());
            if (ctx.user) {
                StringBuffer uname;
                ctx.user->getUserName(uname);
                wu->setUser(uname.str());
                StringBuffer pwd;
                wu->setPassword(ctx.user->getPassword(pwd).str());
            }
            IDFUfileSpec *source = wu->queryUpdateSource();
            IDFUfileSpec *destination = wu->queryUpdateDestination();
            IDFUoptions *options = wu->queryUpdateOptions();
            wu->setCommand(DFUcmd_copy);
            source->setLogicalName(srclfn);
            if (srcdali) {
                source->setForeignDali(srcdali->endpoint());
                if (ctx.srcuser) {
                    StringBuffer uname;
                    StringBuffer pwd;
                    ctx.srcuser->getUserName(uname);
                    ctx.srcuser->getPassword(pwd);
                    source->setForeignUser(uname.str(),pwd.str());
                }
            }
            destination->setLogicalName(dstlfn);
            if (roxieprefix&&*roxieprefix)
                destination->setRoxiePrefix(roxieprefix);
            unsigned nc = ctx.superdestination->numClusters();
            for (unsigned i=0;i<nc;i++) {
                ctx.superdestination->getGroupName(i,tmp.clear());
                if (tmp.length()) {
                    ClusterPartDiskMapSpec spec;
                    if (ctx.superdestination->getClusterPartDiskMapSpec(tmp.str(),spec)) {
                        destination->setClusterPartDiskMapSpec(tmp.str(),spec);
                    }
                    else
                        destination->setGroupName(tmp.str());
//                  StringBuffer basedir;   // not needed as in spec
//                  if (ctx.superdestination->getClusterPartDefaultBaseDir(tmp.str(),basedir))
//                      destination->setClusterPartDefaultBaseDir(tmp.str(),basedir);
                }
            }
            options->setNoSplit(ctx.superoptions->getNoSplit());
            options->setOverwrite(ctx.superoptions->getOverwrite());
            options->setReplicate(ctx.superoptions->getReplicate());
            options->setNoRecover(ctx.superoptions->getNoRecover());
            options->setIfNewer(ctx.superoptions->getIfNewer());
            options->setIfModified(ctx.superoptions->getIfModified());
            options->setCrcCheck(ctx.superoptions->getCrcCheck());
            options->setmaxConnections(ctx.superoptions->getmaxConnections());
            options->setPush(ctx.superoptions->getPush());
            options->setRetry(ctx.superoptions->getRetry());
            options->setCrc(ctx.superoptions->getCrc());
            options->setThrottle(ctx.superoptions->getThrottle());
            options->setTransferBufferSize(ctx.superoptions->getTransferBufferSize());
            options->setVerify(ctx.superoptions->getVerify());
            StringBuffer slave;
            if (ctx.superoptions->getSlavePathOverride(slave))
                options->setSlavePathOverride(slave);
            options->setSubfileCopy(true);

            wu->queryUpdateProgress()->setState(DFUstate_queued); // well, good as
            // should be no need for overwrite
            wuid.set(wu->queryId());
        }
        if (wuid.isEmpty())
            return false;
        StringBuffer eps;
        PROGLOG("%s: Copy %s from %s to %s",wuid.get(),srclfn,srcdali?srcdali->endpoint().getUrlStr(eps).str():"(local)",dstlfn);
        DFUstate state = runWU(wuid);
        StringBuffer tmp;
        PROGLOG("%s: Done: %s",wuid.get(),encodeDFUstate(state,tmp).str());
        ctx.superprogress->setSubInProgress(saveinprogress);
        StringBuffer donewuids;
        ctx.superprogress->getSubDone(donewuids);
        if (donewuids.length())
            donewuids.append(',');
        donewuids.append(wuid.get());
        ctx.superprogress->setSubDone(donewuids.str());
        return (state==DFUstate_finished);
    }

    void doSuperForeignCopy(sSuperCopyContext &ctx,const char *dstlfn,INode *foreigndalinode,const char *srclfn, CDfsLogicalFileName &dlfn)
    {
        ctx.level++;
        Linked<INode> srcdali = foreigndalinode;
        CDfsLogicalFileName slfn;
        slfn.set(srclfn);
        if (slfn.isForeign()) { // trying to confuse me
            SocketEndpoint ep;
            slfn.getEp(ep);
            slfn.clearForeign();
            srcdali.setown(createINode(ep));
        }
        Owned<IPropertyTree> ftree = queryDistributedFileDirectory().getFileTree(srclfn,ctx.srcuser,srcdali, FOREIGN_DALI_TIMEOUT, false);
        if (!ftree.get()) {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",slfn.get(),srcdali?srcdali->endpoint().getUrlStr(s).str():"(local)");
        }
        // now we can create name
        StringBuffer newroxieprefix;
        constructDestinationName(dstlfn,ftree->queryProp("Attr/@roxiePrefix"),ctx.superdestination,dlfn,newroxieprefix);
        if (!srcdali.get()||queryCoven().inCoven(srcdali)) {
            // if dali is local and filenames same
            if (strcmp(slfn.get(),dlfn.get())==0) {
                PROGLOG("File copy of %s not done as file local",slfn.get());
                ctx.level--;
                return;
            }
        }

        // first see if target exists (and remove if does and overwrite specified)
        Owned<IDistributedFile> dfile = queryDistributedFileDirectory().lookup(dlfn,ctx.user,true);
        if (dfile) {
            if (!ctx.superoptions->getOverwrite())
                throw MakeStringException(-1,"Destination file %s already exists",dlfn.get());
            if (!dfile->querySuperFile())
            {
                if (ctx.superoptions->getIfModified()&&
                    (ftree->hasProp("Attr/@fileCrc")&&ftree->getPropInt64("Attr/@size")&&
                    ((unsigned)ftree->getPropInt64("Attr/@fileCrc")==(unsigned)dfile->queryAttributes().getPropInt64("@fileCrc"))&&
                    (ftree->getPropInt64("Attr/@size")==dfile->getFileSize(false,false)))) {
                    PROGLOG("File copy of %s not done as file unchanged",srclfn);
                    return;
                }
            }
            dfile->detach();
            dfile.clear();
        }
        if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_File))==0) {
            StringAttr wuid;
            const char *kind = ftree->queryProp("@kind");
            bool iskey = kind&&(strcmp(kind,"key")==0);
            // note  dstlfn doesn't have roxie prefix
            if (!doSubFileCopy(ctx,dstlfn,srcdali,srclfn,wuid,iskey,newroxieprefix.str()))
                throw MakeStringException(-1,"File %s could not be copied - see %s",dstlfn,wuid.isEmpty()?"unknown":wuid.get());

        }
        else if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0) {
            unsigned numtodo=0;
            StringArray subfiles;
            Owned<IPropertyTreeIterator> piter = ftree->getElements("SubFile");
            ForEach(*piter) {
                numtodo++;
            }
            unsigned numdone=0;
            ForEach(*piter) {
                const char *name = piter->query().queryProp("@name");
                CDfsLogicalFileName dlfnsub;
                dlfnsub.set(name);
                CDfsLogicalFileName dlfnres;
                doSuperForeignCopy(ctx,dlfnsub.get(true),foreigndalinode,name,dlfnres);
                numdone++;
                subfiles.append(dlfnres.get());
                if ((ctx.level==1)&&ctx.feedback)
                    ctx.feedback->displayProgress(numtodo?(numdone*100/numtodo):0,0,"unknown",0,0,"",0,0,0);
            }
            // now construct the superfile
            Owned<IDistributedSuperFile> sfile = queryDistributedFileDirectory().createSuperFile(dlfn.get(),ctx.user,true,false);
            if (!sfile)
                throw MakeStringException(-1,"SuperFile %s could not be created",dlfn.get());
            ForEachItemIn(i,subfiles) {
                sfile->addSubFile(subfiles.item(i));
            }
            if (newroxieprefix.length()) {
                DistributedFilePropertyLock lock(sfile);
                lock.queryAttributes().setProp("@roxiePrefix",newroxieprefix.str());
            }

        }
        else {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s in Dali %s is not a file or superfile",srclfn,srcdali?srcdali->endpoint().getUrlStr(s).str():"(local)");
        }
        if ((ctx.level==1)&&ctx.feedback)
            ctx.feedback->displaySummary("0",0);
        ctx.level--;
    }


    void runSuperCopy(IConstDFUWorkUnit *wu, IConstDFUfileSpec *source,IConstDFUfileSpec *destination,
                      IConstDFUoptions *options,IDFUprogress *progress,IUserDescriptor *userdesc,
                      cProgressReporter &feedback)
    {
        Owned<IDFUWorkUnitFactory> wufactory = getDFUWorkUnitFactory();
        Owned<INode> foreigndalinode = getForeignDali(source);
        StringBuffer fu;
        StringBuffer fp;
        Owned<IUserDescriptor> foreignuserdesc;
        if (source->getForeignUser(fu,fp)) {
             foreignuserdesc.setown(createUserDescriptor());
             foreignuserdesc->set(fu.str(),fp.str());
        }
        else
            foreignuserdesc.set(userdesc);
        StringBuffer srcname;
        source->getLogicalName(srcname);
        if (!srcname.length())
            throw MakeStringException(-1,"Source file not specified");
        StringBuffer dstname;
        destination->getLogicalName(dstname);
        if (!dstname.length())
            throw MakeStringException(-1,"Destination not specified");
        sSuperCopyContext ctx;
        ctx.wufactory = wufactory;
        ctx.srcuser = foreignuserdesc;
        ctx.user = userdesc;
        ctx.superwu = wu;
        ctx.superdestination = destination;
        ctx.superoptions = options;
        ctx.superprogress = progress;
        ctx.feedback = &feedback;
        ctx.level = 0;
        ctx.superprogress->setSubInProgress("");
        ctx.superprogress->setSubDone("");
        CDfsLogicalFileName dlfn;
        doSuperForeignCopy(ctx,dstname.str(),foreigndalinode,srcname, dlfn);

    }

    DFUstate runWU(const char *dfuwuid)
    {
        StringBuffer runningpath;
        // only clear cache when nothing running (bit of a kludge)
        class CenvClear
        {
            atomic_t &running;
        public:
            CenvClear(atomic_t &_running)
                : running(_running)
            {
                if (atomic_dec_and_test(&running)) {
                    Owned<IEnvironmentFactory> envf = getEnvironmentFactory(false);
                    Owned<IConstEnvironment> env = envf->openEnvironment();
                    env->clearCache();
                }
            }
            ~CenvClear()
            {
                atomic_inc(&running);
            }
        } cenvclear(runningflag);
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(dfuwuid,false);
        if (!wu) {
            WARNLOG("DFURUN: Workunit %s not found",dfuwuid);
            return DFUstate_unknown;
        }
        if (dfuServerName.length())
            wu->setDFUServerName(dfuServerName.str());
        StringBuffer logname;
        if (fileMsgHandler && fileMsgHandler->getLogName(logname))
            wu->setDebugValue("dfulog", logname.str(), true);
        IConstDFUfileSpec *source = wu->querySource();
        IConstDFUfileSpec *destination = wu->queryDestination();
        IConstDFUoptions *options = wu->queryOptions();
        Owned<IPropertyTree> opttree = createPTreeFromIPT(options->queryTree());
        StringAttr encryptkey;
        StringAttr decryptkey;
        if (options->getEncDec(encryptkey,decryptkey)) {
            opttree->setProp("@encryptKey",encryptkey);
            opttree->setProp("@decryptKey",decryptkey);
        }
        IDFUprogress *progress = wu->queryUpdateProgress();
        IDistributedFileDirectory &fdir = queryDistributedFileDirectory();
        IDistributedFileSystem &fsys = queryDistributedFileSystem();
        Owned<IUserDescriptor> userdesc = createUserDescriptor();
        Owned<IUserDescriptor> foreignuserdesc;
        StringBuffer username;
        {
            StringBuffer password;
            wu->getUser(username);
            wu->getPassword(password);
            userdesc->set(username.str(),password.str());
        }
        IPropertyTree *recovery;
        IRemoteConnection *recoveryconn;
        Owned<IRemoteConnection> runningconn;
        wu->queryRecoveryStore(recoveryconn,recovery,runningpath.clear());
        DFUstate s = progress->getState();
        switch (s) {
        case DFUstate_aborting:
        case DFUstate_started:                      // not sure what this for
            progress->setState(DFUstate_aborted);
            /* no break */
        case DFUstate_aborted:
            WARNLOG("DFURUN: Workunit %s aborted",dfuwuid);
            return DFUstate_aborted;
        case DFUstate_queued:
            break;
        default:
            WARNLOG("DFURUN: Workunit %s unexpected state %d",dfuwuid,(int)s);
            return s;
        }
        bool replicating=false;
        if (recovery&&recovery->getPropBool("@replicating",false))
            replicating = true;
        progress->setState(DFUstate_started);
        Owned<IDFPartFilter> filter;
        const char *fs = options->queryPartFilter();
        if (fs)
            filter.setown(createPartFilter(fs));
        StringBuffer tmp;
        cProgressReporter feedback(progress);
        cAbortNotify abortnotify;
        wu->subscribeAbort(&abortnotify);
        bool iskey=false;
        StringAttr kind;
        bool multiclusterinsert = false;
        bool multiclustermerge = false;
        bool useserverreplicate = false;
        Owned<IFileDescriptor> multifdesc;
        Owned<IFileDescriptor> foreignfdesc;
        Owned<IFileDescriptor> auxfdesc;        // used for multicopy
        DFUstate  finalstate = DFUstate_finished;
        try {
            DFUcmd cmd = wu->getCommand();
            Owned<IDistributedFile> srcFile;
            Owned<IDistributedFile> dstFile;        // NB not attached
            StringAttr dstName;
            StringAttr srcName;
            StringAttr diffNameSrc;
            StringAttr diffNameDst;
            Owned<INode> foreigndalinode;
            StringAttr oldRoxiePrefix;
            bool foreigncopy = false;
            // first check for 'specials' (e.g. multi-cluster keydiff etc)
            switch (cmd) {
            case DFUcmd_copy:
                {
                    source->getDiffKey(tmp.clear());
                    if (tmp.length())
                        diffNameSrc.set(tmp.str());
                    destination->getDiffKey(tmp.clear());
                    if (tmp.length())
                        diffNameDst.set(tmp.str());
                    source->getLogicalName(tmp.clear());
                    CDfsLogicalFileName srclfn;
                    if (tmp.length())
                        srclfn.set(tmp.str());
                    destination->getLogicalName(tmp.clear());
                    CDfsLogicalFileName dstlfn;
                    if (tmp.length())
                        dstlfn.set(tmp.str());
                    SocketEndpoint foreigndali;
                    if (srclfn.isSet()&&dstlfn.isSet()&&(strcmp(srclfn.get(),dstlfn.get())==0)&&(!source->getForeignDali(foreigndali))) {
                        if (!diffNameSrc.isEmpty()||!diffNameDst.isEmpty())
                            throw MakeStringException(-1,"Cannot add to multi-cluster file using keypatch");
                        multiclusterinsert = true;
                    }
                    break;
                }
            }
            // now fill srcfile for commands that need
            switch (cmd) {
            case DFUcmd_copymerge:
                multiclustermerge = true;
                // fall through
            case DFUcmd_copy:
            case DFUcmd_move:
            case DFUcmd_rename:
            case DFUcmd_replicate:
            case DFUcmd_export:
                {
                    source->getLogicalName(tmp.clear());
                    if (!tmp.length())
                        throw MakeStringException(-1,"Source file not specified");
                    foreigncopy = false;
                    if ((cmd==DFUcmd_copy)||multiclustermerge) {
                        foreigndalinode.setown(getForeignDali(source));
                        foreigncopy = foreigndalinode.get()!=NULL;
                        if (foreigncopy) {
                            StringBuffer fu;
                            StringBuffer fp;
                            if (source->getForeignUser(fu,fp)) {
                                 foreignuserdesc.setown(createUserDescriptor());
                                 foreignuserdesc->set(fu.str(),fp.str());
                            }
                            else
                                foreignuserdesc.set(userdesc);
                        }
                    }
                    if (foreigncopy) {
                        foreignfdesc.setown(queryDistributedFileDirectory().getFileDescriptor(tmp.str(),foreignuserdesc,foreigndalinode));
                        if (!foreignfdesc) {
                            StringBuffer s;
                            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",tmp.str(),foreigndalinode->endpoint().getUrlStr(s).str());
                        }
                        kind.set(foreignfdesc->queryProperties().queryProp("@kind"));
                        oldRoxiePrefix.set(foreignfdesc->queryProperties().queryProp("@roxiePrefix"));
                        iskey = strsame("key", kind);
                        if (destination->getWrap()||iskey)
                            destination->setNumPartsOverride(foreignfdesc->numParts());
                        if (options->getPush()) {// need to set ftslave location
                            StringBuffer progpath;
                            StringBuffer workdir;
                            INode *n = foreignfdesc->queryNode(0);
                            if (n&&getRemoteRunInfo("FTSlaveProcess", "ftslave", NULL, n->endpoint(), progpath, workdir, foreigndalinode, 1000*60*5)) {
                                opttree->setProp("@slave",progpath.str());
                            }
                        }
                        if (options->getSubfileCopy())
                            opttree->setPropBool("@compress",foreignfdesc->isCompressed());
                    }
                    else {
                        srcFile.setown(fdir.lookup(tmp.str(),userdesc,
                              (cmd==DFUcmd_move)||(cmd==DFUcmd_rename)||((cmd==DFUcmd_copy)&&multiclusterinsert)));
                        if (!srcFile)
                            throw MakeStringException(-1,"Source file %s could not be found",tmp.str());
                        oldRoxiePrefix.set(srcFile->queryAttributes().queryProp("@roxiePrefix"));
                        iskey = isFileKey(srcFile);
                        kind.set(srcFile->queryAttributes().queryProp("@kind"));
                        if (destination->getWrap()||(iskey&&(cmd==DFUcmd_copy)))    // keys default wrap for copy
                            destination->setNumPartsOverride(srcFile->numParts());
                        if (options->getSubfileCopy())
                            opttree->setPropBool("@compress",srcFile->isCompressed());
                    }
                    if (destination->getMultiCopy()&&!destination->getWrap()) {
                        Owned<IFileDescriptor> tmpfd = foreigncopy?foreignfdesc.getLink():srcFile->getFileDescriptor();
                        auxfdesc.setown(createMultiCopyFileDescriptor(tmpfd,destination->getNumParts(0)));
                    }
                    srcName.set(tmp.str());
                }
                break;
            }
            // fill dstfile for commands that need it
            switch (cmd) {
            case DFUcmd_copymerge:
            case DFUcmd_copy:
            case DFUcmd_move:
            case DFUcmd_import:
            case DFUcmd_add:
                {
                    destination->getLogicalName(tmp.clear());
                    if (tmp.length())
                    {
                        CDfsLogicalFileName tmpdlfn;
                        StringBuffer newroxieprefix;
                        constructDestinationName(tmp.str(),oldRoxiePrefix,destination,tmpdlfn,newroxieprefix);
                        tmp.clear().append(tmpdlfn.get());
                        bool iswin;
                        if (!destination->getWindowsOS(iswin)) // would normally know!
                            {
                            // set default OS to cluster 0
                            Owned<IGroup> grp=destination->getGroup(0);
                            if (grp.get())
                            {
                                switch (queryOS(grp->queryNode(0).endpoint()))
                                {
                                    case MachineOsW2K:
                                        destination->setWindowsOS(true);
                                        iswin = false;
                                        break;
                                    case MachineOsSolaris:
                                    case MachineOsLinux:
                                        destination->setWindowsOS(false);
                                        iswin = false;
                                        break;
                                };
                            }
                        }
                        if (destination->getWrap())
                        {
                            Owned<IFileDescriptor> fdesc = source?source->getFileDescriptor():NULL;
                            if (fdesc)
                                destination->setNumPartsOverride(fdesc->numParts());
                        }

                        if (options->getFailIfNoSourceFile())
                            opttree->setPropBool("@failIfNoSourceFile", true);

                        if (options->getRecordStructurePresent())
                            opttree->setPropBool("@recordStructurePresent", true);

                        opttree->setPropInt("@expireDays", options->getExpireDays());

                        opttree->setPropBool("@quotedTerminator", options->getQuotedTerminator());
                        Owned<IFileDescriptor> fdesc = destination->getFileDescriptor(iskey,options->getSuppressNonKeyRepeats()&&!iskey);
                        if (fdesc)
                        {
                            if (options->getSubfileCopy()) // need to set destination compressed or not
                            {
                                if (opttree->getPropBool("@compress"))
                                    fdesc->queryProperties().setPropBool("@blockCompressed",true);
                                else
                                    fdesc->queryProperties().removeProp("@blockCompressed");
                            }
                            if (!encryptkey.isEmpty())
                            {
                                fdesc->queryProperties().setPropBool("@encrypted",true);
                                fdesc->queryProperties().setPropBool("@blockCompressed",true);
                            }
                            else if (options->getPreserveCompression())
                            {
                                bool dstCompressed = false;
                                if (srcFile)
                                    dstCompressed = srcFile->isCompressed();
                                else
                                {
                                    IFileDescriptor * srcDesc = (auxfdesc.get() ? auxfdesc.get() : foreignfdesc.get());
                                    dstCompressed = srcDesc && srcDesc->isCompressed();
                                }
                                if (dstCompressed)
                                    fdesc->queryProperties().setPropBool("@blockCompressed",true);
                            }

                            if (multiclusterinsert)
                            {
                                if (foreigncopy)
                                    throw MakeStringException(-1,"Cannot create multi cluster file in foreign file");
                                StringBuffer err;
                                if (!srcFile->checkClusterCompatible(*fdesc,err))
                                    throw MakeStringException(-1,"Incompatible file for multicluster add - %s",err.str());
                            }
                            else if (multiclustermerge)
                            {
                                dstFile.setown(fdir.lookup(tmp.str(),userdesc,true));
                                if (!dstFile)
                                    throw MakeStringException(-1,"Destination for merge %s does not exist",tmp.str());
                                StringBuffer err;
                                if (!dstFile->checkClusterCompatible(*fdesc,err))
                                    throw MakeStringException(-1,"Incompatible file for multicluster merge - %s",err.str());
                            }
                            else
                            {
                                Owned<IDistributedFile> oldfile = fdir.lookup(tmp.str(),userdesc,true);
                                if (oldfile)
                                {
                                    StringBuffer reason;
                                    bool canRemove = oldfile->canRemove(reason);
                                    oldfile.clear();
                                    if (!canRemove)
                                        throw MakeStringException(-1,"%s",reason.str());
                                    if (!options->getOverwrite())
                                        throw MakeStringException(-1,"Destination file %s already exists and overwrite not specified",tmp.str());
                                    if (!fdir.removeEntry(tmp.str(),userdesc))
                                        throw MakeStringException(-1,"Internal error in attempt to remove file %s",tmp.str());
                                }
                            }
                            StringBuffer jobname;
                            fdesc->queryProperties().setProp("@owner", username.str());
                            fdesc->queryProperties().setProp("@workunit", dfuwuid);
                            fdesc->queryProperties().setProp("@job", wu->getJobName(jobname).str());
                            StringBuffer tmpprefix;
                            if (newroxieprefix.length())
                                fdesc->queryProperties().setProp("@roxiePrefix", newroxieprefix.str());
                            if (iskey)
                                fdesc->queryProperties().setProp("@kind", "key");
                            else if (kind.length()) // JCSMORE may not really need separate "if (iskey)" line above
                                fdesc->queryProperties().setProp("@kind", kind);
                            if (multiclusterinsert||multiclustermerge)
                                multifdesc.setown(fdesc.getClear());
                            else
                                dstFile.setown(fdir.createNew(fdesc));
                            dstName.set(tmp.str());
                        }
                    }
                    if (!dstFile&&!multiclusterinsert)
                    {
                        throw MakeStringException(-1,"Destination file %s could not be created",tmp.str());
                    }
                }
                break;
            }

            if (defaultTransferBufferSize&&(opttree->getPropInt("@transferBufferSize",0)==0))
                opttree->setPropInt("@transferBufferSize",defaultTransferBufferSize);

            switch (cmd) {
            case DFUcmd_none:
                break;
            case DFUcmd_copymerge:
            case DFUcmd_copy:
                {
                    if (!replicating) {
                        Owned<IFileDescriptor> patchf;
                        Owned<IFileDescriptor> olddstf;
                        if (diffNameSrc.get()||diffNameDst.get()) {
                            Owned<IFileDescriptor> newf;
                            Owned<IFileDescriptor> oldf;
                            if (foreigncopy)
                                newf.set(foreignfdesc);
                            else
                                newf.setown(srcFile->getFileDescriptor());
                            oldf.setown(queryDistributedFileDirectory().getFileDescriptor(diffNameSrc,foreigncopy?foreignuserdesc:userdesc,foreigncopy?foreigndalinode:NULL));
                            if (!oldf.get()) {
                                StringBuffer s;
                                throw MakeStringException(-1,"Old key file %s could not be found in source",diffNameSrc.get());
                            }
                            olddstf.setown(queryDistributedFileDirectory().getFileDescriptor(diffNameDst,userdesc,NULL));
                            if (!olddstf.get()) {
                                StringBuffer s;
                                throw MakeStringException(-1,"Old key file %s could not be found in destination",diffNameDst.get());
                            }
                            patchf.setown(createFileDescriptor());
                            doKeyDiff(oldf,newf,patchf);
                        }
                        runningconn.setown(setRunning(runningpath.str()));
                        bool needrep = options->getReplicate();
                        ClusterPartDiskMapSpec mspec;
                        if (destination) {
                            if (destination->numClusters()==1) {
                                destination->getClusterPartDiskMapSpec(0,mspec);
                                if (!mspec.isReplicated())
                                    needrep = false;
                            }
                        }
                        else if (multifdesc) {
                            if (multifdesc->numClusters()==1) {
                                if (!multifdesc->queryPartDiskMapping(0).isReplicated())
                                    needrep = false;
                            }
                        }
                        if (needrep)
                            feedback.repmode=cProgressReporter::REPbefore;
                        if (foreigncopy)
                            checkPhysicalFilePermissions(foreignfdesc,userdesc,false);
                        if (patchf) { // patch assumes only 1 cluster
                            // need to create dstpatchf
                            StringBuffer gname;
                            destination->getGroupName(0,gname);
                            if (!gname.length())
                                throw MakeStringException(-1,"No cluster specified for destination");
                            Owned<IGroup> grp = queryNamedGroupStore().lookup(gname.str());
                            if (!grp)
                                throw MakeStringException(-1,"Destination cluster %s not found",gname.str());
                            StringBuffer lname;
                            destination->getLogicalName(lname);
                            lname.append(".__patch__");
                            DFD_OS os;
                            switch (queryOS(grp->queryNode(0).endpoint())) {
                            case MachineOsW2K:
                                os = DFD_OSwindows; break;
                            case MachineOsSolaris:
                            case MachineOsLinux:
                                os = DFD_OSunix; break;
                            default:
                                os = DFD_OSdefault;
                            };
                            Owned<IFileDescriptor> dstpatchf = createFileDescriptor(lname.str(),grp,NULL,os,patchf->numParts());
                            fsys.transfer(patchf, dstpatchf, NULL, NULL, NULL, opttree, &feedback, &abortnotify, dfuwuid);
                            removePartFiles(patchf);
                            Owned<IFileDescriptor> newf = dstFile->getFileDescriptor();
                            doKeyPatch(olddstf,newf,dstpatchf);
                            removePartFiles(dstpatchf);
                            if (!abortnotify.abortRequested()) {
                                if (needrep)
                                    replicating = true;
                                else
                                    dstFile->attach(dstName.get(), userdesc);
                                Audit("COPYDIFF",userdesc,srcName.get(),dstName.get());
                            }
                        }
                        else if (foreigncopy||auxfdesc)
                        {
                            IFileDescriptor * srcDesc = (auxfdesc.get() ? auxfdesc.get() : foreignfdesc.get());
                            fsys.import(srcDesc, dstFile, recovery, recoveryconn, filter, opttree, &feedback, &abortnotify, dfuwuid);

                            if (!abortnotify.abortRequested())
                            {
                                if (needrep)
                                    replicating = true;
                                else
                                    dstFile->attach(dstName.get(), userdesc);
                                Audit("COPY",userdesc,srcName.get(),dstName.get());
                            }
                        }
                        else if (multiclusterinsert||multiclustermerge) {
                            fsys.exportFile(srcFile, multifdesc, recovery, recoveryconn, filter, opttree, &feedback, &abortnotify, dfuwuid);
                            if (!abortnotify.abortRequested()) {
                                if (needrep)
                                    replicating = true;
                                else {
                                    StringBuffer cname;
                                    multifdesc->getClusterLabel(0,cname);
                                    if (cname.length()==0)
                                        multifdesc->getClusterGroupName(0,cname,&queryNamedGroupStore());
                                    (multiclusterinsert?srcFile:dstFile)->addCluster(cname.str(),multifdesc->queryPartDiskMapping(0));
                                }
                                Audit(multiclusterinsert?"COPY":"COPYMERGE",userdesc,srcFile?srcFile->queryLogicalName():NULL,dstName.get());
                            }
                        }
                        else {
                            fsys.copy(srcFile,dstFile,recovery, recoveryconn, filter, opttree, &feedback, &abortnotify, dfuwuid);
                            if (!abortnotify.abortRequested()) {
                                if (needrep)
                                    replicating = true;
                                else
                                    dstFile->attach(dstName.get(),userdesc);
                                Audit("COPY",userdesc,srcFile?srcFile->queryLogicalName():NULL,dstName.get());
                            }
                        }
                        runningconn.clear();
                    }
                }
                break;
            case DFUcmd_remove:
                {
                    source->getLogicalName(tmp.clear());
                    if (tmp.length()) {
                        runningconn.setown(setRunning(runningpath.str()));;
                        fdir.removeEntry(tmp.str(),userdesc);
                        Audit("REMOVE",userdesc,tmp.clear(),NULL);
                        runningconn.clear();
                    }
                    else {
                        throw MakeStringException(-1,"No target name specified for remove");
                    }
                }
                break;
            case DFUcmd_move:
                {
                    runningconn.setown(setRunning(runningpath.str()));
                    fsys.move(srcFile,dstFile,recovery, recoveryconn, filter, opttree, &feedback, &abortnotify, dfuwuid);
                    runningconn.clear();
                    if (!abortnotify.abortRequested()) {
                        dstFile->attach(dstName.get(),userdesc);
                        Audit("MOVE",userdesc,srcFile?srcFile->queryLogicalName():NULL,dstName.get());
                    }
                }
                break;
            case DFUcmd_rename:
                {
                    wu->subscribeAbort(NULL);
                    StringBuffer toname;
                    destination->getLogicalName(toname);
                    if (toname.length()) {
                        unsigned start = msTick();
                        Owned<IDistributedFile> newfile = fdir.lookup(toname.str(),userdesc,true);
                        if (newfile) {
                            // check for rename into multicluster
                            CDfsLogicalFileName dstlfn;
                            dstlfn.set(toname.str());
                            if (dstlfn.getCluster(tmp).length()==0)
                                throw MakeStringException(-1,"Target %s already exists",toname.str());
                        }
                        newfile.clear();
                        StringBuffer fromname(srcName);
                        srcFile.clear();
                        queryDistributedFileDirectory().renamePhysical(fromname.str(),toname.str(),userdesc,NULL);
                        StringBuffer timetaken;
                        timetaken.appendf("%dms",msTick()-start);
                        progress->setDone(timetaken.str(),0,true);
                        Audit("RENAME",userdesc,fromname.str(),toname.str());
                    }
                    else {
                        throw MakeStringException(-1,"No target name specified for rename");
                    }
                }
                break;
            case DFUcmd_replicate:
                {
                    runningconn.setown(setRunning(runningpath.str()));
                    DaftReplicateMode mode = DRMreplicatePrimary;
                    StringBuffer repcluster;
                    bool repeatlast;
                    bool onlyrepeated;
                    switch (options->getReplicateMode(repcluster,repeatlast,onlyrepeated)) {
                    case DFURMprimary:
                        mode = DRMreplicatePrimary;
                        break;
                    case DFURMsecondary:
                        mode = DRMreplicateSecondary;
                        break;
                    case DFURMmissing:
                        mode = DRMcreateMissing;
                        break;
                    }
                    setFileRepeatOptions(*srcFile,repcluster.str(),repeatlast,onlyrepeated);
                    Owned<IFileDescriptor> fdesc = srcFile->getFileDescriptor();
                    fdesc->ensureReplicate();
                    fsys.replicate(fdesc.get(), mode, recovery, recoveryconn, filter, opttree, &feedback, &abortnotify, dfuwuid);
                    runningconn.clear();
                    if (!abortnotify.abortRequested()) {
                        Audit("REPLICATE",userdesc,srcFile?srcFile->queryLogicalName():NULL,NULL);
                        // srcFile->queryPartDiskMapping(0).maxCopies = 2;   // ** TBD ?
                    }
                }
                break;
            case DFUcmd_import:
                {
                    if (!replicating) {
                        runningconn.setown(setRunning(runningpath.str()));
                        Owned<IFileDescriptor> fdesc = source->getFileDescriptor();
                        checkPhysicalFilePermissions(fdesc,userdesc,false);
                        checkSourceTarget(fdesc);
                        bool needrep = options->getReplicate();
                        ClusterPartDiskMapSpec mspec;
                        if (destination) {
                            if (destination->numClusters()==1) {
                                destination->getClusterPartDiskMapSpec(0,mspec);
                                if (!mspec.isReplicated())
                                    needrep = false;
                            }
#ifndef _DEBUG
                            StringBuffer gname;
                            if (!destination->getRemoteGroupOverride()&&!testLocalCluster(destination->getGroupName(0,gname).str())) {
                                throw MakeStringException(-1,"IMPORT cluster %s is not recognized locally",gname.str());
                            }
#endif
                        }
                        if (needrep)
                            feedback.repmode=cProgressReporter::REPbefore;
                        fsys.import(fdesc, dstFile, recovery, recoveryconn, filter, opttree, &feedback, &abortnotify, dfuwuid);
                        if (!abortnotify.abortRequested())
                        {
                            if (needrep && !recovery->getPropBool("@noFileMatch"))
                                replicating = true;
                            else
                                dstFile->attach(dstName.get(), userdesc);
                            Audit("IMPORT",userdesc,dstName.get(),NULL);
                        }
                        runningconn.clear();
                    }
                }
                break;
            case DFUcmd_export:
                {
                    runningconn.setown(setRunning(runningpath.str()));
                    Owned<IFileDescriptor> fdesc = destination->getFileDescriptor(iskey);
                    checkPhysicalFilePermissions(fdesc,userdesc,true);
                    checkSourceTarget(fdesc);
                    fsys.exportFile(srcFile, fdesc, recovery, recoveryconn, filter, opttree, &feedback, &abortnotify, dfuwuid);
                    if (!abortnotify.abortRequested()) {
                        Audit("EXPORT",userdesc,srcFile?srcFile->queryLogicalName():NULL,NULL);
                    }
                    runningconn.clear();
                }
                break;
            case DFUcmd_add:
                {
                    dstFile->attach(dstName.get(),userdesc);
                    Audit("ADD",userdesc,dstName.get(),NULL);
                }
                break;
            case DFUcmd_transfer:
                {
                    runningconn.setown(setRunning(runningpath.str()));
                    Owned<IFileDescriptor> srcdesc = source->getFileDescriptor();
                    checkPhysicalFilePermissions(srcdesc,userdesc,true);
                    Owned<IFileDescriptor> dstdesc = destination->getFileDescriptor();
                    checkPhysicalFilePermissions(dstdesc,userdesc,true);
                    fsys.transfer(srcdesc, dstdesc, recovery, recoveryconn, filter, opttree, &feedback, &abortnotify, dfuwuid);
                    if (!abortnotify.abortRequested()) {
                        Audit("TRANSFER",userdesc,NULL,NULL);
                    }
                    runningconn.clear();
                }
                break;
            case DFUcmd_monitor:
                {
                    CriticalBlock block(monitorsect);
                    // first check done when WU received
                    IDFUmonitor *monitor = wu->queryUpdateMonitor();
                    if (!monitor)
                        break;
                    INode *me = queryMyNode();
                    monitor->setHandlerEp(me->endpoint());
                    StringAttrArray eventstriggered;
                    StringAttrArray eventsfile;
                    if (performMonitor(wu,monitor,source,true,&eventstriggered,&eventsfile,userdesc))
                        finalstate = DFUstate_finished;
                    else
                        finalstate = DFUstate_monitoring;
                    pushEvents(eventstriggered,eventsfile);

                }
                break;
            case DFUcmd_supercopy:
                runSuperCopy(wu,source,destination,options,progress,userdesc,feedback);
                break;
            default:
                throw MakeStringException(-1,"DFURUN: Unsupported command (%d)",(int)cmd);
            }
            if (replicating) {
                switch (cmd) {
                case DFUcmd_copymerge:
                case DFUcmd_copy:
                case DFUcmd_import:{
                        if (feedback.repmode==cProgressReporter::REPbefore)
                            feedback.repmode=cProgressReporter::REPduring;
                        runningconn.setown(setRunning(runningpath.str()));
                        Owned<IFileDescriptor> fdesc = multiclusterinsert?multifdesc.getLink():dstFile->getFileDescriptor();
                        DaftReplicateMode mode = DRMreplicatePrimary;
                        // bit of a kludge here until filecopy supports multi copies
                        for (unsigned i=fdesc->numParts();i>0;i--) {
                            if (fdesc->numCopies(i-1)>2) {
                                mode = DRMcreateMissing;
                                break;
                            }
                        }
                        wu->removeRecoveryStore();
                        wu->queryRecoveryStore(recoveryconn,recovery,runningpath.clear());
                        if (recoveryconn&&recovery) {
                            recovery->setPropBool("@replicating",true);
                            recoveryconn->commit();
                        }
                        fsys.replicate(fdesc.get(), mode, recovery, recoveryconn, filter, opttree, &feedback, &abortnotify, dfuwuid);
                        if (!abortnotify.abortRequested()) {
                            if (multiclusterinsert||multiclustermerge) {
                                StringBuffer cname;
                                multifdesc->getClusterLabel(0,cname);
                                if (cname.length()==0)
                                    multifdesc->getClusterGroupName(0,cname,&queryNamedGroupStore());
                                (multiclusterinsert?srcFile:dstFile)->addCluster(cname.str(),multifdesc->queryPartDiskMapping(0));
                            }
                            else {
                                //dstFile->queryPartDiskMapping(0).maxCopies = 2;           // dont think this is right ** TBD
                                dstFile->attach(dstName.get(),userdesc);
                            }
                            progress->setDone(NULL,0,true);
                            Audit("REPLICATE",userdesc,dstName.get(),NULL);
                        }
                        runningconn.clear();
                    }
                    break;
                }
            }
            wu->removeRecoveryStore();
            wu->subscribeAbort(NULL);
        }
        catch(IException *e) {
            runningconn.clear();
            wu->subscribeAbort(NULL);
            wu->addException(e);
            EXCLOG(e, "DFURUN Exception: ");
            finalstate = DFUstate_failed;
        }
        if ((finalstate != DFUstate_aborted)&&abortnotify.abortRequested())
            finalstate = DFUstate_aborted;
        progress->setState(finalstate);
        wu.clear();
        return finalstate;
    }
};


IDFUengine *createDFUengine()
{
    return new CDFUengine;
}

void stopDFUserver(const char *qname)
{
    Owned<IJobQueue> queue = createJobQueue(qname);
    if (!queue.get()) {
        throw MakeStringException(-1, "Cound not create queue");
    }
    IJobQueueItem *item = createJobQueueItem("!STOP");
    item->setEndpoint(queryMyNode()->endpoint());
    queue->enqueue(item);
}
