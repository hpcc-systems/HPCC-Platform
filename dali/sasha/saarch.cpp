#include "platform.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jregexp.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "dasds.hpp"
#include "saserver.hpp"
#include "salds.hpp"
#include "sacmd.hpp"
#include "workunit.hpp"
#include "environment.hpp"
#include "sautil.hpp"
#include "workunitservices.ipp"


#define DEFAULT_INTERVAL                6       // hours

#define DEFAULT_WORKUNIT_LIMIT          1000
#define DEFAULT_WORKUNIT_CUTOFF         60      // days
#define DEFAULT_DFUWORKUNIT_LIMIT       1000
#define DEFAULT_DFUWORKUNIT_CUTOFF      60      // days
#define DEFAULT_DFURECOVERY_LIMIT       5
#define DEFAULT_DFURECOVERY_CUTOFF      4       // days
#define DEFAULT_CACHEDWORKUNIT_LIMIT    100
#define DEFAULT_CACHEDWORKUNIT_CUTOFF   2       // days



//#define TESTING

static CriticalSection archivingSect;
static const bool keepOldVersions=true;


static const char *splitWUID(const char *wuid,StringBuffer &head)
{
    while (*wuid&&(*wuid!='-')) {
        head.append((char)toupper(*wuid));
        wuid++;
    }
    return wuid;
}

static void splitWUIDpath(const char *wuid,StringBuffer &path)
{
    StringBuffer head;
    splitWUID(wuid,head);
    if (head.length()) {
        if (path.length())
            path.append('/');
        path.append(head);
    }
}

static void mkDateCompare(bool dfu,const char *dt,StringBuffer &out,char fill)
{
    out.clear();
    if (dt&&*dt) {
        while (*dt&&!isdigit(*dt))
            dt++;
        if (dfu)
            out.append('D');
        else
            out.append('W');
        unsigned i=0;
        while (out.length()<9)
            if (dt[i])
                out.append(dt[i++]);
            else
                out.append(fill);
        out.append('-');
        if (dt[i]) // skip '-'
            i++;
        while (dt[i]||(out.length()<16))
            if (dt[i])
                out.append(dt[i++]);
            else
                out.append(fill);
    }
}

void WUiterate(ISashaCommand *cmd, const char *mask)
{
    class CWUData : public CInterface
    {
    public:
        CDateTime modifiedTime;
        Owned<IPropertyTree> wuTree;
    };

    class CArchivedWUReader : public CSimpleInterface
    {
        StringBuffer mask, fromDT, toDT, fromDir, toDir, afterWU;
        bool hasWUSOutput, outputModifiedTime, outputXML;
        ISashaCommand* cmd;

        void getMasks(const char *beforeWU, const char *afterWU, StringBuffer &dirMask, StringBuffer &fileMask)
        {
            //The fromDT and toDT are the filters which defines a search range.
            //The afterWU (beforeWU) defines paging within the search range. 
            //For the first page, a user may define the fromDT and/or toDT without afterWU or beforeWU.
                    
            StringBuffer from, to;
            if (!isEmptyString(afterWU) && (fromDT.isEmpty() || (strcmp(afterWU, fromDT.str()) > 0)))
                from.set(afterWU);
            else if (!fromDT.isEmpty())
                from.set(fromDT.str());
            if (!isEmptyString(beforeWU) && (toDT.isEmpty() || (strcmp(beforeWU, toDT.str()) < 0)))
                to.set(beforeWU);
            else if (!toDT.isEmpty())
                to.set(toDT.str());

            if (!isEmptyString(from))
                splitWUID(from, fromDir);
            if (!isEmptyString(to))
                splitWUID(to, toDir);

            bool unfiltered = mask.isEmpty() || streq(mask.str(), "*");

            //The following code is a clone of the pre-existing code. hmask -> dirMask. tmask -> fileMask.
            //The dirMask is used to filter out the directories (ex: W20171010) which store archived WU files.
            //The fileMask is used to filter out the WU files (ex: W20171010-101010*.xml).
            StringBuffer masktmp;
            if (unfiltered && !from.isEmpty() && !to.isEmpty())
            {
                const char *lo = from.str();
                const char *hi = to.str();
                while (*lo && (toupper(*lo) == toupper(*hi)))
                {
                    masktmp.append((char)toupper(*lo));
                    lo++;
                    hi++;
                }
                masktmp.append("*");
                mask = masktmp.str();
            }

            StringBuffer head;
            if (!mask.isEmpty())
            {
                splitWUID(mask.str(), head);
                if (!head.isEmpty())
                    dirMask = head.str();
                fileMask.set(mask.str()).toUpperCase();
            }
            else
                fileMask.append("*");
            fileMask.append(".xml");
        }
        bool checkDirs(const char *dir)
        {
            if (!fromDir.isEmpty() && (strcmp(dir, fromDir.str()) < 0))
                return false;
            if (!toDir.isEmpty() && (strcmp(dir, toDir.str()) > 0))
                return false;
            return true;
        }
        bool parseAndCheckWUID(bool &checkBeforeOrAfterWU, const char *beforeWU,
            const char *afterWU, StringBuffer &name)
        {
            if (name.length() < 5)
                return false;

            name.setLength(name.length()-4);
            name.toUpperCase();
            const char *wuid = name.str();
            if ((name.length()>6) && strieq(wuid+name.length()-6, "_HINTS"))
                return false;

            if (!mask.isEmpty() && !WildMatch(wuid, mask.str(),true))
                return false;

            if (checkBeforeOrAfterWU)
            {
                if (!isEmptyString(afterWU) && (stricmp(wuid, afterWU) <= 0))
                    return false;
                if (!isEmptyString(beforeWU) && (stricmp(wuid, beforeWU) >= 0))
                    return false;
                if (isEmptyString(beforeWU) || isEmptyString(afterWU))
                    checkBeforeOrAfterWU = false; //Found the starting point. So, do not need to check anymore.
            }
            return true;
        }
        bool checkFilters(IPropertyTree *t)
        {
            const char *owner = cmd->queryOwner();
            const char *cluster = cmd->queryCluster();
            const char *jobName = cmd->queryJobName();
            const char *state = cmd->queryState();
            const char *priority = cmd->queryPriority();
            const char *eclContains = cmd->queryEclContains();
            const char *fileRead = cmd->queryFileRead();
            const char *fileWritten = cmd->queryFileWritten();
            const char *cmdName = cmd->queryDfuCmdName();

            //The following code is a clone of the pre-existing code.
            StringBuffer path, val;
            if (!isEmptyString(owner) && (!t->getProp("@submitID", val.clear()) || !WildMatch(val.str(), owner, true)))
                return false;
            if (!isEmptyString(state) && (!t->getProp(cmd->getDFU()?"Progress/@state":"@state", val.clear()) || !WildMatch(val.str(), state, true)))
                return false;
            if (!isEmptyString(cluster) && (!t->getProp("@clusterName", val.clear()) || !WildMatch(val.str(), cluster, true)))
                return false;
            if (!isEmptyString(jobName) && (!t->getProp("@jobName", val.clear()) || !WildMatch(val.str(), jobName, true)))
                return false;
            if (!isEmptyString(cmdName) && (!t->getProp("@command", val.clear()) || !WildMatch(val.str(), cmdName, true)))
                return false;
            if (!isEmptyString(priority) && (!t->getProp("@priorityClass", val.clear()) || !WildMatch(val.str(), priority, true)))
                return false;
            if (!isEmptyString(fileRead) && !t->hasProp(path.setf("FilesRead/File[@name=~?\"%s\"]", fileRead).str()))
                return false;
            if (!isEmptyString(fileWritten) && !t->hasProp(path.setf("Files/File[@name=~?\"%s\"]", fileWritten).str()))
                return false;
            if (!isEmptyString(eclContains) && !t->hasProp(path.setf("Query[Text=~?\"*%s*\"]", eclContains).str()))
                return false;
            return true;
        }
        bool addOutput(StringArray &outputFields, IPropertyTree *wuTree, CDateTime &modifiedTime, MemoryBuffer &WUSbuf)
        {
            if (hasWUSOutput)
            { //cmd->getAction()==SCA_WORKUNIT_SERVICES_GET
                if (!serializeWUSrow(*wuTree, WUSbuf, false))
                    return false; //Log overflowed?
            }
            else if (outputXML)
            { //cmd->getAction()==SCA_GET
                StringBuffer xml;
                toXML(wuTree, xml);
                if (!cmd->addResult(xml.str()))
                    return false; //Log overflowed?
            }
            else
            {
                if (outputFields.length() == 0)
                    cmd->addId(wuTree->queryName());
                else
                {
                    StringBuffer output = wuTree->queryName();
                    setWUDataTree(wuTree, outputFields, output);
                    cmd->addId(output.str());
                }
                if (outputModifiedTime)
                    cmd->addDT(modifiedTime);
            }
            return true;
        }
        void setWUDataTree(IPropertyTree *wu, StringArray &outputFields, StringBuffer &output)
        {
            ForEachItemIn(i, outputFields)
            {
                const char* outputField = (const char*)outputFields.item(i);

                StringBuffer val;
                bool found = true;
                if (strieq(outputField, "owner"))
                    wu->getProp("@submitID", val);
                else if (strieq(outputField, "cluster"))
                    wu->getProp("@clusterName", val);
                else if (strieq(outputField, "jobname"))
                    wu->getProp("@jobName",val);
                else if (strieq(outputField,"state"))
                    wu->getProp("@state", val);
                else
                    found = false;
                if (found)
                    output.append(',').append(val);
            }
        }

    public:

        CArchivedWUReader(ISashaCommand *_cmd, const char *_mask)
        {
            cmd = _cmd;
            mask.set(_mask);
            hasWUSOutput = cmd->getAction()==SCA_WORKUNIT_SERVICES_GET;
            outputModifiedTime = cmd->getAction()==SCA_LISTDT;
            outputXML = cmd->getAction()==SCA_GET;

            //In WUiterate(), the queryAfter() is used as fromDT and the queryBefore() is used as toDT.
            mkDateCompare(cmd->getDFU(), cmd->queryAfter(), fromDT, '0');
            mkDateCompare(cmd->getDFU(), cmd->queryBefore(), toDT, '9');
        }
        void getWUs()
        {
            const char *owner = cmd->queryOwner();
            const char *cluster = cmd->queryCluster();
            const char *jobName = cmd->queryJobName();
            const char *state = cmd->queryState();
            const char *priority = cmd->queryPriority();
            const char *eclContains = cmd->queryEclContains();
            const char *fileRead = cmd->queryFileRead();
            const char *fileWritten = cmd->queryFileWritten();
            const char *cmdName = cmd->queryDfuCmdName();
            const char *beforeWU = cmd->queryBeforeWU();
            const char *afterWU = cmd->queryAfterWU();
            unsigned numWUs = cmd->getLimit();
            StringBuffer path;
            if (cmd->getDFU())
                getLdsPath("Archive/DFUWorkUnits", path);
            else
                getLdsPath("Archive/WorkUnits", path);
            Owned<IFile> dir = createIFile(path.str());

            StringArray outputFields;
            const char *outputFormat = cmd->queryOutputFormat();
            outputFields.appendList(outputFormat, "|,");

            StringBuffer dirMask, fileMask;
            getMasks(beforeWU, afterWU, dirMask, fileMask);
            bool checkBeforeOrAfterWU = !isEmptyString(beforeWU) || !isEmptyString(afterWU);
            bool sortInc = !isEmptyString(beforeWU) && isEmptyString(afterWU);

            MemoryBuffer WUSbuf; //Not sure what this is.
            if (hasWUSOutput)
                cmd->setWUSresult(WUSbuf);  // swap in/out (in case ever do multiple)

            CIArrayOf<CWUData> wus;
            unsigned wuCount = 0;
            bool overflowed = false;
            StringBuffer output;
            Owned<IDirectoryIterator> dirIterator = dir->directoryFilesSorted(SD_bynameNC, sortInc, dirMask.isEmpty() ? nullptr : dirMask.str(), false, true);
            ForEach(*dirIterator)
            {
                dirIterator->getName(output.clear());
                if (!dirIterator->isDir() || !checkDirs(output.str()))
                    continue;

                Owned<IDirectoryIterator> fileIterator = dirIterator->query().directoryFilesSorted(SD_bynameNC, sortInc, fileMask.str(), false);
                ForEach(*fileIterator)
                {
                    fileIterator->getName(output.clear());
                    if (!parseAndCheckWUID(checkBeforeOrAfterWU, beforeWU, afterWU, output))
                        continue;
    
                    const char *wuid = output.str();
   
                    Owned<IPropertyTree> wuTree;
                    if (outputXML || hasWUSOutput || !isEmptyString(outputFormat) || !isEmptyString(owner)
                        || !isEmptyString(state) || !isEmptyString(cluster) || !isEmptyString(jobName)
                        || !isEmptyString(cmdName) || !isEmptyString(priority) || !isEmptyString(fileRead)
                        || !isEmptyString(fileWritten) || !isEmptyString(eclContains))
                    {
                        try
                        {
                            wuTree.setown(createPTree(fileIterator->query()));
                            if (!wuTree || isEmptyString(wuTree->queryName()) || !checkFilters(wuTree))
                                continue;
                        }
                        catch (IException *e)
                        {
                            VStringBuffer msg("WUiterate: Workunit %s failed to load", wuid);
                            EXCLOG(e,msg.str());
                            e->Release();
                            continue;
                        }
                    }

                    if (sortInc)
                    {//The output should contain WUs from new to old. So, we need to reverse the order before output.
                        Owned<CWUData> wu = new CWUData();
                        if (outputXML || !isEmptyString(outputFormat))
                            wu->wuTree.setown(wuTree.getClear());
                        if (outputModifiedTime)
                            fileIterator->getModifiedTime(wu->modifiedTime);
                        wus.append(*wu.getClear());
                    }
                    else
                    {
                        CDateTime dt;
                        if (outputModifiedTime)
                            fileIterator->getModifiedTime(dt);

                        if (!addOutput(outputFields, wuTree, dt, WUSbuf))
                        {
                            overflowed = true;
                            break;
                        }
                    }
                    wuCount++;
                    if (wuCount == numWUs)
                        break;
                }
                if (overflowed || (wuCount == numWUs))
                    break;
            }
            if (sortInc)
            {
                ForEachItemInRev(i, wus)
                {
                    CWUData &wuData = wus.item(i);
                    if (!addOutput(outputFields, wuData.wuTree, wuData.modifiedTime, WUSbuf))  //Log an error?
                        break;
                }
            }

            if (hasWUSOutput)
                cmd->setWUSresult(WUSbuf);
        }
    };

    Owned<CArchivedWUReader> reader = new CArchivedWUReader(LINK(cmd), mask);
    if (cmd->getArchived())
        reader->getWUs();
    if (cmd->getOnline())
    {//The following code is from the pre-existing code.
        bool dfu = cmd->getDFU();
        const char *beforedt = cmd->queryBefore();
        const char *afterdt = cmd->queryAfter();
        const char *owner = cmd->queryOwner();
        const char *state = cmd->queryState();
        const char *cluster = cmd->queryCluster();
        const char *jobname = cmd->queryJobName();
        const char *outputformat = cmd->queryOutputFormat();
        unsigned start = cmd->getStart(); 
        unsigned num = cmd->getLimit();
        StringBuffer before;
        StringBuffer after;
        StringBuffer tmppath;
        mkDateCompare(dfu,afterdt,after,'0');
        mkDateCompare(dfu,beforedt,before,'9');
        if (cmd->getAction()==SCA_WORKUNIT_SERVICES_GET)
            throw MakeStringException(-1,"SCA_WORKUNIT_SERVICES_GET not implemented for online workunits!");
        bool isWild = !mask || !*mask || isWildString(mask);
        StringBuffer xpath = dfu ? "DFU/WorkUnits" : "WorkUnits";
        if (!isWild)
            xpath.append("/").append(mask);
        Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), 0, 5*60*1000);
        if (conn)
        {
            Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements(isWild ? "*" : NULL);
            unsigned index = 0;
            StringBuffer val;
            ForEach(*iter) {
                IPropertyTree &pt=iter->query();
                const char *wuid = pt.queryName();
                if (index>start+num)
                    break;
        //      PROGLOG("match before=%s after=%s wuid=%s",before.str(),after.str(),wuid);
                if ((!mask||!*mask||!isWild||WildMatch(wuid,mask,true)) &&
                    ((before.length()==0)||(stricmp(wuid,before)<0)) &&
                    ((after.length()==0)||(stricmp(wuid,after)>=0))) {
        //          PROGLOG("matched before=%s after=%s wuid=%s",before.str(),after.str(),wuid);
                    bool hasowner = owner&&*owner;
                    bool hascluster = cluster&&*cluster;
                    bool hasstate = state&&*state;
                    bool hasjobname = jobname&&*jobname;
                    bool hasoutput = outputformat&&*outputformat;
                    bool inrange = (index>=start)&&(index<start+num);
                    if (hasowner||hasstate||hascluster||hasjobname||(hasoutput&&inrange)) {
                        try {
                            if (hasowner&&(!pt.getProp("@submitID",val.clear())||!WildMatch(val.str(),owner,true)))
                                continue;
                            if (hasstate&&(!pt.getProp("@state",val.clear())||!WildMatch(val.str(),state,true)))
                                continue;
                            if (hascluster&&(!pt.getProp("@clusterName",val.clear())||!WildMatch(val.str(),cluster,true)))
                                continue;
                            if (hasjobname&&(!pt.getProp("@jobName",val.clear())||!WildMatch(val.str(),jobname,true)))
                                continue;
                        }
                        catch (IException *e) {
                            StringBuffer msg;
                            msg.appendf("WUiterate: Workunit %s failed", wuid);
                            EXCLOG(e,msg.str());
                            e->Release();
                            continue;
                        }
                    }
                    index++;
                    if (!inrange)
                        continue;
                    StringBuffer name(wuid);
                    if (hasoutput) {
                        char *saveptr;
                        char *parse = strdup(outputformat);
                        char *tok = strtok_r(parse, "|,",&saveptr);
                        while (tok) {
                            val.clear();
                            bool found = true;
                            if (stricmp(tok,"owner")==0)
                                pt.getProp("@submitID",val);
                            else if (stricmp(tok,"cluster")==0)
                                pt.getProp("@clusterName",val);
                            else if (stricmp(tok,"jobname")==0)
                                pt.getProp("@jobName",val);
                            else if (stricmp(tok,"state")==0)
                                pt.getProp("@state",val);
                            else
                                found = false;
                            if (found)
                                name.append(',').append(val);
                            tok = strtok_r(NULL, "|,",&saveptr);
                        }
                        free(parse);
                    }
                    cmd->addId(name.str());
                    if (cmd->getAction()==SCA_GET) {
                        StringBuffer xml;
                        toXML(&pt,xml);
                        if (!cmd->addResult(xml.str()))
                            break;
                    }
                }
                if (index>start+num)
                    break;
            }
        }
    }
}

interface IBranchItem: extends IInterface
{
    virtual int compare(IBranchItem &to)=0;
    virtual bool archive()=0;
    virtual bool backup()=0;
    virtual bool isempty()=0;
    virtual bool qualifies()=0;     // archive
    virtual bool qualifiesBackup()=0;
};


class CBranchArchiver
{
protected:
    Owned<IPropertyTree> props;
    StringAttr branchname;
    StringAttr branchxpath;
    CSashaSchedule schedule;
    unsigned limit;
    bool &stopped;
    IRemoteConnection* conn;
    unsigned cutoffdays;
    unsigned backupdays;    // if 0 not used
    CDateTime cutoff;   // set when run
    CDateTime backupcutoff; // set when run
#ifdef _DEBUG
    bool firsttime;
#endif
public:
    unsigned numlater;
    unsigned numprotected;
    unsigned numnulltimes;

    CBranchArchiver(IPropertyTree *archprops,const char *_branchname,const char *_branchxpath,unsigned deflimit,unsigned definterval,unsigned defcutoff, bool &_stopped) 
        : branchname(_branchname),branchxpath(_branchxpath), stopped(_stopped)
    {
#ifdef _DEBUG
        firsttime = true;
#endif
        if (archprops->hasProp(branchname))
            props.setown(archprops->getPropTree(branchname));
        else
            props.setown(createPTree(branchname));
        schedule.init(props,definterval,definterval/4);
        limit = props->getPropInt("@limit",deflimit);
        cutoffdays = props->getPropInt("@cutoff",defcutoff);
        backupdays = props->getPropInt("@backup",0);
    }

    virtual IBranchItem *createBranchItem(IPropertyTree &e) = 0;

    static int compareBranch(IInterface * const *v1, IInterface * const *v2) // for bAdd only
    {
        IBranchItem *e1 = (IBranchItem *)*v1;
        IBranchItem *e2 = (IBranchItem *)*v2;
        return e1->compare(*e2);
    }

    bool ready()
    {
#ifdef _DEBUG
        if (firsttime) {
            firsttime = false;
            return true;
        }
#endif
        return (limit!=0)&&schedule.ready();
    }

    void run(IRemoteConnection *_conn)
    {
        if (!_conn) 
            return;
        conn = _conn;
        cutoff.setNow();
        cutoff.adjustTime(-60*24*cutoffdays);
        if (backupdays) {
            backupcutoff.setNow();
            backupcutoff.adjustTime(-60*24*backupdays);
        }
        else
            backupcutoff.clear();
        action();
        conn = NULL;
    }

    virtual void action()   // default action is archiving
    {

        PROGLOG("ARCHIVE: Scanning %s limit=%d",branchname.get(),limit);
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements(branchxpath);
        IArrayOf<IBranchItem> branches;
        IArrayOf<IBranchItem> backups;
        unsigned count = 0;
        unsigned emptycount = 0;
        IArrayOf<IBranchItem> toarchive;
        IArrayOf<IBranchItem> tobackup;
        numlater = 0;
        numprotected = 0;
        numnulltimes = 0;
        ForEach(*iter) {
            IPropertyTree &e=iter->query();
            Owned<IBranchItem> item = createBranchItem(e);
            if (!item->isempty()) {
                count++;
                if (item->qualifies())  // archive
                    branches.append(*item.getClear());
                else if (item->qualifiesBackup()) 
                    tobackup.append(*item.getClear());
            }
            else
                emptycount++;
            if (stopped)
                break;
        }
        try {
            conn->commit();
        }
        catch (IException *e) {             // something deleted probably so roll back
            EXCLOG(e,"action commit");
            e->Release();
            conn->rollback();
        }

        PROGLOG("ARCHIVE count=%d ignored=%d later=%d nulltimes=%d protected=%d",count,emptycount,numlater,numnulltimes,numprotected);
        unsigned total=branches.ordinality();
        unsigned done = 0;
        unsigned bdone = 0;
        if (!stopped) {
            unsigned num = (count>limit)?count-limit:0;
            while (branches.ordinality()&&!stopped) {
                Owned<IBranchItem> item = &branches.popGet();
                IInterface *val=item;
                bool added;
                toarchive.bAdd(val,compareBranch,added);
                if (added)
                    item.getClear();    
                if (toarchive.ordinality()>num) {
                    Owned<IBranchItem> itemp = &toarchive.popGet();
                    if (itemp->qualifiesBackup())
                        tobackup.append(*itemp.getClear());
                }
            }
            PROGLOG("ARCHIVE: %s - %d to archive, %d to backup",branchname.get(),toarchive.ordinality(),tobackup.ordinality());
            unsigned start = msTick();
            unsigned start1;
            while (toarchive.ordinality()&&!stopped) {
                start1 = msTick();
                Owned<IBranchItem> item = &toarchive.popGet();
                if (item&&item->archive())
                    done++;
                if (!schedule.checkDurationAndThrottle(start,start1,stopped))
                    break;
            }
            while (tobackup.ordinality()&&!stopped) {
                start1 = msTick();
                Owned<IBranchItem> item = &tobackup.popGet();
                if (item&&item->backup())
                    bdone++;
                if (!schedule.checkDurationAndThrottle(start,start1,stopped))
                    break;
            }
        }
        PROGLOG("ARCHIVE: %s complete (%d archived of %d, %d backed up)",branchname.get(),done,total,bdone);
    }


};

static bool doArchiveDfuWorkUnit(const char *dfuwuid, StringBuffer &res)
{
    CriticalBlock block(archivingSect);
    if (!dfuwuid||!*dfuwuid)
        return false;
#ifdef TESTING
    bool del = false; 
#else
    bool del = true;
#endif
    StringBuffer ldspath("Archive/DFUWorkUnits");
    splitWUIDpath(dfuwuid,ldspath);
    StringBuffer path;
    getLdsPath(ldspath.str(),path);
    addPathSepChar(path).append(dfuwuid).append(".xml");
    Owned<IFile> file = createIFile(path.str());
    res.append("ARCHIVE: ").append(dfuwuid).append(" ");
    if (file) {
        StringBuffer buf;
        StringBuffer xpath("DFU/WorkUnits/");
        xpath.append(dfuwuid);
        try {
            Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ, 5*60*1000);  
            if (conn) {
                toXML(conn->queryRoot(), buf, 0, XML_Format|XML_SortTags);
                Owned<IFileIO> fileio = file->open(IFOcreate);
                if (fileio && (fileio->write(0,buf.length(),buf.str())==buf.length())) {
                    fileio.clear();
                    if (del)
                        conn->close(true);
                    res.append("OK");
                    return true; 
                }
            }
        }
        catch (IException *e) {
            e->errorMessage(res);
            res.append(' ');
            e->Release();
        }
    }
    res.append("FAILED");
    return false;
}


static bool doArchiveWorkUnit(IWorkUnitFactory *wufactory,const char *wuid, StringBuffer &res,bool deleteOwned, bool del, CDateTime *time)
{
    CriticalBlock block(archivingSect);
    if (del)
        res.append("ARCHIVE: ");
    else
        res.append("BACKUP: ");
    res.append(wuid).append(" ");
    if (wufactory) {
        Owned<IWorkUnit> wu;
        try {
            wu.setown(wufactory->updateWorkUnit(wuid));
        }
        catch (IException *e) { // probably locked
            e->errorMessage(res);
            res.append(' ');
            e->Release();
        }   
        if (wu) {
            try {
#ifdef TESTING
                del = false; 
#endif
                StringBuffer ldspath("Archive/WorkUnits");
                splitWUIDpath(wuid,ldspath);
                StringBuffer path;
                getLdsPath(ldspath.str(),path);
                if (keepOldVersions||!del) {
                    StringBuffer renpath(path);
                    addPathSepChar(renpath).append(wuid).append(".xml");
                    Owned<IFile> file = createIFile(renpath.str());
                    try {
                        if (file->exists()) {
                            CDateTime dt;
                            file->getTime(&dt,NULL,NULL);
                            if (time&&(time->compare(dt,false)<0)) {
                                //PROGLOG("%s already backed up",wuid);
                                if (!del) {
                                    res.clear();
                                    return true;
                                }
                            }
                            else {
                                renpath.clear().append(wuid).append('.');
                                dt.getString(renpath).append(".xml");
                                file->rename(renpath.str());
                            }
                        }
                    }
                    catch (IException *e) { // maybe duplicate already?
                        StringBuffer msg;
                        e->errorMessage(msg);
                        WARNLOG("ARCHIVE %s renaming %s to %s",msg.str(),path.str(),renpath.str());
                        e->Release();
                    }
                }
                QUERYINTERFACE(wu.get(), IExtendedWUInterface)->archiveWorkUnit(path.str(),del,true,deleteOwned,true);
                res.append("OK");
                return true;
            }
            catch (IException *e) {
                e->errorMessage(res);
                res.append(' ');
                e->Release();
            }
        }
    }
    res.append("FAILED");
    return false;
}

static bool doRestoreWorkUnit(IWorkUnitFactory *wufactory,const char *wuid, StringBuffer &res)
{
    if (wuid && *wuid)
    {
        CriticalBlock block(archivingSect);
        res.append("RESTORE: ").append(wuid).append(" ");
        StringBuffer ldspath("Archive/WorkUnits");
        splitWUIDpath(wuid,ldspath);
        StringBuffer base;
        getLdsPath(ldspath.str(), base);
        try {
            if (wufactory->restoreWorkUnit(base, wuid, true)) {
                res.append("OK");
                return true;
            }
        }
        catch (IException *e) {
            e->errorMessage(res);
            res.append(' ');
            e->Release();
        }
    }
    res.append("FAILED");
    return false;
}

static bool doRestoreDfuWorkUnit(const char *wuid, StringBuffer &res)
{
    CriticalBlock block(archivingSect);
    res.append("RESTORE: ").append(wuid).append(" ");
    StringBuffer ldspath("Archive/DFUWorkUnits");
    splitWUIDpath(wuid,ldspath);
    StringBuffer path;
    getLdsPath(ldspath.str(),path);
    path.append(wuid).append(".xml");
    try {
        Owned<IFile> file = createIFile(path.str());
        if (file) {
            Owned<IFileIO> fileio = file->open(IFOread);
            if (fileio) {
                Owned<IPropertyTree> pt = createPTree(*fileio);
                StringBuffer buf;
                StringBuffer xpath("DFU/WorkUnits");
                Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, 5*60*1000);  
                if (conn) {
                    IPropertyTree *root = conn->queryRoot();
                    if (!root->hasProp(wuid)) {
                        root->addPropTree(wuid,pt.getClear());
                        res.append("OK");
                        return true;
                    }
                    res.append(" already exists ");
                }
            }
        }
    }
    catch (IException *e) {
        e->errorMessage(res);
        res.append(' ');
        e->Release();
    }
    res.append("FAILED");
    return false; 
}


class CWorkUnitArchiver: public CBranchArchiver
{
    Owned<IWorkUnitFactory> wufactory;
    bool keepResultFiles;
    class cWUBranchItem : public CInterface, implements IBranchItem
    {
        CDateTime &cutoff;
        CDateTime &backupcutoff;
        StringAttr wuid;
        bool iserr;
        bool isprotected;
        CWorkUnitArchiver *parent;
        CDateTime time;
        CDateTime &getTime()
        {
            return time;
        }
    public:
        IMPLEMENT_IINTERFACE;
        cWUBranchItem(CWorkUnitArchiver *_parent,IPropertyTree &e,CDateTime &_cutoff,CDateTime &_backup,unsigned retryinterval) 
            : wuid(e.queryName()), cutoff(_cutoff), backupcutoff(_backup)
        {
            parent = _parent;
            isprotected = e.getPropBool("@protected", false);
            StringBuffer dts;
            CDateTime dt;
            CDateTime now;
            if (!isprotected) { // see if restored
                if (e.getProp("@restoredDate",dts.clear())) {
                    dt.setString(dts);
                    if (dt.compareDate(cutoff)<=0)
                        isprotected = true;
                }
            }
            iserr = e.getPropBool("@archiveError", false);
            if (iserr&&!isprotected&&retryinterval) {
                if (e.getProp("@archiveErrorDate",dts.clear())) {
                    dt.setString(dts.str());
                    now.setNow();
                    dt.adjustTime(retryinterval*60*24);
                    if (now.compareDate(dt)>0)
                        iserr = false;
                }
                else 
                    iserr = false; // old 
                if (!iserr)
                    e.setPropBool("@archiveError", false);
#ifdef _DEBUG
                if (iserr)
                    PROGLOG("ARCHIVE: Err(%s) date %s",wuid.str(),dts.str());
#endif
            }
            getWorkUnitCreateTime(wuid,time);
            // get latest time stamp
            Owned<IPropertyTreeIterator> iter = e.getElements("TimeStamps/TimeStamp/*");
            if (iter) {
                ForEach(*iter) {
                    CDateTime cmp;
                    cmp.setString(iter->query().queryProp(NULL));
                    if (time.isNull()||(!cmp.isNull()&&(cmp.compare(time)>0)))
                        time.set(cmp);
                }
            }
#ifdef _DEBUG
            CDateTime ct;
            getWorkUnitCreateTime(wuid,ct);
            ct.adjustTime(24*60);
            if (ct.compare(time)<0) {
                StringBuffer s1;
                time.getDateString(s1);
                StringBuffer s2;
                ct.getDateString(s2);
                PROGLOG("ARCHIVE: %s recent date %s %s",wuid.str(),s1.str(),s2.str());
            }
#endif
        }
        virtual ~cWUBranchItem() {}
        bool isempty() { 
            bool ret = (wuid[0]!='W')||iserr; 
#ifdef _DEBUG
            if (ret)
                PROGLOG("IGNORING %s %s",wuid.get(),iserr?"err":"");
#endif
            return ret; 
        }
        bool qualifies() 
        { 
            if (isprotected)
                parent->numprotected++;
            else if (time.isNull())
                parent->numnulltimes++;
            else if (cutoff.isNull()||(time.compare(cutoff)>=0))
                parent->numlater++;
            return !isprotected&&!time.isNull()&&!cutoff.isNull()&&(time.compare(cutoff)<0);
        }   
        bool qualifiesBackup() 
        { 
            return !time.isNull()&&!backupcutoff.isNull()&&(time.compare(backupcutoff)<0);
        }   

        virtual int compare(IBranchItem &to)
        {  // time assumed setup by qualifies
            cWUBranchItem *toc = QUERYINTERFACE(&to, cWUBranchItem);
            return getTime().compare(toc->getTime());
        }

        bool archive()
        {
            return parent->archive(wuid,true,&time);
        }

        bool backup()
        {
            return parent->archive(wuid,false,&time);
        }

    };

public:

    IBranchItem *createBranchItem(IPropertyTree &e)
    {
        return new cWUBranchItem(this,e,cutoff,backupcutoff,props->getPropInt("@retryinterval",7));
    }
    
    CWorkUnitArchiver(IPropertyTree *archprops,unsigned definterval,bool &_stopped)
        : CBranchArchiver(archprops,"WorkUnits","WorkUnits/*",DEFAULT_WORKUNIT_LIMIT,definterval,DEFAULT_WORKUNIT_CUTOFF,_stopped)
    {
        wufactory.setown(getWorkUnitFactory());
        keepResultFiles = props->getPropBool("@keepResultFiles");
        PROGLOG("ARCHIVE Workunits: limit=%d, cutoff=%d days, backup=%d days, keepResultFiles=%s",limit,cutoffdays,backupdays,keepResultFiles?"yes":"no");
    }



    bool archive(const char *wuid,bool del,CDateTime *time)
    {
        StringBuffer s;
        if (doArchiveWorkUnit(wufactory,wuid,s,!keepResultFiles,del,time)) {
            if (s.length()) 
                PROGLOG("%s",s.str());
            return true;
        }
#ifndef TESTING
        try {
            Owned<IRemoteConnection> wuconn = querySDS().connect("/WorkUnits", myProcessSession(), 0, 5*60*1000);  
            if (wuconn) {
                IPropertyTree * tree = wuconn->queryRoot()->queryPropTree(wuid);
                if (tree) {
                    tree->setPropBool("@archiveError",true);
                    CDateTime dt;
                    dt.setNow();
                    StringBuffer dts;
                    dt.getString(dts);
                    tree->setProp("@archiveErrorDate",dts.str());
                }
            }
        }
        catch (IException *e) {
            StringBuffer err;
            e->errorMessage(err);
            WARNLOG("setting archiveError: %s",err.str());
            e->Release();
        }
#endif
        if (s.length())
            WARNLOG("%s",s.str());
        return false;
    }


};

class CDFUWorkUnitArchiver: public CBranchArchiver
{

protected:
    class cDFUWUBranchItem : public CInterface, implements IBranchItem
    {
        CDateTime &cutoff;
        StringAttr wuid;
        bool iserr;
        bool isprotected;
        CDFUWorkUnitArchiver *parent;
        CDateTime time;
        CDateTime &getTime()
        {
            return time;
        }
    public:
        IMPLEMENT_IINTERFACE;
        cDFUWUBranchItem(CDFUWorkUnitArchiver *_parent,IPropertyTree &e,CDateTime &_cutoff) 
            : wuid(e.queryName()), cutoff(_cutoff)
        {
            parent = _parent;
            isprotected = e.getPropBool("@protected", false);
            iserr = e.getPropBool("@archiveError", false);
            getWorkUnitCreateTime(wuid,time);
        }
        virtual ~cDFUWUBranchItem() {}
        bool isempty() { return (wuid[0]!='D')||iserr; }
        bool qualifies() 
        { 
            if (isprotected)
                return false;
            if (isprotected)
                parent->numprotected++;
            else if (time.isNull())
                parent->numnulltimes++;
            else if (cutoff.isNull()||(time.compare(cutoff)>=0))
                parent->numlater++;
            return !time.isNull()&&(getTime().compare(cutoff)<0);
        }   
        bool qualifiesBackup() 
        { 
            return false;
        }   

        virtual int compare(IBranchItem &to)
        {  // time assumed setup by qualifies
            cDFUWUBranchItem *toc = QUERYINTERFACE(&to, cDFUWUBranchItem);
            return getTime().compare(toc->getTime());
        }

        bool archive()
        {
            return parent->archive(wuid);
        }

        bool backup()
        {
            ERRLOG("cDFUWUBranchItem backup not supported");
            return true;
        }


    };

public:

    IBranchItem *createBranchItem(IPropertyTree &e)
    {
        return new cDFUWUBranchItem(this,e,cutoff);
    }
    
    CDFUWorkUnitArchiver(IPropertyTree *archprops,unsigned definterval,bool &_stopped)
        : CBranchArchiver(archprops,"DFUworkunits","DFU/WorkUnits/*",DEFAULT_DFUWORKUNIT_LIMIT,DEFAULT_DFUWORKUNIT_CUTOFF,definterval,_stopped)
    {
        PROGLOG("ARCHIVE DFU Workunits: limit=%d, cutoff=%d days",limit,cutoffdays);
    }


    bool archive(const char *wuid)
    {
        StringBuffer s;
        if (doArchiveDfuWorkUnit(wuid,s)) {
            if (s.length())
                PROGLOG("%s",s.str());
            return true;
        }
        if (s.length())
            WARNLOG("%s",s.str());
        return false;
    }


};



class CDFUrecoveryArchiver: public CBranchArchiver
{


    class cDRBranchItem : public CInterface, implements IBranchItem
    {
        Linked<IPropertyTree> tree;
        CDFUrecoveryArchiver *parent;
        CDateTime time;
        bool timeset;
        CDateTime &cutoff;
        CDateTime &getTime()
        {
            StringBuffer timestr;
            if (!timeset&&tree->getProp("@time_started",timestr)) {
                time.setString(timestr.str());
                timeset = true;
            }
            return time;
        }
    public:
        IMPLEMENT_IINTERFACE;
        cDRBranchItem(CDFUrecoveryArchiver *_parent,IPropertyTree &e, CDateTime &_cutoff) 
            : tree(&e), cutoff(_cutoff)
        {
            parent = _parent;
            timeset = false;
        }
        virtual ~cDRBranchItem() {}
        bool isempty() { return false; }
        bool qualifies() 
        { 
            return (getTime().compare(cutoff)<0)&&timeset;
        }   
        bool qualifiesBackup() 
        { 
            return false;
        }   

        virtual int compare(IBranchItem &to)
        {  // time assumed setup by qualifies
            cDRBranchItem *toc = QUERYINTERFACE(&to, cDRBranchItem);
            return getTime().compare(toc->getTime());
        }

        bool archive()
        {
            return parent->archive(tree);
        }

        bool backup()
        {
            ERRLOG("cDRBranchItem backup not supported");
            return true;
        }

    };

public:

    IBranchItem *createBranchItem(IPropertyTree &e)
    {
        return new cDRBranchItem(this,e,cutoff);
    }
    
    CDFUrecoveryArchiver(IPropertyTree *archprops,unsigned definterval,bool &_stopped)
        : CBranchArchiver(archprops,"DFUrecovery","DFU/RECOVERY/job",DEFAULT_DFURECOVERY_LIMIT,definterval,DEFAULT_DFURECOVERY_CUTOFF,_stopped)
    {
        PROGLOG("ARCHIVE DFU Recovery: limit=%d, cutoff=%d days",limit,cutoffdays);
    }

    bool archive(IPropertyTree *tree)
    {
        const char *id = tree->queryProp("@id");
        if (!id|!*id)
            return false;
        try {
            StringBuffer path;
            getLdsPath("Archive/DFUrecovery",path);
            addPathSepChar(path).append(id).append(".xml");
            saveXML(path.str(), tree, 0, XML_Format);
#ifdef TESTING
            bool del = false;
#else
            bool del = true;
#endif
            if (del) {
                IPropertyTree *pt = conn->queryRoot()->queryPropTree("DFU/RECOVERY");
                if (!pt)
                    return false;
                pt ->removeTree(tree);
            }
            PROGLOG("ARCHIVE: DFUrecovery %s archived", id);
            conn->commit();
            return true;
        }
        catch (IException *e) {
            StringBuffer msg;
            msg.appendf("ARCHIVE: DFUrecovery %s failed", id);
            EXCLOG(e,msg.str());
            e->Release();
        }
        return false;
    }


};

class CCachedWorkUnitRemover: public CBranchArchiver
{

public:

    IBranchItem *createBranchItem(IPropertyTree &e)
    {
        return NULL;    // not used
    }
    
    CCachedWorkUnitRemover(IPropertyTree *archprops,unsigned definterval,bool &_stopped)
        : CBranchArchiver(archprops,"CachedWorkUnits","",DEFAULT_CACHEDWORKUNIT_LIMIT,definterval,0,_stopped)
    {
        PROGLOG("CLEANUP: Cached WorkUnits: limit=%d",limit);
    }

    virtual void action()   // overriding default action 
    {
        // check limit exceeded
        PROGLOG("CLEANUP: Scanning CachedWorkUnits");
        IPropertyTree *root = conn->queryRoot();
        Owned<IPropertyTreeIterator> iter1 = root->getElements("CachedWorkUnits/*");
        StringAttrArray branchlist;
        StringBuffer path;
        ForEach(*iter1) {
            Owned<IPropertyTreeIterator> iter2 = iter1->query().getElements("*");
            ForEach(*iter2) {
                path.clear().append("CachedWorkUnits/").append(iter1->query().queryName()).append('/').append(iter2->query().queryName());
                branchlist.append(*new StringAttrItem(path.str()));
            }
        }
        unsigned n = branchlist.ordinality();
        unsigned i = 0;
        unsigned start=msTick();
        while ((i+limit<n)&&!stopped) {
            const char * xpath = branchlist.item(i).text.get();
            PROGLOG("REMOVE CachedWorkUnit: Removing %s",xpath);
            unsigned start1=msTick();
#ifndef TESTING         
            try {
                {
                    Owned<IRemoteConnection> delconn = querySDS().connect(xpath, myProcessSession(), RTM_LOCK_READ | RTM_DELETE_ON_DISCONNECT, 5*60*1000);  
                }
            }
            catch (IException *e) {
                StringBuffer msg;
                msg.append("CLEANUP: CachedWorkUnit failed ").append(xpath);
                EXCLOG(e,msg.str());
                e->Release();
            }
#endif
            if (!schedule.checkDurationAndThrottle(start,start1,stopped))
                break;
            i++;
        }
        PROGLOG("CLEANUP: CachedWorkUnits complete (%d removed of %d)",i,n);
    }


};




class CSashaArchiverServer: public ISashaServer, public Thread
{  

    bool stopped;
    Semaphore stopsem;
    Owned<IConstEnvironment> env;
public:
    IMPLEMENT_IINTERFACE;

    CSashaArchiverServer()
        : Thread("CSashaArchiverServer")
    {
        Owned<IEnvironmentFactory> f = getEnvironmentFactory();
        env.setown(f->openEnvironment());
        stopped = false;
    }

    ~CSashaArchiverServer()
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
        join();
    }

    void runArchiver(CBranchArchiver &archiver,Owned<IRemoteConnection> &conn)
    {
        if (!stopped&&archiver.ready()) {
            if (!conn.get()) {
                env->clearCache();  // need to do for some archive operations
                conn.setown(querySDS().connect("/", myProcessSession(), 0, 5*60*1000));  
            }
            archiver.run(conn);
        }
    }


    int run()
    {
        Owned<IPropertyTree> archprops = serverConfig->getPropTree("Archiver");
        if (!archprops)
            archprops.setown(createPTree("Archiver"));
        unsigned definterval = archprops->getPropInt("@interval",DEFAULT_INTERVAL); // no longer used
        if (definterval==0)
            definterval = DEFAULT_INTERVAL;
        CWorkUnitArchiver wuarchiver(archprops,definterval,stopped);
        CDFUWorkUnitArchiver dfuwuarchiver(archprops,definterval,stopped);
        CDFUrecoveryArchiver drarchiver(archprops,definterval,stopped);
        CCachedWorkUnitRemover cwuremove(archprops,definterval,stopped);
        while (!stopped) {
            try {
                Owned<IRemoteConnection> conn;
                runArchiver(wuarchiver,conn);
                runArchiver(dfuwuarchiver,conn);
                runArchiver(drarchiver,conn);
                runArchiver(cwuremove,conn);
            }
            catch (IException *e) {
                EXCLOG(e,"SASHA ARCHIVE SERVER");
                if (!stopped) {
                    requestStop(e);
                    stopped = true;
                }
                e->Release();
                break;
            }
            stopsem.wait(60*1000);  // poll every minute
        }
        return 0;
    }


} *sashaArchiverServer = NULL;

ISashaServer *createSashaArchiverServer()
{
    assertex(!sashaArchiverServer); // initialization problem
    sashaArchiverServer = new CSashaArchiverServer();
    return sashaArchiverServer;
}


bool processArchiverCommand(ISashaCommand *cmd)
{
    // TBD NEED TIMEOUT!
    if (!cmd)
        return false;
    try {
        unsigned n=cmd->numIds();
        // need to copy input ids
        StringAttrArray ids;
        unsigned i;
        for (i=0;i<n;i++)
            ids.append(*new StringAttrItem(cmd->queryId(i)));
        cmd->clearIds();
        cmd->clearResults();
        switch (cmd->getAction()) {
        case SCA_ARCHIVE:
        case SCA_BACKUP:
        case SCA_RESTORE: {
                Owned<IWorkUnitFactory> wufactory;
                if (!cmd->getDFU())
                    wufactory.setown(getWorkUnitFactory());
                StringBuffer ret;
                PROGLOG("COMMAND: %s",(cmd->getAction()==SCA_ARCHIVE)?"ARCHIVE":((cmd->getAction()==SCA_RESTORE)?"RESTORE":"BACKUP"));
                unsigned i;
                for (i=0;i<n;i++) {
                    const char *wuid = ids.item(i).text.get();
                    ret.clear();
                    if (cmd->getAction()!=SCA_RESTORE) {
                        if (cmd->getDFU())
                            doArchiveDfuWorkUnit(wuid,ret); 
                        else
                            doArchiveWorkUnit(wufactory,wuid,ret,true,cmd->getAction()==SCA_ARCHIVE,NULL);
                    }
                    else  if (cmd->getDFU()) 
                        doRestoreDfuWorkUnit(wuid,ret); 
                    else
                        doRestoreWorkUnit(wufactory,wuid,ret); 
                    if (ret.length())
                        cmd->addId(ret.str());  // should be result probably but keep bwd compatible
                    PROGLOG("%s",ret.str());
                }
            }
            break;
        case SCA_LIST: 
        case SCA_GET: 
        case SCA_WORKUNIT_SERVICES_GET: 
        case SCA_LISTDT: 
            {
                if (n) {
                    for (i=0;i<n;i++)
                        WUiterate(cmd,ids.item(i).text.get());
                }
                else
                    WUiterate(cmd,NULL);
            }
            break;
        default:
            return false;
        }
    }
    catch (IException *e) {
        EXCLOG(e,"processArchiverCommand");
        e->Release();
        return false;
    }
    return true;
}
