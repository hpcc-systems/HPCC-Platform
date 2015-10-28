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

#include "jfile.hpp"
#include "jiface.hpp"
#include "jprop.hpp"
#include "jutil.hpp"


#include "thgraphmaster.ipp"
#include "thorport.hpp"
#include "thormisc.hpp"

#include "daaudit.hpp"
#include "dadfs.hpp"
#include "dalienv.hpp"
#include "dasess.hpp"
#include "dautils.hpp"
#include "thmfilemanager.hpp"
#include "thexception.hpp"

#include "workunit.hpp"

#define CHECKPOINTSCOPE "checkpoints"
#define TMPSCOPE "temporary"

//#define TRACE_RESOLVE

static IThorFileManager *fileManager = NULL;

typedef OwningStringHTMapping<IDistributedFile> CIDistributeFileMapping;
class CFileManager : public CSimpleInterface, implements IThorFileManager
{
    OwningStringSuperHashTableOf<CIDistributeFileMapping> fileMap;
    bool replicateOutputs;


    StringBuffer &_getPublishPhysicalName(CJobBase &job, const char *logicalName, unsigned partno, const char *groupName, IGroup *group, StringBuffer &res)
    {
        // JCSMORE - this is daft functionality!
        // I think used by pipe progs create a logical entry at run time and writing to physicals
        // Problems:
        // i) it writes non replicated entry.
        // ii) It publishes immediately not when done!
        // iii) it is not removing existing physicals first
        // If we really want this, it should replicate when done somehow, and only publish at end.
        Owned<IDistributedFile> file = lookup(job, logicalName, false, true);
        StringBuffer scopedName;
        addScope(job, logicalName, scopedName);
        if (group) // publishing
        {
            if (partno >= group->ordinality())
                throw MakeThorException(TE_NoSuchPartForLogicalFile , "No such part number (%d) for logical file : %s", partno, scopedName.str());
            if (file)
            {
                file.clear();
                // JCSMORE delete orphan file parts here..
                // i.e. shouldn't it call
                // dfd.removeEntry(scopedName.str(), userDesc);
                // And/Or queryDistributedFileDirectory().removePhysical(scopedName.str(), 0, NULL, NULL, userDesc);
            }
            // overwrite assumed allowed here
            StringArray clusters;
            clusters.append(groupName);
            IArrayOf<IGroup> groups;
            groups.append(*LINK(group));
            Owned<IFileDescriptor> fileDesc = create(job, scopedName, clusters, groups, true, TDWnoreplicate);
            publish(job, scopedName, *fileDesc, &file);
        }
        else
        {
            if (!file)
                throw MakeThorException(TE_LogicalFileNotFound, "getPhysicalName: Logical file doesn't exist (%s)", scopedName.str());
            if (partno >= file->numParts())
                throw MakeThorException(TE_NoSuchPartForLogicalFile , "No such part number (%d) for logical file : %s", partno, logicalName);
        }
        Owned<IDistributedFilePart> part = file->getPart(partno);
        RemoteFilename rfn;
        part->getFilename(rfn).getRemotePath(res);
        return res;
    }

    bool scanDFS(const char *pattern, const char *scope, StringArray &results)
    {
        // TBD
        return false;
    }

    unsigned fixTotal(CJobBase &job, IArrayOf<IGroup> &groups, unsigned &offset)
    {
        offset = 0;
        unsigned max = 0;

        ForEachItemIn(g, groups)
        {
            IGroup *group = &groups.item(g);
            if (group->ordinality() <= queryDfsGroup().ordinality()) // i.e. cluster will make >= parts than target group (if more wraps)
            {
                if (queryDfsGroup().ordinality() > max) max = queryDfsGroup().ordinality();
            }
            else
            {
                // Check options to shrink 'large' tgtfile to this cluster size
                const char *wideDestOptStr = globals->queryProp("@wideDestOpt");
                if (wideDestOptStr)
                {
                    if (0 == stricmp("smallMiddle", wideDestOptStr))
                    {
                        if (groups.ordinality() > 1)
                            throwUnexpected();
                        GroupRelation relation = queryDfsGroup().compare(group);
                        if (GRbasesubset == relation)
                        {
                            offset = group->rank(&queryDfsGroup().queryNode(0));
                            if (offset)
                                throwUnexpected(); // would require desc->setPartOffset(n); type functionality.
                        }
                        return queryDfsGroup().ordinality();
                    }
                    else if (0 == stricmp("smallStart", wideDestOptStr))
                    {
                        if (queryDfsGroup().ordinality() > max) max = queryDfsGroup().ordinality();
                    }
                }
                // else leave wide and try to target subset - if want smaller file on bigger remote clusters should use "smallStart" option
                if (group->ordinality() > max) max = group->ordinality();
            }
        }
        return max;
    }

    void remove(IDistributedFile &file, IUserDescriptor &userDesc)
    {
        StringBuffer lfn;
        file.getLogicalName(lfn);
        CDfsLogicalFileName dlfn;
        dlfn.set(lfn.str());
        if (dlfn.isExternal())
        {
            RemoteFilename rfn;
            dlfn.getExternalFilename(rfn);
            StringBuffer path;
            rfn.getPath(path);
            OwnedIFile iFile = createIFile(path.str());
            if (iFile->exists() && !iFile->remove())
                throw MakeThorException(0, "Failed to remove external file: %s", lfn.str());
        }
        else
            file.detach();
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFileManager()
    {
        replicateOutputs = globals->getPropBool("@replicateOutputs");
    }
    StringBuffer &mangleLFN(CJobBase &job, const char *lfn, StringBuffer &out)
    {
        out.append(lfn).append("__").append(job.queryWuid());
        return out;
    }
    StringBuffer &addScope(CJobBase &job, const char *logicalname, StringBuffer &ret, bool temporary=false, bool paused=false)
    {
        StringBuffer tmp;
        bool root=false;
        while (' ' == *logicalname)
            ++logicalname;
        if ('~' == *logicalname)
        {
            root=true;
            ++logicalname;
        }
        if (temporary)
        {
            StringBuffer tail;
            tmp.append(queryDfsXmlBranchName(DXB_Internal)).append("::");
            const char *user = job.queryUser();
            if (user && *user)
                tmp.append(user).append("::");
            if (paused)
            {
                tmp.append(PAUSETMPSCOPE);
                CDfsLogicalFileName dfslfn;
                dfslfn.set(logicalname);
                dfslfn.getTail(tail);
                logicalname = tail.str();
            }
            else
                tmp.append(job.queryUseCheckpoints() ? CHECKPOINTSCOPE : TMPSCOPE);
            tmp.append("::");

            mangleLFN(job, logicalname, tmp);
        }
        else
        {
            if (!root)
            {
                tmp.append(job.queryScope()).trim();
                if (tmp.length())
                {
                    tmp.toLowerCase();
                    tmp.append("::");
                }
            }
            tmp.append(logicalname).clip();
        }
        normalizeLFN(tmp.str(), ret);
        return ret;
    }

// IThorFileManager impl.
    void clearCacheEntry(const char *name)
    {
        fileMap.remove(name);
    }

    void noteFileRead(CJobBase &job, IDistributedFile *file, bool extended)
    {
        Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
        wu->noteFileRead(file);

        StringArray clusters;
        file->getClusterNames(clusters);
        StringBuffer outs;
        outs.appendf(",FileAccess,Thor,%s,%s,%s,%s,%s,%s,%" I64F "d,%d",
                        extended?"EXTEND":"READ",
                        globals->queryProp("@nodeGroup"),
                        job.queryUser(),
                        file->queryLogicalName(),
                        job.queryWuid(),
                        job.queryGraphName(),
                        file->getFileSize(false, false),clusters.ordinality());
        ForEachItemIn(i,clusters) {
            outs.append(',').append(clusters.item(i));
        }
        LOG(daliAuditLogCat,"%s",outs.str());
    }

    IDistributedFile *lookup(CJobBase &job, const char *logicalName, bool temporary=false, bool optional=false, bool reportOptional=false, bool updateAccessed=true)
    {
        StringBuffer scopedName;
        bool paused = false;
        if (temporary && job.queryResumed())
        { // need to check if previously published.
            StringBuffer pausedName;
            addScope(job, logicalName, pausedName, temporary, true); // temporary ignore if paused==true
            if (job.queryWorkUnit().queryFileUsage(pausedName.str())) // 0 == doesn't exist
            {
                paused = true;
                scopedName.set(pausedName.str());
            }
        }
        if (!paused)
            addScope(job, logicalName, scopedName, temporary);
        CIDistributeFileMapping *fileMapping = fileMap.find(scopedName.str());
        if (fileMapping)
            return &fileMapping->get();

        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(scopedName.str(), job.queryUserDescriptor());
        if (file && 0 == file->numParts())
        {
            if (file->querySuperFile())
            {
                if (optional)
                    file.clear();
                else
                    throw MakeStringException(TE_MachineOrderNotFound, "Superfile %s has no content\n", scopedName.str());
            }
            else
                throw MakeStringException(-1, "Unexpected, standard file %s contains no parts", scopedName.str());
        }
        if (!file)
        {
            if (!optional)
                throw MakeStringException(TE_MachineOrderNotFound, "Missing logical file %s\n", scopedName.str());
            if (reportOptional)
            {
                Owned<IThorException> e = MakeThorException(0, "Input file '%s' was missing but declared optional", scopedName.str());
                e->setAction(tea_warning);
                reportExceptionToWorkunit(job.queryWorkUnit(), e);
            }
            return NULL;
        }
        if (updateAccessed)
            file->setAccessed();
        return LINK(file);
    }

    IFileDescriptor *create(CJobBase &job, const char *logicalName, StringArray &groupNames, IArrayOf<IGroup> &groups, bool overwriteok, unsigned helperFlags=0, bool nonLocalIndex=false, unsigned restrictedWidth=0)
    {
        bool temporary = 0 != (helperFlags&TDXtemporary);
        bool jobReplicate = 0 != job.getWorkUnitValueInt("replicateOutputs", replicateOutputs);
        bool replicate = 0 != jobReplicate && !temporary && 0==(helperFlags&TDWnoreplicate);
        bool persistent = 0 != (helperFlags&TDWpersist);
        bool extend = 0 != (helperFlags&TDWextend);
        bool jobTemp = 0 != (helperFlags&TDXjobtemp);

        LOG(MCdebugInfo, thorJob, "createLogicalFile ( %s )", logicalName);

        Owned<IDistributedFile> efile;
        CDfsLogicalFileName dlfn;
        if (!temporary)
        {
            if (!dlfn.setValidate(logicalName))
                throw MakeStringException(99, "Cannot publish %s, invalid logical name", logicalName);
            if (dlfn.isForeign())
                throw MakeStringException(99, "Cannot publish to a foreign Dali: %s", logicalName);
            efile.setown(queryDistributedFileDirectory().lookup(dlfn, job.queryUserDescriptor(), true));
            if (efile)
            {
                if (!extend && !overwriteok)
                    throw MakeStringException(TE_OverwriteNotSpecified, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", logicalName);
            }
        }

        StringAttr wuidStr(job.queryWorkUnit().queryWuid());
        StringAttr userStr(job.queryWorkUnit().queryUser());
        StringAttr jobStr(job.queryWorkUnit().queryJobName());
        if (overwriteok && (!temporary || job.queryUseCheckpoints()))
        {
            if (!temporary)
            {
                // removing dfs entry, factor out space for each file part used if previously considered by this wuid.
                Owned<IWorkUnit> workunit = &job.queryWorkUnit().lock();
                Owned<IPropertyTreeIterator> fileIter = &workunit->getFileIterator();
                bool found=false;
                ForEach (*fileIter)
                {
                    if (0 == stricmp(logicalName, fileIter->query().queryProp("@name")))
                    {
                        found = true;
                        break;
                    }
                }
                if (found)
                {
                    workunit->releaseFile(logicalName);
                    Owned<IDistributedFile> f = queryDistributedFileDirectory().lookup(dlfn, job.queryUserDescriptor());
                    if (f)
                    {
                        unsigned p, parts = f->numParts();
                        for (p=0; p<parts; p++)
                        {
                            Owned<IDistributedFilePart> part = f->getPart(p);
                            offset_t sz = part->getFileSize(false, false);
                            if (-1 != sz)
                                job.addNodeDiskUsage(p, -(__int64)sz);
                        }
                    }
                }
            }
            if (efile.get())
            {
                __int64 fs = efile->getFileSize(false,false);
                StringArray clusters;
                unsigned c=0;
                for (; c<efile->numClusters(); c++)
                {
                    StringBuffer clusterName;
                    efile->getClusterName(c, clusterName);
                    clusters.append(clusterName);
                }
                remove(*efile, *job.queryUserDescriptor());
                efile.clear();
                efile.setown(queryDistributedFileDirectory().lookup(dlfn, job.queryUserDescriptor(), true));
                if (!efile.get())
                {
                    ForEachItemIn(c, clusters)
                    {
                        LOG(daliAuditLogCat,",FileAccess,Thor,DELETED,%s,%s,%s,%s,%s,%" I64F "d,%s",
                                        globals->queryProp("@name"),
                                        userStr.str(),
                                        logicalName,
                                        wuidStr.str(),
                                        job.queryGraphName(),fs,clusters.item(c));
                    }
                }
            }
        }
        Owned<IFileDescriptor> desc;
        if (!temporary && dlfn.isExternal())
            desc.setown(createExternalFileDescriptor(dlfn.get()));
        else
        {
            desc.setown(createFileDescriptor());
            if (temporary)
                desc->queryProperties().setPropBool("@temporary", temporary);
            if (persistent)
                desc->queryProperties().setPropBool("@persistent", persistent);
            desc->queryProperties().setProp("@workunit", wuidStr.str());
            desc->queryProperties().setProp("@job", jobStr.str());
            desc->queryProperties().setProp("@owner", userStr.str());

            // if supporting different OS's in CLUSTER this should be checked where addCluster called
            DFD_OS os = DFD_OSdefault;
            EnvMachineOS thisOs = queryOS(groups.item(0).queryNode(0).endpoint());
            switch (thisOs)
            {
                case MachineOsW2K:
                    os = DFD_OSwindows;
                    break;
                case MachineOsLinux:
                case MachineOsSolaris:
                    os = DFD_OSunix;
                    break;
            };

            unsigned offset = 0;
            unsigned total;
            if (restrictedWidth)
                total = restrictedWidth;
            else
                total = fixTotal(job, groups, offset);
            if (nonLocalIndex)
                ++total;
            StringBuffer dir;
            if (temporary && !job.queryUseCheckpoints()) 
                dir.append(queryTempDir(false));
            else
            {
                // NB: always >= 1 groupNames
                StringBuffer curDir;
                ForEachItemIn(gn, groupNames)
                {
                    if (!getConfigurationDirectory(globals->queryPropTree("Directories"), "data", "thor", groupNames.item(gn), curDir))
                        makePhysicalPartName(logicalName, 0, 0, curDir, false, os); // legacy
                    if (!dir.length())
                        dir.swapWith(curDir);
                    else
                    {
                        if (!streq(curDir, dir))
                            throw MakeStringException(0, "When targeting multiple clusters on a write, the clusters must have the same target directory");
                        curDir.clear();
                    }
                }
                curDir.swapWith(dir);
                // places logical filename directory in 'dir'
                makePhysicalPartName(logicalName, 0, 0, dir, false, os, curDir.str());
            }
            desc->setDefaultDir(dir.str());

            StringBuffer partmask;
            getPartMask(partmask,logicalName,total);
            desc->setNumParts(total);
            desc->setPartMask(partmask);
            // desc->setPartOffset(offset); // possible future requirement

            ForEachItemIn(g, groups)
            {
                ClusterPartDiskMapSpec mspec;
                mspec.defaultCopies = replicate?DFD_DefaultCopies:DFD_NoCopies; // may be changed on publish to reflect always backed up on thor cluster
                const char *groupname = groupNames.item(g);
                if (groupname && *groupname)
                    desc->addCluster(groupname, &groups.item(g), mspec);
                else
                    desc->addCluster(&groups.item(g), mspec);
            }
        }
        if (!temporary && !jobTemp)
            job.addCreatedFile(logicalName);
        return LINK(desc);
    }

    void publish(CJobBase &job, const char *logicalName, IFileDescriptor &fileDesc, Owned<IDistributedFile> *publishedFile=NULL, unsigned partOffset=0, bool createMissingParts=true)
    {
        IPropertyTree &props = fileDesc.queryProperties();
        bool temporary = props.getPropBool("@temporary");
        if (!temporary || job.queryUseCheckpoints())
            queryDistributedFileDirectory().removeEntry(logicalName, job.queryUserDescriptor());

        if (!temporary && createMissingParts && !isFileKey(fileDesc.queryProperties()))
        {
            // create empty parts for a fileDesc being published that is larger than this clusters
            unsigned clusterIdx = 0;
            for (; clusterIdx<fileDesc.numClusters(); clusterIdx++)
            {
                if (job.querySlaves() < fileDesc.numParts())
                {
                    StringBuffer clusterName;
                    fileDesc.getClusterGroupName(clusterIdx, clusterName, &queryNamedGroupStore());
                    PROGLOG("Creating blank parts for file '%s', cluster '%s'", logicalName, clusterName.str());
                    unsigned p=0;
                    while (p<fileDesc.numParts())
                    {
                        if (p == partOffset)
                            p += job.querySlaves();
                        IPartDescriptor *partDesc = fileDesc.queryPart(p);
                        CDateTime createTime, modifiedTime;
                        unsigned c=0;
                        for (; c<partDesc->numCopies(); c++)
                        {
                            RemoteFilename rfn;
                            partDesc->getFilename(c, rfn);
                            StringBuffer path;
                            rfn.getPath(path);
                            try
                            {
                                ensureDirectoryForFile(path.str());
                                OwnedIFile iFile = createIFile(path.str());
                                OwnedIFileIO iFileIO = iFile->open(IFOcreate);
                                iFileIO.clear();
                                // ensure copies have matching datestamps, as they would do normally (backupnode expects it)
                                if (partDesc->numCopies() > 1)
                                {
                                    if (0 == c)
                                        iFile->getTime(&createTime, &modifiedTime, NULL);
                                    else
                                        iFile->setTime(&createTime, &modifiedTime, NULL);
                                }
                            }
                            catch (IException *e)
                            {
                                if (0 == c)
                                    throw;
                                Owned<IThorException> e2 = MakeThorException(e);
                                e->Release();
                                e2->setAction(tea_warning);
                                job.fireException(e2);
                            }
                        }
                        partDesc->queryProperties().setPropInt64("@size", 0);
                        p++;
                    }
                }
                clusterIdx++;
            }
        }       
        // thor clusters are backed up so if replicateOutputs set *always* assume a replicate
        if (replicateOutputs && (!temporary || job.queryUseCheckpoints()))
        {
            // this potentially modifies fileDesc but I think OK at this point
           fileDesc.ensureReplicate();
        }
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(&fileDesc);
        if (temporary && !job.queryUseCheckpoints())
        {
            fileMap.replace(*new CIDistributeFileMapping(logicalName, *LINK(file))); // cache takes ownership
            return;
        }
        file->setAccessed();
        if (publishedFile)
            publishedFile->set(file);
        __int64 fs = file->getFileSize(false,false);
        if (fs!=-1)
            file->queryAttributes().setPropInt64("@size",fs);
        if (file->isCompressed())
        {
            fs = file->getDiskSize(false,false);
            if (fs!=-1)
                file->queryAttributes().setPropInt64("@compressedSize",fs);
        }
        file->attach(logicalName, job.queryUserDescriptor());
        unsigned c=0;
        for (; c<fileDesc.numClusters(); c++)
        {
            StringBuffer clusterName;
            fileDesc.getClusterGroupName(c, clusterName, &queryNamedGroupStore());
            LOG(daliAuditLogCat,",FileAccess,Thor,CREATED,%s,%s,%s,%s,%s,%" I64F "d,%s",
                            globals->queryProp("@nodeGroup"),
                            job.queryUser(),
                            file->queryLogicalName(),
                            job.queryWuid(),
                            job.queryGraphName(),
                            fs,clusterName.str());
        }
    }

    StringBuffer &getPublishPhysicalName(CJobBase &job, const char *logicalName, unsigned partno, StringBuffer &res)
    {
        return _getPublishPhysicalName(job, logicalName, partno, globals->queryProp("@nodeGroup"), &queryDfsGroup(), res);
    }

    StringBuffer &getPhysicalName(CJobBase &job, const char *logicalName, unsigned partno, StringBuffer &res)
    {
        return _getPublishPhysicalName(job, logicalName, partno, NULL, NULL, res);
    }

    unsigned __int64 getFileOffset(CJobBase &job, const char *logicalName, unsigned partno)
    {
        Owned<IDistributedFile> file = lookup(job, logicalName, false);
        StringBuffer scopedName;
        addScope(job, logicalName, scopedName);
        if (!file)
            throw MakeThorException(TE_LogicalFileNotFound, "getFileOffset: Logical file doesn't exist (%s)", scopedName.str());

        if (partno >= file->numParts())
            throw MakeThorException(TE_NoSuchPartForLogicalFile , "No such part number (%d) for logical file : %s", partno, scopedName.str());
        Owned<IDistributedFilePart> part = file->getPart(partno);
        return part->queryAttributes().getPropInt64("@offset");;
    }
    virtual bool scanLogicalFiles(CJobBase &job, const char *_pattern, StringArray &results)
    {
        if (strcspn(_pattern, "*?") == strlen(_pattern))
        {
            results.append(_pattern);
            return true;
        }
        StringBuffer pattern;
        addScope(job,_pattern,pattern,false);
        return scanDFS(pattern.str(),job.queryScope(),results);
    }
};

void initFileManager()
{
    assertex(!fileManager);
    fileManager = new CFileManager;
}

IThorFileManager &queryThorFileManager()
{
    return *fileManager;
}

void configureFileDescriptor(const char *logicalName, IFileDescriptor &fileDesc)
{
    unsigned __int64 offset = 0;
    Owned<IPartDescriptorIterator> iter = fileDesc.getIterator();
    bool noSize = false;
    ForEach (*iter)
    {
        IPartDescriptor &part = iter->query();
        IPropertyTree &props = part.queryProperties();
        if (!noSize) // don't want to set if failed do we?
            props.setPropInt64("@offset", offset);
        if (props.hasProp("@size"))
        {
            if (noSize)
                WARNLOG("Some parts of logical file \"%s\" have sizes others do not!", logicalName);
            else
                offset += props.getPropInt64("@size");
        }
        else
            noSize = true;
    }
}

IFileDescriptor *getConfiguredFileDescriptor(IDistributedFile &file)
{
    Owned<IFileDescriptor> fileDesc = file.getFileDescriptor();
    Owned<IDistributedFilePartIterator> iter = file.getIterator();
    unsigned partn = 0;
    // ensure @size's present as some activities rely upon e.g. @offset's
    // NH->JCS queries won't have a size here - does this matter?
    ForEach (*iter)
    {
        IDistributedFilePart &part = iter->query();
        IPartDescriptor *partDesc = fileDesc->queryPart(partn);
        try {
            offset_t sz = part.getFileSize(true, false);             
            partDesc->queryProperties().setPropInt64("@size", sz);
        }
        catch (IDFS_Exception *e) {
            if (e->errorCode()!=DFSERR_CannotFindPartFileSize)
                throw;
            e->Release();
        }
        partn++;
    }
    configureFileDescriptor(file.queryLogicalName(), *fileDesc);
    return LINK(fileDesc);
}

unsigned getGroupOffset(IGroup &fileGroup, IGroup &group)
{
    unsigned fileWidth = fileGroup.ordinality();

    unsigned offset = 0;
    if (fileWidth > group.ordinality()) // file wider than the cluster, fileWidth-clusterWidth parts will be blank
    {
        const char *wideDestOptStr = globals->queryProp("@wideDestOpt");
        // default to "wideMiddle"
        if (NULL == wideDestOptStr || 0 == stricmp("wideMiddle", wideDestOptStr)) // try to fill file parts that overlap this cluster
        {
            GroupRelation relation = group.compare(&fileGroup);
            if (GRbasesubset == relation)
            {
                offset = fileGroup.rank(&group.queryNode(0));
                assertex(RANK_NULL != offset);
            }
        }
    }
    return offset;
}

void fillClusterArray(CJobBase &job, const char *filename, StringArray &clusters, IArrayOf<IGroup> &groups)
{
    if (!clusters.ordinality())
    {
        groups.append(*LINK(&queryDfsGroup()));
        clusters.append(globals->queryProp("@nodeGroup"));
    }
    else
    {
        const char *cluster = clusters.item(0);
        Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
        if (!group)
            throw MakeStringException(0, "Could not find cluster group %s for file: %s", cluster, filename);
        EnvMachineOS os = queryOS(group->queryNode(0).endpoint());
        unsigned clusterIdx = 1;
        loop
        {
            groups.append(*LINK(group));
            if (clusterIdx>=clusters.ordinality())
                break;
            cluster = clusters.item(clusterIdx++);
            group.setown(queryNamedGroupStore().lookup(cluster));
            if (!group)
                throw MakeStringException(0, "Could not find cluster group %s for file: %s", cluster, filename);
            if (MachineOsUnknown != os)
            {
                EnvMachineOS thisOs = queryOS(group->queryNode(0).endpoint());
                if (MachineOsUnknown != thisOs && thisOs != os)
                    throw MakeStringException(0, "UNSUPPORTED: multiple clusters with different target OS's. File: %s", filename);
            }
            // check for overlap
            ForEachItemIn(g,groups)
            {
                IGroup &agrp = groups.item(g);
                if (GRdisjoint != agrp.compare(group))
                    throw MakeStringException(0, "Target cluster '%s', overlaps with target cluster '%s'", clusters.item(clusterIdx-1), clusters.item(g));
            }
        }
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    ::Release(fileManager);
}

