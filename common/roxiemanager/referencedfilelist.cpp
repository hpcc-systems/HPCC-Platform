/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

#include "referencedfilelist.hpp"

#include "jptree.hpp"
#include "workunit.hpp"
#include "eclhelper.hpp"
#include "dautils.hpp"
#include "dadfs.hpp"
#include "dasess.hpp"

#define WF_LOOKUP_TIMEOUT (1000*15)  // 15 seconds

bool getIsOpt(const IPropertyTree &graphNode)
{
    if (graphNode.hasProp("att[@name='_isOpt']"))
        return graphNode.getPropBool("att[@name='_isOpt']/@value", false);
    else
        return graphNode.getPropBool("att[@name='_isIndexOpt']/@value", false);
}

const char *skipForeign(const char *name, StringBuffer *ip=NULL)
{
    if (*name=='~')
        name++;
    const char *d1 = strstr(name, "::");
     if (d1)
     {
        StringBuffer cmp;
        if (strieq("foreign", cmp.append(d1-name, name).trim().str()))
        {
            // foreign scope - need to strip off the ip and port
            d1 += 2;  // skip ::

            const char *d2 = strstr(d1,"::");
            if (d2)
            {
                if (ip)
                    ip->append(d2-d1, d1).trim();
                d2 += 2;
                while (*d2 == ' ')
                    d2++;

                name = d2;
            }
        }
    }
    return name;
}

class ReferencedFile : public CInterface, implements IReferencedFile
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFile(const char *name, bool isSubFile=false) : flags(0)
    {
        StringBuffer ip;
        logicalName.append(skipForeign(name, &ip));
        if (ip.length())
            foreignNode.setown(createINode(ip.str()));
        if (isSubFile)
            flags |= RefSubFile;
    }

    void reset()
    {
        flags &= RefSubFile;
    }
    void processLocalFileInfo(IDistributedFile *df, const char *cluster, StringArray *subfiles);
    void processRemoteFileTree(IPropertyTree *tree, bool foreign, StringArray *subfiles);

    void resolveLocal(const char *cluster, IUserDescriptor *user, StringArray *subfiles);
    void resolveRemote(IUserDescriptor *user, INode *remote, const char *cluster, bool checkLocalFirst, StringArray *subfiles);
    void resolve(const char *cluster, IUserDescriptor *user, INode *remote, bool checkLocalFirst, StringArray *subfiles);

    virtual const char *getLogicalName(){return logicalName.str();}
    virtual unsigned getFlags(){return flags;}
    virtual const SocketEndpoint &getForeignIP(){return foreignNode->endpoint();}
    virtual void cloneInfo(IDFUhelper *helper, IUserDescriptor *user, INode *remote, const char *cluster, bool overwrite=false);
    void cloneSuperInfo(IUserDescriptor *user, INode *remote, bool overwrite);

public:
    StringBuffer logicalName;
    Owned<INode> foreignNode;

    unsigned flags;
};

void ReferencedFile::processLocalFileInfo(IDistributedFile *df, const char *cluster, StringArray *subfiles)
{
    IDistributedSuperFile *super = df->querySuperFile();
    if (super)
    {
        flags |= RefFileSuper;
        if (subfiles)
        {
            Owned<IDistributedFileIterator> it = super->getSubFileIterator();
            ForEach(*it)
            {
                IDistributedFile &sub = it->query();
                StringBuffer name;
                sub.getLogicalName(name);
                subfiles->append(name.str());
            }
        }
    }
    else
    {
        if (!cluster || !*cluster)
            return;
        if (df->findCluster(cluster)==NotFound)
            flags |= RefFileNotOnCluster;
    }
}

void ReferencedFile::processRemoteFileTree(IPropertyTree *tree, bool foreign, StringArray *subfiles)
{
    flags |= RefFileRemote;
    if (foreign)
        flags |= RefFileForeign;
    if (streq(tree->queryName(), queryDfsXmlBranchName(DXB_SuperFile)))
    {
        flags |= RefFileSuper;
        if (subfiles)
        {
            Owned<IPropertyTreeIterator> it = tree->getElements("SubFile");
            ForEach(*it)
                subfiles->append(it->query().queryProp("@name"));
        }
    }

}

void ReferencedFile::resolveLocal(const char *cluster, IUserDescriptor *user, StringArray *subfiles)
{
    reset();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user);
    if(df)
        processLocalFileInfo(df, cluster, subfiles);
    else
        flags |= RefFileNotFound;
}

void ReferencedFile::resolveRemote(IUserDescriptor *user, INode *remote, const char *cluster, bool checkLocalFirst, StringArray *subfiles)
{
    reset();
    IDistributedFileDirectory &dir = queryDistributedFileDirectory();
    if (checkLocalFirst)
    {
        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user);
        if(df)
            return processLocalFileInfo(df, cluster, subfiles);
    }
    Owned<IPropertyTree> tree;
    if (foreignNode)
        tree.setown(dir.getFileTree(logicalName.str(), foreignNode, user, WF_LOOKUP_TIMEOUT, false, false));
    if (!tree && remote)
        tree.setown(dir.getFileTree(logicalName.str(), remote, user, WF_LOOKUP_TIMEOUT, false, false));
    if (tree)
        return processRemoteFileTree(tree, false, subfiles);
    else
    {
        flags |= RefFileNotFound;
        return;
    }
}

void ReferencedFile::resolve(const char *cluster, IUserDescriptor *user, INode *remote, bool checkLocalFirst, StringArray *subfiles)
{
    if (foreignNode || remote)
        return resolveRemote(user, remote, cluster, checkLocalFirst, subfiles);
    return resolveLocal(cluster, user, subfiles);
}

void ReferencedFile::cloneInfo(IDFUhelper *helper, IUserDescriptor *user, INode *remote, const char *cluster, bool overwrite)
{
    if (flags & RefFileSuper)
        return;
    if (!(flags & (RefFileRemote | RefFileForeign | RefFileNotOnCluster)))
        return;

    StringBuffer addr;
    if (flags & RefFileForeign)
        foreignNode->endpoint().getUrlStr(addr);
    else if (remote)
        remote->endpoint().getUrlStr(addr);

    try
    {
        helper->createSingleFileClone(logicalName.str(), logicalName.str(), cluster,
            DFUcpdm_c_replicated_by_d, true, NULL, user, addr.str(), NULL, overwrite, false);
    }
    catch (IException *e)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG(e);
        e->Release();
    }
    catch (...)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG("Unknown error copying file info for %s, from %s", logicalName.str(), addr.str());
    }
}

void ReferencedFile::cloneSuperInfo(IUserDescriptor *user, INode *remote, bool overwrite)
{
    if (!(flags & RefFileRemote))
        return;

    try
    {
        Owned<IPropertyTree> tree;
        IDistributedFileDirectory &dir = queryDistributedFileDirectory();
        if (foreignNode)
            tree.setown(dir.getFileTree(logicalName.str(), foreignNode, user, WF_LOOKUP_TIMEOUT, false, false));
        if (!tree && remote)
            tree.setown(dir.getFileTree(logicalName.str(), remote, user, WF_LOOKUP_TIMEOUT, false, false));
        if (!tree)
            return;

        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user);
        if(df)
        {
            if (!overwrite)
                return;
           df->detach();
           df.clear();
        }

        Owned<IDistributedSuperFile> superfile = dir.createSuperFile(logicalName.str(), true, false, user);
        Owned<IPropertyTreeIterator> subfiles = tree->getElements("SubFile");
        ForEach(*subfiles)
        {
            const char *name = subfiles->query().queryProp("@name");
            if (name && *name)
                superfile->addSubFile(name, false, NULL, false);
        }
    }
    catch (IException *e)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG(e);
        e->Release();
    }
    catch (...)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG("Unknown error copying superfile info for %s", logicalName.str());
    }
}

class ReferencedFileList : public CInterface, implements IReferencedFileList
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFileList(const char *username, const char *pw)
    {
        if (username && pw)
        {
            user.setown(createUserDescriptor());
            user->set(username, pw);
        }
    }

    virtual void addFile(const char *ln);
    virtual void addFiles(StringArray &files);
    virtual void addFilesFromWorkUnit(IConstWorkUnit *cw);

    virtual IReferencedFileIterator *getFiles();
    virtual void cloneFileInfo(bool overwrite, bool cloneSuperInfo);
    virtual void cloneRelationships();
    virtual void cloneAllInfo(bool overwrite, bool cloneSuperInfo)
    {
        cloneFileInfo(overwrite, cloneSuperInfo);
        cloneRelationships();
    }
    virtual void resolveFiles(const char *process, const char *remoteIP, bool checkLocalFirst, bool addSubFiles);
    void resolveSubFiles(StringArray &subfiles, bool checkLocalFirst);

public:
    Owned<IUserDescriptor> user;
    Owned<INode> remote;
    MapStringToMyClass<ReferencedFile> map;
    StringAttr process;
};

class ReferencedFileIterator : public CInterface, implements IReferencedFileIterator
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFileIterator(ReferencedFileList *_list)
    {
        list.set(_list);
        iter.setown(new HashIterator(list->map));
    }

    virtual bool first()
    {
        return iter->first();
    }

    virtual bool next()
    {
        return iter->next();
    }
    virtual bool isValid()
    {
        return iter->isValid();
    }
    virtual IReferencedFile  & query()
    {
        return *dynamic_cast<IReferencedFile*>(list->map.mapToValue(&iter->query()));
    }

    virtual ReferencedFile  & queryObject()
    {
        return *(list->map.mapToValue(&iter->query()));
    }

public:
    Owned<ReferencedFileList> list;
    Owned<HashIterator> iter;
};

bool isIndexActivity(ThorActivityKind kind)
{
    switch (kind)
    {
        case TAKindexexists:
        case TAKindexwrite:
        case TAKindexread:
        case TAKindexcount:
        case TAKindexnormalize:
        case TAKindexaggregate:
        case TAKindexgroupaggregate:
        case TAKindexgroupexists:
        case TAKindexgroupcount:
            return true;
        default:
            return false;
    }
}

void ReferencedFileList::addFile(const char *ln)
{
    Owned<ReferencedFile> file = new ReferencedFile(ln);
    if (file->logicalName.length() && !map.getValue(file->getLogicalName()))
    {
        const char *refln = file->getLogicalName();
        map.setValue(refln, file.getClear());
    }
}

void ReferencedFileList::addFiles(StringArray &files)
{
    ForEachItemIn(i, files)
        addFile(files.item(i));
}

void ReferencedFileList::addFilesFromWorkUnit(IConstWorkUnit *cw)
{
    Owned<IConstWUGraphIterator> graphs = &cw->getGraphs(GraphTypeActivities);
    ForEach(*graphs)
    {
        Owned <IPropertyTree> xgmml = graphs->query().getXGMMLTree(false);
        Owned<IPropertyTreeIterator> iter = xgmml->getElements("//node[att/@name='_fileName']");
        ForEach(*iter)
        {
            IPropertyTree &node = iter->query();
            const char *logicalName = node.queryProp("att[@name='_fileName']/@value");
            if (!logicalName)
                continue;
            ThorActivityKind kind = (ThorActivityKind) node.getPropInt("att[@name='_kind']/@value", TAKnone);
            //not likely to be part of roxie queries, but for forward compatibility:
            if(kind==TAKdiskwrite || kind==TAKindexwrite || kind==TAKcsvwrite || kind==TAKxmlwrite)
                continue;
            if (node.getPropBool("att[@name='_isSpill']/@value") ||
                node.getPropBool("att[@name='_isTransformSpill']/@value"))
                continue;
            addFile(logicalName);
        }
    }
}

void ReferencedFileList::resolveSubFiles(StringArray &subfiles, bool checkLocalFirst)
{
    StringArray childSubFiles;
    ForEachItemIn(i, subfiles)
    {
        Owned<ReferencedFile> file = new ReferencedFile(subfiles.item(i), true);
        if (file->logicalName.length() && !map.getValue(file->getLogicalName()))
        {
            file->resolve(process.get(), user, remote, checkLocalFirst, &childSubFiles);
            const char *ln = file->getLogicalName();
            map.setValue(ln, file.getClear());
        }
    }
    if (childSubFiles.length())
        resolveSubFiles(childSubFiles, checkLocalFirst);
}

void ReferencedFileList::resolveFiles(const char *_process, const char *remoteIP, bool checkLocalFirst, bool expandSuperFiles)
{
    process.set(_process);
    remote.setown((remoteIP && *remoteIP) ? createINode(remoteIP, 7070) : NULL);
    StringArray subfiles;
    {
        ReferencedFileIterator files(this);
        ForEach(files)
            files.queryObject().resolve(process, user, remote, checkLocalFirst, expandSuperFiles ? &subfiles : NULL);
    }
    if (expandSuperFiles)
        resolveSubFiles(subfiles, checkLocalFirst);
}

void ReferencedFileList::cloneFileInfo(bool overwrite, bool cloneSuperInfo)
{
    Owned<IDFUhelper> helper = createIDFUhelper();
    ReferencedFileIterator files(this);
    ForEach(files)
        files.queryObject().cloneInfo(helper, user, remote, process, overwrite);
    if (cloneSuperInfo)
        ForEach(files)
            files.queryObject().cloneSuperInfo(user, remote, overwrite);
}

void ReferencedFileList::cloneRelationships()
{
    if (!remote || remote->endpoint().isNull())
        return;

    StringBuffer addr;
    remote->endpoint().getUrlStr(addr);
    IDistributedFileDirectory &dir = queryDistributedFileDirectory();
    ReferencedFileIterator files(this);
    ForEach(files)
    {
        ReferencedFile &file = files.queryObject();
        if (!(file.getFlags() & RefFileRemote))
            continue;
        Owned<IFileRelationshipIterator> iter = dir.lookupFileRelationships(file.getLogicalName(), NULL,
            NULL, NULL, NULL, NULL, NULL, addr.str(), WF_LOOKUP_TIMEOUT);

        ForEach(*iter)
        {
            IFileRelationship &r=iter->query();
            const char* assoc = r.querySecondaryFilename();
            if (!assoc)
                continue;
            if (*assoc == '~')
                assoc++;
            IReferencedFile *refAssoc = map.getValue(assoc);
            if (refAssoc && !(refAssoc->getFlags() & RefFileCopyInfoFailed))
            {
                dir.addFileRelationship(file.getLogicalName(), assoc, r.queryPrimaryFields(), r.querySecondaryFields(),
                    r.queryKind(), r.queryCardinality(), r.isPayload(), r.queryDescription());
            }
        }
    }
}

IReferencedFileIterator *ReferencedFileList::getFiles()
{
    return new ReferencedFileIterator(this);
}

IReferencedFileList *createReferencedFileList(const char *user, const char *pw)
{
    return new ReferencedFileList(user, pw);
}
