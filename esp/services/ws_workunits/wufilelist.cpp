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

#include "wufilelist.hpp"

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

class WuReferencedFile : public CInterface, implements IWuReferencedFile
{
public:
    IMPLEMENT_IINTERFACE;
    WuReferencedFile(const char *name, bool isSubFile=false) : flags(0)
    {
        StringBuffer ip;
        logicalName.append(skipForeign(name, &ip));
        if (ip.length())
            foreignNode.setown(createINode(ip.str()));
        if (isSubFile)
            flags |= WuRefSubFile;
    }

    void reset()
    {
        flags &= WuRefSubFile;
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

void WuReferencedFile::processLocalFileInfo(IDistributedFile *df, const char *cluster, StringArray *subfiles)
{
    IDistributedSuperFile *super = df->querySuperFile();
    if (super)
    {
        flags |= WuRefFileSuper;
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
            flags |= WuRefFileNotOnCluster;
    }
}

void WuReferencedFile::processRemoteFileTree(IPropertyTree *tree, bool foreign, StringArray *subfiles)
{
    flags |= WuRefFileRemote;
    if (foreign)
        flags |= WuRefFileForeign;
    if (streq(tree->queryName(), queryDfsXmlBranchName(DXB_SuperFile)))
    {
        flags |= WuRefFileSuper;
        if (subfiles)
        {
            Owned<IPropertyTreeIterator> it = tree->getElements("SubFile");
            ForEach(*it)
                subfiles->append(it->query().queryProp("@name"));
        }
    }

}

void WuReferencedFile::resolveLocal(const char *cluster, IUserDescriptor *user, StringArray *subfiles)
{
    reset();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user);
    if(df)
        processLocalFileInfo(df, cluster, subfiles);
    else
        flags |= WuRefFileNotFound;
}

void WuReferencedFile::resolveRemote(IUserDescriptor *user, INode *remote, const char *cluster, bool checkLocalFirst, StringArray *subfiles)
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
        tree.setown(dir.getFileTree(logicalName.str(), foreignNode, user, WF_LOOKUP_TIMEOUT, false));
    if (!tree && remote)
        tree.setown(dir.getFileTree(logicalName.str(), remote, user, WF_LOOKUP_TIMEOUT, false));
    if (tree)
        return processRemoteFileTree(tree, false, subfiles);
    else
    {
        flags |= WuRefFileNotFound;
        return;
    }
}

void WuReferencedFile::resolve(const char *cluster, IUserDescriptor *user, INode *remote, bool checkLocalFirst, StringArray *subfiles)
{
    if (foreignNode || remote)
        return resolveRemote(user, remote, cluster, checkLocalFirst, subfiles);
    return resolveLocal(cluster, user, subfiles);
}

void WuReferencedFile::cloneInfo(IDFUhelper *helper, IUserDescriptor *user, INode *remote, const char *cluster, bool overwrite)
{
    if (flags & WuRefFileSuper)
        return;

    StringBuffer addr;
    if (flags & WuRefFileForeign)
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
        flags |= WuRefFileCopyInfoFailed;
        DBGLOG(e);
        e->Release();
    }
    catch (...)
    {
        flags |= WuRefFileCopyInfoFailed;
        DBGLOG("Unknown error copying file info for %s, from %s", logicalName.str(), addr.str());
    }
}

void WuReferencedFile::cloneSuperInfo(IUserDescriptor *user, INode *remote, bool overwrite)
{
    if (!(flags & WuRefFileRemote))
        return;

    try
    {
        Owned<IPropertyTree> tree;
        IDistributedFileDirectory &dir = queryDistributedFileDirectory();
        if (foreignNode)
            tree.setown(dir.getFileTree(logicalName.str(), foreignNode, user, WF_LOOKUP_TIMEOUT, false));
        if (!tree && remote)
            tree.setown(dir.getFileTree(logicalName.str(), remote, user, WF_LOOKUP_TIMEOUT, false));
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
        flags |= WuRefFileCopyInfoFailed;
        DBGLOG(e);
        e->Release();
    }
    catch (...)
    {
        flags |= WuRefFileCopyInfoFailed;
        DBGLOG("Unknown error copying superfile info for %s", logicalName.str());
    }
}

class WuReferencedFileList : public CInterface, implements IWuReferencedFileList
{
public:
    IMPLEMENT_IINTERFACE;
    WuReferencedFileList(IConstWorkUnit *_cw, const char *remoteIP, const char *username, const char *pw) : cw(_cw)
    {
        if (username && pw)
        {
            user.setown(createUserDescriptor());
            user->set(username, pw);
        }
        if (remoteIP && *remoteIP)
            remote.setown(createINode(remoteIP, 7070));
        gatherFileNamesFromGraphs();
    }

    void gatherFileNamesFromGraphs();
    virtual IWuReferencedFileIterator *getFiles();
    virtual void cloneFileInfo(bool overwrite, bool cloneSuperInfo);
    virtual void cloneRelationships();
    virtual void cloneAllInfo(bool overwrite, bool cloneSuperInfo)
    {
        cloneFileInfo(overwrite, cloneSuperInfo);
        cloneRelationships();
    }
    virtual void resolveFiles(const char *process, bool checkLocalFirst, bool addSubFiles);
    void resolveSubFiles(StringArray &subfiles, bool checkLocalFirst);

public:
    Linked<IConstWorkUnit> cw;
    Owned<IUserDescriptor> user;
    Owned<INode> remote;
    MapStringToMyClass<WuReferencedFile> map;
    StringAttr cluster;
};

class WuReferencedFileIterator : public CInterface, implements IWuReferencedFileIterator
{
public:
    IMPLEMENT_IINTERFACE;
    WuReferencedFileIterator(WuReferencedFileList *_list)
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
    virtual IWuReferencedFile  & query()
    {
        return *dynamic_cast<IWuReferencedFile*>(list->map.mapToValue(&iter->query()));
    }

public:
    Owned<WuReferencedFileList> list;
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

void WuReferencedFileList::gatherFileNamesFromGraphs()
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
            Owned<WuReferencedFile> file = new WuReferencedFile(logicalName);
            if (file->logicalName.length() && !map.getValue(file->getLogicalName()))
            {
                const char *ln = file->getLogicalName();
                map.setValue(ln, file.getClear());
            }
        }
    }
}

void WuReferencedFileList::resolveSubFiles(StringArray &subfiles, bool checkLocalFirst)
{
    StringArray childSubFiles;
    ForEachItemIn(i, subfiles)
    {
        Owned<WuReferencedFile> file = new WuReferencedFile(subfiles.item(i), true);
        if (file->logicalName.length() && !map.getValue(file->getLogicalName()))
        {
            file->resolve(cluster.get(), user, remote, checkLocalFirst, &childSubFiles);
            const char *ln = file->getLogicalName();
            map.setValue(ln, file.getClear());
        }
    }
    if (childSubFiles.length())
        resolveSubFiles(childSubFiles, checkLocalFirst);
}

void WuReferencedFileList::resolveFiles(const char *process, bool checkLocalFirst, bool expandSuperFiles)
{
    cluster.set(process);
    StringArray subfiles;
    {
        Owned<IWuReferencedFileIterator> files = getFiles();
        ForEach(*files)
            files->query().resolve(process, user, remote, checkLocalFirst, expandSuperFiles ? &subfiles : NULL);
    }
    if (expandSuperFiles)
        resolveSubFiles(subfiles, checkLocalFirst);
}

void WuReferencedFileList::cloneFileInfo(bool overwrite, bool cloneSuperInfo)
{
    Owned<IDFUhelper> helper = createIDFUhelper();
    Owned<IWuReferencedFileIterator> files = getFiles();
    ForEach(*files)
    {
        IWuReferencedFile &file = files->query();
        if (cloneSuperInfo && (file.getFlags() & WuRefFileSuper))
            file.cloneSuperInfo(user, remote, overwrite);
        else if (file.getFlags() & (WuRefFileRemote | WuRefFileForeign | WuRefFileNotOnCluster))
            file.cloneInfo(helper, user, remote, cluster, overwrite);
    }
}

void WuReferencedFileList::cloneRelationships()
{
    if (!remote || remote->endpoint().isNull())
        return;

    StringBuffer addr;
    remote->endpoint().getUrlStr(addr);
    IDistributedFileDirectory &dir = queryDistributedFileDirectory();
    Owned<IWuReferencedFileIterator> files = getFiles();
    ForEach(*files)
    {
        IWuReferencedFile &file = files->query();
        if (!(file.getFlags() & WuRefFileRemote))
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
            IWuReferencedFile *refAssoc = map.getValue(assoc);
            if (refAssoc && !(refAssoc->getFlags() & WuRefFileCopyInfoFailed))
            {
                dir.addFileRelationship(file.getLogicalName(), assoc, r.queryPrimaryFields(), r.querySecondaryFields(),
                    r.queryKind(), r.queryCardinality(), r.isPayload(), r.queryDescription());
            }
        }
    }
}

IWuReferencedFileIterator *WuReferencedFileList::getFiles()
{
    return new WuReferencedFileIterator(this);
}

IWuReferencedFileList *createReferencedFileList(IConstWorkUnit *cw, const char *remoteIP, const char *user, const char *pw)
{
    return new WuReferencedFileList(cw, remoteIP, user, pw);
}
