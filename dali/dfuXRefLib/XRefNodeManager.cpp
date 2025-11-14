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

// XRefNodeManager.cpp: implementation of the CXRefNodeManager class.
//
//////////////////////////////////////////////////////////////////////

#include "jiface.hpp"
#include "jstring.hpp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jfile.hpp"

#include "mpcomm.hpp"
#include "platform.h"
#include "jlib.hpp"
#include "mpbase.hpp"
#include "daclient.hpp"
#include "dadiags.hpp"
#include "danqs.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "dautils.hpp"
#include "daft.hpp"
#include "rmtfile.hpp"

#include "XRefFilesNode.hpp"
#include "XRefNodeManager.hpp"

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

IXRefNodeManager * CreateXRefNodeFactory()
{
    return new CXRefNodeManager();
}



IXRefNode * CXRefNodeManager::getXRefNode(const char* NodeName)
{
    //DBGLOG("Node Name %s:",NodeName);

    StringBuffer xpath;
    xpath.appendf("Cluster[@name=\"%s\"]", NodeName);
    //if not exists, add DFU/XREF/ClusterName branch to SDS
    //not linked
    Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_CREATE_QUERY|RTM_NONE ,INFINITE);
    IPropertyTree* cluster_ptree = conn->queryRoot()->queryPropTree(xpath.str());
    conn->commit();
    if (cluster_ptree==0)
    {
        DBGLOG("Cluster[@name=%s] can't be found under /DFU/XREF", NodeName);
        return 0 ;
    }
    return new CXRefNode(NodeName,conn);

}

IXRefNode * CXRefNodeManager::CreateXRefNode(const char* NodeName)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE ,INFINITE);
    IPropertyTree* xref_ptree = conn->queryRoot();
    IPropertyTree* cluster_ptree = xref_ptree->addPropTree("Cluster", createPTree());
    cluster_ptree->setProp("@name",NodeName);
    conn->commit();
    conn->changeMode(RTM_NONE);
    return new CXRefNode(NodeName,conn);
}



CXRefNode::CXRefNode()
{
    m_bChanged = false;
}

CXRefNode::~CXRefNode()
{
}

CXRefNode::CXRefNode(const char* NodeName, IRemoteConnection *_conn)
{
    //DBGLOG("CXRefNode::CXRefNode(const char* NodeName)");

    m_bChanged = false;
    m_conn.set(_conn);

    StringBuffer xpath;
    xpath.appendf("Cluster[@name=\"%s\"]", NodeName);

    IPropertyTree* cluster_ptree = m_conn->queryRoot()->queryPropTree(xpath.str());
    m_XRefTree.set(cluster_ptree);
    m_XRefTree->getProp("@name",m_origName);
    rootDir.set(m_XRefTree->queryProp("@rootdir"));
    //DBGLOG("returning from CXRefNode::CXRefNode(const char* NodeName)");

}

bool CXRefNode::useSasha()
{
    if (!m_conn)
        return false;
    return m_conn->queryRoot()->getPropBool("@useSasha");
}


//IConstXRefNode
StringBuffer & CXRefNode::getName(StringBuffer & str)
{
    if(m_XRefTree.get())
        m_XRefTree->getProp("@name",str);
    return str;
}

StringBuffer& CXRefNode::getStatus(StringBuffer & str)
{
    if(m_XRefTree.get())
        m_XRefTree->getProp("@status",str);
    return str;
}


StringBuffer & CXRefNode::getLastModified(StringBuffer & str)
{
    if(m_XRefTree.get())
        m_XRefTree->getProp("@modified",str);
    return str;
}

template <typename NodeType>
NodeType *CXRefNode::getOrCreateFilesNode(const char *branchName, Owned<IXRefFilesNode> &cached)
{
    if (!cached.get())
    {
        const char *xrefPath = m_XRefTree->queryProp("@xrefPath");
        IPropertyTree *branch = nullptr;

        if (isEmptyString(xrefPath))
        {
            // Dali-based storage: use branch from m_XRefTree
            branch = m_XRefTree->queryPropTree(branchName);
            if (!branch)
            {
                branch = m_XRefTree->addPropTree(branchName, createPTree());
                commit();
            }
        }
        // File-based storage doesn't need a branch

        StringBuffer tmpbuf;
        NodeType *node = new NodeType(branch, getName(tmpbuf).str(), rootDir.str());

        if (!isEmptyString(xrefPath))
            node->setXRefPath(xrefPath, branchName);

        cached.setown(node);
    }
    return static_cast<NodeType*>(cached.getLink());
}

IXRefFilesNode* CXRefNode::getLostFiles()
{
    return getOrCreateFilesNode<CXRefFilesNode>("Lost", m_lost);
}

IXRefFilesNode* CXRefNode::getFoundFiles()
{
    return getOrCreateFilesNode<CXRefFilesNode>("Found", m_found);
}

IXRefFilesNode* CXRefNode::getOrphanFiles()
{
    return getOrCreateFilesNode<CXRefOrphanFilesNode>("Orphans", m_orphans);
}

StringBuffer &CXRefNode::serializeBranch(const char *branchName, Owned<IPropertyTree> &cached, StringBuffer &buf)
{
    buf.clear();
    const char *xrefPath = m_XRefTree->queryProp("@xrefPath");

    if (xrefPath && *xrefPath)
    {
        // File-based storage: load from file
        try
        {
            StringBuffer filepath(xrefPath);
            addPathSepChar(filepath).append(branchName).append(".xml");
            MemoryBuffer xmlContent;
            readXrefBranch(branchName, filepath.str(), xrefMaxFileSize, xmlContent); // NB: includes null terminator
            size32_t len = xmlContent.length();
            if (len)
                buf.setBuffer(len, (char *)xmlContent.detach(), len-1); // NB: StringBuffer::setBuffer strLen (3rd param) includes the null terminator
        }
        catch (IException *e)
        {
            StringBuffer errMsg;
            OWARNLOG("XRefNode: Failed to load %s from file: %s", branchName, e->errorMessage(errMsg).str());
            e->Release();
        }
    }
    else
    {
        // Dali-based storage: use "data" attribute
        if (!cached.get())
        {
            IPropertyTree *branch = m_XRefTree->queryPropTree(branchName);
            if (!branch)
            {
                branch = m_XRefTree->addPropTree(branchName, createPTree());
                commit();
            }
            cached.set(branch);
        }
        MemoryBuffer data;
        cached->getPropBin("data", data);
        if (data.length())
        {
            buf.append(data.length(), data.toByteArray());
        }
    }
    return buf;
}

void CXRefNode::deserializeBranch(const char *branchName, Owned<IPropertyTree> &cached, IPropertyTree &inTree)
{
    const char *xrefPath = m_XRefTree->queryProp("@xrefPath");

    if (xrefPath && *xrefPath)
    {
        // File-based storage: save to file
        try
        {
            StringBuffer filepath(xrefPath);
            addPathSepChar(filepath).append(branchName).append(".xml");

            StringBuffer datastr;
            toXML(&inTree, datastr);

            Owned<IFile> file = createIFile(filepath.str());
            Owned<IFileIO> fileIO = file->open(IFOcreate);
            if (fileIO)
            {
                fileIO->write(0, datastr.length(), datastr.str());
                fileIO->close();
            }
        }
        catch (IException *e)
        {
            StringBuffer errMsg;
            OWARNLOG("XRefNode: Failed to save %s to file: %s", branchName, e->errorMessage(errMsg).str());
            e->Release();
        }
    }
    else
    {
        // Dali-based storage: save to "data" attribute
        if (!cached.get())
        {
            IPropertyTree *branch = m_XRefTree->queryPropTree(branchName);
            if (!branch)
            {
                branch = m_XRefTree->addPropTree(branchName, createPTree());
                commit();
            }
            cached.set(branch);
        }
        StringBuffer datastr;
        toXML(&inTree, datastr);
        cached->setPropBin("data", datastr.length(), (void*)datastr.str());
    }
}

StringBuffer &CXRefNode::serializeMessages(StringBuffer &buf)
{
    return serializeBranch("Messages", m_messages, buf);
}

void CXRefNode::deserializeMessages(IPropertyTree& inTree)
{
    deserializeBranch("Messages", m_messages, inTree);
}

StringBuffer &CXRefNode::serializeDirectories(StringBuffer &buf)
{
    return serializeBranch("Directories", m_directories, buf);
}

void CXRefNode::deserializeDirectories(IPropertyTree& inTree)
{
    deserializeBranch("Directories", m_directories, inTree);
}

static bool deleteEmptyDir(IFile *dir)
{
    // this is a bit odd - basically we already know no files but there may be empty sub-dirs
    Owned<IDirectoryIterator> iter = dir->directoryFiles(NULL,false,true);
    bool candelete = true;
    ForEach(*iter)
    {
        if (iter->isDir())
        {
            Owned<IFile> file = &iter->get();
            try
            {
                if (!deleteEmptyDir(file))
                    candelete = false;
            }
            catch (IException *e)
            {
                EXCLOG(e,"deleteEmptyDir");
                candelete = false;
            }
        }
        else
            candelete = false;
    }
    if (!candelete)
        return false;
    static CriticalSection sect;
    CriticalBlock block(sect);      // don't want to actually remove in parallel
    dir->remove();
    return !dir->exists();
}

static bool recursiveCheckEmptyScope(IPropertyTree &ct)
{
    Owned<IPropertyTreeIterator> iter = ct.getElements("*");
    ForEach(*iter) {
        IPropertyTree &item = iter->query();
        const char *n = item.queryName();
        if (!n||(strcmp(n,queryDfsXmlBranchName(DXB_Scope))!=0))
            return false;
        if (!recursiveCheckEmptyScope(item))
            return false;
    }
    return true;
}



static void emptyScopes()
{
    PROGLOG("Removing empty scopes");
    Owned<IDFScopeIterator> iter = queryDistributedFileDirectory().getScopeIterator(UNKNOWN_USER,NULL,true,true);//MORE:Pass IUserDescriptor
    CDfsLogicalFileName dlfn;
    StringBuffer s;
    StringArray toremove;
    ForEach(*iter) {
        CDfsLogicalFileName dlfn;
        StringBuffer scope;
        scope.append(iter->query());
        dlfn.set(scope.str(),"x");
        dlfn.makeScopeQuery(s.clear(),true);
        Owned<IRemoteConnection> conn = querySDS().connect(s.str(),myProcessSession(),RTM_LOCK_READ, INFINITE);
        if (!conn)  
            DBGLOG("Could not connect to '%s' using %s",iter->query(),s.str());
        else {
            if (recursiveCheckEmptyScope(*conn->queryRoot())) {
                PROGLOG("Empty scope %s",iter->query());
                toremove.append(iter->query());
            }
        }
    }
    iter.clear();
    ForEachItemIn(i,toremove) {
        PROGLOG("Removed scope %s",toremove.item(i));
        queryDistributedFileDirectory().removeEmptyScope(toremove.item(i));
    }
}



bool CXRefNode::removeEmptyDirectories(StringBuffer &errstr)
{
    StringBuffer dataStr;
    serializeDirectories(dataStr);
    if (dataStr.length()==0)
        return true;
    Owned<IPropertyTree> t = createPTreeFromXMLString(dataStr.str());
    Owned<IPropertyTreeIterator> iter = t->getElements("Directory");
    const char *clustername = t->queryProp("Cluster");
    if (!clustername||!*clustername)
        return false;
    Owned<IGroup> group = queryNamedGroupStore().lookup(clustername);
    if (!group) {
        OERRLOG("%s cluster not found",clustername);
        errstr.appendf("ERROR: %s cluster not found",clustername);
        return false;
    }
    StringArray dellist;
    PointerArray todelete;
    ForEach(*iter) {
        IPropertyTree &dir = iter->query();
        if (dir.getPropInt64("Num")==0) {
            const char *dirname = dir.queryProp("Name");
            if (dirname&&*dirname) {
                dellist.append(dirname);
                todelete.append(&dir);
            }
        }
    }
    dellist.sortAsciiReverse(false);
    ForEachItemIn(di,dellist) {
        const char *dirname = dellist.item(di);
        class casyncfor: public CAsyncFor
        {
            IGroup *grp;
            StringAttr name;

        public:
            casyncfor(IGroup *_grp,const char *_name)
                : name(_name)
            {
                grp = _grp;
            }
            void Do(unsigned i)
            {
                RemoteFilename rfn;
                rfn.setPath(grp->queryNode(i).endpoint(),name);
                StringBuffer eps;
                try
                {
                    Owned<IFile> dir = createIFile(rfn);
                    if (deleteEmptyDir(dir))
                        PROGLOG("Removed '%s'",dir->queryFilename());
                    else
                        UWARNLOG("Could not remove '%s'",dir->queryFilename());
                }
                catch (IException *e)
                {
                    EXCLOG(e,"Could not remove directory");
                    e->Release();
                }
            }
        } afor(group,dirname);
        afor.For(group->ordinality(),10,false,true);
    }
    iter.clear();
    ForEachItemInRev(i,todelete) 
        t->removeTree((IPropertyTree *)todelete.item(i)); // probably should check succeeded above but next run will correct
    toXML(t,dataStr.clear());
    m_directories->setPropBin("data",dataStr.length(),(void*)dataStr.str());
    emptyScopes();
    return true;
}




    //IXRefNode
void CXRefNode::setName(const char* str)
{
    m_XRefTree->setProp("@name",str);
    if (m_origName.length() == 0)
        m_origName.append(str);
}

void CXRefNode::setStatus(const char* str)
{
    m_XRefTree->setProp("@status",str);
}


StringBuffer& CXRefNode::getCluster(StringBuffer& Cluster)
{
    Cluster.append(m_ClusterName.str());
    return Cluster;
}

void CXRefNode::setCluster(const char* str)
{
    m_ClusterName.clear();
    m_ClusterName.append(str);
}

void CXRefNode::setLastModified(IJlibDateTime& dt )
{
    SCMStringBuffer datestr,timestr;
    dt.getDateString(datestr);
    dt.getTimeString(timestr);
    StringBuffer tmpstr(datestr.str());
    tmpstr.append(" ");
    tmpstr.append(timestr.str());
    m_XRefTree->setProp("@modified",tmpstr.str());
}

void CXRefNode::BuildXRefData(IPropertyTree & pTree,const char* Cluster)
{

//  DBGLOG("CXRefNode::BuildXRefData");
    if(m_XRefTree.get() == 0)
        throw MakeStringException(-1, "No XRef Dali Tree available");

    Owned<IXRefFilesNode> lost = getLostFiles();
    Owned<IXRefFilesNode> found = getFoundFiles() ;
    Owned<IXRefFilesNode> orphan = getOrphanFiles();

    IPropertyTree* pSubTree = pTree.queryPropTree("Orphans");
    pSubTree->setProp("Cluster",Cluster);
    orphan->Deserialize(*pSubTree);

    pSubTree = pTree.queryPropTree("Lost");
    pSubTree->setProp("Cluster",Cluster);
    lost->Deserialize(*pSubTree);

    pSubTree = pTree.queryPropTree("Found");
    pSubTree->setProp("Cluster",Cluster);
    found->Deserialize(*pSubTree);

    pSubTree = pTree.queryPropTree("Messages");
    pSubTree->setProp("Cluster",Cluster);
    deserializeMessages(*pSubTree);

    pSubTree = pTree.queryPropTree("Directories");
    pSubTree->setProp("Cluster",Cluster);
    deserializeDirectories(*pSubTree);

    Owned<IJlibDateTime> dt =  createDateTimeNow();
    setLastModified(*dt);
    setStatus("Generated");
    commit();

}


bool CXRefNode::IsChanged()
{
    if ((m_orphans.get() && m_orphans->IsChanged() == true) ||
        (m_lost.get() && m_lost->IsChanged() == true) ||
        (m_found.get() && m_found->IsChanged() == true) ||
         m_bChanged == true )
         return true;
    return false;
}

void CXRefNode::SetChanged(bool bChanged)
{
    m_bChanged = bChanged;
}


void CXRefNode::commit()
{
    CriticalBlock b(commitCrit);
    if(m_conn == 0)
        return;
    Owned<IXRefFilesNode> lost = getLostFiles();
    Owned<IXRefFilesNode> found = getFoundFiles() ;
    Owned<IXRefFilesNode> orphan = getOrphanFiles();
    lost->Commit();
    found->Commit();
    orphan->Commit();
    m_conn->commit();

}

void CXRefNode::progress(const char *text)
{
    DBGLOG("PROGRESS: %s\n",text);
    setStatus(text);
    commit();
}
void CXRefNode::error(const char *text)
{
    OERRLOG("ERROR: %s\n",text);
    setStatus(text);
    commit();
}

// Explicit template instantiations
template CXRefFilesNode *CXRefNode::getOrCreateFilesNode<CXRefFilesNode>(const char *branchName, Owned<IXRefFilesNode> &cached);
template CXRefOrphanFilesNode *CXRefNode::getOrCreateFilesNode<CXRefOrphanFilesNode>(const char *branchName, Owned<IXRefFilesNode> &cached);
