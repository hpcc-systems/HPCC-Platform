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

// XRefFilesNode1.cpp: implementation of the CXRefFilesNode class.
//
//////////////////////////////////////////////////////////////////////

#include "XRefFilesNode.hpp"

#include "dautils.hpp"

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////


CXRefFilesNode::CXRefFilesNode(IPropertyTree& baseNode,const char* cluster,const char *_rootdir) 
  : m_baseTree(baseNode), rootdir(_rootdir)
{
    baseNode.setProp("@Cluster",cluster);
    m_bChanged = false;
    prefixName.append(cluster);
}

bool CXRefFilesNode::IsChanged()
{
    return m_bChanged;
}

void CXRefFilesNode::Commit()
{
    if (m_bChanged)
        Deserialize(getDataTree());
    m_bChanged = false;
}

StringBuffer& CXRefFilesNode::Serialize(StringBuffer& outStr)
{
    if (!m_bChanged && _data.length() > 0)
    {
        outStr.append(_data);
        return outStr;
    }
    _data.clear();
    MemoryBuffer buff;
    m_baseTree.getPropBin("data",buff);
    if (buff.length())
    {
        outStr.append(buff.length(),buff.toByteArray());
        _data.append(outStr);
    }
    return outStr;
}

void CXRefFilesNode::Deserialize(IPropertyTree& inTree)
{
    CleanTree(inTree);
    StringBuffer datastr;
    toXML(&inTree,datastr);
    m_baseTree.setPropBin("data",datastr.length(),(void*)datastr.toCharArray());
}

IPropertyTree* CXRefFilesNode::FindNode(const char* NodeName)
{
    StringBuffer xpath;
    xpath.clear().appendf("File/[Partmask=\"%s\"]", NodeName);
    StringBuffer tmpbuf;
    return getDataTree().getBranch(xpath.str());
}
        
IPropertyTree& CXRefFilesNode::getDataTree()
{
    if (m_DataTree.get() == 0)
    {
        StringBuffer dataStr;
        Serialize(dataStr);
        m_DataTree.setown(createPTreeFromXMLString(dataStr.str()));
    }
    return *m_DataTree.get();
}

static bool checkPartsInCluster(const char *title,const char *clustername, IPropertyTree *subBranch, StringBuffer &errstr,bool exists)
{
    Owned<IGroup> group = queryNamedGroupStore().lookup(clustername);
    if (!group) {
        ERRLOG("%s cluster not found",clustername);
        errstr.appendf("ERROR: %s cluster not found",clustername);
        return false;
    }
    Owned<IPropertyTreeIterator> partItr =  subBranch->getElements("Part");
    unsigned i;
    StringBuffer xpath;
    unsigned n = group->ordinality();
    ForEach(*partItr) {
        IPropertyTree& part = partItr->query();
        unsigned pn = part.getPropInt("Num");
        for (int rep=0;rep<2;rep++) {
            i = 0;
            loop {
                i++;
                xpath.clear().appendf(rep?"RNode[%d]":"Node[%d]",i);
                if (!part.hasProp(xpath.str())) 
                    break;
                SocketEndpoint ep(part.queryProp(xpath.str()));
                ep.port = 0;
                rank_t gn = group->rank(ep);
                if (group->rank(ep)==RANK_NULL) {
                    StringBuffer eps;
                    ERRLOG("%s %s Part %d on %s is not in cluster %s",title,rep?"Replicate":"Primary",pn,ep.getUrlStr(eps).str(),clustername);
                    errstr.appendf("ERROR: %s %s part %d on %s is not in cluster %s",title,rep?"Replicate":"Primary",pn,ep.getUrlStr(eps).str(),clustername);
                    return false;
                }
                if (exists) {
                    if ((pn-1+rep)%n==gn) {
                        ERRLOG("Logical file for %s exists (part not orphaned?)",title);
                        errstr.appendf("Logical file for %s exists (part not orphaned?)",title);
                        return false;
                    }
                }

            }
        }
    }
    return true;
}



bool CXRefFilesNode::RemovePhysical(const char *Partmask,IUserDescriptor* udesc, const char *clustername, StringBuffer &errstr)
{   
    size32_t startlen = errstr.length();
    IPropertyTree* subBranch = FindNode(Partmask);
    if (!subBranch) {
        ERRLOG("%s branch not found",Partmask);
        errstr.appendf("ERROR: %s branch not found",Partmask);
        return false;
    }
    // sanity check file doesn't (now) exist 
    bool exists = false;
    StringBuffer lfn;
    if (LogicalNameFromMask(Partmask,lfn)) {
        if (queryDistributedFileDirectory().exists(lfn.str(),true)) 
            exists = true;
    }
    if (!checkPartsInCluster(Partmask,clustername,subBranch,errstr,exists))
        return false;


    RemoteFilenameArray files;
    int numparts = subBranch->getPropInt("Numparts");
    Owned<IPropertyTreeIterator> partItr =  subBranch->getElements("Part");
    ForEach(*partItr)
    {
        IPropertyTree& part = partItr->query();
        StringBuffer remoteFile;
        expandMask(remoteFile, Partmask, part.getPropInt("Num")-1, numparts);
        /////////////////////////////////
        StringBuffer xpath;
        unsigned i = 0;
        loop {
            i++;
            xpath.clear().appendf("Node[%d]",i);
            if (!part.hasProp(xpath.str())) 
                break;
            SocketEndpoint ip(part.queryProp(xpath.str()));
            RemoteFilename rmtFile;
            rmtFile.setPath(ip,remoteFile.str()); 
            files.append(rmtFile);
        }
        i = 0;
        loop {
            i++;
            xpath.clear().appendf("RNode[%d]",i);
            if (!part.hasProp(xpath.str())) 
                break;
            SocketEndpoint ip(part.queryProp(xpath.str()));
            RemoteFilename rmtFile;
            StringBuffer replicateFile;
            if (setReplicateDir(remoteFile.str(),replicateFile))  
                rmtFile.setPath(ip,replicateFile.str());        // old semantics
            else
                rmtFile.setPath(ip,remoteFile.str()); 
            files.append(rmtFile);
        }

    }
        
    CriticalSection crit;

    class casyncfor: public CAsyncFor
    {
        RemoteFilenameArray &files;
        StringBuffer &errstr;
        CriticalSection &crit;
    public:
        casyncfor(RemoteFilenameArray &_files, StringBuffer &_errstr, CriticalSection &_crit)
            : files(_files), errstr(_errstr), crit(_crit)
        {
        }
        void Do(unsigned idx)
        {
            try{
                Owned<IFile> _remoteFile =  createIFile(files.item(idx));
                DBGLOG("Removing physical part at %s",_remoteFile->queryFilename());
                if (_remoteFile->exists()) {
                    if (!_remoteFile->remove()) {
                        StringBuffer errname;
                        files.item(idx).getRemotePath(errname);
                        ERRLOG("Could not delete file %s",errname.str());
                        CriticalBlock block(crit);
                        if (errstr.length())
                            errstr.append('\n');
                        errstr.appendf("ERROR: Could not delete file %s",errname.str());
                    }
                }
            }
            catch(IException* e)
            {
                StringBuffer s(" deleting logical part ");
                files.item(idx).getRemotePath(s);
                EXCLOG(e,s.str());
                CriticalBlock block(crit);
                if (errstr.length())
                    errstr.append('\n');
                errstr.append("ERROR: ");
                e->errorMessage(errstr);
                errstr.append(s);
                e->Release();
            }
            catch(...)
            {
                StringBuffer errname;
                files.item(idx).getRemotePath(errname);
                DBGLOG("Unknown Exception caught while deleting logical part %s",errname.str());
                CriticalBlock block(crit);
                if (errstr.length())
                    errstr.append('\n');
                errstr.appendf("ERROR: Unknown Exception caught while deleting logical part %s",errname.str());
            }

        }
    } afor(files,errstr,crit);  
    afor.For(files.ordinality(),10,false,true);
    if (!RemoveTreeNode(Partmask))                 
    {
        ERRLOG("Error Removing XRef Branch %s",Partmask);
        return false;
    }
    m_bChanged = true;
    return errstr.length()==startlen;
}

bool CXRefFilesNode::RemoveLogical(const char* LogicalName,IUserDescriptor* udesc, const char *clustername, StringBuffer &errstr)
{
    StringBuffer xpath;
    xpath.clear().appendf("File/[Name=\"%s\"]", LogicalName);
    StringBuffer tmpbuf;
        

    IPropertyTree* pLogicalFileNode =  getDataTree().getBranch(xpath.str());
    if (!pLogicalFileNode) {
        ERRLOG("Branch %s not found",xpath.str());
        errstr.appendf("Branch %s not found",xpath.str());
        return false;
    }
    if (!checkPartsInCluster(LogicalName,clustername,pLogicalFileNode,errstr,false))
        return false;
    if (queryDistributedFileDirectory().existsPhysical(LogicalName,udesc)) {
        ERRLOG("Logical file %s all parts exist (not lost?))",LogicalName);
        errstr.appendf("Logical file %s all parts exist (not lost?))",LogicalName);
        return false;
    }
    if (!getDataTree().removeTree(pLogicalFileNode)) {                  
        ERRLOG("Removing XRef Branch %s", xpath.str());
        errstr.appendf("Removing XRef Branch %s", xpath.str());
        return false;
    }
    m_bChanged = true;
    queryDistributedFileDirectory().removeEntry(LogicalName,udesc);
    return true;
}

bool CXRefFilesNode::AttachPhysical(const char *Partmask,IUserDescriptor* udesc, const char *clustername, StringBuffer &errstr)
{
    IPropertyTree* subBranch = FindNode(Partmask);
    if (!subBranch) {
        ERRLOG("%s node not found",Partmask);
        errstr.appendf("ERROR: %s node not found",Partmask);
        return false;
    }
    if (!checkPartsInCluster(Partmask,clustername,subBranch,errstr,false))
        return false;

    StringBuffer logicalName;
    if (!LogicalNameFromMask(Partmask,logicalName)) {
        ERRLOG("%s - could not attach",Partmask);
        errstr.appendf("ERROR: %s - could not attach",Partmask);
        return false;
    }

    if (queryDistributedFileDirectory().exists(logicalName.toCharArray()))
    {
        ERRLOG("Logical File %s already Exists. Can not reattach to Dali",logicalName.str());
        errstr.appendf("Logical File %s already Exists. Can not reattach to Dali",logicalName.str());
        return false;
    }
    StringBuffer drive,path,tail,ext;
    splitFilename(Partmask, &drive, &path, &tail, &ext);
    //set directory info
    StringBuffer dir;
    dir.append(drive.str());
    dir.append(path.str());

    Owned<IFileDescriptor> fileDesc = createFileDescriptor();
    fileDesc->setDefaultDir(dir.str());
    //use the logical name as the title....
    fileDesc->setTraceName(logicalName.str());

    IPropertyTree & attr = fileDesc->queryProperties();
    //attr.setProp("@size",subBranch->queryProp("Size"));  we don't know size (this value isn't right!)

    unsigned numparts = subBranch->getPropInt("Numparts");

    Owned<IPropertyTreeIterator> partItr =  subBranch->getElements("Part");
    for (partItr->first(); partItr->isValid(); partItr->next())
    {
        IPropertyTree& part = partItr->query();

            //get the full file path 
        StringBuffer remoteFilePath;
        expandMask(remoteFilePath, Partmask, part.getPropInt("Num")-1, numparts);

        StringBuffer _drive,_path,_tail,_ext,_filename;
        splitFilename(remoteFilePath.str(), &_drive, &_path, &_tail, &_ext);
        _filename.append(_tail.str());
        _filename.append(_ext.str());
                
        const char* _node = part.queryProp("Node[1]");
        if (!_node||!*_node)
            _node = part.queryProp("RNode[1]");
        if (!*_node||!*_node) {
            ERRLOG("%s - could not attach (missing part info)",Partmask);
            errstr.appendf("ERROR: %s - could not attach (missing part info)",Partmask);
            return false;
        }
        Owned<INode> node = createINode(_node);
        DBGLOG("Setting number %d for Node %s and name %s",part.getPropInt("Num")-1,_node,_filename.str());
        //Num is 0 based...
        fileDesc->setPart(part.getPropInt("Num")-1,node,_filename.str());
    }

    Owned<IDistributedFile> dFile = queryDistributedFileDirectory().createNew(fileDesc);
    dFile->attach(logicalName.toCharArray(),NULL,udesc);

    if (!RemoveTreeNode(Partmask)) {                   
        ERRLOG("Removing XRef Branch %s",Partmask);
        errstr.appendf("ERROR: Removing XRef Branch %s",Partmask);
        return false;
    }
    m_bChanged = true;
    return true;
}

void CXRefFilesNode::DirectoryFromMask(const char* Partmask,StringBuffer& directory)
{
    if(*Partmask == 0)
        return;
    const char *in = Partmask;
    int counter = 0;
    while (*in)
    {
        if (*in == '.')
            break;
        directory.append(*in);
    }
}
bool CXRefFilesNode::LogicalNameFromMask(const char* fname,StringBuffer& logicalName)
{
    CDfsLogicalFileName lfn;
    if (!lfn.setFromMask(fname,rootdir))  
        return false;
    logicalName.append(lfn.get());
    return true;
}

bool CXRefFilesNode::RemoveTreeNode(const char* NodeName)
{
    IPropertyTree* subBranch = FindNode(NodeName);
    if (!subBranch)
        return false;
    StringBuffer tmpbuf;
    return getDataTree().removeTree(subBranch);
}

bool CXRefFilesNode::RemoveRemoteFile(const char* fileName,  const char* ipAddress)
{
    SocketEndpoint ip;
    ip.set(ipAddress);
    RemoteFilename rmtFile;
    rmtFile.setPath(ip,fileName); // filename shhould be full windows or unix path
    
    Owned<IFile> _remoteFile =  createIFile(rmtFile);
    if (_remoteFile->exists())
        return _remoteFile->remove();
    return false;
}

////////////////////////////////////////////////////////////////////////////////////
//
//
////////////////////////////////////////////////////////////////////////////////////
CXRefOrphanFilesNode::CXRefOrphanFilesNode(IPropertyTree& baseNode,const char* cluster,const char* rootdir) 
  : CXRefFilesNode(baseNode,cluster,rootdir)
{
}
void CXRefOrphanFilesNode::CleanTree(IPropertyTree& inTree)
{
    Owned<IPropertyTreeIterator> Itr =  inTree.getElements("*");
    Itr->first();
    int partcount = 0;
    while(Itr->isValid())
    {
        IPropertyTree& node = Itr->query();
        if(strcmp(node.queryName(),"Part") == 0)
        {
            partcount++;
        }
        else if(node.hasChildren())
        {
            CleanTree(node);
        }
        Itr->next();
    }
    if(partcount != 0)
            inTree.setPropInt("Partsfound",partcount);
}
