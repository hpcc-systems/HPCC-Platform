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

// XRefFilesNode1.cpp: implementation of the CXRefFilesNode class.
//
//////////////////////////////////////////////////////////////////////

#include "XRefFilesNode.hpp"

#include "jlzw.hpp"
#include "dautils.hpp"

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////


CXRefFilesNode::CXRefFilesNode(IPropertyTree *baseNode,const char* cluster,const char *_rootdir)
  : m_baseTree(baseNode), rootdir(_rootdir)
{
    if (baseNode)
        baseNode->setProp("@Cluster",cluster);
    m_bChanged = false;
    prefixName.append(cluster);
}

void CXRefFilesNode::setXRefPath(const char *_xrefPath, const char *_branchName)
{
    xrefPath.set(_xrefPath);
    branchName.set(_branchName);
}

bool CXRefFilesNode::IsChanged()
{
    return m_bChanged;
}

void CXRefFilesNode::Commit()
{
    if (m_bChanged)
        Deserialize(queryDataTree());
    m_bChanged = false;
}

MemoryBuffer &CXRefFilesNode::queryData()
{
    if (m_bChanged || (0 == _data.length()))
    {
        _data.clear();

        // If using file-based storage, load from file
        if (xrefPath.length() > 0 && branchName.length() > 0)
        {
            try
            {
                StringBuffer filepath(xrefPath);
                addPathSepChar(filepath).append(branchName).append(".xml");

                Owned<IFile> file = createIFile(filepath.str());
                if (file->exists())
                {
                    Owned<IFileIO> fileIO = file->open(IFOread);
                    if (fileIO)
                    {
                        offset_t fileSize = file->size();
                        if (fileSize > 0 && fileSize < 0x10000000) // Sanity check: < 256MB
                        {
                            _data.ensureCapacity((size32_t)fileSize);
                            size32_t bytesRead = fileIO->read(0, (size32_t)fileSize, _data.reserve((size32_t)fileSize));
                            _data.setLength(bytesRead);
                        }
                    }
                }
            }
            catch (IException *e)
            {
                StringBuffer errMsg;
                OWARNLOG("XRefFilesNode: Failed to load from file '%s/%s.xml': %s", 
                         xrefPath.str(), branchName.str(), e->errorMessage(errMsg).str());
                e->Release();
            }
        }
        else
        {
            assertex(m_baseTree);
            // Old method: load from "data" attribute in Dali
            m_baseTree->getPropBin("data", _data);
        }
    }
    return _data;
}

StringBuffer& CXRefFilesNode::Serialize(StringBuffer& outStr)
{
    MemoryBuffer &_data = queryData();
    outStr.append(_data.length(), _data.toByteArray());
    return outStr;
}

void CXRefFilesNode::Deserialize(IPropertyTree& inTree)
{
    CleanTree(inTree);
    StringBuffer datastr;
    toXML(&inTree,datastr);

    // If using file-based storage, save to file
    if (xrefPath.length() > 0 && branchName.length() > 0)
    {
        try
        {
            StringBuffer filepath(xrefPath);
            addPathSepChar(filepath).append(branchName).append(".xml");

            Owned<IFile> file = createIFile(filepath.str());
            Owned<IFileIO> fileIO = file->open(IFOcreate);
            if (fileIO)
            {
                fileIO->write(0, datastr.length(), datastr.str());
                fileIO->close();
            }
            else
            {
                OWARNLOG("XRefFilesNode: Failed to save to file '%s'", filepath.str());
            }
        }
        catch (IException *e)
        {
            StringBuffer errMsg;
            OWARNLOG("XRefFilesNode: Failed to save to file '%s/%s.xml': %s", 
                     xrefPath.str(), branchName.str(), e->errorMessage(errMsg).str());
            e->Release();
        }
    }
    else
    {
        assertex(m_baseTree);
        // Old method: save to "data" attribute in Dali
        m_baseTree->setPropBin("data",datastr.length(),(void*)datastr.str());
    }
}

IPropertyTree* CXRefFilesNode::FindNode(const char* NodeName)
{
    StringBuffer xpath;
    xpath.clear().appendf("File/[Partmask=\"%s\"]", NodeName);
    return queryDataTree().getBranch(xpath.str());
}
        
IPropertyTreeIterator *CXRefFilesNode::getMatchingFiles(const char *match, const char *type)
{
    StringBuffer xpath;
    xpath.clear().appendf("File/[%s=\"%s\"]", type, match);
    return queryDataTree().getElements(xpath.str());
}

IPropertyTree& CXRefFilesNode::queryDataTree()
{
    if (m_DataTree.get() == 0)
    {
        MemoryBuffer &data = queryData();
        m_DataTree.setown(createPTreeFromXMLString(data.length(), data.toByteArray()));
    }
    return *m_DataTree.get();
}

static bool checkPartsInCluster(const char *title,const char *clustername, IPropertyTree *subBranch, StringBuffer &errstr,bool exists)
{
    Owned<IGroup> group = queryNamedGroupStore().lookup(clustername);
    if (!group)
    {
        OERRLOG("%s cluster not found",clustername);
        errstr.appendf("ERROR: %s cluster not found",clustername);
        return false;
    }
    Owned<IPropertyTreeIterator> partItr =  subBranch->getElements("Part");
    unsigned i;
    StringBuffer xpath;
    unsigned n = group->ordinality();
    ForEach(*partItr)
    {
        IPropertyTree& part = partItr->query();
        unsigned pn = part.getPropInt("Num");
        for (int rep=0;rep<2;rep++)
        {
            i = 0;
            for (;;)
            {
                i++;
                xpath.clear().appendf(rep?"RNode[%d]":"Node[%d]",i);
                if (!part.hasProp(xpath.str())) 
                    break;
                SocketEndpoint ep(part.queryProp(xpath.str()));
                ep.port = 0;
                rank_t gn = group->rank(ep);
                if (group->rank(ep)==RANK_NULL)
                {
                    StringBuffer eps;
                    OERRLOG("%s %s Part %d on %s is not in cluster %s",title,rep?"Replicate":"Primary",pn,ep.getEndpointHostText(eps).str(),clustername);
                    errstr.appendf("ERROR: %s %s part %d on %s is not in cluster %s",title,rep?"Replicate":"Primary",pn,ep.getEndpointHostText(eps).str(),clustername);
                    return false;
                }
                if (exists)
                {
                    if ((pn-1+rep)%n==gn)
                    {
                        OERRLOG("Logical file for %s exists (part not orphaned?)",title);
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
    if (!subBranch)
    {
        OERRLOG("%s branch not found",Partmask);
        errstr.appendf("ERROR: %s branch not found",Partmask);
        return false;
    }
    // sanity check file doesn't (now) exist 
    bool exists = false;
    StringBuffer lfn;
    if (LogicalNameFromMask(Partmask,lfn))
    {
        if (queryDistributedFileDirectory().exists(lfn.str(),udesc,true)) 
            exists = true;
    }
    if (!checkPartsInCluster(Partmask,clustername,subBranch,errstr,exists))
        return false;


    int numparts = subBranch->getPropInt("Numparts");
    bool isDirPerPart = subBranch->getPropBool("IsDirPerPart");
    unsigned numStripedDevices = getNumPlaneStripes(clustername);

    StringBuffer path;
    const char * fileMask = splitDirTail(Partmask, path);
    unsigned lfnHash = getFilenameHash(lfn.str());
    size32_t rootLen = rootdir.length()+1; // Include trailing path separator
    StringBuffer remoteFile(rootLen, path.str());

    RemoteFilenameArray files;
    Owned<IPropertyTreeIterator> partItr =  subBranch->getElements("Part");
    ForEach(*partItr)
    {
        IPropertyTree& part = partItr->query();
        unsigned partNum = part.getPropInt("Num");

        unsigned stripeNum = calcStripeNumber(partNum, lfnHash, numStripedDevices);
        if (stripeNum)
            addPathSepChar(remoteFile.append('d').append(stripeNum));

        remoteFile.append(path.length()-rootLen, path.str()+rootLen);

        if (isDirPerPart)
            addPathSepChar(remoteFile.append(partNum));

        expandMask(remoteFile, fileMask, partNum-1, numparts);
        /////////////////////////////////
        StringBuffer xpath;
        unsigned i = 0;
        for (;;)
        {
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
        for (;;)
        {
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
        remoteFile.setLength(rootLen);
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
                if (_remoteFile->exists())
                {
                    if (!_remoteFile->remove())
                    {
                        StringBuffer errname;
                        files.item(idx).getRemotePath(errname);
                        OERRLOG("Could not delete file %s",errname.str());
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
        OERRLOG("Error Removing XRef Branch %s",Partmask);
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
        

    IPropertyTree* pLogicalFileNode =  queryDataTree().getBranch(xpath.str());
    if (!pLogicalFileNode)
    {
        OERRLOG("Branch %s not found",xpath.str());
        errstr.appendf("Branch %s not found",xpath.str());
        return false;
    }
    if (!checkPartsInCluster(LogicalName,clustername,pLogicalFileNode,errstr,false))
        return false;
    if (queryDistributedFileDirectory().existsPhysical(LogicalName,udesc))
    {
        OERRLOG("Logical file %s all parts exist (not lost?))",LogicalName);
        errstr.appendf("Logical file %s all parts exist (not lost?))",LogicalName);
        return false;
    }
    if (!queryDataTree().removeTree(pLogicalFileNode))
    {
        OERRLOG("Removing XRef Branch %s", xpath.str());
        errstr.appendf("Removing XRef Branch %s", xpath.str());
        return false;
    }
    m_bChanged = true;
    queryDistributedFileDirectory().removeEntry(LogicalName,udesc);
    return true;
}

bool CXRefFilesNode::AttachPhysical(const char *Partmask, IUserDescriptor* udesc, const char *clustername, StringBuffer &errstr)
{
    IPropertyTree* subBranch = FindNode(Partmask);
    if (!subBranch)
    {
        OERRLOG("%s node not found", Partmask);
        errstr.appendf("ERROR: %s node not found", Partmask);
        return false;
    }
    if (!checkPartsInCluster(Partmask, clustername, subBranch, errstr, false))
        return false;

    StringBuffer logicalName;
    if (!LogicalNameFromMask(Partmask, logicalName))
    {
        OERRLOG("%s - could not attach", Partmask);
        errstr.appendf("ERROR: %s - could not attach", Partmask);
        return false;
    }

    if (queryDistributedFileDirectory().exists(logicalName.str(), udesc))
    {
        OERRLOG("Logical File %s already Exists. Can not reattach to Dali", logicalName.str());
        errstr.appendf("Logical File %s already Exists. Can not reattach to Dali", logicalName.str());
        return false;
    }

    StringBuffer prefix;
    Owned<const IStoragePlane> storagePlane = getDataStoragePlane(clustername, true);
    addPathSepChar(prefix.append(storagePlane->queryPrefix()));
    if (!startsWith(Partmask, prefix))
    {
        OERRLOG("File path %s does not start with plane prefix %s. Can not reattach to Dali", Partmask, prefix.str());
        errstr.appendf("ERROR: File path %s does not start with plane prefix %s. Can not reattach to Dali", Partmask, prefix.str());
        return false;
    }

    unsigned lfnHash = getFilenameHash(logicalName.str());
    unsigned prefixLen = prefix.length();
    unsigned numStripedDevices = storagePlane->queryNumStripes();
    bool isDirPerPart = storagePlane->queryDirPerPart();

    StringBuffer scope, mask, defaultDir;
    splitFilename(Partmask+prefixLen, nullptr, &scope, &mask, &mask);
    defaultDir.append(prefix).append(scope);

    Owned<IFileDescriptor> fileDesc = createFileDescriptor();
    fileDesc->setDefaultDir(defaultDir.str());

    //use the logical name as the title....
    fileDesc->setTraceName(logicalName.str());

    unsigned numparts = subBranch->getPropInt("Numparts");

    bool isCompressed = false;
    bool first = true;
    offset_t totalSize = 0;
    Owned<IPropertyTreeIterator> partItr =  subBranch->getElements("Part");
    StringBuffer filePath;
    for (partItr->first(); partItr->isValid(); partItr->next())
    {
        IPropertyTree& part = partItr->query();

        unsigned partNo = part.getPropInt("Num")-1;
        makePhysicalPartName(logicalName.str(), partNo+1, numparts, filePath.clear(), 0, DFD_OSdefault, prefix.str(), isDirPerPart, calcStripeNumber(partNo, lfnHash, numStripedDevices));
        const char *filename = strrchr(filePath.str(), PATHSEPCHAR) + 1;

        const char* _node = part.queryProp("Node[1]");
        if (!_node||!*_node)
            _node = part.queryProp("RNode[1]");
        if (!_node||!*_node) {
            OERRLOG("%s - could not attach (missing part info)", Partmask);
            errstr.appendf("ERROR: %s - could not attach (missing part info)", Partmask);
            return false;
        }
        Owned<INode> node = createINode(_node);
        DBGLOG("Setting number %d for Node %s and name %s", partNo, _node, filename);

        RemoteFilename rfn;
        rfn.setPath(node->endpoint(), filePath);
        Owned<IFile> iFile = createIFile(rfn);
        offset_t physicalSize = iFile->size();
        bool partCompressed = isCompressedFile(iFile);
        if (first)
        {
            first = false;
            isCompressed = partCompressed;
        }
        else if (isCompressed != partCompressed)
        {
            VStringBuffer err("%s - could not attach (mixed compressed/non-compressed physical parts detected)", Partmask);
            OERRLOG("%s", err.str());
            errstr.append(err.str());
            return false;
        }
        Owned<IPropertyTree> partProps = createPTree("Part");
        if (isCompressed)
            partProps->setPropInt64("@compressedSize", physicalSize);
        else
            partProps->setPropInt64("@size", physicalSize);
        totalSize += physicalSize;

        fileDesc->setPart(partNo, node, filename, partProps);
    }
    IPropertyTree &props = fileDesc->queryProperties();
    if (isCompressed)
    {
        props.setPropBool("@blockCompressed", true);
        props.setPropInt64("@compressedSize", totalSize);
    }
    else
        props.setPropInt64("@size", totalSize);

    if (isDirPerPart)
        props.setPropInt("@flags", static_cast<int>(FileDescriptorFlags::dirperpart));

    if (numStripedDevices>1)
        fileDesc->queryPartDiskMapping(0).numStripedDevices = numStripedDevices;

    // Set group so that the file can be associated with correct cluster
    fileDesc->setClusterGroup(0, queryNamedGroupStore().lookup(clustername));
    fileDesc->setClusterGroupName(0, clustername);

    Owned<IDistributedFile> dFile = queryDistributedFileDirectory().createNew(fileDesc);
    dFile->attach(logicalName.str(), udesc);

    if (!RemoveTreeNode(Partmask)) {
        OERRLOG("Removing XRef Branch %s", Partmask);
        errstr.appendf("ERROR: Removing XRef Branch %s", Partmask);
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
    return queryDataTree().removeTree(subBranch);
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
CXRefOrphanFilesNode::CXRefOrphanFilesNode(IPropertyTree *baseNode,const char* cluster,const char* rootdir) 
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
