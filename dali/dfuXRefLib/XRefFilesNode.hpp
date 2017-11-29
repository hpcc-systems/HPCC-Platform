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

// XRefFilesNode1.h: interface for the CXRefFilesNode class.
//
//////////////////////////////////////////////////////////////////////

#ifndef XREFFILESNODE_HPP
#define XREFFILESNODE_HPP

#include "jiface.hpp"
#include "jstring.hpp"
#include "jptree.hpp"
#include "rmtfile.hpp"
#include "dadfs.hpp"
#include "jlog.hpp"

interface IUserDescriptor;

interface IXRefFilesNode : extends IInterface
{
    virtual StringBuffer& Serialize(StringBuffer& outStr) = 0;
    virtual void Deserialize(IPropertyTree& inTree) = 0;
    virtual bool RemovePhysical(const char* Partmask,IUserDescriptor* udesc,const char *clustername, StringBuffer &errstr) = 0;
    virtual bool AttachPhysical(const char* Partmask,IUserDescriptor* udesc,const char *clustername, StringBuffer &errstr) = 0;
    virtual bool RemoveLogical(const char* LogicalName,IUserDescriptor* udesc,const char *clustername,StringBuffer &errstr) = 0;
    virtual IPropertyTreeIterator *getMatchingFiles(const char *match, const char *type) = 0;
    virtual bool IsChanged() = 0;
    virtual void Commit() = 0;
};

class CXRefFilesNode : implements IXRefFilesNode, public CSimpleInterface
{
protected:
    bool m_bChanged;
    IPropertyTree& m_baseTree;
    Owned<IPropertyTree> m_DataTree;
    MemoryBuffer _data;
    StringBuffer prefixName;
    StringAttr rootdir;
private:
    IPropertyTree* FindNode(const char* NodeName);
    IPropertyTree& queryDataTree();
    void DirectoryFromMask(const char* Partmask,StringBuffer& directory);
    bool LogicalNameFromMask(const char* Partmask,StringBuffer& logicalName);
    bool RemoveTreeNode(const char* NodeName);
    bool RemoveRemoteFile(const char* fileName,  const char* ipAddress);
    MemoryBuffer &queryData();
    virtual void CleanTree(IPropertyTree& inTree){}
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CXRefFilesNode(IPropertyTree& baseNode,const char* cluster, const char *rootdir);
    virtual ~CXRefFilesNode(){};
    virtual bool IsChanged() override;
    void Commit() override;
    virtual StringBuffer& Serialize(StringBuffer& outStr) override;
    virtual void Deserialize(IPropertyTree& inTree) override;
    virtual bool RemovePhysical(const char* Partmask,IUserDescriptor* udesc,const char *clustername,StringBuffer &errstr) override;
    virtual bool RemoveLogical(const char* LogicalName,IUserDescriptor* udesc,const char *clustername,StringBuffer &errstr) override;
    virtual bool AttachPhysical(const char* Partmask,IUserDescriptor* udesc,const char *clustername,StringBuffer &errstr) override;
    virtual IPropertyTreeIterator *getMatchingFiles(const char *match, const char *type) override;
};

class CXRefOrphanFilesNode : public CXRefFilesNode
{
public:
    CXRefOrphanFilesNode(IPropertyTree& baseNode,const char* cluster,const char* rootdir);
    void CleanTree(IPropertyTree& inTree);
};
#endif // !XREFFILESNODE
