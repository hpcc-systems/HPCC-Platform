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
    virtual bool IsChanged() = 0;
    virtual void Commit() = 0;
};

class CXRefFilesNode : public CSimpleInterface , implements IXRefFilesNode
{
protected:
    IPropertyTree& m_baseTree;
    Owned<IPropertyTree> m_DataTree;
    bool m_bChanged;
    StringBuffer _data;
    StringBuffer prefixName;
    StringAttr rootdir;
private:
    IPropertyTree* FindNode(const char* NodeName);
    IPropertyTree& getDataTree();
    void DirectoryFromMask(const char* Partmask,StringBuffer& directory);
    bool LogicalNameFromMask(const char* Partmask,StringBuffer& logicalName);
    bool RemoveTreeNode(const char* NodeName);
    bool RemoveRemoteFile(const char* fileName,  const char* ipAddress);
    virtual void CleanTree(IPropertyTree& inTree){}
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CXRefFilesNode(IPropertyTree& baseNode,const char* cluster, const char *rootdir);
    virtual ~CXRefFilesNode(){};
    virtual bool IsChanged();
    void Commit();
    virtual StringBuffer& Serialize(StringBuffer& outStr);
    virtual void Deserialize(IPropertyTree& inTree);
    virtual bool RemovePhysical(const char* Partmask,IUserDescriptor* udesc,const char *clustername,StringBuffer &errstr);
    virtual bool RemoveLogical(const char* LogicalName,IUserDescriptor* udesc,const char *clustername,StringBuffer &errstr);
    virtual bool AttachPhysical(const char* Partmask,IUserDescriptor* udesc,const char *clustername,StringBuffer &errstr);

};

class CXRefOrphanFilesNode : public CXRefFilesNode
{
public:
    CXRefOrphanFilesNode(IPropertyTree& baseNode,const char* cluster,const char* rootdir);
    void CleanTree(IPropertyTree& inTree);
};
#endif // !XREFFILESNODE
