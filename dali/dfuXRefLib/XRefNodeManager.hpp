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

// XRefNodeManager.h: interface for the CXRefNodeManager class.
//
//////////////////////////////////////////////////////////////////////
#ifndef XREFNODEMANAGER_HPP
#define XREFNODEMANAGER_HPP

#include "jstring.hpp"
#include "dasds.hpp"


#ifdef _WIN32
    #ifdef DFUXREFLIB_EXPORTS
        #define DFUXREFNODEMANAGERLIB_API __declspec(dllexport)
    #else
        #define DFUXREFNODEMANAGERLIB_API __declspec(dllimport)
    #endif
#else
    #define DFUXREFNODEMANAGERLIB_API
#endif


#include "XRefFilesNode.hpp"


interface IXRefProgressCallback: extends IInterface
{
    virtual void progress(const char *text)=0;
    virtual void error(const char *text)=0;
};



interface IConstXRefNode : extends IInterface
{
    virtual StringBuffer & getName(StringBuffer & str) = 0;
    virtual StringBuffer & getLastModified(StringBuffer & str)   = 0;
    virtual StringBuffer& getXRefData(StringBuffer & buff)   = 0;
    virtual StringBuffer& getStatus(StringBuffer & buff) = 0;
    virtual IXRefFilesNode* getLostFiles() = 0;
    virtual IXRefFilesNode* getFoundFiles() = 0;
    virtual IXRefFilesNode* getOrphanFiles() = 0;
    virtual StringBuffer& getCluster(StringBuffer& Cluster) = 0;
    virtual bool useSasha() = 0;
};

interface IXRefNode :  extends IConstXRefNode
{
    virtual void setName(const char* str) = 0;
    virtual void BuildXRefData(IPropertyTree & pTree,const char* Cluster) = 0;
    virtual void setStatus(const char* str) = 0;
    virtual void commit() = 0;
    virtual void setCluster(const char* str) = 0;
    virtual StringBuffer &serializeMessages(StringBuffer &buf) = 0;
    virtual void deserializeMessages(IPropertyTree& inTree) = 0;
    virtual StringBuffer &serializeDirectories(StringBuffer &buf) = 0;
    virtual void deserializeDirectories(IPropertyTree& inTree) = 0;
    virtual bool removeEmptyDirectories(StringBuffer &errstr) = 0;
};

interface IXRefNodeManager :  extends IInterface
{
    virtual IXRefNode * getXRefNode(const char* NodeName) = 0;
    virtual IXRefNode * CreateXRefNode(const char* NodeName) = 0;
};

extern DFUXREFNODEMANAGERLIB_API IXRefNodeManager * CreateXRefNodeFactory();

class CXRefNode : public CInterface, implements IXRefNode ,implements IXRefProgressCallback
{
private:
    Owned<IRemoteConnection> m_conn;
    StringBuffer m_origName;
    Owned<IPropertyTree> m_XRefTree;
    Owned<IPropertyTree> m_XRefDataTree;
    Owned<IXRefFilesNode> m_orphans;
    Owned<IXRefFilesNode> m_lost;
    Owned<IXRefFilesNode> m_found;
    Owned<IPropertyTree> m_messages;
    Owned<IPropertyTree> m_directories;
    bool m_bChanged;
    StringBuffer m_dataStr;
    StringBuffer m_ClusterName;
    Mutex commitMutex;

private:
    IPropertyTree& getDataTree();

public:
    IMPLEMENT_IINTERFACE;
    CXRefNode(); 
    CXRefNode(IPropertyTree* pTreeRoot); 
    CXRefNode(const char* clusterName,IRemoteConnection *conn); 
    virtual ~CXRefNode();
    bool IsChanged();
    void SetChanged(bool bChanged);

    //IXRefProgressCallback
    void progress(const char *text);
    void error(const char *text);

    //IConstXRefNode
    StringBuffer & getName(StringBuffer & str);
    StringBuffer & getLastModified(StringBuffer & str);
    bool useSasha();
    
    StringBuffer& getXRefData(StringBuffer & buff);
    StringBuffer& getStatus(StringBuffer & buff);
    void setLastModified(IJlibDateTime& DateTime);
    IXRefFilesNode* getLostFiles();
    IXRefFilesNode* getFoundFiles();
    IXRefFilesNode* getOrphanFiles();
    StringBuffer &serializeMessages(StringBuffer &buf);
    void deserializeMessages(IPropertyTree& inTree);
    StringBuffer &serializeDirectories(StringBuffer &buf);
    void deserializeDirectories(IPropertyTree& inTree);
    bool removeEmptyDirectories(StringBuffer &errstr);
    //IXRefNode
    void setName(const char* str);
    
    void setXRefData(StringBuffer & buff);
    void setXRefData(IPropertyTree & pTree);

    void BuildXRefData(IPropertyTree & pTree,const char* Cluster);

    void setStatus(const char* str);
    void commit();

    virtual StringBuffer& getCluster(StringBuffer& Cluster);
    virtual void setCluster(const char* str);


};

class CXRefNodeManager : public CInterface, implements IXRefNodeManager
{
public:
    IMPLEMENT_IINTERFACE;
    CXRefNodeManager(){};
    virtual ~CXRefNodeManager(){};
    IXRefNode * getXRefNode(const char* NodeName);
    IXRefNode * CreateXRefNode(const char* NodeName);
};



#endif // 
