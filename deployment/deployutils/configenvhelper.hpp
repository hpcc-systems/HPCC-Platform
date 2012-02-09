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
#if !defined(AFX_DEPLOYUTILS_CONFIGENVHELPER_HPP__INCLUDED_)
#define AFX_DEPLOYUTILS_CONFIGENVHELPER_HPP__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000


//#include "jliball.hpp"
interface IPropertyTree;
#include <vector>

using namespace std;
typedef vector<IPropertyTree*> IPropertyTreePtrArray;

#include "Constants.h"

class CConfigEnvHelper
{
public:
    CConfigEnvHelper(IPropertyTree* pRoot):m_numDataCopies(0),m_numChannels(0){m_pRoot.set(pRoot);}
    ~CConfigEnvHelper(){}
    bool handleRoxieOperation(const char* cmd, const char* xmlStr);
    void addComponent(const char* pszBuildSet, StringBuffer& sbNewName, IPropertyTree* pCompTree);
    bool handleThorTopologyOp(const char* cmd, const char* xmlStr, StringBuffer& sMsg);

private:
    IPropertyTree* getSoftwareNode(const char* compType, const char* compName);
    bool addRoxieServers(const char* xmlStr);
    bool checkComputerUse(/*IPropertyTree* pComputerNode*/ const char* szComputer, IPropertyTree* pParentNode) const;
    bool makePlatformSpecificAbsolutePath(const char* computer, StringBuffer& path);
    IPropertyTree* addLegacyServer(const char* name, IPropertyTree* pServer, IPropertyTree*pFarm, const char* roxieClusterName);
    void setComputerState(IPropertyTree* pNode, COMPUTER_STATE state);
    void setAttribute(IPropertyTree* pNode, const char* szName, const char* szValue);
    void mergeServiceAuthenticationWithBinding(IPropertyTree* pBinding, 
                                                                                 IPropertyTree* pProperties, 
                                                                                 IPropertyTree* pNewProperties, 
                                                                                 const char* NodeName);
    IPropertyTree* lookupComputerByName(const char* szName) const;
    void createUniqueName(const char* szPrefix, const char* parent, StringBuffer& sbName);
    IPropertyTree* addNode(const char* szTag, IPropertyTree* pParentNode, IPropertyTree* pInsertAfterNode=NULL);
    IPropertyTree* addNode(IPropertyTree*& pNode, IPropertyTree* pParentNode, IPropertyTree* pInsertAfterNode=NULL);
    void renameInstances(IPropertyTree* pRoxieCluster);
    IPropertyTree* findLegacyServer(IPropertyTree* pRoxieCluster, const char* name);
    void deleteFarm(IPropertyTree* pRoxieCluster, const char* pszFarm);
    void deleteServer(IPropertyTree* pRoxieCluster, const char* pszFarm, const char* pszServer);
    bool deleteRoxieServers(const char* xmlArg);
    bool EnsureInRange(const char* psz, UINT low, UINT high, const char* caption);
    bool handleRoxieSlaveConfig(const char* params);
    bool handleReplaceRoxieServer(const char* xmlArg);
    void addReplicateConfig(IPropertyTree* pSlaveNode, int channel, const char* drive, const char* netAddress, IPropertyTree* pRoxie);
    bool GenerateCyclicRedConfig(IPropertyTree* pRoxie, IPropertyTreePtrArray& computers, const char* copies, const char* pszOffset,
                                                             const char* dir1, const char* dir2, const char* dir3);
    bool GenerateOverloadedConfig(IPropertyTree* pRoxie, IPropertyTreePtrArray& computers, const char* copies,
                                                                const char* dir1, const char* dir2, const char* dir3);
    bool GenerateFullRedConfig(IPropertyTree* pRoxie, int copies, IPropertyTreePtrArray& computers, const char* dir1);
    void RemoveSlaves(IPropertyTree* pRoxie, bool bLegacySlaves/*=false*/);
    void RenameThorInstances(IPropertyTree* pThor);
    void UpdateThorAttributes(IPropertyTree* pParentNode);
    bool AddNewNodes(IPropertyTree* pThor, const char* szType, int nPort, IPropertyTreePtrArray& computers, bool validate, bool skipExisting, StringBuffer& usageList);
    bool CheckTopologyComputerUse(IPropertyTree* pComputerNode, IPropertyTree* pParentNode, StringBuffer& usageList) const;
    IPropertyTree* GetProcessNode(IPropertyTree* pThor, const char* szProcess) const;
    Linked<IPropertyTree> m_pRoot;
    int m_numDataCopies;
    int m_numChannels;
};



#endif // !defined(AFX_DEPLOYUTILS_CONFIGENVHELPER_HPP__INCLUDED_)
