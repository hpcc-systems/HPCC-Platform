/*#############################################################################

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
#############################################################################*/

/////////////////////////////////////////////////////////////////////////////
//
// WizardInputs.h : interface of the WizardInputs class
//
/////////////////////////////////////////////////////////////////////////////
#ifndef WIZARDINPUTS_HPP_INCL
#define WIZARDINPUTS_HPP_INCL

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include <string>
#include <vector>
#include <map>
#include "jptree.hpp"
#include "jliball.hpp"
#include "jiface.hpp"
#include "computerpicker.hpp"
#include "deployutils.hpp"
interface IPropertyTree;


using std::string;
using std::map;

class CInstDetails : public CInterface, implements IInterface
{
  public: 
    CInstDetails(){};
    CInstDetails(StringBuffer compName, StringBuffer ipAssigned):m_compName(compName)
    {
      m_ipAssigned.append(ipAssigned.str());
    };
    virtual ~CInstDetails(){};

    IMPLEMENT_IINTERFACE;
    StringArray& getIpAssigned() { return m_ipAssigned;}
    StringBuffer getCompName() { return m_compName; }
    void setParams(const char* compName, const char* value)
    {
      m_compName.clear().append(compName);
      if(m_ipAssigned.length() > 0)
        m_ipAssigned.kill();

      m_ipAssigned.append(value);
    } 

  private : 
     StringArray m_ipAssigned;
     StringBuffer m_compName;
};

//CInstDetails* getInstDetails(const char* buildSetName, const char* compName);

//---------------------------------------------------------------------------
//  WizardInputs class declaration
//---------------------------------------------------------------------------
class CWizardInputs : public CInterface, implements IInterface
{
// Construction
public:
  CWizardInputs(){};
  CWizardInputs(const char* xmlArg, const char* service, IPropertyTree* cfg, MapStringTo<StringBuffer>* dirMap);
  virtual ~CWizardInputs(); 
  
  void setEnvironment();
  void setWizardIPList(const StringArray ipArray);
  void setWizardRules();
  CInstDetails* getServerIPMap(const char* compName, const char* buildSetName,const IPropertyTree* pEnvTree, unsigned numOfNode = 1);
  void applyOverlappingRules(const char* compName, const char* buildSetName, unsigned startpos);
  count_t getNumOfInstForIP(StringBuffer ip);
  void generateSoftwareTree(IPropertyTree* pTree);
  void addInstanceToTree(IPropertyTree* pTree, StringBuffer attrName, const char* processName, const char* buildSetName, const char* instName);
  void getDefaultsForWizard(IPropertyTree* pTree);
  void addRoxieThorClusterToEnv(IPropertyTree* pNewEnvTree, CInstDetails* pInstDetails, const char* buildSetName, bool genRoxieOnDemand = false);
  unsigned getCntForAlreadyAssignedIPS(const char* buildsetName);
  void addToCompIPMap(const char* buildSetName, const char* value, const char* compName);
  void getEspBindingInformation(IPropertyTree* pNewEnvTree);
  StringBuffer& getDbUser() { return m_dbuser;}
  StringBuffer& getDbPassword() { return m_dbpassword;}
  void addTopology(IPropertyTree* pNewEnvTree);
  IPropertyTree* createTopologyForComp(IPropertyTree* pNewEnvTree, const char* component);
  IPropertyTree* getConfig() { return m_cfg;}
  StringBuffer& getService() { return m_service;}
  void checkForDependencies();
  void checkAndAddDependComponent(const char* key);
  unsigned getNumOfNodes(const char* compName);
  void setTopologyParam();

 IMPLEMENT_IINTERFACE;
  bool generateEnvironment(StringBuffer& envXml);
  IPropertyTree* createEnvironment();

private:
  void addComponentToSoftware(IPropertyTree* pNewEnvTree, IPropertyTree* pBuildSet);
  StringArray& getIpAddrMap(const char* buildsetName);

private:
  typedef StringArray* StringArrayPtr;
  typedef MapStringTo<StringArrayPtr> MapStringToStringArray;
    
   //for rules
   count_t m_maxCompOnNode;
   StringArray m_doNotGenComp;
   StringArray m_compOnAllNodes;
   StringArray m_doNotGenOptOnComps;
   StringArray m_clusterForTopology;
   MapStringToStringArray m_compForTopology; 
   MapStringToStringArray m_invalidServerCombo;
   unsigned m_supportNodes;
   unsigned m_roxieNodes;
   unsigned m_thorNodes;
   unsigned m_thorSlavesPerNode;
   unsigned m_roxieAgentRedChannels;
   unsigned m_roxieAgentRedOffset;
   bool m_roxieOnDemand;
   
   StringArray m_ipaddress;
   StringArray m_ipaddressSupport;
   MapStringToMyClass<CInstDetails> m_compIpMap; 
   Owned<IPropertyTree> m_pXml;
   IPropertyTree* m_cfg;
   StringBuffer m_service;
   StringBuffer m_dbuser;
   StringBuffer m_dbpassword;
   StringBuffer m_roxieAgentRedType;
   Owned<IPropertyTree> m_buildSetTree;
   Owned<IProperties> m_algProp;
   MapStringTo<StringBuffer>* m_overrideDirs;
};
#endif // !defined(WIZARDINPUTS_HPP__INCLUDED_)
