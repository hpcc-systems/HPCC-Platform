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
/////////////////////////////////////////////////////////////////////////////
//
// ComputerPicker.cpp : implementation file
//
/////////////////////////////////////////////////////////////////////////////
#include "computerpicker.hpp"
#include "XMLTags.h"

/*static*/MapStrToChar CComputerPicker::s_prefixMap;

//---------------------------------------------------------------------------
//  CComputerPicker
//---------------------------------------------------------------------------
CComputerPicker::CComputerPicker()
{
  if (s_prefixMap.empty())
    CreateComponentTypePrefixMap();
}

CComputerPicker::~CComputerPicker()
{
}


//---------------------------------------------------------------------------
//  SetRootNode
//---------------------------------------------------------------------------
void CComputerPicker::SetRootNode(const IPropertyTree* pNode) 
{
  m_pRootNode = pNode;

  CreateComputerFilterTree();
  Refresh();
}

//generate a map containing unique prefix char based on component types, which can either
//be process names, "Domain", "ComputerType" or "Subnet".
//
/*static*/
void CComputerPicker::CreateComponentTypePrefixMap()
{
  s_prefixMap["RoxieCluster"     ] = 'C';
  s_prefixMap["DaliServerProcess"] = 'D';
  s_prefixMap["EspProcess"       ] = 'E';
  s_prefixMap["HoleCluster"      ] = 'H';
  s_prefixMap["LDAPServerProcess"] = 'L';
  s_prefixMap["PluginProcess"    ] = 'P';
  s_prefixMap["SpareProcess"     ] = 'S';
  s_prefixMap["ThorCluster"      ] = 'T';
  s_prefixMap["DfuServerProcess" ] = 'U';
  s_prefixMap["SybaseProcess"    ] = 'Y';
  s_prefixMap["EclAgentProcess"  ] = 'a';
  s_prefixMap["EclServerProcess" ] = 'c';
  s_prefixMap["Domain"           ] = 'd';
  s_prefixMap["ComputerType"     ] = 'e';
  s_prefixMap["FTSlaveProcess"   ] = 'f';
  s_prefixMap["EspService"       ] = 's';
  s_prefixMap["Topology"         ] = 't';
  s_prefixMap["Subnet"           ] = 'u';
}


//returns a unique prefix char based on a component type, which can either
//be a process name, "Domain", "ComputerType" or "Subnet".
//
/*static*/
char CComputerPicker::GetPrefixForComponentType(const char* componType)
{
  char prefix = ' ';

  MapStrToChar::const_iterator i = s_prefixMap.find(componType);
  if (i != s_prefixMap.end())
    prefix = (*i).second;

  return prefix;
}

const char* CComputerPicker::GetComponentTypeForPrefix(char chPrefix)
{
  const char* szCompType = "";

  //this is inefficient and should be replaced by another map from prefix
  //to component type.  However, this is only used for tooltip where time
  //is not an issue.
  MapStrToChar::const_iterator i    = s_prefixMap.begin();
  MapStrToChar::const_iterator iEnd = s_prefixMap.end();

  for (; i != iEnd; i++)
    if ((*i).second == chPrefix)
    {
      szCompType = (*i).first.c_str();
      break;
    }

    return szCompType;
}

//---------------------------------------------------------------------------
//  ResetUsageMap
//---------------------------------------------------------------------------
void CComputerPicker::ResetUsageMap()
{
  MapStrToStrArray::iterator i    = m_usageMap.begin();
  MapStrToStrArray::iterator iEnd = m_usageMap.end();

  //empty all vectors keyed by all computers
  for (; i != iEnd; i++)
  {
    stringarray& usage = (*i).second;

    stringarray::iterator i2    = usage.begin();
    stringarray::iterator iEnd2 = usage.end();

    for (; i2 != iEnd2; i2++)
    {
      std::string& component = *i2;
      const char* szComponent = component.c_str();

      if (szComponent && *szComponent != '+')
        component[0] = '+';
    }
  }
}

bool CComputerPicker::GetUsage(const char* szComputer, StringBuffer& sUsage, 
                               bool bIncludeComponentType) const
{
  MapStrToStrArray::const_iterator iUsage = m_usageMap.find(szComputer);
  bool bInclude = false;//computer isn't explicitly requested for by the user
  bool bExclude = false;//computer isn't explicitly excluded by the user

  if (iUsage != m_usageMap.end())
  {
    const stringarray& usage = (*iUsage).second;

    stringarray::const_iterator i2    = usage.begin();
    stringarray::const_iterator iEnd2 = usage.end();

    for (; i2 != iEnd2; i2++)
    {
      const std::string& component = *i2;
      const char* szComponent = component.c_str();

      if (szComponent)
      {
        char chStatus = *szComponent++;
        if (chStatus == 'I')
          bInclude = true;
        else
          if (chStatus == '-')
            bExclude = true;

        char chPrefix = *szComponent++;
        //gather usage anyway even for excluded components
        //since that needs to be shown in case another component
        //has explicitly asked this computer to be included

        //don't show usage info for domains, computer types and subnets
        if (chPrefix != 'd' && chPrefix != 'e' && chPrefix != 'u')
        {
          //if (!sUsage.IsEmpty())
          if (sUsage.length() > 0)
            sUsage.append(bIncludeComponentType ? '\n' : ' ');

          if (bIncludeComponentType)
          {
            StringBuffer sType = GetComponentTypeForPrefix(chPrefix);
            //if (!sType.IsEmpty())
            if (sUsage.length() > 0)
            {
              sUsage.append(sType);
              sUsage.append(" - ");
            }
          }

          sUsage.append(szComponent); //skip status and prefix characters
        }
      }
    }

    //if this computer has to be excluded because of some component and 
    //then don't use it unless it is explicitly included by the user 
    //for another
    if (bExclude && bInclude)
      bExclude = false;
  }
  return !bExclude;
}

//---------------------------------------------------------------------------
//  NoteUsage
//---------------------------------------------------------------------------
void CComputerPicker::NoteUsage(const char *computer, const char* componType,
                                const char *name, char status/*='+'*/)
{
  if (computer && *computer && componType && *componType && name && *name)
  {
    stringarray& usage = m_usageMap[computer];
    std::string sName;
    sName += status;
    sName += GetPrefixForComponentType(componType);
    sName += name;
    name = sName.c_str() + 1; //skip status

    //insert this entry in the array if not present already
    stringarray::iterator i = usage.begin();
    stringarray::iterator iEnd = usage.end();

    for (; i != iEnd; i++)
    {
      std::string& component = *i;
      const char* szCompName = component.c_str();

      if (szCompName && *szCompName && !strcmp(szCompName+1, name))
      {
        if (status != '-' || *szCompName != 'I')//don't mark unused if status is 'I'
          component[0] = status;
        break;
      }
    }
    if (i == iEnd)
      usage.push_back(sName);
  }
}

//---------------------------------------------------------------------------
//  CreateFilterTree
//---------------------------------------------------------------------------
void CComputerPicker::CreateComputerFilterTree()
{
  m_pFilterTree.clear();
  m_pFilterTree.setown(createPTree("Filter"));
  IPropertyTree* pComponents= m_pFilterTree->addPropTree("Components",    createPTree("Components"));
  IPropertyTree* pDomains   = m_pFilterTree->addPropTree("Domains",       createPTree("Domains"));
  IPropertyTree* pCompTypes = m_pFilterTree->addPropTree("ComputerTypes", createPTree("ComputerTypes"));
  IPropertyTree* pSubnets   = m_pFilterTree->addPropTree("Subnets",       createPTree("Subnets"));

  // Generate computer usage map
  if (m_pRootNode)
  {
    //collect all software components using any computers
    Owned<IPropertyTreeIterator> iter = m_pRootNode->getElements("Software/*");
    ForEach(*iter)
    {
      IPropertyTree &component = iter->query();
      const char* szProcessName = component.queryName();
      const char* szCompName    = component.queryProp(XML_ATTR_NAME);

      const bool bThorCluster = strcmp(szProcessName, "ThorCluster")==0;

      if (bThorCluster || strcmp(szProcessName, "HoleCluster")==0 || strcmp(szProcessName, "RoxieCluster")==0)
      {
        Owned<IPropertyTreeIterator> instance = component.getElements("*[@computer]");
        ForEach(*instance)
        {
          const char* szComputer = instance->query().queryProp("@computer");
          NoteFilter(pComponents, szProcessName, szCompName, szComputer);
          NoteUsage(szComputer, szProcessName, szCompName);
        }
      }
      else
      {
        Owned<IPropertyTreeIterator> instance = component.getElements("Instance");
        ForEach(*instance)
        {
          const char* szComputer = instance->query().queryProp("@computer");
          NoteFilter(pComponents, szProcessName, szCompName, szComputer);
          NoteUsage(szComputer, szProcessName, szCompName);
        }

        const char* szComputer = component.queryProp("@computer");
        if (szComputer)
        {
          NoteFilter(pComponents, szProcessName, szCompName, szComputer);
          NoteUsage(szComputer, szProcessName, szCompName);
        }

        //if this is a dali server then get backup computer, if any
        if (strcmp(szProcessName, "DaliServerProcess")==0)
        {
          const char* szBackupComputer = component.queryProp("@backupComputer");
          NoteFilter(pComponents, szProcessName, szCompName, szBackupComputer);
          NoteUsage(szBackupComputer, szProcessName, szCompName);
        }
      }
    }

    iter.setown(m_pRootNode->getElements("Hardware/Domain"));
    ForEach(*iter)
    {
      const char* szDomain = iter->query().queryProp(XML_ATTR_NAME);
      StringBuffer xPath;
      xPath.appendf("Hardware/Computer[@domain='%s']", szDomain);

      //enumerate all computers in this domain
      Owned<IPropertyTreeIterator> icomputer = m_pRootNode->getElements(xPath);
      ForEach(*icomputer)
      {
        const char* szComputer = icomputer->query().queryProp("@name");

        NoteFilter(pDomains, "Domain", szDomain, szComputer);
        NoteUsage(szComputer, "Domain", szDomain);
      }
    }

    iter.setown(m_pRootNode->getElements("Hardware/ComputerType"));
    ForEach(*iter)
    {
      const char* szComputerType = iter->query().queryProp(XML_ATTR_NAME);
      StringBuffer xPath;
      xPath.appendf("Hardware/Computer[@computerType='%s']", szComputerType);

      //enumerate all computers with this computer type
      Owned<IPropertyTreeIterator> icomputer = m_pRootNode->getElements(xPath);
      ForEach(*icomputer)
      {
        const char* szComputer = icomputer->query().queryProp("@name");

        NoteFilter(pCompTypes, "ComputerType", szComputerType, szComputer);
        NoteUsage(szComputer, "ComputerType", szComputerType);
      }
    }

    //enumerate all computers and process their subnets
    Owned<IPropertyTreeIterator> icomputer = m_pRootNode->getElements("Hardware/Computer");
    ForEach(*icomputer)
    {
      IPropertyTree* pComputer = &icomputer->query();
      const char* szComputer   = pComputer->queryProp("@name");
      const char* szNetAddress = pComputer->queryProp("@netAddress");

      if (szComputer && *szComputer && szNetAddress && *szNetAddress)
      {
        char szSubnet[128];
        strcpy(szSubnet, szNetAddress);

        char* pchDot = strrchr(szSubnet, '.');

        if (pchDot)
        {
          strcpy(pchDot+1, "0");

          //TRACE3("%s(%s)=> %s\n", szComputer, pComputer->queryProp("@netAddress"), szSubnet);
          NoteFilter(pSubnets, "Subnet", szSubnet, szComputer);
          NoteUsage(szComputer, "Subnet", szSubnet);
        }
      }
    }
  }
}


//---------------------------------------------------------------------------
//  NoteFilter
//---------------------------------------------------------------------------
void CComputerPicker::NoteFilter(IPropertyTree* pFilter, const char *componentType, 
                                 const char *component, const char* computer)
{
  if (component && *component && 
    componentType && *componentType)
  {
    StringBuffer sComponentType(componentType);
    const char* psz = strstr(sComponentType.str(), "Process");
    if (psz)
      sComponentType.remove(psz - sComponentType.str(), strlen("Process"));

    StringBuffer xPath;
    xPath.appendf("%s[@name='%s']", sComponentType.str(), component);

    IPropertyTree* pComponentType = pFilter->queryPropTree(xPath);
    if (!pComponentType)
    {
      pComponentType = pFilter->addPropTree(sComponentType, createPTree(sComponentType));
      pComponentType->addProp("@name", component);
    }

    if (computer && *computer)
    {
      IPropertyTree* pComputer = pComponentType->addPropTree( "Computer", createPTree() );
      pComputer->addProp("@name", computer);
      pComputer->addPropBool("@__bHidden", true);
    }
  }   
}

void CComputerPicker::Refresh()
{
  // Passed all filters so add the computers left in the usage map
  int iItem = 0;
  m_pComputerTree.clear();
  m_pComputerTree.setown(createPTree("ComputerList"));
  Owned<IPropertyTreeIterator> iComputer = m_pRootNode->getElements(XML_TAG_HARDWARE"/"XML_TAG_COMPUTER);
  ForEach (*iComputer)
  {
    // Must have a valid name
    IPropertyTree* pNode = &iComputer->query();
    const char* szComputer = pNode->queryProp(XML_ATTR_NAME);
    StringBuffer sUsage;

    if (szComputer && *szComputer && GetUsage(szComputer, sUsage, false))
    {
      IPropertyTree* pComponentType = m_pComputerTree->addPropTree("Machine", createPTree("Machine"));
      pComponentType->addProp("@name", szComputer);
      pComponentType->addProp("@netAddress", pNode->queryProp(XML_ATTR_NETADDRESS));
      pComponentType->addProp("@usage", sUsage.str());
    }
  }
}

void CComputerPicker::ApplyFilter(const char* szSubTreeName, 
                                  const char* szIncAttrib, 
                                  char chStatus,
                                  StringBuffer& sFilterApplied)
{
  StringBuffer xPath;
  xPath.appendf("%s/*[@%s]", szSubTreeName, szIncAttrib);

  Owned<IPropertyTreeIterator> iter = m_pFilterTree->getElements(xPath);
  ForEach(*iter)
  {
    IPropertyTree* pComponent = &iter->query();
    const char*         szCompType = pComponent->queryName();
    const char*         szCompName = pComponent->queryProp("@name");

    if (sFilterApplied.length() > 0)
      sFilterApplied.append(", ");

    sFilterApplied.append(szCompName);

    Owned<IPropertyTreeIterator> icomputer = pComponent->getElements("Computer[@name]");
    ForEach(*icomputer)
    {
      const char* szComputer = icomputer->query().queryProp("@name");

      if (szComputer)
        NoteUsage(szComputer, szCompType, szCompName, chStatus);
    }
  }
}

