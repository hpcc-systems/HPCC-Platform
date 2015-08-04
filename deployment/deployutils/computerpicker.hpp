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
/////////////////////////////////////////////////////////////////////////////
//
// ComputerPicker.h : interface of the CComputerPicker class
//
/////////////////////////////////////////////////////////////////////////////
#if !defined(AFX_DEPLOYUTILS_COMPUTERPICKER_HPP__INCLUDED_)
#define AFX_DEPLOYUTILS_COMPUTERPICKER_HPP__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include <string>
#include <vector>
#include <map>
#include "jptree.hpp"


typedef std::vector<std::string> stringarray;
typedef std::vector<IPropertyTree*> IPropertyTreePtrArray;

class MapStrToChar : public std::map<std::string, char>
{
};

class MapStrToStrArray : public std::map<std::string, stringarray>
{
};

//---------------------------------------------------------------------------
//  CComputerPicker class declaration
//---------------------------------------------------------------------------
class CComputerPicker
{
// Construction
public:
    CComputerPicker();   // standard constructor
   virtual ~CComputerPicker();

public:
   void SetRootNode(const IPropertyTree* pNode);
   void Refresh();
   const IPropertyTree* getComputerTree(){return m_pComputerTree;}
   const IPropertyTree* getFilterTree(){return m_pFilterTree;}

private:
   static void CreateComponentTypePrefixMap();
   static char GetPrefixForComponentType(const char* componType);
   static const char* GetComponentTypeForPrefix(char chPrefix);

   void ResetUsageMap();
   void NoteUsage(const char *computer, const char* componType, const char *name, char status='+');
   bool GetUsage(const char *computer, StringBuffer& sUsage, bool bIncludeComponentType) const;

   void CreateComputerFilterTree();
   void NoteFilter(IPropertyTree* pFilter, const char *componentType, 
                   const char *component, const char* computer);

   void ApplyFilter(const char* szSubTreeName, const char* szIncAttrib, 
                    char chStatus, StringBuffer& sFilterApplied);

private:
   static MapStrToChar s_prefixMap;
   MapStrToStrArray m_usageMap;

   Owned<IPropertyTree> m_pFilterTree;
   Owned<IPropertyTree> m_pComputerTree;
   const IPropertyTree* m_pRootNode;
};
//---------------------------------------------------------------------------
//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif // !defined(AFX_DEPLOYUTILS_COMPUTERPICKER_HPP__INCLUDED_)
