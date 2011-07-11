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
