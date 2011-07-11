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
//////////////////////////////////////////////////////////////////////
//
//  BuilsSet.h
//
//////////////////////////////////////////////////////////////////////
#if !defined(AFX_DEPLOYUTILS_BUILDSET_HPP__INCLUDED_)
#define AFX_DEPLOYUTILS_BUILDSET_HPP__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#ifdef DEPLOYUTILS_EXPORTS
  #define DEPLOYUTILS_API __declspec(dllexport)
#else
  #define DEPLOYUTILS_API __declspec(dllimport)
#endif

interface IConstEnvironment;
//---------------------------------------------------------------------------
//  Function prototypes
//---------------------------------------------------------------------------
extern DEPLOYUTILS_API IPropertyTree *loadInstallSet(IPropertyTree *pBuild, IPropertyTree *pBuildSet, IConstEnvironment* pConstEnv);
extern DEPLOYUTILS_API IPropertyTree *loadSchema(IPropertyTree *pBuild, IPropertyTree *pBuildSet, StringBuffer& sSchemaPath, IConstEnvironment* pConstEnv);
IPropertyTree *loadDefaultSchema();
extern DEPLOYUTILS_API bool connectBuildSet(IPropertyTree* pBuild, IPropertyTree* pBuildSet, StringBuffer& buildSetPath, IConstEnvironment* pConstEnv);

//---------------------------------------------------------------------------
#endif // !defined(AFX_DEPLOYUTILS_BUILDSET_HPP__INCLUDED_)
