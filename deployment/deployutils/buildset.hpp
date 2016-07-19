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
  #define DEPLOYUTILS_API DECL_EXPORT
#else
  #define DEPLOYUTILS_API DECL_IMPORT
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
