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
#ifndef DEPLOYUTILS_HPP_INCL
#define DEPLOYUTILS_HPP_INCL

#ifdef DEPLOYUTILS_EXPORTS
  #define DEPLOYUTILS_API __declspec(dllexport)
#else
  #define DEPLOYUTILS_API __declspec(dllimport)
#endif

//disable the harmless warning about very long symbol names > 255 chars in debug mode
//this is typical with STL
#pragma warning( disable : 4786 )


#include "jiface.hpp"
#include "jthread.hpp"
#include "environment.hpp"
interface IPropertyTree;
interface IProperties;
interface IXslProcessor;
interface IXslTransform;
interface IConstEnvironment;
interface IEnvDeploymentEngine;
interface IDeploymentCallback;
class StringAttr;
class CWizardInputs;

//---------------------------------------------------------------------------
// Factory functions
//---------------------------------------------------------------------------
extern DEPLOYUTILS_API bool generateHeadersFromXsd(const IPropertyTree* pEnv, const char* xsdName, const char* jsName);
extern DEPLOYUTILS_API bool generateHeaders(const IPropertyTree* pEnv, IConstEnvironment* pConstEnv);
extern DEPLOYUTILS_API bool generateHeadersFromEnv(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut = false);
extern DEPLOYUTILS_API bool generateHardwareHeaders(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut = false, IPropertyTree* pCompTree = NULL);
extern DEPLOYUTILS_API bool generateBuildHeaders(const IPropertyTree* pEnv, bool isPrograms, StringBuffer& sbDefn, bool writeOut = false);
extern DEPLOYUTILS_API bool generateHeaderForTopology(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut = false);
extern DEPLOYUTILS_API bool generateHeadersForEnvSettings(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut = false);
extern DEPLOYUTILS_API bool generateHeadersForEnvXmlView(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut = false);
extern DEPLOYUTILS_API bool getComputersListWithUsage(const IPropertyTree* pEnv, StringBuffer& sbComputers, StringBuffer& sbFilter);
extern DEPLOYUTILS_API IPropertyTree* generateTreeFromXsd(const IPropertyTree* pEnv, IPropertyTree* pSchema, const char* compName, bool allSubTypes = true, 
                                                          bool cloudFlag = false, CWizardInputs* pWInputs = NULL, bool genOptional = true);
extern DEPLOYUTILS_API bool checkComponentReferences(const IPropertyTree* pEnv, IPropertyTree* pCompNode, const char* compName, StringBuffer& sMsg, const char* szNewName=NULL);
extern DEPLOYUTILS_API bool generateHeaderForComponent(const IPropertyTree* pEnv, IPropertyTree* pSchema, const char* compName);
extern DEPLOYUTILS_API const char* getUniqueName(const IPropertyTree* pEnv, StringBuffer& sName, const char* xpath, const char* category);
extern DEPLOYUTILS_API void getTabNameArray(const IPropertyTree* pEnv, IPropertyTree* pSchema, const char* compName, StringArray& strArray);
extern DEPLOYUTILS_API void getDefnPropTree(const IPropertyTree* pEnv, IPropertyTree* pSchema, const char* compName, IPropertyTree* pDefTree, StringBuffer xpath);
extern DEPLOYUTILS_API void getDefnString(const IPropertyTree* pEnv, IPropertyTree* pSchema, const char* compName, StringBuffer& compDefn, StringBuffer& viewChildNodes, StringBuffer& multiRowNodes);
extern DEPLOYUTILS_API IPropertyTree* getNewRange(const IPropertyTree* pEnvRoot, const char* prefix, const char* domain, const char* cType, const char* startIP, const char* endIP);
extern DEPLOYUTILS_API void getCommonDir(const IPropertyTree* pEnv, const char* catType, const char* buildSetName, const char* compName, StringBuffer& sbVal);
extern DEPLOYUTILS_API void deleteRecursive(const char* path);

extern DEPLOYUTILS_API bool handleRoxieOperation(IPropertyTree* pEnv, const char* cmd, const char* xmlStr);
extern DEPLOYUTILS_API bool handleThorTopologyOp(IPropertyTree* pEnv, const char* cmd, const char* xmlStr, StringBuffer& sMsg);
extern DEPLOYUTILS_API void addComponentToEnv(IPropertyTree* pEnv, const char* buildSet, StringBuffer& sbNewName, IPropertyTree* pCompTree);
extern DEPLOYUTILS_API bool onChangeAttribute(const IPropertyTree* pEnv, IConstEnvironment* pConstEnv, const char* attrName, IPropertyTree* pOnChange, IPropertyTree*& pNode, IPropertyTree* pParentNode, 
                                              int position, const char* szNewValue, const char* prevValue, const char* buildset);
extern DEPLOYUTILS_API bool xsltTransform(const StringBuffer& xml, const char* sheet, IProperties *params, StringBuffer& ret);
extern DEPLOYUTILS_API void UpdateRefAttributes(IPropertyTree* pEnv, const char* szPath, const char* szAttr, const char* szOldVal, const char* szNewVal);
extern DEPLOYUTILS_API bool ensureUniqueName(const IPropertyTree* pEnv, IPropertyTree* pParentNode, const char* szText);
extern DEPLOYUTILS_API void addInstanceToCompTree(const IPropertyTree* pEnvRoot,const IPropertyTree* pInstance,StringBuffer& dups,StringBuffer& resp, IConstEnvironment* pConstEnv);
extern DEPLOYUTILS_API void formIPList(const char* ip, StringArray& formattedIpList);
extern DEPLOYUTILS_API void buildEnvFromWizard(const char * xml, const char* service,IPropertyTree* cfg, StringBuffer& envXml, MapStringTo<StringBuffer>* dirMap=NULL);
extern DEPLOYUTILS_API void runScript(StringBuffer& output, StringBuffer& errMsg, const char* pathToScript);
extern DEPLOYUTILS_API bool validateIPS(const char* ipAddressList);
extern DEPLOYUTILS_API void getSummary(const IPropertyTree* pEnvRoot, StringBuffer& respXmlStr , bool prepareLink);
extern DEPLOYUTILS_API void mergeAttributes(IPropertyTree* pTo, IPropertyTree* pFrom);
extern DEPLOYUTILS_API void addEspBindingInformation(const char* xmlArg, IPropertyTree* pEnvRoot, StringBuffer& sbNewName, IConstEnvironment* pConstEnv);
extern DEPLOYUTILS_API bool updateDirsWithConfSettings(IPropertyTree* pEnvRoot, IProperties* pParams, bool ovrLog = true, bool ovrRun = true);
extern DEPLOYUTILS_API bool validateEnv(IConstEnvironment* pConstEnv, bool abortOnException = true);
#endif
