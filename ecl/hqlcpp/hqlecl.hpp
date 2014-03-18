/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#ifndef HQLECL_INCL
#define HQLECL_INCL

#include "jcomp.hpp"
#include "workunit.hpp"
#include "hql.hpp"
#include "hqlcpp.hpp"
#include "hqlexpr.hpp"

interface IWorkUnit;

extern HQLCPP_API double getECLcomplexity(IHqlExpression * exprs, IErrorReceiver * errs, IWorkUnit *wu, ClusterType targetClusterType);
extern HQLCPP_API void dumpActivityCounts();

enum EclGenerateTarget { 
    EclGenerateNone, 
    EclGenerateCpp, 
    EclGenerateDll, 
    EclGenerateExe 
};

interface IHqlExprDllGenerator : extends IInterface
{
public:
    virtual void addLibrary(const char * name) = 0;
    virtual bool generateDll(ICppCompiler * _compiler) = 0;
    virtual bool generateExe(ICppCompiler * _compiler) = 0;
    virtual bool generatePackage(const char * packageName) = 0;
    virtual offset_t getGeneratedSize() const = 0;
    virtual bool processQuery(OwnedHqlExpr & parsedQuery, EclGenerateTarget generateTarget) = 0;
    virtual void setMaxCompileThreads(unsigned max) = 0;
    virtual void addManifest(const char *filename) = 0;
    virtual void addManifestFromArchive(IPropertyTree *archive) = 0;
    virtual void addWebServiceInfo(IPropertyTree *wsinfo) = 0;
    virtual void setSaveGeneratedFiles(bool value) = 0;
};

extern HQLCPP_API IHqlExprDllGenerator * createDllGenerator(IErrorReceiver * errs, const char *wuname, const char * targetdir, IWorkUnit *wu, const char * template_dir, ClusterType targetClusterType, ICodegenContextCallback * ctxCallback, bool checkForLocalFileUploads, bool okToAbort);


//Extract a single level of external libraries.
extern HQLCPP_API ClusterType queryClusterType(IConstWorkUnit * wu, ClusterType prevType);
extern HQLCPP_API IHqlExpression * extractExternalLibraries(HqlExprArray & libraries, IHqlExpression * query);
extern HQLCPP_API unsigned getLibraryCRC(IHqlExpression * library);
extern HQLCPP_API void setWorkunitHash(IWorkUnit * wu, IHqlExpression * expr);

#endif
