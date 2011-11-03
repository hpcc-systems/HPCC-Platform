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
#ifndef HQLECL_INCL
#define HQLECL_INCL

#include "jcomp.hpp"
#include "workunit.hpp"
#include "hql.hpp"
#include "hqlcpp.hpp"
#include "hqlexpr.hpp"

interface IWorkUnit;

extern HQLCPP_API void generateCppPrototypes(IHqlScope * scope, const char * name, const char * template_dir);
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
    virtual bool processQuery(IHqlExpression * expr, EclGenerateTarget generateTarget) = 0;
    virtual void setMaxCompileThreads(unsigned max) = 0;
    virtual void addManifest(const char *filename) = 0;
    virtual void addManifestFromArchive(IPropertyTree *archive) = 0;
    virtual void addResourceIncludes(IPropertyTree *includes) = 0;
    virtual void addWebServiceInfo(IPropertyTree *wsinfo) = 0;
    virtual void setSaveGeneratedFiles(bool value) = 0;
};

extern HQLCPP_API IHqlExprDllGenerator * createDllGenerator(IErrorReceiver * errs, const char *wuname, const char * targetdir, IWorkUnit *wu, const char * template_dir, ClusterType targetClusterType, ICodegenContextCallback * ctxCallback, bool checkForLocalFileUploads);


//Extract a single level of external libraries.
extern HQLCPP_API ClusterType queryClusterType(IConstWorkUnit * wu, ClusterType prevType);
extern HQLCPP_API IHqlExpression * extractExternalLibraries(HqlExprArray & libraries, IHqlExpression * query);
extern HQLCPP_API unsigned getLibraryCRC(IHqlExpression * library);
extern HQLCPP_API void setWorkunitHash(IWorkUnit * wu, IHqlExpression * expr);

#endif
