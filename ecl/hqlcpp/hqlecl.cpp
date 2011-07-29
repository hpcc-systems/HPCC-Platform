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

#include "build-config.h"
#include "jliball.hpp"
#include "jmisc.hpp"
#include "jstream.hpp"

#include "hql.hpp"
#include "hqlexpr.hpp"
#include "hqlecl.hpp"
#include "hqlthql.hpp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqllib.ipp"
#include "hqlutil.hpp"
#include "hqlhtcpp.ipp"
#include "hqlwcpp.hpp"
#include "hqllib.ipp"
#include "hqlttcpp.ipp"

#include "workunit.hpp"
#include "thorplugin.hpp"

#define MAIN_MODULE_TEMPLATE        "thortpl.cpp"
#define HEADER_TEMPLATE             "thortpl.hpp"
#define CHILD_MODULE_TEMPLATE       "childtpl.cpp"

#define PROTO_TEMPLATE "prototpl.cpp"

class NullContextCallback : public CInterface, implements ICodegenContextCallback
{
    IMPLEMENT_IINTERFACE

    virtual void noteCluster(const char *clusterName) {}
    virtual void registerFile(const char * filename, const char * description) {}
    virtual bool allowAccess(const char * category) { return true; }
};

class HqlDllGenerator : public CInterface, implements IHqlExprDllGenerator, implements IAbortRequestCallback
{
public:
    HqlDllGenerator(IErrorReceiver * _errs, const char * _wuname, const char * _targetdir, IWorkUnit * _wu, const char * _template_dir, ClusterType _targetClusterType, ICodegenContextCallback * _ctxCallback, bool _checkForLocalFileUploads) :
        errs(_errs), wuname(_wuname), targetDir(_targetdir), wu(_wu), template_dir(_template_dir), targetClusterType(_targetClusterType), ctxCallback(_ctxCallback), checkForLocalFileUploads(_checkForLocalFileUploads)
    {
        if (!ctxCallback)
            ctxCallback.setown(new NullContextCallback);
        noOutput = true;
        defaultMaxCompileThreads = 1;
        generateTarget = EclGenerateNone;
        code.setown(createCppInstance(wu, wuname));
        deleteGenerated = false;
    }
    IMPLEMENT_IINTERFACE

    virtual void addLibrary(const char * name);
    virtual bool processQuery(IHqlExpression * expr, EclGenerateTarget _generateTarget);
    virtual bool generateDll(ICppCompiler * compiler);
    virtual bool generateExe(ICppCompiler * compiler);
    virtual bool generatePackage(const char * packageName);
    virtual void setMaxCompileThreads(unsigned value) { defaultMaxCompileThreads = value; }
    virtual void setWebServiceInfo(IPropertyTree * webServiceInfo) { if (webServiceInfo) code->addWebServices(webServiceInfo); }

    virtual double getECLcomplexity(IHqlExpression * exprs);
    virtual void generateCppPrototypes(IHqlScope * scope);
    virtual void setSaveGeneratedFiles(bool value) { deleteGenerated = !value; }

protected:
    void addCppName(const char * filename);
    void addLibrariesToCompiler();
    void addWorkUnitAsResource();
    void calculateHash(IHqlExpression * expr);
    bool doCompile(ICppCompiler * compiler);
    void doExpand(HqlCppTranslator & translator);
    void expandCode(const char * templateName, const char * ext, IHqlCppInstance * code, bool multiFile, unsigned pass, CompilerType compiler);
    void flushResources();
    bool generateCode(IHqlExpression * exprs);
    void insertStandAloneCode();
    void setWuState(bool ok);
    inline bool abortRequested();

protected:
    Linked<IErrorReceiver> errs;
    const char * wuname;
    StringAttr targetDir;
    Linked<IWorkUnit> wu;
    Owned<IHqlCppInstance> code;
    const char * template_dir;
    ClusterType targetClusterType;
    Linked<ICodegenContextCallback> ctxCallback;
    unsigned defaultMaxCompileThreads;
    StringArray sourceFiles;
    StringArray libraries;
    bool checkForLocalFileUploads;
    bool noOutput;
    EclGenerateTarget generateTarget;
    bool deleteGenerated;
};


//---------------------------------------------------------------------------

static IHqlExpression * processMetaCommands(HqlCppTranslator & translator, IWorkUnit * wu, IHqlExpression * expr, ICodegenContextCallback *ctxCallback);

void HqlDllGenerator::addCppName(const char * filename)
{
    if (wu)
    {
        Owned<IWUQuery> query = wu->updateQuery();
        associateLocalFile(query, FileTypeCpp, filename, pathTail(filename), 0);
        ctxCallback->registerFile(filename, "Workunit CPP");
    }
}

void HqlDllGenerator::addLibrary(const char * name)
{
    libraries.append(name);
}


void HqlDllGenerator::addLibrariesToCompiler()
{
    unsigned idx=0;
    loop
    {
        const char * lib = code->queryLibrary(idx);
        if (!lib)
            break;

        PrintLog("Adding library: %s", lib);
        addLibrary(lib);
        idx++;
    }
}


void HqlDllGenerator::expandCode(const char * templateName, const char * ext, IHqlCppInstance * code, bool multiFile, unsigned pass, CompilerType compiler)
{
    StringBuffer fullname;
    addDirectoryPrefix(fullname, targetDir).append(wuname).append(ext);

    Owned<IFile> out = createIFile(fullname.str());
    Owned<ITemplateExpander> expander = createTemplateExpander(out, templateName, template_dir);
    if (!expander)
        throwError2(HQLERR_CouldNotOpenTemplateXatY, templateName, template_dir ? template_dir : ".");

    Owned<ISectionWriter> writer = createCppWriter(*code, compiler);
    Owned<IProperties> props = createProperties(true);
    if (multiFile)
    {
        props->setProp("multiFile", true);
        props->setProp("pass", pass);
    }
    StringBuffer headerName;
    headerName.append(wuname).append(".hpp");
    props->setProp("headerName", headerName.str());
    props->setProp("outputName", fullname.str());

    expander->generate(*writer, pass, props);

    if (!deleteGenerated)
        addCppName(fullname);
}


//---------------------------------------------------------------------------

bool HqlDllGenerator::processQuery(IHqlExpression * expr, EclGenerateTarget _generateTarget)
{
    generateTarget = _generateTarget;
    assertex(wu->getHash());
    unsigned prevCount = errs->errCount();
    bool ok = generateCode(expr);
    code->flushHints();
    wu->commit();

    if (!ok)
        return false;

    if (errs->errCount() != prevCount)
        return false;

    switch (generateTarget)
    {
    case EclGenerateNone:
    case EclGenerateCpp:
        return true;
    }
    
    flushResources();
    addLibrariesToCompiler();

    // Free up memory before the c++ compile occurs
    code.clear();
    return true;
}

bool HqlDllGenerator::generateDll(ICppCompiler * compiler)
{
    if (noOutput || !compiler)
        return true;

    bool ok = doCompile(compiler);
    setWuState(ok);
    return ok;
}

bool HqlDllGenerator::generateExe(ICppCompiler * compiler)
{
    if (noOutput || !compiler)
        return true;

    compiler->setCreateExe(true);

    bool ok = doCompile(compiler);
    setWuState(ok);
    return ok;
}

bool HqlDllGenerator::generatePackage(const char * packageName)
{
    return false;
}

bool HqlDllGenerator::generateCode(IHqlExpression * exprs)
{
    wu->resetBeforeGeneration();

    noOutput = true;
    {
        // ensure warnings/errors are available before we do the processing...
        wu->commit();

        MTIME_SECTION (timer, "Generate_code");
        unsigned time = msTick();
        HqlCppTranslator translator(errs, wuname, code, targetClusterType, ctxCallback);
        OwnedHqlExpr query = processMetaCommands(translator, wu, exprs, ctxCallback);

        bool ok = false;
        try
        {
            if (!translator.buildCpp(*code, query))
            {
                wu->setState(WUStateCompleted);
                return true;
            }
            translator.finalizeResources();
            translator.expandFunctions(true);
        }
        catch (IECLError * e)
        {
            StringBuffer s;
            errs->reportError(e->errorCode(), e->errorMessage(s).str(), e->getFilename(), e->getLine(), e->getColumn(), e->getPosition());
            e->Release();
            return false;
        }
        catch (IException * e)
        {
            if (e->errorCode() != HQLERR_ErrorAlreadyReported)
            {
                unsigned errcode = e->errorCode() ? e->errorCode() : 1;
                StringBuffer s;
                errs->reportError(errcode, e->errorMessage(s).str(), NULL, 0, 0, 1);
            }
            e->Release();
            return false;
        }
        catch (RELEASE_CATCH_ALL)
        {
            errs->reportError(99, "Unknown error", NULL, 0, 0, 1);
            return false;
        }

        if (generateTarget == EclGenerateExe)
            insertStandAloneCode();

        wu->commit();
        
        //Commit work unit so can view graphs etc. while compiling the C++
        if ((generateTarget == EclGenerateNone) || wu->getDebugValueBool("OnlyCheckQuery", false))
        {
            wu->setState(WUStateCompleted);
            return false;
        }

        doExpand(translator);
        if (wu->getDebugValueBool("addTimingToWorkunit", true))
            wu->setTimerInfo("EclServer: generate code", NULL, msTick()-time, 1, 0);

        wu->commit();
        addWorkUnitAsResource();
    }
    noOutput = false;
    return true;
}

void HqlDllGenerator::addWorkUnitAsResource()
{
    SCMStringBuffer wuXML;
    exportWorkUnitToXML(wu, wuXML);
    code->addCompressResource("WORKUNIT", 1000, wuXML.length(), wuXML.str());
}


void HqlDllGenerator::insertStandAloneCode()
{
    BuildCtx ctx(static_cast<HqlCppInstance &>(*code), goAtom);
    ctx.addQuotedCompound("int main(int argc, const char *argv[])");
    ctx.addQuoted("return start_query(argc, argv);\n");
}


void HqlDllGenerator::doExpand(HqlCppTranslator & translator)
{
    unsigned startExpandTime = msTick();
    unsigned numExtraFiles = translator.getNumExtraCppFiles();
    bool isMultiFile = translator.spanMultipleCppFiles() && (numExtraFiles != 0);
    expandCode(MAIN_MODULE_TEMPLATE, ".cpp", code, isMultiFile, 0, translator.queryOptions().targetCompiler);
    if (isMultiFile)
    {
        expandCode(HEADER_TEMPLATE, ".hpp", code, true, 0, translator.queryOptions().targetCompiler);
        for (unsigned i= 0; i < translator.getNumExtraCppFiles(); i++)
        {
            StringBuffer fullext;
            fullext.append("_").append(i+1).append(".cpp");
            expandCode(CHILD_MODULE_TEMPLATE, fullext, code, true, i+1, translator.queryOptions().targetCompiler);

            StringBuffer fullname;
            fullname.append(wuname).append("_").append(i+1);
            sourceFiles.append(fullname);
        }
    }

    unsigned endExpandTime = msTick();
    if (wu->getDebugValueBool("addTimingToWorkunit", true))
        wu->setTimerInfo("EclServer: write c++", NULL, endExpandTime-startExpandTime, 1, 0);
}

bool HqlDllGenerator::abortRequested()
{
    return (wu && wu->aborting());
}

bool HqlDllGenerator::doCompile(ICppCompiler * compiler)
{
    ForEachItemIn(i, sourceFiles)
        compiler->addSourceFile(sourceFiles.item(i));

    unsigned maxThreads = wu->getDebugValueInt("maxCompileThreads", defaultMaxCompileThreads);
    compiler->setMaxCompileThreads(maxThreads);

    bool debug = wu->getDebugValueBool("debugQuery", false);
    compiler->setDebug(debug);
    compiler->setDebugLibrary(debug);
    if (!debug)
    {
        int optimizeLevel = wu->getDebugValueInt("optimizeLevel", targetClusterType == RoxieCluster ? 3 : -1);
        if (optimizeLevel != -1)
            compiler->setOptimizeLevel(optimizeLevel);
    }
#ifdef __64BIT__
    bool target64bit = wu->getDebugValueBool("target64bit", true);
#else
    bool target64bit = wu->getDebugValueBool("target64bit", false);
#endif
    compiler->setTargetBitLength(target64bit ? 64 : 32);

    ForEachItemIn(idx, libraries)
        compiler->addLibrary(libraries.item(idx));

    StringBuffer options;
    StringBufferAdaptor linkOptionAdaptor(options);
    wu->getDebugValue("linkOptions", linkOptionAdaptor);
    compiler->addLinkOption(options.str());
    options.clear();
    StringBufferAdaptor optionAdaptor(options);
    wu->getDebugValue("compileOptions", optionAdaptor);
    compiler->addCompileOption(options.str());

    compiler->setAbortChecker(this);

    MTIME_SECTION (timer, "Compile_code");
    unsigned time = msTick();
    PrintLog("Compiling %s", wuname);
    bool ok = compiler->compile();
    if(ok)
        PrintLog("Compiled %s", wuname);
    else
        PrintLog("Failed to compile %s", wuname);
    time = msTick()-time;
    if (wu->getDebugValueBool("addTimingToWorkunit", true))
        wu->setTimerInfo("EclServer: compile code", NULL, time, 1, 0);

    //Keep the files if there was a compile error.
    if (ok && deleteGenerated)
    {
        StringBuffer temp;
        remove(temp.clear().append(wuname).append(".cpp").str());
        remove(temp.clear().append(wuname).append(".hpp").str());
        ForEachItemIn(i, sourceFiles)
        {
            temp.clear().append(sourceFiles.item(i)).append(".cpp");
            remove(temp.str());
        }
    }
    return ok;
}

void HqlDllGenerator::flushResources()
{
    StringBuffer resname(wuname);
#ifdef _WIN32
    resname.append(".res");
#else
    resname.append(".res.o");
#endif
    if (code)
        code->flushResources(resname.str(), ctxCallback);
}


void HqlDllGenerator::setWuState(bool ok)
{
    if(ok)
    {
        if(checkForLocalFileUploads && wu->requiresLocalFileUpload())
            wu->setState(WUStateUploadingFiles);
        else
            wu->setState(WUStateCompiled);
    }
    else
        wu->setState(WUStateFailed);
}


double HqlDllGenerator::getECLcomplexity(IHqlExpression * exprs)
{
    Owned<IHqlCppInstance> code = createCppInstance(wu, NULL);

    HqlCppTranslator translator(errs, "temp", code, targetClusterType, NULL);
    OwnedHqlExpr query = processMetaCommands(translator, wu, exprs, ctxCallback);

    return translator.getComplexity(*code, query);
}


void HqlDllGenerator::generateCppPrototypes(IHqlScope * scope)
{
    Owned<IHqlCppInstance> code = createCppInstance(NULL, NULL);

    HqlCppTranslator translator(NULL, wuname, code, ThorCluster, NULL);
    translator.buildServicePrototypes(scope);

    expandCode(PROTO_TEMPLATE, NULL, code, false, 0, translator.queryOptions().targetCompiler);
}



extern HQLCPP_API double getECLcomplexity(IHqlExpression * exprs, IErrorReceiver * errs, IWorkUnit *wu, ClusterType targetClusterType)
{
    HqlDllGenerator generator(errs, "unknown", NULL, wu, NULL, targetClusterType, NULL, false);
    return generator.getECLcomplexity(exprs);
}

extern HQLCPP_API void generateCppPrototypes(IHqlScope * scope, const char * name, const char *template_dir)
{
    HqlDllGenerator generator(NULL, name, NULL, NULL, template_dir, ThorCluster, NULL, false);
    generator.generateCppPrototypes(scope);
}

extern HQLCPP_API IHqlExprDllGenerator * createDllGenerator(IErrorReceiver * errs, const char *wuname, const char * targetdir, IWorkUnit *wu, const char * template_dir, ClusterType targetClusterType, ICodegenContextCallback *ctxCallback, bool checkForLocalFileUploads)
{
    return new HqlDllGenerator(errs, wuname, targetdir, wu, template_dir, targetClusterType, ctxCallback, checkForLocalFileUploads);
}

/*
extern HQLCPP_API void addLibraryReference(IWorkUnit * wu, IHqlLibraryInfo * library, const char * lid)
{
    StringBuffer libraryName;
    library->getName(libraryName);

    Owned<IWULibrary> entry = wu->updateLibraryByName(libraryName.str());
    entry->setMajorVersion(library->queryMajorVersion());
    entry->setMinorVersion(library->queryMinorVersion());
    entry->setCrc(library->queryInterfaceCrc());
    if (lid && *lid)
        entry->setLID(lid);
}
*/

extern HQLCPP_API ClusterType queryClusterType(IConstWorkUnit * wu, ClusterType prevType)
{
    ClusterType targetClusterType = prevType;
    if (wu->getDebugValueBool("forceRoxie", false))
        targetClusterType = RoxieCluster;
    else if (prevType == NoCluster)
        targetClusterType = ThorCluster;

    SCMStringBuffer targetText;
    wu->getDebugValue("targetClusterType", targetText);
    targetClusterType = getClusterType(targetText.s.str(), targetClusterType);

    if ((targetClusterType != RoxieCluster) && wu->getDebugValueBool("forceFakeThor", false))
        targetClusterType = HThorCluster;

    return targetClusterType;
}

static IHqlExpression * processMetaCommands(HqlCppTranslator & translator, IWorkUnit * wu, IHqlExpression * expr, ICodegenContextCallback *ctxCallback)
{
    NewThorStoredReplacer transformer(translator, wu, ctxCallback);

    translator.traceExpression("before process meta commands", expr);

    transformer.analyse(expr);
    if (!transformer.needToTransform())
        return LINK(expr);
    return transformer.transform(expr);
}


extern HQLCPP_API unsigned getLibraryCRC(IHqlExpression * library)
{
    assertex(library->getOperator() == no_funcdef);
    return getExpressionCRC(library->queryChild(0));
}


void setWorkunitHash(IWorkUnit * wu, IHqlExpression * expr)
{
    //Assuming builds come from different branches this will change the crc for each one.
    unsigned cacheCRC = crc32(BUILD_TAG, strlen(BUILD_TAG), ACTIVITY_INTERFACE_VERSION);
    cacheCRC += getExpressionCRC(expr);
#ifdef _WIN32
    cacheCRC++; // make sure CRC is different in windows/linux
#endif
#ifdef __64BIT__
    cacheCRC += 2; // make sure CRC is different for different host platform (shouldn't really matter if cross-compiling working properly, but fairly harmless)
#endif
    IExtendedWUInterface *ewu = queryExtendedWU(wu);
    cacheCRC = ewu->calculateHash(cacheCRC);
    wu->setHash(cacheCRC);
}

