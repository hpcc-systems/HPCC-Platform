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

#ifdef _DEBUG
#define IS_DEBUG_BUILD          true
#else
#define IS_DEBUG_BUILD          false
#endif

constexpr const char * headerTemplate = R"!!($?doNotIncludeInGeneratedCode$
/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */

$?$/* Template for generating a header file for a multi-cpp file query */

@mainprototypes@

@parenthelpers@
)!!";

constexpr const char * mainTemplate = R"!!($?doNotIncludeInGeneratedCode$
/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */

$?$/* Template for generating thor/hthor/roxie output */
#if defined(__clang__) || (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 2))
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#endif
#include "eclinclude4.hpp"
@include@
@prototype@
$?multiFile$#include "$headerName$"
$?$@literal@
@declare@
@helper@

@go@

extern "C" ECL_API IEclProcess* createProcess()
{
    @init@
    return new MyEclProcess;
}

@userFunction@
)!!";

constexpr const char * childTemplate = R"!!($?doNotIncludeInGeneratedCode$
/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################*/

$?$/* Template for generating a child module for query */
#if defined(__clang__) || (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 2))
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#endif
#include "eclinclude4.hpp"
@include@
@prototype@

#include "$headerName$"

@helper@
@userFunction@
)!!";

class NullContextCallback : implements ICodegenContextCallback, public CInterface
{
public:
    NullContextCallback(IWorkUnit * _wu) : workunit(_wu) {}
    IMPLEMENT_IINTERFACE

    virtual void noteCluster(const char *clusterName) override {}
    virtual void pushCluster(const char *clusterName) override {}
    virtual void popCluster() override {}
    virtual bool allowAccess(const char * category, bool isSigned) override { return true; }
    virtual IHqlExpression *lookupDFSlayout(const char *filename, IErrorReceiver &errs, const ECLlocation &location, bool isOpt) const override { return nullptr; }
    virtual unsigned lookupClusterSize() const override { return 0; }
    virtual void getTargetPlatform(StringBuffer & result) override
    {
        workunit->getDebugValue("targetClusterType", StringBufferAdaptor(result));
    }
protected:
    Linked<IWorkUnit> workunit;
};

class HqlDllGenerator : implements IHqlExprDllGenerator, implements IAbortRequestCallback, public CInterface
{
public:
    HqlDllGenerator(IErrorReceiver * _errs, const char * _wuname, const char * _targetdir, IWorkUnit * _wu, ClusterType _targetClusterType, ICodegenContextCallback * _ctxCallback, bool _checkForLocalFileUploads, bool _okToAbort) :
        errs(_errs), wuname(_wuname), targetDir(_targetdir), wu(_wu), targetClusterType(_targetClusterType), ctxCallback(_ctxCallback), checkForLocalFileUploads(_checkForLocalFileUploads), okToAbort(_okToAbort)
    {
        if (!ctxCallback)
            ctxCallback.setown(new NullContextCallback(_wu));
        noOutput = true;
        defaultMaxCompileThreads = 1;
        generateTarget = EclGenerateNone;
        code.setown(createCppInstance(wu, wuname));
        deleteGenerated = false;
        totalGeneratedSize = 0;
    }
    IMPLEMENT_IINTERFACE

    virtual void addLibrary(const char * name);
    virtual bool processQuery(OwnedHqlExpr & parsedQuery, EclGenerateTarget _generateTarget);
    virtual bool generateDll(ICppCompiler * compiler);
    virtual bool generateExe(ICppCompiler * compiler);
    virtual bool generatePackage(const char * packageName);
    virtual void setMaxCompileThreads(unsigned value) { defaultMaxCompileThreads = value; }
    virtual void addManifest(const char *filename) { code->addManifest(filename, ctxCallback); }
    virtual void addManifestsFromArchive(IPropertyTree *archive) { code->addManifestsFromArchive(archive, ctxCallback); }
    virtual void addWebServiceInfo(IPropertyTree *wsinfo){ code->addWebServiceInfo(wsinfo); }

    virtual double getECLcomplexity(IHqlExpression * exprs);
    virtual void setSaveGeneratedFiles(bool value) { deleteGenerated = !value; }

protected:
    void addCppName(const char * filename, unsigned minActivity, unsigned maxActivity);
    void addLibrariesToCompiler();
    void addWorkUnitAsResource();
    void calculateHash(IHqlExpression * expr);
    bool doCompile(ICppCompiler * compiler);
    void doExpand(HqlCppTranslator & translator);
    void expandCode(StringBuffer & filename, const char * templateName, bool isHeader, IHqlCppInstance * code, bool multiFile, unsigned pass, CompilerType compiler);
    void flushResources();
    bool generateCode(HqlQueryContext & query);
    bool generateFullFieldUsageStatistics(HqlCppTranslator & translator, IHqlExpression * query);
    IPropertyTree * generateSingleFieldUsageStatistics(IHqlExpression * expr, const char * variety, const IPropertyTree * exclude);
    offset_t getGeneratedSize() const;
    void insertStandAloneCode();
    void setWuState(bool ok);
    inline bool abortRequested();

protected:
    Linked<IErrorReceiver> errs;
    const char * wuname;
    StringAttr targetDir;
    Linked<IWorkUnit> wu;
    Owned<IHqlCppInstance> code;
    ClusterType targetClusterType;
    Linked<ICodegenContextCallback> ctxCallback;
    unsigned defaultMaxCompileThreads;
    StringArray temporaryDirectories;
    StringArray sourceFiles;
    StringArray sourceFlags;
    BoolArray sourceIsTemp;
    StringArray libraries;
    StringArray objects;
    offset_t totalGeneratedSize;
    bool checkForLocalFileUploads;
    bool noOutput;
    EclGenerateTarget generateTarget;
    bool deleteGenerated;
    bool okToAbort;
};


//---------------------------------------------------------------------------

static void processMetaCommands(HqlCppTranslator & translator, IWorkUnit * wu, HqlQueryContext & query, ICodegenContextCallback *ctxCallback);

void HqlDllGenerator::addCppName(const char * filename, unsigned minActivity, unsigned maxActivity)
{
    if (wu)
    {
        Owned<IWUQuery> query = wu->updateQuery();
        associateLocalFile(query, FileTypeCpp, filename, pathTail(filename), 0, minActivity, maxActivity);
    }
}

void HqlDllGenerator::addLibrary(const char * name)
{
    libraries.append(name);
}


void HqlDllGenerator::addLibrariesToCompiler()
{
    unsigned idx=0;
    for (;;)
    {
        const char * lib = code->queryLibrary(idx);
        if (!lib)
            break;
        LOG(MCuserInfo,"Adding library: %s", lib);
        addLibrary(lib);
        idx++;
    }
    idx=0;
    for (;;)
    {
        const char * obj = code->queryObjectFile(idx);
        if (!obj)
            break;
        LOG(MCuserInfo,"Adding object file: %s", obj);
        objects.append(obj);
        idx++;
    }
    idx=0;
    for (;;)
    {
        const char * src = code->querySourceFile(idx);
        if (!src)
            break;
        LOG(MCuserInfo,"Adding source file: %s", src);
        sourceFiles.append(src);
        sourceFlags.append(code->querySourceFlags(idx));
        sourceIsTemp.append(code->querySourceIsTemp(idx));
        idx++;
    }
    idx=0;
    for (;;)
    {
        const char * dir = code->queryTempDirectory(idx);
        if (!dir)
            break;
        LOG(MCuserInfo,"Adding temporary directory: %s", dir);
        temporaryDirectories.append(dir);
        idx++;
    }
}


void HqlDllGenerator::expandCode(StringBuffer & filename, const char * codeTemplate, bool isHeader, IHqlCppInstance * code, bool multiFile, unsigned pass, CompilerType compiler)
{
    filename.clear().append(wuname);
    if (pass != 0)
        filename.append("_").append(pass);
    const char * ext = isHeader ? ".hpp" : ".cpp";
    filename.append(ext);

    StringBuffer fullname;
    addDirectoryPrefix(fullname, targetDir).append(filename);

    Owned<IFile> out = createIFile(fullname.str());
    Owned<ITemplateExpander> expander = createTemplateExpander(out, codeTemplate);

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

    totalGeneratedSize += out->size();

    if (!deleteGenerated)
    {
        unsigned minActivity, maxActivity;
        code->getActivityRange(pass, minActivity, maxActivity);
        addCppName(fullname, minActivity, maxActivity);
    }
}


//---------------------------------------------------------------------------

bool HqlDllGenerator::processQuery(OwnedHqlExpr & parsedQuery, EclGenerateTarget _generateTarget)
{
    generateTarget = _generateTarget;
    assertex(wu->getHash());
    unsigned prevCount = errs->errCount();

    HqlQueryContext query;
    query.expr.setown(parsedQuery.getClear());
    bool ok = generateCode(query);
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

static bool isSetMeta(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_compound:
    case no_comma:
        return isSetMeta(expr->queryChild(0)) && isSetMeta(expr->queryChild(1));
    case no_setmeta:
        return true;
    default:
        return false;
    }
}

IPropertyTree * HqlDllGenerator::generateSingleFieldUsageStatistics(IHqlExpression * expr, const char * variety, const IPropertyTree * exclude)
{
    //Generate each into a new workunit
//    Owned<IWorkUnit> localWu = createLocalWorkUnit();
    IWorkUnit * localWu = wu;
    //Generate the code into a new instance each time so it doesn't hang around eating memory
    Owned<IHqlCppInstance> localCode = createCppInstance(localWu, wuname);


    HqlQueryContext query;
    query.expr.set(expr);

    resetUniqueId();
    wu->resetBeforeGeneration();
    HqlCppTranslator translator(errs, wuname, localCode, targetClusterType, ctxCallback);

    try
    {
        translator.buildCpp(*localCode, query);
        return translator.gatherFieldUsage(variety, exclude);
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
        return NULL;
    }
    catch (RELEASE_CATCH_ALL)
    {
        errs->reportError(99, "Unknown error", NULL, 0, 0, 1);
        return NULL;
    }
}

bool HqlDllGenerator::generateFullFieldUsageStatistics(HqlCppTranslator & translator, IHqlExpression* query)
{
    LinkedHqlExpr savedQuery = query;

    //Strip any #options etc. and check that the query consists of a single OUTPUT() action
    while ((savedQuery->getOperator() == no_compound) && isSetMeta(savedQuery->queryChild(0)))
        savedQuery.set(savedQuery->queryChild(1));

    if (savedQuery->getOperator() != no_output)
        throw MakeStringException(0, "#option('GenerateFullFieldUsage') requires the query to be a single OUTPUT()");

    Owned<IPropertyTree> allFields = createPTree("usage");
    IHqlExpression * dataset = savedQuery->queryChild(0);

    //First calculate which files are used if no fields are extracted from the output i.e. shared by all output fields
    //Use COUNT(dataset) as the query to test that
    OwnedHqlExpr countDataset = createValue(no_count, makeIntType(8, false), LINK(dataset));
    Owned<IPropertyTree> countFilesUsed = generateSingleFieldUsageStatistics(countDataset, "__shared__", NULL);
    if (countFilesUsed)
        allFields->addPropTree(countFilesUsed->queryName(), LINK(countFilesUsed));

    //Now project the output dataset down to a single field, and generate the file/field dependencies for each of those
    OwnedHqlExpr selSeq = createUniqueSelectorSequence();
    OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
    RecordSelectIterator iter(dataset->queryRecord(), left, false);
    ForEach(iter)
    {
        IHqlExpression * cur = iter.query();
        IHqlExpression * field = cur->queryChild(1);
        OwnedHqlExpr record = createRecord(field);
        OwnedHqlExpr self = getSelf(record);
        OwnedHqlExpr assign = createAssign(createSelectExpr(LINK(self), LINK(field)), LINK(cur));
        OwnedHqlExpr transform = createValue(no_transform, makeTransformType(record->getType()), LINK(assign), NULL);

        HqlExprArray args;
        args.append(*LINK(dataset));
        args.append(*LINK(transform));
        args.append(*LINK(selSeq));
        OwnedHqlExpr project = createDataset(no_hqlproject,args);
        OwnedHqlExpr projectedOutput = replaceChildDataset(savedQuery, project, 0);

        StringBuffer variety;
        getExprIdentifier(variety, cur);

        //Generate each into a new property tree, and only include fields not shared by all other output fields.
        Owned<IPropertyTree> filesUsed = generateSingleFieldUsageStatistics(projectedOutput, variety.str(), countFilesUsed);
        if (filesUsed)
            allFields->addPropTree(filesUsed->queryName(), LINK(filesUsed));

        //Generate progress so far
        translator.writeFieldUsage(targetDir, allFields, NULL);
    }

    translator.writeFieldUsage(targetDir, allFields, NULL);
    return false;
}

bool HqlDllGenerator::generateCode(HqlQueryContext & query)
{
    noOutput = true;
    {
        // ensure warnings/errors are available before we do the processing...
        addTimeStamp(wu, SSTcompilestage, "compile:generate", StWhenStarted);
        wu->commit();

        cycle_t startCycles = get_cycles_now();
        HqlCppTranslator translator(errs, wuname, code, targetClusterType, ctxCallback);
        processMetaCommands(translator, wu, query, ctxCallback);
        translator.exportWarningMappings();

        if (wu->getDebugValueBool("generateFullFieldUsage", false))
        {
            //Ensure file information is generated.  It only partially works with field information at the moment.
            //(The merging/difference logic would need to improve, but would be relatively simple if it was useful.)
            wu->setDebugValueInt("reportFileUsage", 1, true);
            return generateFullFieldUsageStatistics(translator, query.expr);
        }

        try
        {
            if (!translator.buildCpp(*code, query))
            {
                wu->setState(WUStateCompleted);
                return true;
            }
            translator.generateStatistics(targetDir, NULL);
            translator.finalizeResources();
            translator.expandFunctions(true);
        }
        catch (IError * e)
        {
            if (e->errorCode() != HQLERR_ErrorAlreadyReported)
            {
                StringBuffer s;
                errs->reportError(e->errorCode(), e->errorMessage(s).str(), e->getFilename(), e->getLine(), e->getColumn(), e->getPosition());
            }
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

        if (wu->getDebugValueBool("saveEclTempFiles", false) || wu->getDebugValueBool("saveCppTempFiles", false) || wu->getDebugValueBool("saveCpp", false))
            setSaveGeneratedFiles(true);

        doExpand(translator);
        unsigned __int64 elapsed = cycle_to_nanosec(get_cycles_now() - startCycles);
        updateWorkunitStat(wu, SSTcompilestage, "compile:generate", StTimeElapsed, NULL, elapsed);

        if (wu->getDebugValueBool("addMemoryToWorkunit", true))
        {
            memsize_t peakVm, peakResident;
            getPeakMemUsage(peakVm, peakResident);
            if (peakResident)
                wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTcompilestage, "compile", StSizePeakMemory, NULL, peakResident, 1, 0, StatsMergeReplace);
        }


        wu->commit();
        addWorkUnitAsResource();
    }
    noOutput = false;
    return true;
}

void HqlDllGenerator::addWorkUnitAsResource()
{
    StringBuffer wuXML;
    exportWorkUnitToXML(wu, wuXML, false, false, false);
    code->addCompressResource("WORKUNIT", wuXML.length(), wuXML.str(), NULL, 1000);
}


void HqlDllGenerator::insertStandAloneCode()
{
    BuildCtx ctx(static_cast<HqlCppInstance &>(*code), goAtom);
    ctx.addQuotedFunction("int main(int argc, const char *argv[])");
    ctx.addQuotedLiteral("return start_query(argc, argv);\n");
}


void HqlDllGenerator::doExpand(HqlCppTranslator & translator)
{
    CCycleTimer elapsedTimer;
    addTimeStamp(wu, SSTcompilestage, "compile:write c++", StWhenStarted);

    bool isMultiFile = translator.spanMultipleCppFiles();
    CompilerType targetCompiler = translator.queryOptions().targetCompiler;

    StringBuffer fullname;
    expandCode(fullname, mainTemplate, false, code, isMultiFile, 0, targetCompiler);
    sourceFiles.append(fullname);
    sourceFlags.append(nullptr);
    sourceIsTemp.append(true);
    if (isMultiFile)
    {
        expandCode(fullname, headerTemplate, true, code, true, 0, targetCompiler);
        for (unsigned i= 0; i < translator.getNumExtraCppFiles(); i++)
        {
            expandCode(fullname, childTemplate, false, code, true, i+1, targetCompiler);
            sourceFiles.append(fullname);
            sourceFlags.append(nullptr);
            sourceIsTemp.append(true);
        }
    }

    updateWorkunitStat(wu, SSTcompilestage, "compile:write c++", StTimeElapsed, NULL, elapsedTimer.elapsedNs());
}

bool HqlDllGenerator::abortRequested()
{
    return (wu && wu->aborting());
}

bool HqlDllGenerator::doCompile(ICppCompiler * compiler)
{
    cycle_t startCycles = get_cycles_now();
    addTimeStamp(wu, SSTcompilestage, "compile:compile c++", StWhenStarted);
    ForEachItemIn(i, sourceFiles)
        compiler->addSourceFile(sourceFiles.item(i), sourceFlags.item(i));

    unsigned maxThreads = wu->getDebugValueInt("maxCompileThreads", defaultMaxCompileThreads);
    compiler->setMaxCompileThreads(maxThreads);

    bool debug = wu->getDebugValueBool("debugQuery", false);
    bool debugLibrary = debug;  // should be wu->getDebugValueBool("debugLibrary", IS_DEBUG_BUILD); change for 3.8
    compiler->setDebug(debug);
    compiler->setDebugLibrary(debugLibrary);
    if (!debug)
    {
        int optimizeLevel = wu->getDebugValueInt("optimizeLevel", targetClusterType == RoxieCluster ? 2 : -1);
        if (optimizeLevel != -1)
            compiler->setOptimizeLevel(optimizeLevel);
    }
#ifdef __64BIT__
    // ARMFIX: Map all the uses of this property and make sure
    // they're not used to mean x86_64 (it shouldn't, though)
    bool target64bit = wu->getDebugValueBool("target64bit", true);
#else
    bool target64bit = wu->getDebugValueBool("target64bit", false);
#endif
    compiler->setTargetBitLength(target64bit ? 64 : 32);

    ForEachItemIn(idx, libraries)
        compiler->addLibrary(libraries.item(idx));
    ForEachItemIn(idx2, objects)
        compiler->addObjectFile(objects.item(idx2));

    StringBuffer options;
    StringBufferAdaptor linkOptionAdaptor(options);
    wu->getDebugValue("linkOptions", linkOptionAdaptor);
    compiler->addLinkOption(options.str());
    options.clear();
    StringBufferAdaptor optionAdaptor(options);
    wu->getDebugValue("compileOptions", optionAdaptor);
    compiler->addCompileOption(options.str());

    if (okToAbort)
        compiler->setAbortChecker(this);

    LOG(MCuserInfo,"Compiling %s", wuname);
    bool ok = compiler->compile();
    if(ok)
        LOG(MCuserInfo,"Compiled %s", wuname);
    else
        UERRLOG("Failed to compile %s", wuname);

    bool reportCppWarnings = wu->getDebugValueBool("reportCppWarnings", false);
    IArrayOf<IError> errors;
    compiler->extractErrors(errors);
    ForEachItemIn(iErr, errors)
    {
        IError & cur = errors.item(iErr);
        if (isError(&cur) || reportCppWarnings)
            errs->report(&cur);
    }

    unsigned __int64 elapsed = cycle_to_nanosec(get_cycles_now() - startCycles);
    updateWorkunitStat(wu, SSTcompilestage, "compile:compile c++", StTimeElapsed, NULL, elapsed);

    //Keep the files if there was a compile error.
    if (ok && deleteGenerated)
    {
        StringBuffer temp;
        removeFileTraceIfFail(temp.clear().append(wuname).append(".hpp").str());
        ForEachItemIn(i, sourceFiles)
        {
            if (sourceIsTemp.item(i))
                removeFileTraceIfFail(sourceFiles.item(i));
        }
    }
    ForEachItemIn(j, temporaryDirectories)
    {
        Owned<IFile> tempDir = createIFile(temporaryDirectories.item(j));
        if (tempDir)
            recursiveRemoveDirectory(tempDir);
    }
    return ok;
}

void HqlDllGenerator::flushResources()
{
    if (code)
        code->flushResources(wuname, ctxCallback);
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

    HqlQueryContext query;
    query.expr.set(exprs);
    processMetaCommands(translator, wu, query, ctxCallback);

    return translator.getComplexity(*code, query.expr);
}

offset_t HqlDllGenerator::getGeneratedSize() const
{
    return totalGeneratedSize;
}

extern HQLCPP_API double getECLcomplexity(IHqlExpression * exprs, IErrorReceiver * errs, IWorkUnit *wu, ClusterType targetClusterType)
{
    HqlDllGenerator generator(errs, "unknown", NULL, wu, targetClusterType, NULL, false, false);
    return generator.getECLcomplexity(exprs);
}


extern HQLCPP_API IHqlExprDllGenerator * createDllGenerator(IErrorReceiver * errs, const char *wuname, const char * targetdir, IWorkUnit *wu, ClusterType targetClusterType, ICodegenContextCallback *ctxCallback, bool checkForLocalFileUploads, bool okToAbort)
{
    return new HqlDllGenerator(errs, wuname, targetdir, wu, targetClusterType, ctxCallback, checkForLocalFileUploads, okToAbort);
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
        targetClusterType = ThorLCRCluster;

    SCMStringBuffer targetText;
    wu->getDebugValue("targetClusterType", targetText);
    targetClusterType = getClusterType(targetText.s.str(), targetClusterType);

    if ((targetClusterType != RoxieCluster) && wu->getDebugValueBool("forceFakeThor", false))
        targetClusterType = HThorCluster;

    return targetClusterType;
}

static void processMetaCommands(HqlCppTranslator & translator, IWorkUnit * wu, HqlQueryContext & query, ICodegenContextCallback *ctxCallback)
{
    NewThorStoredReplacer transformer(translator, wu, ctxCallback);

    translator.traceExpression("before process meta commands", query.expr);

    transformer.analyse(query.expr);
    if (transformer.needToTransform())
        query.expr.setown(transformer.transform(query.expr));
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
// make sure CRC is different for different host platform
// shouldn't really matter if cross-compiling working properly,
// but fairly harmless.
#ifdef _ARCH_X86_
    cacheCRC += 1;
#endif
#ifdef _ARCH_X86_64_
    cacheCRC += 2;
#endif
// In theory, ARM and x86 workunits should be totally different, but...
#ifdef _ARCH_ARM32_
    cacheCRC += 3;
#endif
#ifdef _ARCH_ARM64_
    cacheCRC += 4;
#endif
    IExtendedWUInterface *ewu = queryExtendedWU(wu);
    cacheCRC = ewu->calculateHash(cacheCRC);
    wu->setHash(cacheCRC);
}

void recordQueueFilePrefixes(IWorkUnit * wu, IPropertyTree * configuration)
{
    Owned<IPropertyTreeIterator> iter = configuration->getElements("queues");
    ForEach(*iter)
    {
        IPropertyTree & cur = iter->query();
        const char * name = cur.queryProp("@name");
        const char * prefix = cur.queryProp("@prefix");
        if (!prefix)
            prefix = "";
        wu->setApplicationValue("prefix", name, prefix, true);
    }
}
