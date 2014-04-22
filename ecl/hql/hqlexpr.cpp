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
#include "platform.h"
#include "build-config.h"

#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #undef new
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

#include "jlib.hpp"
#include "jmisc.hpp"
#include "jfile.hpp"
#include "jiter.ipp"
#include "jexcept.hpp"
#include "jmutex.hpp"
#include "junicode.hpp"
#include "jutil.hpp"
#include "hql.hpp"
#include "hqlexpr.ipp"
#include "hqlattr.hpp"
#include "hqlgram.hpp"
#include "hqlfold.hpp"
#include "hqlthql.hpp"
#include "hqlpmap.hpp"
#include "hqlerrors.hpp"
#include "hqlerror.hpp"
#include "hqlplugins.hpp"
#include "javahash.tpp"
#include "hqltrans.ipp"
#include "hqlutil.hpp"
#include "hqlvalid.hpp"
#include "hqlmeta.hpp"
#include "workunit.hpp"
#include "hqlrepository.hpp"
#include "hqldesc.hpp"
#include "hqlir.hpp"

//This nearly works - but there are still some examples which have problems - primarily libraries, old parameter syntax, enums and other issues.

//#define ANNOTATE_EXPR_POSITION
#define ANNOTATE_DATASET_POSITION

//#define NEW_VIRTUAL_DATASETS
#define HQL_VERSION_NUMBER 30
//#define ALL_MODULE_ATTRS_VIRTUAL
//#define _REPORT_EXPRESSION_LEAKS

//#define TRACE_THIS
//#define CONSISTENCY_CHECK
//#define GATHER_LINK_STATS
//#define VERIFY_EXPR_INTEGRITY
//#define CHECK_RECORD_CONSISTENCY

// To debug a symbol in the C++ generated code, use SEARCH_NAME*
// and set a breakpoint on debugMatchedName() below
#ifdef _DEBUG
//#define DEBUG_SCOPE
//#define CHECK_RECORD_CONSISTENCY
//#define PARANOID
//#define SEARCH_NAME1   "v1"
//#define SEARCH_NAME2   "v2"
//#define CHECK_SELSEQ_CONSISTENCY
//#define GATHER_COMMON_STATS
#define VERIFY_EXPR_INTEGRITY

#if defined(SEARCH_NAME1) || defined(SEARCH_NAME2)
static void debugMatchedName() {}
#endif

#ifdef DEBUG_TRACK_INSTANCEID
static int checkSeqId(unsigned __int64 seqid, unsigned why)
{
    switch (seqid)
    {
    //Add a case statement here for each expression being tracked.
    case 0:
        break;
    default:
        return 0;
    }

    //Return values are here to allow breakpoints to be set - not because they are useful
    //Add breakpoint on the switch to break on all reasons.
    switch (why)
    {
    case 0:             //Created
        return 1;
    case 1:             // Linked
        return 2;
    case 2:             // Released
        return 3;
    case 3:             // Destroyed
        return 4;
    }
    return 0; // Unknown reason
}

#define CHECK_EXPR_SEQID(x) checkSeqId(seqid, x)

#else
#define CHECK_EXPR_SEQID(x)
#endif
#else
#define CHECK_EXPR_SEQID(x)
#endif

#define STDIO_BUFFSIZE 0x10000     // 64K

class HqlExprCache : public JavaHashTableOf<IHqlExpression>
{
public:
    HqlExprCache() : JavaHashTableOf<IHqlExpression>(false) {}

protected:
    virtual bool matchesFindParam(const void * _element, const void * _key, unsigned fphash) const
    { 
        const IHqlExpression * element = static_cast<const IHqlExpression *>(_element);
        const IHqlExpression * key = static_cast<const IHqlExpression *>(_key);
        return (element->getHash() == fphash) && element->equals(*key);
    }

    virtual unsigned getTableLimit(unsigned max)
    {
        return max/2;
    }
};

static Mutex * transformMutex;
static CriticalSection * transformCS;
static Semaphore * transformSemaphore;
static HqlExprCache *exprCache;
static CriticalSection * nullIntCS;
static CriticalSection * unadornedCS;
static CriticalSection * sourcePathCS;

static ITypeInfo * nullType;
static IValue * blank;
static IHqlExpression * cachedActiveTableExpr;
static IHqlExpression * cachedSelfExpr;
static IHqlExpression * cachedSelfReferenceExpr;
static IHqlExpression * cachedNoBody;
static IHqlExpression * cachedNullRecord;
static IHqlExpression * cachedNullRowRecord;
static IHqlExpression * cachedOne;
static IHqlExpression * cachedLocalAttribute;
static IHqlExpression * constantTrue;
static IHqlExpression * constantFalse;
static IHqlExpression * defaultSelectorSequenceExpr;
static IHqlExpression * newSelectAttrExpr;
static IHqlExpression * recursiveExpr;
static IHqlExpression * processingMarker;
static IHqlExpression * mergePendingMarker;
static IHqlExpression * mergeNoMatchMarker;
static IHqlExpression * nullIntValue[9][2];
static CriticalSection * exprCacheCS;
static CriticalSection * crcCS;
static KeptAtomTable * sourcePaths;

#ifdef GATHER_COMMON_STATS
static unsigned commonUpCount[no_last_pseudoop];
static unsigned commonUpClash[no_last_pseudoop];
static unsigned commonUpAnnCount[annotate_max];
static unsigned commonUpAnnClash[annotate_max];
#endif

#ifdef GATHER_LINK_STATS
static unsigned __int64 numLinks;
static unsigned __int64 numReleases;
static unsigned __int64 numCreateLinks;
static unsigned __int64 numCreateReleases;
static unsigned __int64 numTransformerLinks;
static unsigned __int64 numTransformerReleases;
static unsigned __int64 numSetExtra;
static unsigned __int64 numSetExtraSame;
static unsigned __int64 numSetExtraUnlinked;
static unsigned numLocks;
static unsigned numNestedLocks;
static unsigned maxNestedLocks;
static unsigned numNestedExtra;
static unsigned insideCreate;
#endif

#ifdef _REPORT_EXPRESSION_LEAKS
static char activeSource[256];
void setActiveSource(const char * filename)
{
    if (filename)
    {
        strncpy(activeSource, filename, sizeof(activeSource));
        activeSource[sizeof(activeSource)-1]= 0;
    }
    else
        activeSource[0] = 0;
}
#else
void setActiveSource(const char * filename)
{
}
#endif


MODULE_INIT(INIT_PRIORITY_HQLINTERNAL)
{
    transformMutex = new Mutex;
    transformCS = new CriticalSection;
    transformSemaphore = new Semaphore(NUM_PARALLEL_TRANSFORMS);
    exprCacheCS = new CriticalSection;
    crcCS = new CriticalSection;
    exprCache = new HqlExprCache;
    nullIntCS = new CriticalSection;
    unadornedCS = new CriticalSection;
    sourcePathCS = new CriticalSection;

    nullType = makeNullType();
    sourcePaths = new KeptAtomTable;
    blank = createStringValue("",(unsigned)0);
    cachedActiveTableExpr = createValue(no_activetable);
    cachedSelfExpr = createValue(no_self, makeRowType(NULL));
    cachedSelfReferenceExpr = createValue(no_selfref);
    cachedNullRecord = createRecord()->closeExpr();
    OwnedHqlExpr nonEmptyAttr = createAttribute(_nonEmpty_Atom);
    cachedNullRowRecord = createRecord(nonEmptyAttr);
    cachedOne = createConstant(1);
    cachedLocalAttribute = createAttribute(localAtom);
    constantTrue = createConstant(createBoolValue(true));
    constantFalse = createConstant(createBoolValue(false));
    defaultSelectorSequenceExpr = createAttribute(_selectorSequence_Atom);
    newSelectAttrExpr = createExprAttribute(newAtom);
    recursiveExpr = createAttribute(recursiveAtom);
    processingMarker = createValue(no_processing, makeNullType());
    mergePendingMarker = createValue(no_merge_pending, makeNullType());
    mergeNoMatchMarker = createValue(no_merge_nomatch, makeNullType());
    cachedNoBody = createValue(no_nobody, makeNullType());
    return true;
}
MODULE_EXIT()
{
    for (unsigned i=0; i<=8; i++)
    {
        ::Release(nullIntValue[i][0]);
        ::Release(nullIntValue[i][1]);
    }
    cachedNoBody->Release();
    mergeNoMatchMarker->Release();
    mergePendingMarker->Release();
    processingMarker->Release();
    recursiveExpr->Release();
    newSelectAttrExpr->Release();
    defaultSelectorSequenceExpr->Release();
    constantFalse->Release();
    constantTrue->Release();
    blank->Release();
    cachedLocalAttribute->Release();
    cachedOne->Release();
    cachedActiveTableExpr->Release();
    cachedSelfReferenceExpr->Release();
    cachedSelfExpr->Release();
    cachedNullRowRecord->Release();
    cachedNullRecord->Release();
    nullType->Release();

    ClearTypeCache();

#ifdef _REPORT_EXPRESSION_LEAKS
    if (exprCache->count())
    {
#if 0 // Place debugging code inside here
        JavaHashIteratorOf<IHqlExpression> iter(*exprCache, false);
        ForEach(iter)
        {
            IHqlExpression & ret = iter.query();
        }
#endif
        fprintf(stderr, "%s Hash table contains %d entries\n", activeSource, exprCache->count());
    }
#endif

    ::Release(sourcePaths);
    delete sourcePathCS;
    delete unadornedCS;
    delete nullIntCS;
    exprCache->Release();
    delete exprCacheCS;
    delete crcCS;
    delete transformMutex;
    delete transformCS;
    delete transformSemaphore;
}


#ifdef GATHER_COMMON_STATS
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    printf("op,cnt,clash");
    for (unsigned i=0; i < no_last_pseudoop; i++)
    {
        if (commonUpCount[i])
            printf("%s,%d,%d\n", getOpString((node_operator)i), commonUpCount[i], commonUpClash[i]);
    }
    for (unsigned j=0; j < annotate_max; j++)
    {
        if (commonUpAnnCount[j])
            printf("%d,%d,%d\n", j, commonUpAnnCount[j], commonUpAnnClash[j]);
    }
    fflush(stdout);
}
#endif

#ifdef GATHER_LINK_STATS
static void showStats()
{
    printf("Links = %"I64F"d(%"I64F"d) releases = %"I64F"d(%"I64F"d)\n", numLinks, numTransformerLinks, numReleases, numTransformerReleases);
    printf("Create Links = %"I64F"d releases = %"I64F"d\n", numCreateLinks, numCreateReleases);
    printf("setExtra = %"I64F"d setExtraSame = %"I64F"d setExtraUnlinked = %"I64F"d\n", numSetExtra, numSetExtraSame, numSetExtraUnlinked);
    printf("numLocks = %d nestedLocks = %d maxNested=%d nestedLockExtra = %d\n", numLocks, numNestedLocks, maxNestedLocks, numNestedExtra);
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    showStats();
}
#endif

extern HQL_API void clearCacheCounts()
{
#ifdef GATHER_COMMON_STATS
    _clear(commonUpCount);
    _clear(commonUpClash);
    _clear(commonUpAnnCount);
    _clear(commonUpAnnClash);
#endif
}

static IHqlExpression * doCreateSelectExpr(HqlExprArray & args);

//==============================================================================================================

//createSourcePath actually uses a kept hash table at the moment, because the source filename is stored in
//each attribute in the grammar, and the large number of links/releases have a noticeable effect (5%).
//To release on demand sourcePaths should become an AtomTable, The link in the calls below should be deleted, 
//and sourcePath in ECLlocation needs to become linked.
ISourcePath * createSourcePath(const char *value)
{
    if (!value)
        return NULL;
    CriticalBlock crit(*sourcePathCS);
    return (ISourcePath *)LINK(sourcePaths->addAtom(value));
}

ISourcePath * createSourcePath(size32_t len, const char *value)
{
    if (!value || !len)
        return NULL;
    char * nullTerminated = (char *)alloca(len+1);
    memcpy(nullTerminated, value, len);
    nullTerminated[len] = 0;
    CriticalBlock crit(*sourcePathCS);
    return (ISourcePath*)LINK(sourcePaths->addAtom(nullTerminated));
}

//==============================================================================================================

bool HqlExprArray::containsBody(IHqlExpression & expr)
{
    IHqlExpression * body = expr.queryBody();
    ForEachItem(i)
    {
        if (item(i).queryBody() == body)
            return true;
    }
    return false;
}

IHqlExpression * queryAttribute(IAtom * prop, const HqlExprArray & exprs)
{
    ForEachItemIn(idx, exprs)
    {
        IHqlExpression & cur = (IHqlExpression &)exprs.item(idx);
        if (cur.isAttribute() && (cur.queryName() == prop))
            return &cur;
    }
    return NULL;
}


IHqlExpression * queryAttributeInList(IAtom * search, IHqlExpression * cur)
{
    if (cur)
    {
        switch (cur->getOperator())
        {
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            if (cur->queryName() == search)
                return cur;
            break;
        case no_comma:
            IHqlExpression * match = queryAttributeInList(search, cur->queryChild(0));
            if (match)
                return match;
            return queryAttributeInList(search, cur->queryChild(1));
        }
    }
    return NULL;
}

IHqlExpression * queryOperatorInList(node_operator search, IHqlExpression * cur)
{
    if (cur)
    {
        node_operator op = cur->getOperator();
        if (op == search)
            return cur;
        if (op != no_comma)
            return NULL;

        IHqlExpression * match = queryOperatorInList(search, cur->queryChild(0));
        if (match)
            return match;
        return queryOperatorInList(search, cur->queryChild(1));
    }
    return NULL;
}

extern HQL_API IHqlExpression * queryOperator(node_operator search, const HqlExprArray & args)
{
    ForEachItemIn(i, args)
    {
        IHqlExpression & cur = args.item(i);
        if (cur.getOperator() == search)
            return &cur;
    }
    return NULL;
}


extern HQL_API IHqlExpression * queryAnnotation(IHqlExpression * expr, annotate_kind search)
{
    loop
    {
        annotate_kind kind = expr->getAnnotationKind();
        if (kind == search)
            return expr;
        if (kind == annotate_none)
            return NULL;
        expr = expr->queryBody(true);
    }
}

extern HQL_API IHqlExpression * cloneAnnotationKind(IHqlExpression * donor, IHqlExpression * expr, annotate_kind search)
{
    loop
    {
        annotate_kind kind = donor->getAnnotationKind();
        if (kind == annotate_none)
            return LINK(expr);
        IHqlExpression * donorBody = donor->queryBody(true);
        if (kind == search)
        {
            OwnedHqlExpr mapped = cloneAnnotationKind(donorBody, expr, search);
            return donor->cloneAnnotation(mapped);
        }
        donor = donorBody;
    }
}

extern HQL_API IHqlExpression * cloneInheritedAnnotations(IHqlExpression * donor, IHqlExpression * expr)
{
    loop
    {
        annotate_kind kind = donor->getAnnotationKind();
        if (kind == annotate_none)
            return LINK(expr);
        IHqlExpression * donorBody = donor->queryBody(true);
        switch (kind)
        {
        case annotate_location:
        case annotate_meta:
            {
                OwnedHqlExpr mapped = cloneInheritedAnnotations(donorBody, expr);
                return donor->cloneAnnotation(mapped);
            }
        }
        donor = donorBody;
    }
}

IHqlExpression * cloneMissingAnnotations(IHqlExpression * donor, IHqlExpression * body)
{
    annotate_kind kind = donor->getAnnotationKind();
    if (kind == annotate_none)
        return LINK(body);

    OwnedHqlExpr newbody = cloneMissingAnnotations(donor->queryBody(true), body);
    if (queryAnnotation(newbody, kind))
        return newbody.getClear();
    return donor->cloneAnnotation(newbody);
}

extern HQL_API IHqlExpression * forceCloneSymbol(IHqlExpression * donor, IHqlExpression * expr)
{
    OwnedHqlExpr result = cloneAnnotationKind(donor, expr, annotate_symbol);
    assertex(expr != result);
    return result.getClear();
}

extern HQL_API IHqlExpression * cloneSymbol(IHqlExpression * donor, IIdAtom * newname, IHqlExpression * newbody, IHqlExpression * newfuncdef, HqlExprArray * operands)
{
    assertex(donor->getAnnotationKind() == annotate_symbol);
    IHqlNamedAnnotation * annotation = static_cast<IHqlNamedAnnotation *>(donor->queryAnnotation());
    return annotation->cloneSymbol(newname, newbody, newfuncdef, operands);
}


extern HQL_API IHqlExpression * queryAnnotationAttribute(IAtom * search, IHqlExpression * annotation)
{
    unsigned i=0;
    IHqlExpression * cur;
    while ((cur = annotation->queryAnnotationParameter(i++)) != NULL)
    {
        if (cur->queryName() == search && cur->isAttribute())
            return cur;
    }
    return NULL;
}

extern HQL_API IHqlExpression * queryMetaAttribute(IAtom * search, IHqlExpression * expr)
{
    loop
    {
        annotate_kind kind = expr->getAnnotationKind();
        if (kind == annotate_none)
            return NULL;
        if (kind == annotate_meta)
        {
            IHqlExpression * cur = queryAnnotationAttribute(search, expr);
            if (cur)
                return cur;
        }
        expr = expr->queryBody(true);
    }
}

extern HQL_API void gatherMetaAttributes(HqlExprArray & matches, IAtom * search, IHqlExpression * expr)
{
    loop
    {
        annotate_kind kind = expr->getAnnotationKind();
        if (kind == annotate_none)
            return;
        if (kind == annotate_meta)
        {
            unsigned i=0;
            IHqlExpression * cur;
            while ((cur = expr->queryAnnotationParameter(i++)) != NULL)
            {
                //It's possible we may want to implement this whole function as a member function and allow
                //information to be stored in a non expression format, and only create expressions when requested.
                //may end up less efficient in the end.
                if (cur->queryName() == search && cur->isAttribute())
                    matches.append(*LINK(cur));
            }
        }
        expr = expr->queryBody(true);
    }
}

extern HQL_API void gatherAttributes(HqlExprArray & matches, IAtom * search, IHqlExpression * expr)
{
    unsigned kids = expr->numChildren();
    for (unsigned i = 0; i < kids; i++)
    {
        IHqlExpression *kid = expr->queryChild(i);
        if (kid->isAttribute() && kid->queryName()==search)
            matches.append(*LINK(kid));
    }
}

extern HQL_API IHqlExpression * queryLocation(IHqlExpression * expr)
{
    IHqlExpression * best = NULL;
    loop
    {
        annotate_kind kind = expr->getAnnotationKind();
        if (kind == annotate_none)
            return best;
        if (kind == annotate_location)
            return expr;
        best = expr;
        expr = expr->queryBody(true);
    }
}


extern HQL_API void gatherLocations(HqlExprCopyArray & matches, IHqlExpression * expr)
{
    loop
    {
        annotate_kind kind = expr->getAnnotationKind();
        if (kind == annotate_none)
            return;
        if (kind == annotate_location || kind == annotate_symbol)
            matches.append(*expr);
        expr = expr->queryBody(true);
    }
}


extern HQL_API IHqlExpression * queryFunctionDefaults(IHqlExpression * expr)
{
    return queryFunctionParameterDefaults(expr->queryType());
}


//==============================================================================================================

static bool isSameText(IFileContents * text1, IFileContents * text2)
{
    if (text1 == text2)
        return true;
    if (!text1 || !text2)
        return false;
    unsigned len1 = text1->length();
    if (len1 != text2->length())
        return false;
    return memcmp(text1->getText(), text2->getText(), len1) == 0;
}

void getFileContentText(StringBuffer & result, IFileContents * contents)
{
    unsigned len = contents->length();
    const char * text = contents->getText();
    if ((len >= 3) && (memcmp(text, UTF8_BOM, 3) == 0))
    {
        len -= 3;
        text += 3;
    }
    result.append(len, text);
}

//---------------------------------------------------------------------------------------------------------------------

void HqlParseContext::addForwardReference(IHqlScope * owner, IHasUnlinkedOwnerReference * child)
{
    forwardLinks.append(*new ForwardScopeItem(owner, child));
}

IPropertyTree * HqlParseContext::queryEnsureArchiveModule(const char * name, IHqlScope * scope)
{
    return ::queryEnsureArchiveModule(archive, name, scope);
}

void HqlParseContext::setGatherMeta(const MetaOptions & options)
{
    metaOptions = options;
    metaState.gatherNow = !options.onlyGatherRoot;
    metaTree.setown(createPTree("Meta"));
}


static void setDefinitionText(IPropertyTree * target, const char * prop, IFileContents * contents)
{
    StringBuffer sillyTempBuffer;
    getFileContentText(sillyTempBuffer, contents);  // We can't rely on IFileContents->getText() being null terminated..
    target->setProp(prop, sillyTempBuffer);

    ISourcePath * sourcePath = contents->querySourcePath();
    target->setProp("@sourcePath", sourcePath->str());
}

void HqlParseContext::noteBeginAttribute(IHqlScope * scope, IFileContents * contents, IIdAtom * name)
{
    if (queryArchive())
    {
        const char * moduleName = scope->queryFullName();

        IPropertyTree * module = queryEnsureArchiveModule(moduleName, scope);
        IPropertyTree * attr = queryArchiveAttribute(module, name->str());
        if (!attr)
            attr = createArchiveAttribute(module, name->str());

        setDefinitionText(attr, "", contents);
    }

    if (checkBeginMeta())
    {
        ISourcePath * sourcePath = contents->querySourcePath();

        IPropertyTree * attr = metaTree->addPropTree("Source", createPTree("Source"));
        setFullNameProp(attr, "@name", scope->queryFullName(), name->str());
        attr->setProp("@sourcePath", sourcePath->str());
        metaState.nesting.append(attr);
    }

    if (globalDependTree)
    {
        IPropertyTree * attr = globalDependTree->addPropTree("Attribute", createPTree("Attribute"));
        attr->setProp("@module", scope->queryFullName());
        attr->setProp("@name", name->str());
        //attr->setPropInt("@flags", symbol->getObType());  MORE
    }
}

void HqlParseContext::noteBeginQuery(IHqlScope * scope, IFileContents * contents)
{
    if (queryArchive())
    {
        const char * moduleName = scope->queryFullName();
        if (moduleName && *moduleName)
        {
            IPropertyTree * module = queryEnsureArchiveModule(moduleName, scope);
            setDefinitionText(module, "Text", contents);
        }
    }

    if (checkBeginMeta())
    {
        ISourcePath * sourcePath = contents->querySourcePath();

        IPropertyTree * attr = metaTree->addPropTree("Query", createPTree("Query"));
        attr->setProp("@sourcePath", sourcePath->str());
        metaState.nesting.append(attr);
    }
}

void HqlParseContext::noteBeginModule(IHqlScope * scope, IFileContents * contents)
{
    if (queryArchive())
    {
        const char * moduleName = scope->queryFullName();
        if (moduleName && *moduleName)
        {
            IPropertyTree * module = queryEnsureArchiveModule(moduleName, scope);
            setDefinitionText(module, "Text", contents);
        }
    }

    if (checkBeginMeta())
    {
        ISourcePath * sourcePath = contents->querySourcePath();

        IPropertyTree * attr = metaTree->addPropTree("Source", createPTree("Source"));
        attr->setProp("@sourcePath", sourcePath->str());
        metaState.nesting.append(attr);
    }
}

void HqlParseContext::noteEndAttribute()
{
    if (checkEndMeta())
        finishMeta();
}

void HqlParseContext::noteEndQuery()
{
    if (checkEndMeta())
        finishMeta();
}

void HqlParseContext::noteEndModule()
{
    if (checkEndMeta())
        finishMeta();
}

void HqlParseContext::noteFinishedParse(IHqlScope * scope)
{
    if (metaState.gatherNow)
        expandScopeSymbolsMeta(metaState.nesting.tos(), scope);
}


void HqlParseContext::notePrivateSymbols(IHqlScope * scope)
{
    if (metaState.gatherNow)
        expandScopeSymbolsMeta(metaState.nesting.tos(), scope);
}


bool HqlParseContext::checkBeginMeta()
{
    if (!metaTree)
        return false;
    if (!metaOptions.onlyGatherRoot)
        return true;
    metaState.gatherNow = (metaState.nesting.ordinality() == 0);
    return metaState.gatherNow;
}

bool HqlParseContext::checkEndMeta()
{
    if (!metaTree)
        return false;
    if (!metaOptions.onlyGatherRoot)
        return true;
    bool wasGathering = metaState.gatherNow;
    metaState.gatherNow = (metaState.nesting.ordinality() == 2);
    return wasGathering;
}

void HqlParseContext::finishMeta()
{
    metaState.nesting.pop();
}

//---------------------------------------------------------------------------------------------------------------------


extern HQL_API IPropertyTree * createAttributeArchive()
{
    Owned<IPropertyTree> archive = createPTree("Archive");
    archive->setProp("@build", BUILD_TAG);
    archive->setProp("@eclVersion", LANGUAGE_VERSION);
    return archive.getClear();
}

IPropertyTree * queryEnsureArchiveModule(IPropertyTree * archive, const char * name, IHqlScope * scope)
{
    //MORE: Move this into a member of the parse context to also handle dependencies.
    StringBuffer lowerName;
    lowerName.append(name).toLowerCase();

    StringBuffer xpath,s;
    xpath.append("Module[@key=\"").append(lowerName).append("\"]");
    IPropertyTree * module = archive->queryPropTree(xpath);
    if (!module)
    {
        module = archive->addPropTree("Module", createPTree());
        module->setProp("@name", name ? name : "");
        module->setProp("@key", lowerName);
        if (scope)
        {
            unsigned flagsToSave = (scope->getPropInt(flagsAtom, 0) & PLUGIN_SAVEMASK);
            if (flagsToSave)
                module->setPropInt("@flags", flagsToSave);
            scope->getProp(pluginAtom, s.clear());
            if (s.length())
            {
                module->setProp("@fullname", s.str());

                StringBuffer pluginName(s.str());
                getFileNameOnly(pluginName, false);
                module->setProp("@plugin", pluginName.str());
            }
            scope->getProp(versionAtom, s.clear());
            if (s.length())
                module->setProp("@version", s.str());
        }
    }
    return module;
}

extern HQL_API IPropertyTree * queryArchiveAttribute(IPropertyTree * module, const char * name)
{
    StringBuffer lowerName, xpath;
    lowerName.append(name).toLowerCase();
    xpath.append("Attribute[@key=\"").append(lowerName).append("\"]");
    return module->queryPropTree(xpath);
}

extern HQL_API IPropertyTree * createArchiveAttribute(IPropertyTree * module, const char * name)
{
    StringBuffer lowerName;
    lowerName.append(name).toLowerCase();
    IPropertyTree * attr = module->addPropTree("Attribute", createPTree());
    attr->setProp("@name", name);
    attr->setProp("@key", lowerName);
    return attr;
}

//---------------------------------------------------------------------------------------------------------------------

void HqlLookupContext::noteBeginAttribute(IHqlScope * scope, IFileContents * contents, IIdAtom * name)
{
    if (queryNestedDependTree())
        createDependencyEntry(scope, name);

    parseCtx.noteBeginAttribute(scope, contents, name);
}


void HqlLookupContext::noteBeginQuery(IHqlScope * scope, IFileContents * contents)
{
    parseCtx.noteBeginQuery(scope, contents);
}


void HqlLookupContext::noteBeginModule(IHqlScope * scope, IFileContents * contents)
{
    parseCtx.noteBeginModule(scope, contents);
}


void HqlLookupContext::noteExternalLookup(IHqlScope * parentScope, IHqlExpression * expr)
{
    if (queryArchive())
    {
        node_operator op = expr->getOperator();
        if ((op == no_remotescope) || (op == no_mergedscope))
        {
            //Ensure the archive contains entries for each module - even if nothing is accessed from it
            //It would be preferrable to only check once, but adds very little time anyway.
            IHqlScope * resolvedScope = expr->queryScope();
            parseCtx.queryEnsureArchiveModule(resolvedScope->queryFullName(), resolvedScope);
        }
    }

    if (curAttrTree && !dependents.contains(*expr))
    {
        node_operator op = expr->getOperator();
        if ((op != no_remotescope) && (op != no_mergedscope))
        {
            dependents.append(*expr);

            const char * moduleName = parentScope->queryFullName();
            if (moduleName)
            {
                IPropertyTree * depend = curAttrTree->addPropTree("Depend", createPTree());
                depend->setProp("@module", moduleName);
                depend->setProp("@name", expr->queryName()->str());
            }
        }
    }
}


void HqlLookupContext::createDependencyEntry(IHqlScope * parentScope, IIdAtom * id)
{
    const char * moduleName = parentScope->queryFullName();
    const char * nameText = id->lower()->str();

    StringBuffer xpath;
    xpath.append("Attr[@module=\"").append(moduleName).append("\"][@name=\"").append(nameText).append("\"]");

    IPropertyTree * attr = queryNestedDependTree()->queryPropTree(xpath.str());
    if (!attr)
    {
        attr = queryNestedDependTree()->addPropTree("Attr", createPTree());
        attr->setProp("@module", moduleName);
        attr->setProp("@name", nameText);
    }
    curAttrTree.set(attr);
}

//---------------------------------------------------------------------------------------------------------------------

const char *getOpString(node_operator op)
{
    switch(op)
    {
    case no_band: return "&";
    case no_bor: return "|";
    case no_bxor: return " BXOR ";
    case no_mul: return "*";
    case no_div: return "/";
    case no_modulus: return "%";
    case no_exp: return "EXP";
    case no_round: return "ROUND";
    case no_roundup: return "ROUNDUP";
    case no_truncate: return "ROUNDDOWN";
    case no_power: return "POWER";
    case no_ln: return "LN";
    case no_sin: return "SIN";
    case no_cos: return "COS";
    case no_tan: return "TAN";  
    case no_asin: return "ASIN";
    case no_acos: return "ACOS";
    case no_atan: return "ATAN";
    case no_atan2: return "ATAN2";
    case no_sinh: return "SINH";
    case no_cosh: return "COSH";
    case no_tanh: return "TANH";
    case no_log10: return "LOG";
    case no_sqrt: return "SQRT";
    case no_negate: return "-";
    case no_sub: return " - ";
    case no_add: return " + ";
    case no_addfiles: return " + ";
    case no_merge: return "MERGE";
    case no_concat: return " | ";
    case no_eq: return "=";
    case no_ne: return "<>";
    case no_lt: return "<";
    case no_le: return "<=";
    case no_gt: return ">";
    case no_ge: return ">=";
    case no_order: return "<=>";
    case no_unicodeorder: return "UNICODEORDER";
    case no_not: return "NOT ";
    case no_and: return " AND ";
    case no_or: return " OR ";
    case no_xor: return " XOR ";
    case no_notin: return " NOT IN ";
    case no_in: return " IN ";
    case no_indict: return " IN ";
    case no_notbetween: return " NOT BETWEEN ";
    case no_between: return " BETWEEN ";
    case no_comma: return ",";
    case no_compound: return ",";
    case no_count: case no_countlist: case no_countdict: return "COUNT";
    case no_counter: return "COUNTER";
    case no_countgroup: return "COUNT";
    case no_distribution: return "DISTRIBUTION";
    case no_max: case no_maxgroup: case no_maxlist: return "MAX";
    case no_min: case no_mingroup: case no_minlist: return "MIN";
    case no_sum: case no_sumgroup: case no_sumlist: return "SUM";
    case no_ave: case no_avegroup: return "AVG";
    case no_variance: case no_vargroup: return "VARIANCE";
    case no_covariance: case no_covargroup: return "COVARIANCE";
    case no_correlation: case no_corrgroup: return "CORRELATION";
    case no_map: return "MAP";
    case no_if: return "IF";
    case no_mapto: return "=>";
    case no_constant: return "<constant>";
    case no_field: return "<field>";
    case no_exists: case no_existslist: case no_existsdict: return "EXISTS";
    case no_existsgroup: return "EXISTS";
    case no_select: return ".";
    case no_table: return "DATASET";
    case no_temptable: return "<temptable>"; 
    case no_workunit_dataset: return "<workunit_dataset>"; 
    case no_scope: return "MODULE";
    case no_remotescope: return "<scope>";
    case no_mergedscope: return "<scope>";
    case no_privatescope: return "<private_scope>";
    case no_list: return "<list>";
    case no_selectmap: return "SELECT_MAP";
    case no_selectnth: return "SELECT_NTH";
    case no_filter: return "FILTER";
    case no_param: return "<parameter>";
    case no_within: return "WITHIN ";
    case no_notwithin: return "NOT WITHIN ";

    case no_index: return "<index>";
    case no_all: return "ALL";
    case no_left: return "LEFT";
    case no_right: return "RIGHT";
    case no_outofline: return "OUTOFLINE";
    case no_dedup: return "DEDUP";
    case no_enth: return "ENTH";
    case no_sample: return "SAMPLE";
    case no_sort: return "SORT";
    case no_subsort: return "SUBSORT";
    case no_sorted: return "SORTED";
    case no_choosen: return "CHOOSEN";
    case no_choosesets: return "CHOOSESETS";
    case no_buildindex: return "BUILDINDEX";
    case no_output: return "OUTPUT";
    case no_record: return "RECORD";
    case no_fetch: return "FETCH";
    case no_compound_fetch: return "compoundfetch";
    case no_join: return "JOIN";
    case no_selfjoin: return "JOIN";
    case no_newusertable: return "NEWTABLE";
    case no_usertable: return "TABLE";
    case no_aggregate: return "AGGREGATE";
    case no_which: return "WHICH";
    case no_case: return "CASE";
    case no_choose: return "CHOOSE";
    case no_rejected: return "REJECTED";
    case no_evaluate: return "EVALUATE";
    case no_cast: return "CAST";
    case no_implicitcast: return "<implicit-cast>";
    case no_external: return "<external>"; 
    case no_externalcall: return "<external call>";
    case no_macro: return "<macro>";
    case no_failure: return "FAILURE";
    case no_success: return "SUCCESS";
    case no_recovery: return "RECOVERY";

    case no_sql: return "SQL";
    case no_flat: return "FLAT";
    case no_csv: return "CSV";
    case no_xml: return "XML";

    case no_when: return "WHEN";
    case no_priority: return "PRIORITY";
    case no_rollup: return "ROLLUP";
    case no_iterate: return "ITERATE";
    case no_assign: return ":=";
    case no_asstring: return "ASSTRING";
    case no_assignall: return "ASSIGNALL";

    case no_update: return "update";

    case no_alias: return "alias";
    case no_denormalize: return "DENORMALIZE";
    case no_denormalizegroup: return "DENORMALIZE";
    case no_normalize: return "NORMALIZE";
    case no_group: return "GROUP";
    case no_grouped: return "GROUPED";

    case no_unknown: return "unknown";
    case no_any: return "any";
    case no_is_null: return "ISNULL";
    case no_is_valid: return "ISVALID";

    case no_abs: return "ABS";
    case no_substring: return "substring";
    case no_newaggregate: return "TABLE";
    case no_trim: return "trim";
    case no_realformat: return "REALFORMAT";
    case no_intformat: return "INTFORMAT";
    case no_regex_find: return "REGEXFIND";
    case no_regex_replace: return "REGEXREPLACE";

    case no_current_date: return "current_date";
    case no_current_time: return "current_time";
    case no_current_timestamp: return "current time stamp";
    case no_cogroup: return "COGROUP";
    case no_cosort: return "COSORT";
    case no_sortlist: return "SORTBY";
    case no_recordlist: return "[]";
    case no_transformlist: return "[]";

    case no_transformebcdic: return "EBCDIC";
    case no_transformascii: return "ASCII";
    case no_hqlproject: return "PROJECT";
    case no_dataset_from_transform: return "DATASET";
    case no_newtransform: return "NEWTRANSFORM";
    case no_transform: return "TRANSFORM";
    case no_attr: return "no_attr";
    case no_attr_expr: return "no_attr_expr";
    case no_attr_link: return "no_attr_link";
    case no_self: return "SELF";
    case no_selfref: return "SELF";
    case no_thor: return "THOR";
    case no_distribute: return "DISTRIBUTE";
    case no_distributed: return "DISTRIBUTED";
    case no_keyeddistribute: return "DISTRIBUTE";

    case no_rank: return "RANK";
    case no_ranked: return "RANKED";
    case no_ordered: return "no_ordered";
    case no_hash: return "HASH";
    case no_hash32: return "HASH32";
    case no_hash64: return "HASH64";
    case no_hashmd5: return "HASHMD5";

    case no_none: return "no_none";
    case no_notnot: return "no_notnot";
    case no_range: return "no_range";
    case no_rangeto: return "no_rangeto";
    case no_rangefrom: return "no_rangefrom";
    case no_service: return "no_service";
    case no_mix: return "no_mix";
    case no_funcdef: return "no_funcdef";
    case no_wait: return "no_wait";
    case no_notify: return "no_notify";
    case no_event: return "no_event";
    case no_persist: return "PERSIST";
    case no_omitted: return "no_omitted";
    case no_setconditioncode: return "no_setconditioncode";
    case no_selectfields: return "no_selectfields";
    case no_quoted: return "no_quoted";
    case no_variable: return "no_variable";
    case no_bnot: return "no_bnot";
    case no_charlen: return "LENGTH";
    case no_sizeof: return "SIZEOF";
    case no_offsetof: return "OFFSETOF";
    case no_postinc: return "no_postinc";
    case no_postdec: return "no_postdec";
    case no_preinc: return "no_preinc";
    case no_predec: return "no_predec";
    case no_pselect: return "no_pselect";
    case no_address: return "no_address";
    case no_deref: return "no_deref";
    case no_nullptr: return "NULL";
    case no_decimalstack: return "no_decimalstack";
    case no_typetransfer: return "TRANSFER";
    case no_apply: return "APPLY";
    case no_pipe: return "PIPE";
    case no_cloned: return "no_cloned";
    case no_cachealias: return "no_cachealias";
    case no_joined: return "JOINED";
    case no_lshift: return "<<";
    case no_rshift: return ">>";
    case no_colon: return ":";
    case no_global: return "GLOBAL";
    case no_stored: return "STORED";
    case no_checkpoint: return "checkpoint";
    case no_compound_indexread: return "compound_indexread";
    case no_compound_diskread: return "compound_diskread";
    case no_translated: return "no_translated";
    case no_ifblock: return "IFBLOCK";
    case no_crc: return "HASHCRC";
    case no_random: return "RANDOM";
    case no_childdataset: return "no_childdataset";
    case no_envsymbol: return "no_envsymbol";
    case no_null: return "[]";
    case no_ensureresult: return "ensureresult";
    case no_getresult: return "getresult";
    case no_setresult: return "setresult";
    case no_extractresult: return "extractresult";

    case no_type: return "TYPE";
    case no_position: return "no_position";
    case no_bound_func: return "no_bound_func";
    case no_bound_type: return "no_bound_type";
    case no_hint: return "no_hint";
    case no_metaactivity: return "no_metaactivity";
    case no_loadxml: return "no_loadxml";
    case no_fieldmap: return "no_fieldmap";
    case no_template_context: return "no_template_context";
    case no_nofold: return "NOFOLD";
    case no_nohoist: return "NOHOIST";
    case no_fail: return "FAIL";
    case no_filepos: return "no_filepos";
    case no_file_logicalname: return "no_file_logicalname";
    case no_alias_project: return "no_alias_project";
    case no_alias_scope: return "no_alias_scope";   
    case no_sequential: return "SEQUENTIAL";
    case no_parallel: return "PARALLEL";
    case no_actionlist: return "no_actionlist";
    case no_nolink: return "no_link";
    case no_workflow: return "no_workflow";
    case no_workflow_action: return "no_workflow_action";
    case no_failcode: return "FAILCODE";
    case no_failmessage: return "FAILMESSAGE";
    case no_eventname: return "EVENTNAME";
    case no_eventextra: return "EVENTEXTRA";
    case no_independent: return "INDEPENDENT";
    case no_keyindex: return "INDEX";
    case no_newkeyindex: return "INDEX";
    case no_keyed: return "KEYED";
    case no_split: return "SPLIT";
    case no_subgraph: return "no_subgraph";
    case no_dependenton: return "no_dependenton";
    case no_spill: return "SPILL";
    case no_setmeta: return "no_setmeta";
    case no_throughaggregate: return "no_throughaggregate";
    case no_joincount: return "JOINCOUNT";
    case no_countcompare: return "no_countcompare";
    case no_limit: return "LIMIT";

    case no_fromunicode: return "FROMUNICODE";
    case no_tounicode: return "TOUNICODE";
    case no_keyunicode: return "KEYUNICODE";
    case no_parse: return "PARSE";
    case no_newparse: return "PARSE";
    case no_skip: return "SKIP";
    case no_matched: return "MATCHED";
    case no_matchtext: return "MATCHTEXT";
    case no_matchlength: return "MATCHLENGTH";
    case no_matchposition: return "MATCHPOSITION";
    case no_matchunicode: return "MATCHUNICODE";
    case no_matchrow: return "MATCHROW";
    case no_matchutf8: return "MATCHUTF8";
    case no_pat_select: return "/";
    case no_pat_index: return "[]";
    case no_pat_const: return "no_pat_const";
    case no_pat_pattern: return "PATTERN";
    case no_pat_follow: return "no_pat_follow";
    case no_pat_first: return "FIRST";
    case no_pat_last: return "LAST";
    case no_pat_repeat: return "REPEAT";
    case no_pat_instance: return "no_pat_instance";
    case no_pat_anychar: return "ANY";
    case no_pat_token: return "TOKEN";
    case no_pat_imptoken: return "no_imp_token";
    case no_pat_set: return "no_pat_set";
    case no_pat_checkin: return "IN";
    case no_pat_x_before_y: return "BEFORE";
    case no_pat_x_after_y: return "AFTER";
    case no_pat_before_y: return "ASSERT BEFORE";
    case no_pat_after_y: return "ASSERT AFTER";
    case no_pat_beginpattern: return "no_pat_beginpattern";
    case no_pat_endpattern: return "no_pat_endpattern";
    case no_pat_checklength: return "LENGTH";
    case no_pat_use: return "USE";
    case no_pat_validate: return "VALIDATE";
    case no_topn: return "TOPN";
    case no_outputscalar: return "OUTPUT";
    case no_penalty: return "PENALTY";
    case no_rowdiff: return "ROWDIFF";
    case no_wuid: return "WUID";
    case no_featuretype: return "no_featuretype";
    case no_pat_guard: return "GUARD";
    case no_xmltext: return "XMLTEXT";
    case no_xmlunicode: return "XMLUNICODE";
    case no_xmlproject: return "XMLPROJECT";
    case no_newxmlparse: return "PARSE";
    case no_xmlparse: return "PARSE";
    case no_xmldecode: return "XMLDECODE";
    case no_xmlencode: return "XMLENCODE";
    case no_pat_featureparam: return "no_pat_featureparam";
    case no_pat_featureactual: return "no_pat_featureactual";
    case no_pat_featuredef: return "no_pat_featuredef";
    case no_evalonce: return "no_evalonce";
    case no_distributer: return "DISTRIBUTE";
    case no_impure: return "<impure>";
    case no_addsets: return "+";
    case no_rowvalue: return "no_rowvalue";
    case no_pat_case: return "CASE";
    case no_pat_nocase: return "NOCASE";
    case no_evaluate_stmt: return "EVALUATE";
    case no_return_stmt: return "RETURN";
    case no_activetable: return "<Active>";
    case no_preload: return "PRELOAD";
    case no_createset: return "SET";
    case no_assertkeyed: return "KEYED";
    case no_assertwild: return "WILD";
    case no_httpcall: return "HTTPCALL";
    case no_soapcall: return "SOAPCALL";
    case no_soapcall_ds: return "SOAPCALL";
    case no_newsoapcall: return "SOAPCALL";
    case no_newsoapcall_ds: return "SOAPCALL";
    case no_soapaction_ds: return "SOAPCALL";
    case no_newsoapaction_ds: return "SOAPCALL";
    case no_temprow: return "ROW"; 
    case no_projectrow: return "ROW"; 
    case no_createrow: return "ROW"; 
    case no_activerow: return "<ActiveRow>";
    case no_newrow: return "<NewRow>";
    case no_catch: return "CATCH";
    case no_reference: return "no_reference";
    case no_callback: return "no_callback";
    case no_keyedlimit: return "LIMIT";
    case no_keydiff: return "KEYDIFF";
    case no_keypatch: return "KEYPATCH";
    case no_returnresult: return "no_returnresult";
    case no_id2blob: return "no_id2blob";
    case no_blob2id: return "no_blob2id";
    case no_anon: return "no_anon";
    case no_embedbody: return "no_embedbody";
    case no_sortpartition: return "no_sortpartition";
    case no_define: return "DEFINE";
    case no_globalscope: return "GLOBAL";
    case no_forcelocal: return "LOCAL";
    case no_typedef: return "typedef";
    case no_matchattr: return "no_matchattr";
    case no_pat_production: return "no_pat_production";
    case no_guard: return "no_guard";
    case no_datasetfromrow: return "DATASET"; 
    case no_assertconstant: return "no_assertconstant";
    case no_clustersize: return "no_clustersize";
    case no_compound_disknormalize: return "no_compound_disknormalize";
    case no_compound_diskaggregate: return "no_compound_diskaggregate";
    case no_compound_diskcount: return "no_compound_diskcount";
    case no_compound_diskgroupaggregate: return "no_compound_diskgroupaggregate";
    case no_compound_indexnormalize: return "no_compound_indexnormalize";
    case no_compound_indexaggregate: return "no_compound_indexaggregate";
    case no_compound_indexcount: return "no_compound_indexcount";
    case no_compound_indexgroupaggregate: return "no_compound_indexgroupaggregate";
    case no_compound_childread: return "no_compound_childread";
    case no_compound_childnormalize: return "no_compound_childnormalize";
    case no_compound_childaggregate: return "no_compound_childaggregate";
    case no_compound_childcount: return "no_compound_childcount";
    case no_compound_childgroupaggregate: return "no_compound_childgroupaggregate";
    case no_compound_selectnew: return "no_compound_selectnew";
    case no_compound_inline: return "no_compound_inline";
    case no_setworkflow_cond: return "no_setworkflow_cond";
    case no_nothor: return "NOTHOR";
    case no_call: return "no_call";
    case no_getgraphresult: return "GetGraphResult";
    case no_setgraphresult: return "SetGraphResult";
    case no_assert: return "ASSERT";
    case no_assert_ds: return "ASSERT";
    case no_namedactual: return "no_namedactual";
    case no_combine: return "COMBINE";
    case no_combinegroup: return "COMBINE";
    case no_rows: return "ROWS";
    case no_rollupgroup: return "ROLLUP";
    case no_regroup: return "REGGROUP";
    case no_inlinetable: return "DATASET";
    case no_spillgraphresult: return "spillgraphresult";
    case no_enum: return "ENUM";
    case no_pat_or: return "|";
    case no_loop: return "LOOP";
    case no_loopbody: return "no_loopbody";
    case no_cluster: return "CLUSTER";
    case no_forcenolocal: return "NOLOCAL";
    case no_allnodes: return "ALLNODES";
    case no_last_op: return "no_last_op";
    case no_pat_compound: return "no_pat_compound";
    case no_pat_begintoken: return "no_pat_begintoken";
    case no_pat_endtoken: return "no_pat_endtoken";
    case no_pat_begincheck: return "no_pat_begincheck";
    case no_pat_endcheckin: return "no_pat_endcheckin";
    case no_pat_endchecklength: return "no_pat_endchecklength";
    case no_pat_beginseparator: return "no_pat_beginseparator";
    case no_pat_endseparator: return "no_pat_endseparator";
    case no_pat_separator: return "no_pat_separator";
    case no_pat_beginvalidate: return "no_pat_beginvalidate";
    case no_pat_endvalidate: return "no_pat_endvalidate";
    case no_pat_dfa: return "no_pat_dfa";
    case no_pat_singlechar: return "no_pat_singlechar";
    case no_pat_beginrecursive: return "no_pat_beginrecursive";
    case no_pat_endrecursive: return "no_pat_endrecursive";
    case no_pat_utf8single: return "no_pat_utf8single";
    case no_pat_utf8lead: return "no_pat_utf8lead";
    case no_pat_utf8follow: return "no_pat_utf8follow";
    case no_sequence: return "__SEQUENCE__";
    case no_forwardscope: return "MODULE";
    case no_virtualscope: return "MODULE";
    case no_concretescope: return "MODULE";
    case no_purevirtual: return "__PURE__";
    case no_internalselect: return "no_internalselect";
    case no_delayedselect: return "no_delayedselect";
//  case no_func: return "no_func";
    case no_libraryselect: return "no_libraryselect";
    case no_libraryscope: return "MODULE";
    case no_libraryscopeinstance: return "MODULE";
    case no_libraryinput: return "LibraryInput";
    case no_process: return "PROCESS";
    case no_thisnode: return "THISNODE";
    case no_graphloop: return "GRAPH";
    case no_rowset: return "ROWSET";
    case no_loopcounter: return "COUNTER";
    case no_getgraphloopresult: return "GraphLoopResult";
    case no_setgraphloopresult: return "ReturnGraphLoopResult";
    case no_rowsetindex: return "no_rowsetindex";
    case no_rowsetrange: return "RANGE";
    case no_assertstepped: return "STEPPED";
    case no_assertsorted: return "SORTED";
    case no_assertgrouped: return "GROUPED";
    case no_assertdistributed: return "DISTRIBUTED";
    case no_datasetlist: return "<dataset-list>";
    case no_mergejoin: return "MERGEJOIN";
    case no_nwayjoin: return "JOIN";
    case no_nwaymerge: return "MERGE";
    case no_stepped: return "STEPPED";
    case no_getgraphloopresultset: return "N-way GraphLoopResult";
    case no_attrname: return "name";
    case no_nonempty: return "NONEMPTY";
    case no_filtergroup: return "HAVING";
    case no_rangecommon: return "no_rangecommon";
    case no_section: return "SECTION";
    case no_nobody: return "NOBODY";
    case no_deserialize: return "no_deserialize";
    case no_serialize: return "no_serialize";
    case no_eclcrc: return "ECLCRC";
    case no_pure: return "<pure>";
    case no_pseudods: return "<pseudods>";
    case no_top: return "TOP";
    case no_uncommoned_comma: return ",";
    case no_nameof: return "__NAMEOF__";
    case no_processing: return "no_processing";
    case no_merge_pending: return "no_merge_pending";
    case no_merge_nomatch: return "no_merge_nomatch";
    case no_toxml: return "TOXML";
    case no_catchds: return "CATCH";
    case no_readspill: return "no_readspill";
    case no_writespill: return "no_writespill";
    case no_commonspill: return "no_commonspill";
    case no_forcegraph: return "GRAPH";
    case no_sectioninput: return "no_sectioninput";
    case no_related: return "no_related";
    case no_definesideeffect: return "no_definessideeffect";
    case no_executewhen: return "WHEN";
    case no_callsideeffect: return "no_callsideeffect";
    case no_fromxml: return "FROMXML";
    case no_preservemeta: return "order-tracking";
    case no_normalizegroup: return "NORMALIZE";
    case no_indirect: return "no_indirect";
    case no_selectindirect: return ".<>";
    case no_isomitted: return "no_isomitted";
    case no_getenv: return "GETENV";
    case no_once: return "ONCE";
    case no_persist_check: return "no_persist_check";
    case no_create_initializer: return "no_create_initializer";
    case no_owned_ds: return "no_owned_ds";
    case no_complex: return ",";
    case no_assign_addfiles: return "+=";
    case no_debug_option_value: return "__DEBUG__";
    case no_dataset_alias: return "TABLE";
    case no_childquery: return "no_childquery";
    case no_createdictionary: return "DICTIONARY";
    case no_chooseds: return "CHOOSE";
    case no_datasetfromdictionary: return "DATASET";
    case no_delayedscope: return "no_delayedscope";
    case no_assertconcrete: return "no_assertconcrete";
    case no_unboundselect: return "no_unboundselect";
    case no_id: return "no_id";
    case no_orderedactionlist: return "ORDERED";

    case no_unused6:
    case no_unused13: case no_unused14: case no_unused15:
    case no_unused25: case no_unused28: case no_unused29:
    case no_unused30: case no_unused31: case no_unused32: case no_unused33: case no_unused34: case no_unused35: case no_unused36: case no_unused37: case no_unused38:
    case no_unused40: case no_unused41: case no_unused42: case no_unused43: case no_unused44: case no_unused45: case no_unused46: case no_unused47: case no_unused48: case no_unused49:
    case no_unused50: case no_unused52:
    case no_unused80:
    case no_unused81:
    case no_unused83:
    case no_unused101:
    case no_unused102:
        return "unused";
    /* if fail, use "hqltest -internal" to find out why. */
    default: assertex(false); return "???";
    }
}


node_operator getReverseOp(node_operator op)
{
    switch (op)
    {
    case no_lt: return no_gt;
    case no_gt: return no_lt;
    case no_le: return no_ge;
    case no_ge: return no_le;
    case no_eq:
    case no_ne:
        return op;
    default:
        assertex(!"Should not be called");
    }
    return op;
}



node_operator getInverseOp(node_operator op)
{
    switch (op)
    {
    case no_not: return no_notnot;
    case no_notnot: return no_not;
    case no_eq: return no_ne;
    case no_ne: return no_eq;
    case no_lt: return no_ge;
    case no_le: return no_gt;
    case no_gt: return no_le;
    case no_ge: return no_lt;
    case no_notin: return no_in;
    case no_in: return no_notin;
    case no_notbetween: return no_between;
    case no_between: return no_notbetween;
//  case no_notwithin: return no_within;
//  case no_within: return no_notwithin;
    default:
        return no_none;
    }
}

bool checkConstant(node_operator op)
{
    switch (op)
    {
    case no_field:
    case no_externalcall:                   // not constant because we may not be able to load the dll + fold it.
    case no_external:
    case no_select:
    case no_stored:
    case no_persist:
    case no_checkpoint:
    case no_once:
    case no_getresult:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_variable:
    case no_httpcall:
    case no_soapcall:
    case no_soapcall_ds:
    case no_soapaction_ds:
    case no_newsoapcall:
    case no_newsoapcall_ds:
    case no_newsoapaction_ds:
    case no_filepos:
    case no_file_logicalname:
    case no_failcode:
    case no_failmessage:
    case no_eventname:
    case no_eventextra:
    case no_skip:
    case no_assert:
    case no_assert_ds:
    case no_fail:
    case no_left:
    case no_right:
    case no_sizeof:
    case no_offsetof:
    case no_xmltext:
    case no_xmlunicode:
    case no_xmlproject:
    case no_matched:
    case no_matchtext:
    case no_matchunicode:
    case no_matchlength:
    case no_matchposition:
    case no_matchrow:
    case no_matchutf8:
    case no_when:
    case no_priority:
    case no_event:
    case no_independent:
    case no_embedbody:
    case no_translated:
    case no_assertkeyed:            // not sure about this - might be better to implement in the constant folder
    case no_assertstepped:
    case no_colon:
    case no_globalscope:
    case no_param:
    case no_matchattr:
    case no_keyindex:
    case no_newkeyindex:
    case no_joined:
    case no_activerow:
    case no_newrow:
    case no_output:
    case no_pat_featuredef:
    case no_assertwild:
    case no_call:
    case no_cluster:
    case no_forcenolocal:
    case no_allnodes:
    case no_thisnode:
    case no_internalselect:
    case no_purevirtual:
    case no_libraryinput:
    case no_libraryselect:
    case no_rowsetindex:
    case no_rowsetrange:
    case no_counter:
    case no_loopcounter:
    case no_sequence:
    case no_table:
        return false;
    // following are currently not implemented in the const folder - can enable if they are.
    case no_global:
    case no_rank: 
    case no_ranked:
    case no_typetransfer:
    case no_hash:
    case no_hash32:
    case no_hash64:
    case no_hashmd5:
    case no_crc:
    case no_is_valid:
    case no_is_null:
    case no_xmldecode:
    case no_xmlencode:
    case no_sortpartition:
    case no_clustersize:
        return false;
    }
    return true;
}

int getPrecedence(node_operator op)
{
    switch (op)
    {
    case no_field:
    case no_constant:
    case no_null:
        return 12;
    case no_pat_select:
        return 11;
    case no_substring:
        return 10;

    case no_negate:
    case no_cast:
    case no_implicitcast:           // GH->RKC- Need to check child?
        return 9;

    case no_mul:
    case no_div:
    case no_modulus:
        return 8;

    case no_concat:
    case no_add:
    case no_sub:
        return 7;

    case no_lshift:
    case no_rshift:
        return 6;

    case no_band:
    case no_bor:
    case no_bxor:
        return 5;

    case no_between:
    case no_notbetween:
    case no_in:
    case no_notin:
    case no_indict:
    case no_comma:
    case no_compound:
    case no_eq:
    case no_ne:
    case no_lt:
    case no_le:
    case no_gt:
    case no_ge:
    case no_within:
    case no_notwithin:
        return 4;

    case no_not:
    case no_pat_follow:
        return 3;

    case no_and:
        return 2;

    case no_pat_or:
    case no_or:
    case no_xor:
        return 1;

    case no_abs:
    case no_mapto:
    case no_which:
    case no_choose:
    case no_rejected:
    case no_exp:
    case no_round:
    case no_roundup:
    case no_truncate:
    case no_power:
    case no_ln:
    case no_sin:
    case no_cos:
    case no_tan:
    case no_asin:
    case no_acos:
    case no_atan:
    case no_atan2:
    case no_sinh:
    case no_cosh:
    case no_tanh:
    case no_log10:
    case no_sqrt:
    case NO_AGGREGATEGROUP:
    case no_trim:
    case no_intformat:
    case no_realformat:
    case no_regex_find:
    case no_regex_replace:
    case no_fromunicode:
    case no_tounicode:
    case no_keyunicode:
    case no_toxml:
         return 0;

    case no_compound_indexread:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_inline:
    case no_filter:
    case no_limit:
    case no_catchds:
    case no_distribution:
    case NO_AGGREGATE:
    case no_keyedlimit:
    case no_keydiff:
    case no_keypatch:
        return -1;

    default:
        return 11;
    }
}


childDatasetType getChildDatasetType(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_none:
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_all:
    case no_table:
    case no_temptable:
    case no_inlinetable:
    case no_xmlproject:
    case no_workunit_dataset:
    case no_field:
    case no_left:
    case no_right:
    case no_self:
    case no_top:
    case no_keyindex:
    case no_newkeyindex:
    case no_getresult:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_comma:
    case no_compound:
    case no_fail:
    case no_skip:
    case no_activetable:
    case no_httpcall:
    case no_soapcall:
    case no_newsoapcall:
    case no_externalcall:                       // None in the sense it is generally used for.
    case no_alias:
    case no_id2blob:
    case no_embedbody:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
    case no_createrow:
    case no_param:
    case no_typetransfer:
    case no_translated:
    case no_call:
    case no_rows:
    case no_external:
    case no_delayedselect:
    case no_libraryselect:
    case no_internalselect:
    case no_unboundselect:
    case no_purevirtual:
    case no_libraryinput:
    case no_rowsetindex:
    case no_rowsetrange:
    case no_definesideeffect:
    case no_callsideeffect:
    case no_fromxml:
    case no_dataset_from_transform:
        return childdataset_none;
    case no_group:
    case no_grouped:
    case no_distribute:
    case no_distributed:
    case no_cosort:
    case no_keyed:
    case no_sort:
    case no_subsort:
    case no_sorted:
    case no_stepped:
    case no_transformebcdic:
    case no_transformascii:
    case no_selectfields:
    case no_newaggregate:
    case no_newusertable:
    case no_usertable:
    case no_alias_project:
    case no_cachealias:
    case no_choosen:
    case no_choosesets:
    case no_enth:
    case no_filter:
    case no_sample:
    case no_compound_indexread:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_metaactivity:
    case no_throughaggregate:
    case no_countcompare:
    case no_fieldmap:
    case NO_AGGREGATE:
    case no_output:
    case no_buildindex:
    case no_apply:
    case no_distribution:
    case no_within:
    case no_notwithin:
    case no_parse:
    case no_xmlparse:
    case no_compound_fetch:
    case no_topn:
    case no_distributer:
    case no_createset:
    case no_keypatch:
    case no_assert_ds:
    case no_assertsorted:
    case no_assertgrouped:
    case no_assertdistributed:
    case no_extractresult:
        return childdataset_dataset;
    case no_keyedlimit:
    case no_preload:
    case no_limit:
    case no_catchds:
    case no_forcegraph:
    case no_owned_ds:
    case no_dataset_alias:
    case no_split:
    case no_spill:
    case no_activerow:
    case no_alias_scope:
    case no_executewhen:  //second argument is independent of the other arguments
    case no_selectnth:
    case no_readspill:
    case no_commonspill:
    case no_writespill:
    case no_newrow:
    case no_returnresult:
    case no_setgraphresult:
    case no_setgraphloopresult:
    case no_spillgraphresult:
        return childdataset_dataset_noscope;
    case no_setresult:
    case no_sizeof:
    case no_offsetof:
    case no_nameof:
    case no_blob2id:
    case no_subgraph:
    case no_deserialize:
    case no_serialize:
        if (expr->queryChild(0)->isDataset())
            return childdataset_dataset_noscope;
        return childdataset_none;
    case no_pipe:
        if (expr->queryChild(0)->isDataset())
            return childdataset_dataset;
        return childdataset_none;
    case no_cloned:
    case no_colon:
    case no_globalscope:
    case no_nofold:
    case no_nohoist:
    case no_section:
    case no_thor:
    case no_catch:
    case no_forcelocal:
    case no_nothor:
    case no_cluster:
    case no_forcenolocal:
    case no_allnodes:
    case no_thisnode:
    case no_sectioninput:
    case no_outofline:
        if (expr->isDataset())
            return childdataset_dataset_noscope;
        return childdataset_none;
    case no_ensureresult:
        return childdataset_dataset_noscope;
    case no_preservemeta:
        //Only has a single dataset - but fields are referenced via active selector, so use the many option
        return childdataset_many;
    case no_newxmlparse:
    case no_newparse:
    case no_soapcall_ds:
    case no_soapaction_ds:
    case no_newsoapcall_ds:
    case no_newsoapaction_ds:
        return childdataset_datasetleft;
    case no_keyeddistribute:
        return childdataset_leftright;
    case no_select:
        if (expr->hasAttribute(newAtom) && expr->isDataset())
            return childdataset_dataset;
        return childdataset_none;
    case no_sub:
        return expr->isDataset() ? childdataset_many_noscope : childdataset_none;
    case no_if:
        return expr->isDataset() ? childdataset_if : childdataset_none;
    case no_map:
        return expr->isDataset() ? childdataset_map : childdataset_none;
    case no_case:
        return expr->isDataset() ? childdataset_case : childdataset_none;
    case no_normalize:
        if (expr->queryChild(1)->isDataset())
            return childdataset_leftright;
        return childdataset_left;
    case no_chooseds:
        return childdataset_many_noscope;
    case no_merge:
    case no_regroup:
    case no_cogroup:
        return childdataset_many;              //NB: sorted() attribute on merge uses no_activetable as the selector, not the left dataset
    case no_nonempty:
    case no_datasetlist:
    case no_addfiles:
    case no_keydiff:
    case no_related:
        return childdataset_many_noscope;   // two arbitrary inputs
    case no_hqlproject:
    case no_projectrow:
    case no_loop:
    case no_graphloop:
    case no_filtergroup:
    case no_normalizegroup:
    case no_rollupgroup:
        return childdataset_left;
    case no_denormalize:
    case no_denormalizegroup:
    case no_fetch:
    case no_join:
    case no_joincount:
    case no_combine:
    case no_combinegroup:   // Hmm really LEFT,rows but that's a bit too nasty.  RIGHT can be the first rhs record
    case no_process:
    case no_aggregate:
        return childdataset_leftright;
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
        return childdataset_nway_left_right;
    case no_selfjoin:
        return childdataset_same_left_right;
    case no_dedup:
    case no_rollup:
        return childdataset_top_left_right;
    case no_iterate:
        return childdataset_same_left_right;
    case no_update:
        return childdataset_left;
    case no_evaluate:
        return childdataset_evaluate;
    case no_mapto:
    case no_funcdef:
    case no_template_context:
    case no_variable:
    case no_quoted:
    case no_reference:
    case no_pselect:
        return childdataset_none;       // a lie.
    default:
        if (!expr->isDataset() || hasReferenceModifier(expr->queryType()))
            return childdataset_none;
        assertex(!"Need to implement getChildDatasetType() for dataset operator");
        break;
    }
    return childdataset_none;
}

inline unsigned doGetNumChildTables(IHqlExpression * dataset)
{
    switch (dataset->getOperator())
    {
    case no_sub:
        if (dataset->isDataset())
            return 2;
        return 0;
    case no_merge:
    case no_regroup:
    case no_datasetlist:
    case no_nonempty:
    case no_cogroup:
    case no_chooseds:
        {
            unsigned ret = 0;
            ForEachChild(idx, dataset)
            {
                if (dataset->queryChild(idx)->isDataset())
                    ret++;
            }
            return ret;
        }
    case no_addfiles:
    case no_denormalize:
    case no_denormalizegroup:
    case no_join:
    case no_fetch:
    case no_joincount:
    case no_keyeddistribute:
    case no_keydiff:
    case no_combine:
    case no_combinegroup:
    case no_process:
    case no_related:
        return 2;
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
        return 0;       //??
    case no_selfjoin:
    case no_alias_project:
    case no_alias_scope:
    case no_newaggregate:
    case no_apply:
    case no_cachealias:
    case no_choosen:
    case no_choosesets:
    case no_cosort:
    case no_dedup:
    case no_distribute:
    case no_distributed:
    case no_preservemeta:
    case no_enth:
    case no_filter:
    case no_group:
    case no_grouped:
    case no_iterate:
    case no_keyed:
    case no_metaactivity:
    case no_hqlproject:
    case no_rollup:
    case no_sample:
    case no_selectnth:
    case no_sort:
    case no_subsort:
    case no_sorted:
    case no_stepped:
    case no_assertsorted:
    case no_assertgrouped:
    case no_assertdistributed:
    case no_selectfields:
    case no_compound_indexread:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_transformascii:
    case no_transformebcdic:
    case no_newusertable:
    case no_aggregate:
    case no_usertable:
    case NO_AGGREGATE:
    case no_output:
    case no_buildindex:
    case no_distribution:
    case no_split:
    case no_spill:
    case no_commonspill:
    case no_readspill:
    case no_writespill:
    case no_throughaggregate:
    case no_countcompare:
    case no_within:
    case no_notwithin:
    case no_limit:
    case no_catchds:
    case no_fieldmap:
    case no_parse:
    case no_xmlparse:
    case no_newparse:
    case no_newxmlparse:
    case no_compound_fetch:
    case no_topn:
    case no_distributer:
    case no_preload:
    case no_createset:
    case no_soapcall_ds:
    case no_soapaction_ds:
    case no_newsoapcall_ds:
    case no_newsoapaction_ds:
    case no_activerow:
    case no_newrow:
    case no_keyedlimit:
    case no_keypatch:
    case no_returnresult:
    case no_setgraphresult:
    case no_setgraphloopresult:
    case no_projectrow:
    case no_assert_ds:
    case no_rollupgroup:
    case no_spillgraphresult:
    case no_loop:
    case no_graphloop:
    case no_extractresult:
    case no_filtergroup:
    case no_forcegraph:
    case no_executewhen:
    case no_normalizegroup:
    case no_owned_ds:
    case no_dataset_alias:
    case no_ensureresult:
        return 1;
    case no_deserialize:
    case no_serialize:
        if (dataset->queryChild(0)->isDataset())
            return 1;
        return 0;
    case no_childdataset:
    case no_left:
    case no_right:
    case no_self:
    case no_top:
    case no_temptable:
    case no_inlinetable:
    case no_xmlproject:
    case no_table:
    case no_workunit_dataset:
    case no_field:
    case no_none:
    case no_funcdef:
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_all:
    case no_keyindex:
    case no_newkeyindex:
    case no_getresult:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_fail:
    case no_skip:
    case no_activetable:
    case no_httpcall:
    case no_soapcall:
    case no_newsoapcall:
    case no_externalcall:                       // None in the sense it is generally used for.
    case no_alias:
    case no_id2blob:
    case no_embedbody:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
    case no_param:
    case no_translated:
    case no_call:
    case no_rows:
    case no_external:
    case no_rowsetindex:
    case no_rowsetrange:
    case no_definesideeffect:
    case no_callsideeffect:
    case no_fromxml:
    case no_dataset_from_transform:
        return 0;
    case no_delayedselect:
    case no_unboundselect:
    case no_libraryselect:
    case no_internalselect:
    case no_purevirtual:
    case no_libraryinput:
        return 0;
    case no_subgraph:
    case no_sizeof:
    case no_offsetof:
    case no_nameof:
    case no_blob2id:
    case no_setresult:
        if (dataset->queryChild(0)->isDataset())
            return 1;
        return 0;
    case no_select:
        if (dataset->hasAttribute(newAtom) && dataset->isDataset())
            return 1;
        return 0;
    case no_normalize:
        if (dataset->queryChild(1)->isDataset())
            return 2;
        return 1;
    case no_cloned:
    case no_colon:
    case no_globalscope:
    case no_nofold:
    case no_nohoist:
    case no_section:
    case no_thor:
    case no_pipe:
    case no_catch:
    case no_forcelocal:
    case no_nothor:
    case no_cluster:
    case no_forcenolocal:
    case no_allnodes:
    case no_thisnode:
    case no_sectioninput:
    case no_outofline:
        if (dataset->isDataset())
            return 1;
        return 0;
    case no_if:
    case no_case:
    case no_map:
        //assertex(!"Error: IF getNumChildTables() needs special processing");
        return 0;
    case no_sequential:
    case no_parallel:
    case no_orderedactionlist:
        return 0;
    case no_quoted:
    case no_variable:
        return 0;
    case no_mapto:
    case no_compound:
        return 0;       // a lie.
    default:
        if (!dataset->isDataset())
            return 0;
        PrintLogExprTree(dataset, "missing operator: ");
        assertex(false);
        return 0;
    }
}

unsigned getNumChildTables(IHqlExpression * dataset)
{
    unsigned num = doGetNumChildTables(dataset);
#ifdef _DEBUG
    switch (getChildDatasetType(dataset))
    {
    case childdataset_none: 
    case childdataset_nway_left_right:
        assertex(num==0); 
        break;
    case childdataset_dataset: 
    case childdataset_datasetleft: 
    case childdataset_left: 
    case childdataset_same_left_right:
    case childdataset_top_left_right:
    case childdataset_dataset_noscope: 
        assertex(num==1); 
        break;
    case childdataset_leftright:
        if (dataset->getOperator() != no_aggregate)
            assertex(num==2); 
        break;
    case childdataset_many_noscope:
    case childdataset_many:
        break;
    case childdataset_if:
    case childdataset_case:
    case childdataset_map:
        assertex(num==0); 
        break;
    case childdataset_evaluate:
        assertex(num==1); 
        break;
    default:
        UNIMPLEMENTED;
    }
#endif
    return num;
}

node_operator queryHasRows(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_denormalizegroup:
    case no_combinegroup:
    case no_aggregate:
        return no_right;
    case no_loop:
    case no_graphloop:
    case no_rollupgroup:
    case no_nwayjoin:
    case no_filtergroup:
    case no_mergejoin:
        return no_left;
    }
#ifdef _DEBUG
    //This would imply a missing entry above....
    assertex(!expr->queryAttribute(_rowsid_Atom));
#endif
    return no_none;
}

bool definesColumnList(IHqlExpression * dataset)
{
    node_operator op = dataset->getOperator();
    switch (op)
    {
    case no_merge: // MORE - is this right?
    case no_addfiles:
    case no_regroup:
    case no_nonempty:
        return true;    // a fake
    case no_alias_project:
    case no_alias_scope:
    case no_sub:
    case no_cachealias:
    case no_choosen:
    case no_choosesets:
    case no_cloned:
    case no_cosort:
    case no_dedup:
    case no_distribute:
    case no_distributed:
    case no_preservemeta:
    case no_enth:
    case no_filter:
    case no_group:
    case no_grouped:
    case no_keyed:
    case no_metaactivity:
    case no_nofold:
    case no_nohoist:
    case no_section:
    case no_sample:
    case no_sort:
    case no_subsort:
    case no_sorted:
    case no_stepped:
    case no_assertsorted:
    case no_assertgrouped:
    case no_assertdistributed:
    case no_thor:
    case no_compound_indexread:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_split:
    case no_spill:
    case no_writespill:
    case no_throughaggregate:
    case no_limit:
    case no_catchds:
    case no_compound_fetch:
    case no_topn:
    case no_keyeddistribute:
    case no_preload:
    case no_catch:
    case no_keyedlimit:
    case no_keydiff:
    case no_keypatch:
    case no_activerow:
    case no_assert_ds:
    case no_spillgraphresult:
    case no_sectioninput:
    case no_forcegraph:
    case no_related:
    case no_outofline:
    case no_fieldmap:
    case no_owned_ds:
        return false;
    case no_iterate:
    case no_rollup:
    case no_loop:
    case no_graphloop:
    case no_newrow:         //only used while transforming
    case no_newaggregate:
    case no_childdataset:
    case no_denormalize:
    case no_denormalizegroup:
    case no_fetch:
    case no_join:
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
    case no_selfjoin:
    case no_pipe:
    case no_transformebcdic:
    case no_transformascii:
    case no_hqlproject:
    case no_left:
    case no_normalize:
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_all:
    case no_none:
    case no_funcdef:
    case no_right:
    case no_selectfields:
    case no_selectnth:
    case no_self:
    case no_top:
    case no_table:
    case no_temptable:
    case no_inlinetable:
    case no_xmlproject:
    case no_workunit_dataset:
    case no_newusertable:
    case no_aggregate:
    case no_usertable:
    case no_keyindex:
    case no_newkeyindex:
    case no_if:
    case no_map:
    case no_case:
    case no_chooseds:
    case no_colon:
    case no_globalscope:
    case no_nothor:
    case no_getresult:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_joincount:
    case no_parse:
    case no_xmlparse:
    case no_newparse:
    case no_newxmlparse:
    case no_fail:
    case no_skip:
    case no_activetable:
    case no_httpcall:
    case no_soapcall:
    case no_soapcall_ds:
    case no_newsoapcall:
    case no_newsoapcall_ds:
    case no_alias:
    case no_id2blob:
    case no_embedbody:
    case no_externalcall:
    case no_projectrow:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
    case no_forcelocal:                 // for the moment this defines a table, otherwise the transforms get rather tricky.
    case no_forcenolocal:
    case no_allnodes:
    case no_thisnode:
    case no_createrow:
    case no_typetransfer:
    case no_translated:
    case no_call:
    case no_param:
    case no_rows:
    case no_combine:
    case no_combinegroup:
    case no_rollupgroup:
    case no_cluster:
    case no_internalselect:
    case no_delayedselect:
    case no_unboundselect:
    case no_libraryselect:
    case no_purevirtual:
    case no_libraryinput:
    case no_process:
    case no_rowsetindex:
    case no_rowsetrange:
    case no_filtergroup:
    case no_deserialize:
    case no_serialize:
    case no_commonspill:
    case no_readspill:
    case no_fromxml:
    case no_normalizegroup:
    case no_cogroup:
    case no_dataset_alias:
    case no_dataset_from_transform:
    case no_mapto:
        return true;
    case no_select:
    case no_field:
        {
            type_t tc = dataset->queryType()->getTypeCode();
            assertex(tc == type_table || tc == type_groupedtable || tc == type_dictionary);
            return true;
        }
    case no_comma:
        return false;
    case no_compound:
    case no_executewhen:
        return false;
    default:
        if (!dataset->isDataset())
            return false;
        PrintLogExprTree(dataset, "definesColumnList() missing operator: ");
        assertex(false);
        return true;
    }
}

unsigned queryTransformIndex(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    unsigned pos;
    switch (op)
    {
    case no_createrow:
    case no_typetransfer:
        pos = 0;
        break;
    case no_transformebcdic:
    case no_transformascii:
    case no_hqlproject:
    case no_projectrow:
    case no_iterate:
    case no_rollupgroup:
    case no_xmlproject:
        pos = 1;
        break;
    case no_newkeyindex:
    case no_newaggregate:
    case no_newusertable:
    case no_aggregate:
    case no_normalize:
    case no_xmlparse:
    case no_rollup:
    case no_combine:
    case no_combinegroup:
    case no_process:
    case no_nwayjoin:
        pos = 2;
        break;
    case no_fetch:
    case no_join:
    case no_selfjoin:
    case no_joincount:
    case no_denormalize:
    case no_denormalizegroup:
    case no_parse:
    case no_soapcall:
    case no_newxmlparse:
        pos = 3;
        break;
    case no_newparse:
    case no_newsoapcall:            // 4 because input(2) gets transformed.
    case no_soapcall_ds:
        pos = 4;
        break;
    case no_newsoapcall_ds:
        pos = 5;
        break;
    default:
        throwUnexpectedOp(op);
    }
#ifdef _DEBUG
    assertex(expr->queryChild(pos)->isTransform());
#endif
    return pos;
}

IHqlExpression * queryNewColumnProvider(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_alias:
        return expr->queryRecord();
    case no_createrow:
    case no_typetransfer:
        return expr->queryChild(0);
    case no_usertable:
    case no_selectfields:
    case no_transformebcdic:
    case no_transformascii:
    case no_hqlproject:
    case no_dataset_from_transform:
    case no_keyindex:
    case no_projectrow:
    case no_iterate:
    case no_rollupgroup:
    case no_xmlproject:
    case no_deserialize:
    case no_serialize:
        return expr->queryChild(1);
    case no_newkeyindex:
    case no_aggregate:
    case no_newaggregate:
    case no_newusertable:
    case no_normalize:
    case no_xmlparse:
    case no_rollup:
    case no_combine:
    case no_combinegroup:
    case no_process:
    case no_nwayjoin:
        return expr->queryChild(2);
    case no_fetch:
    case no_join:
    case no_selfjoin:
    case no_joincount:
    case no_denormalize:
    case no_denormalizegroup:
    case no_parse:
    case no_soapcall:
    case no_httpcall:
    case no_newxmlparse:
        return expr->queryChild(3);
    case no_newparse:
    case no_newsoapcall:            // 4 because input(2) gets transformed.
    case no_soapcall_ds:
        return expr->queryChild(4); 
    case no_newsoapcall_ds:
        return expr->queryChild(5);
    default:
        return NULL;
    }
}

IHqlExpression * queryDatasetGroupBy(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_newaggregate:
    case no_newusertable:
    case no_aggregate:
        return queryRealChild(expr, 3);
    case no_selectfields:
    case no_usertable:
        return queryRealChild(expr, 2);
    }
    return NULL;
}

bool datasetHasGroupBy(IHqlExpression * expr)
{
    IHqlExpression * grouping = queryDatasetGroupBy(expr);
    if (grouping)// && !grouping->isConstant())
        return true;
    return false;
}



bool isAggregateDataset(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_aggregate:
    case no_newaggregate:
        return true;
    case no_selectfields:
    case no_usertable:
        {
            IHqlExpression * grouping = expr->queryChild(2);
            if (grouping && !grouping->isAttribute())
                return true;
            return expr->queryChild(1)->isGroupAggregateFunction();
        }
    default:
        return false;
    }
}


bool isAggregatedDataset(IHqlExpression * expr)
{
    loop
    {
        if (isAggregateDataset(expr))
            return true;
        switch (expr->getOperator())
        {
        case no_newusertable:
        case no_selectfields:
        case no_usertable:
            expr = expr->queryChild(0);
            break;
        default:
            return false;
        }
    }
}


IHqlExpression * ensureExprType(IHqlExpression * expr, ITypeInfo * type, node_operator castOp)
{
    ITypeInfo * qualifiedType = expr->queryType();
    if (qualifiedType == type)
        return LINK(expr);

    ITypeInfo * exprType = queryUnqualifiedType(qualifiedType);
    if (exprType == queryUnqualifiedType(type))
        return LINK(expr);

    type_t tc = type->getTypeCode();
    if (tc == type_any)
        return LINK(expr);

    if (tc == type_set)
    {
        ITypeInfo * childType = type->queryChildType();
        if (!childType)
            return LINK(expr);
        if (exprType)
        {
            ITypeInfo * exprChildType = exprType->queryChildType();
            if (exprChildType && isSameBasicType(childType, exprChildType))
                return LINK(expr);
        }
        HqlExprArray values;

        if (expr->getOperator() == no_list)
            expr->unwindList(values, expr->getOperator());
        else if (expr->getOperator() == no_null)
            return createValue(no_list, LINK(type));
        else if (exprType->getTypeCode() == type_array)
        {
            if (!childType || exprType->queryChildType() == childType)
                return LINK(expr);
            return createValue(castOp, LINK(type), LINK(expr));
        }
        else if (exprType->getTypeCode() == type_set)
        {
            if (!childType || isDatasetType(childType))
                return LINK(expr);
            if (expr->getOperator() == no_all)
                return createValue(no_all, LINK(type));
            if (expr->getOperator() == no_alias_scope)
            {
                HqlExprArray args;
                unwindChildren(args, expr);
                IHqlExpression * cast = ensureExprType(expr->queryChild(0), type, castOp);
                args.replace(*cast,0);
                return createValue(no_alias_scope, cast->getType(), args);
            }
            return createValue(castOp, LINK(type), LINK(expr));
        }
        else
        {
            // cvt single values to list
            values.append(*LINK(expr));
        }

        Owned<ITypeInfo> promotedType;
        ForEachItemIn(idx, values)
        {
            IHqlExpression * cast = ensureExprType(&values.item(idx), childType);
            values.replace(*cast, idx);
            ITypeInfo * castType = cast->queryType();
            if (!promotedType)
                promotedType.set(castType);
            else if (!isSameBasicType(promotedType, castType))
            {
                if (promotedType->getSize() != UNKNOWN_LENGTH)
                {
                    promotedType.setown(getStretchedType(UNKNOWN_LENGTH, childType));
                }
            }
        }

        if (promotedType && (childType->getSize() != UNKNOWN_LENGTH))
            type = makeSetType(promotedType.getClear());
        else
            type->Link();
        return createValue(no_list, type, values);
    }
    else if (tc == type_alien)
    {
        return ensureExprType(expr, type->queryPromotedType(), castOp);
    }
    else if (type->getSize() == UNKNOWN_LENGTH)
    {
        if (exprType)
        {
            //Optimize away casts to unknown length if the rest of the type matches.
            if (exprType->getTypeCode() == tc)
            {
                //cast to STRING/DATA/VARSTRING/UNICODE/VARUNICODE means ensure that the expression has this base type.
                if ((tc == type_data) || (tc == type_qstring))
                {
                    return LINK(expr);
                }
                else if (tc == type_unicode || tc == type_varunicode || tc == type_utf8)
                {
                    if (type->queryLocale() == exprType->queryLocale())
                        return LINK(expr);
                }
                else if (tc == type_string || tc == type_varstring)
                {
                    if ((type->queryCharset() == exprType->queryCharset()) &&
                        (type->queryCollation() == exprType->queryCollation()))
                        return LINK(expr);
                }
                else if (tc == type_decimal)
                {
                    if (type->isSigned() == exprType->isSigned())
                        return LINK(expr);
                }
            }

            /*
            The following might produce better code, but it generally makes things worse.....
            if ((exprType->getSize() != UNKNOWN_LENGTH) && (isStringType(exprType) || isUnicodeType(exprType)))
            {
                Owned<ITypeInfo> stretchedType = getStretchedType(exprType->getStringLen(), type);
                return ensureExprType(expr, stretchedType, castOp);
            }
            */
        }
    }

    node_operator op = expr->getOperator();
    if (op == no_null)
    {
        assertex(expr->queryRecord());
        //The no_null can differ from the expected type by (i) link counting (ii) record annotations
        assertex(recordTypesMatch(type, exprType));
        return createNullExpr(type);
    }

    IValue * value = expr->queryValue();
    if (value && type->assignableFrom(exprType))    // this last condition is unnecessary, but changes some persist crcs if removed
    {
        value = value->castTo(type);
        if (value)
            return createConstant(value);
    }

    switch (tc)
    {
    case type_row:
    case type_transform:
        {
            switch (exprType->getTypeCode())
            {
            case type_row:
            case type_transform:
                if (recordTypesMatch(type, exprType))
                    return LINK(expr);
                break;
            }
        }
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        if (recordTypesMatch(type, exprType))
        {
            if (tc==type_dictionary && !expr->isDictionary())
                return createDictionary(no_createdictionary, LINK(expr));
            else
                return LINK(expr);
        }
        assertex(!expr->isDictionary());
        break;
    case type_scope:
    case type_function:
        return LINK(expr);
    }

    //MORE: Casts of datasets should create a dataset - but there is no parameter to determine the type from...
    switch (tc)
    {
    case type_table:
    case type_groupedtable:
        {
            if (!expr->queryRecord())
                return LINK(expr);      // something seriously wrong - will get picked up elsewhere...
            if(!queryOriginalRecord(type))
                 throwUnexpectedX("Cast to DATASET with no record TYPE specified");   //  cf. HPCC-9847
            OwnedHqlExpr transform = createRecordMappingTransform(no_newtransform, queryOriginalRecord(type), expr->queryNormalizedSelector());
            if (transform)
                return createDatasetF(no_newusertable, LINK(expr), LINK(queryOriginalRecord(type)), LINK(transform), NULL);
            //Need to create a project of the dataset - error if can't
            break;
        }
    case type_row:
        {
            OwnedHqlExpr input = isAlwaysActiveRow(expr) ? LINK(expr) : createRow(no_newrow, LINK(expr));
            OwnedHqlExpr transform = createRecordMappingTransform(no_newtransform, queryOriginalRecord(type), input);
            if (transform)
                return createRow(no_createrow, LINK(transform));
            //Need to create a project of the dataset - error if can't
            break;
        }
    }

    if (op == no_skip)
        return createValue(no_skip, LINK(type), LINK(expr->queryChild(0)));

    return createValue(castOp, LINK(type), LINK(expr));
}

extern HQL_API IHqlExpression * ensureExprType(IHqlExpression * expr, ITypeInfo * type)
{
    return ensureExprType(expr, type, no_implicitcast);
}

extern HQL_API IHqlExpression * getCastExpr(IHqlExpression * expr, ITypeInfo * type)
{
    return ensureExprType(expr, type, no_cast);
}

extern HQL_API IHqlExpression * normalizeListCasts(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_cast:
    case no_implicitcast:
        {
            IHqlExpression * arg = expr->queryChild(0);
            OwnedHqlExpr normalized = normalizeListCasts(arg);
            OwnedHqlExpr ret = ensureExprType(normalized, expr->queryType());
            if (ret == expr->queryBody())
                return LINK(expr);
            return expr->cloneAllAnnotations(ret);
        }
    default:
        return LINK(expr);
    }
}


extern HQL_API IHqlExpression * simplifyFixedLengthList(IHqlExpression * expr)
{
    if (expr->getOperator() != no_list)
        return LINK(expr);

    ITypeInfo * listType = expr->queryType();
    ITypeInfo * elemType = listType->queryChildType();
    if (!elemType || (elemType->getSize() != UNKNOWN_LENGTH))
        return LINK(expr);

    unsigned max = expr->numChildren();
    if (max == 0)
        return LINK(expr);

    unsigned elemSize = UNKNOWN_LENGTH;
    ForEachChild(i, expr)
    {
        unsigned thisSize = expr->queryChild(i)->queryType()->getStringLen();
        if (thisSize == UNKNOWN_LENGTH)
            return LINK(expr);
        if (i == 0)
            elemSize = thisSize;
        else if (elemSize != thisSize)
            return LINK(expr);
    }

    HqlExprArray args;
    unwindChildren(args, expr);
    return createValue(no_list, makeSetType(getStretchedType(elemSize, elemType)), args);
}

extern HQL_API IHqlExpression * expandBetween(IHqlExpression * expr)
{
    IHqlExpression * test = expr->queryChild(0);
    IHqlExpression * lower = expr->queryChild(1);
    IHqlExpression * upper = expr->queryChild(2);
    if (expr->getOperator() == no_between)
        return createBoolExpr(no_and,
                            createBoolExpr(no_ge, LINK(test), LINK(lower)),
                            createBoolExpr(no_le, LINK(test), LINK(upper)));
    else
        return createBoolExpr(no_or,
                            createBoolExpr(no_lt, LINK(test), LINK(lower)),
                            createBoolExpr(no_gt, LINK(test), LINK(upper)));
}


//==============================================================================================================

#ifdef DEBUG_TRACK_INSTANCEID
static unsigned __int64 exprseqid;
#endif

CHqlExpression::CHqlExpression(node_operator _op)
{
#ifdef DEBUG_TRACK_INSTANCEID
    //Not thread safe, but not much use if multi threading anyway.
    seqid = ++exprseqid;
#endif
    op = _op;
    for (unsigned i=0; i < NUM_PARALLEL_TRANSFORMS; i++)
    {
        transformExtra[i] = NULL;
        transformDepth[i] = 0;
    }
    hashcode = 0;
    cachedCRC = 0;
    attributes = NULL;

    initFlagsBeforeOperands();

    CHECK_EXPR_SEQID(0);
}

void CHqlExpression::appendOperands(IHqlExpression * arg0, ...)
{
    if (!arg0)
        return;

    unsigned numArgs = 1;
    va_list argscount;
    va_start(argscount, arg0);
    for (;;)
    {
        IHqlExpression *parm = va_arg(argscount, IHqlExpression *);
        if (!parm)
            break;
        numArgs++;
    }
    va_end(argscount);

    operands.ensure(numArgs);
    doAppendOperand(*arg0);
    va_list args;
    va_start(args, arg0);
    for (;;)
    {
        IHqlExpression *parm = va_arg(args, IHqlExpression *);
        if (!parm)
            break;
#ifdef _DEBUG
        assertex(QUERYINTERFACE(parm, IHqlExpression));
#endif
        doAppendOperand(*parm);
    }
    va_end(args);
}

IIdAtom * CHqlExpression::queryId() const
{
    assertex(!isAttribute());
    return NULL;
}

void CHqlExpression::setOperands(HqlExprArray & _ownedOperands)
{
    unsigned max = _ownedOperands.ordinality();
    if (max)
    {
        operands.swapWith(_ownedOperands);
        for (unsigned i=0; i < max; i++)
            onAppendOperand(operands.item(i), i);
    }
}

void CHqlExpression::initFlagsBeforeOperands()
{
    //NB: The following code is not allowed to access queryType()!
    infoFlags = 0;
    infoFlags2 = 0;
    if (::checkConstant(op))    infoFlags2 |= HEF2constant;
    switch (op)
    {
    case NO_AGGREGATEGROUP:
        infoFlags |= HEFfunctionOfGroupAggregate|HEFtransformDependent;
        infoFlags2 &= ~(HEF2constant);
        break;
    case no_random:
        infoFlags2 &= ~(HEF2constant);
        infoFlags |= HEFvolatile;
        break;
    case no_wait:
        infoFlags2 |= HEF2globalAction;
        break;
    case no_apply:
    case no_buildindex:
    case no_distribution:
    case no_keydiff:
    case no_keypatch:
    case no_returnresult:
    case no_setgraphresult:
    case no_setgraphloopresult:
    case no_impure:
    case no_outputscalar:
    case no_ensureresult:
    case no_definesideeffect:
    case no_callsideeffect:
        infoFlags2 &= ~(HEF2constant);
        infoFlags |= HEFaction;
        break;
    case no_fail:
    case no_assert:
    case no_assert_ds:
        infoFlags2 &= ~(HEF2constant);
        break;
    case no_purevirtual:
        infoFlags |= HEFcontextDependentException;
        break;
    case no_output:
    case no_setresult:
    case no_extractresult:
        // don't mark as impure because temporary results don't get commoned up.  Need another flag to mean has side-effects, 
        // but can be commoned up because repeating it will have the same effect.
        infoFlags2 &= ~(HEF2constant);      
        break;
    case no_skip:
        infoFlags |= HEFcontainsSkip;
        break;
    case no_alias:
        infoFlags |= HEFcontainsAlias|HEFcontainsAliasLocally;
        break;
    case no_dataset_alias:
        if (!queryAttribute(_normalized_Atom))
            infoFlags |= HEFcontainsDatasetAliasLocally;
        break;
    case no_activetable:
    case no_activerow:
        infoFlags2 &= ~(HEF2constant);
        infoFlags |= HEFcontainsActiveDataset|HEFcontainsActiveNonSelector;
        break;
    case no_left:
    case no_right:
    case no_top:
        infoFlags2 &= ~(HEF2constant);
        infoFlags |= HEFcontainsActiveDataset;
        break;
    case no_nohoist:
        infoFlags |= HEFcontextDependent;
        infoFlags2 &= ~HEF2constant;
        break;
    case no_nofold:
    case no_section:            // not so sure about this...
    case no_sectioninput:
    case no_wuid:
    case no_getenv:
        infoFlags2 &= ~HEF2constant;
        break;
    case no_counter:
        infoFlags |= (HEFcontainsCounter);
        break;
    case no_loopcounter:
        infoFlags |= HEFgraphDependent;
        break;
    case no_matched:
    case no_matchtext:
    case no_matchunicode:
    case no_matchlength:
    case no_matchposition:
    case no_matchattr:
    case no_matchrow:
    case no_matchutf8:
        infoFlags2 &= ~(HEF2constant);
        infoFlags |= HEFcontainsNlpText;
        break;
    case no_failcode:
    case no_failmessage:
//  case no_eventname:
//  case no_eventextra:
        //MORE: Really should improve on contextDependentException
        //however we would need to make sure they were cleared by the correct transform
        infoFlags2 &= ~(HEF2constant);
        infoFlags |= HEFonFailDependent;
        break;
    case no_variable:
    case no_quoted:
        infoFlags2 &= ~(HEF2constant);
        break;
    case no_xmltext:
    case no_xmlunicode:
    case no_xmlproject:
        infoFlags2 &= ~(HEF2constant);
        infoFlags |= HEFcontainsXmlText;
        break;
    case no_assertkeyed:
    case no_assertwild:
        infoFlags |= HEFassertkeyed;
        break;
    case no_assertstepped:
        infoFlags2 |= HEF2assertstepped;
        break;
    case no_filepos:
    case no_file_logicalname:
        infoFlags |= HEFcontainsActiveDataset|HEFcontainsActiveNonSelector;
        break;
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_libraryinput:
        infoFlags |= HEFgraphDependent;
        break;
    case no_internalselect:
        infoFlags |= HEFinternalSelect;
        break;
    case no_colon:
        infoFlags2 |= HEF2workflow;
        break;
    case no_decimalstack:
        infoFlags |= HEFaction;
        break;
    case no_getresult:
    case no_workunit_dataset:
        infoFlags |= HEFaccessRuntimeContext;
        break;
    }
}

void CHqlExpression::updateFlagsAfterOperands()
{
//  DBGLOG("%p: Create(%s) type = %lx", (unsigned)(IHqlExpression *)this, getOpString(op), (unsigned)type);
    switch (op)
    {
    case no_pure:
        infoFlags &= ~(HEFvolatile|HEFaction|HEFthrowds|HEFthrowscalar|HEFcontainsSkip);
        break;
    case no_record:
        {
            unsigned num = numChildren();
            unsigned idx;
            for (idx = 0; idx < num; idx++)
            {
                IHqlExpression * cur = queryChild(idx);
                if (cur->getOperator() == no_field)
                {
                    //MORE: Should cope with 
                    IHqlExpression * value = cur->queryChild(0);
                    if (value && value->isGroupAggregateFunction())
                    {
                        infoFlags |= HEFfunctionOfGroupAggregate;
                        break;
                    }
                }
            }
            infoFlags &= ~(HEFcontextDependentException|HEFcontainsActiveDataset|HEFcontainsActiveNonSelector);
        }
        break;
    case no_self:
        {
            ITypeInfo * thisType = queryType();
            if (!thisType || !isPatternType(thisType))
            {
                infoFlags |= (HEFtransformDependent|HEFcontainsActiveDataset|HEFcontainsActiveNonSelector);
                infoFlags2 |= HEF2containsSelf;
            }
            break;
        }
    case no_selfref:            // not sure about what flags
        {
            ITypeInfo * thisType = queryType();
            if (!thisType || !isPatternType(thisType))
                infoFlags |= (HEFtransformDependent|HEFcontainsActiveDataset|HEFcontainsActiveNonSelector);
            break;
        }
    case no_fail:
    case no_assert:
    case no_assert_ds:
        switch (queryType()->getTypeCode())
        {
        case type_dictionary:
        case type_table:
        case type_groupedtable:
            infoFlags |= HEFthrowds;
            break;
        default:
            infoFlags |= HEFthrowscalar;
        }
        infoFlags |= HEFoldthrows;
        break;
#if 0
    //A good idea, but once temptables are marked as constant, I really should start constant folding them,
    //otherwise the folder complains they weren't folded.  Would be ok if values were fully expanded at normalize() time.
    //In fact it would provide the basis for compile time record processing - which might have some interesting uses.
    case no_temptable:
        infoFlags |= (queryChild(0)->getInfoFlags() & (HEFconstant));
        if (queryChild(2))
            infoFlags &= (queryChild(2)->getInfoFlags() | ~(HEFconstant));
        break;
#endif
    case no_inlinetable:
        infoFlags2 |= (queryChild(0)->getInfoFlags2() & (HEF2constant));
        break;
    case no_globalscope:
        infoFlags2 |= HEF2mustHoist;
        if (hasAttribute(optAtom))
            break;
        //fall through
    case no_colon:
        if (!isDataset() && !isDatarow())
        {
            infoFlags &= ~(HEFcontainsActiveDataset|HEFcontainsActiveNonSelector|HEFcontainsDataset);
        }
        break;
    case NO_AGGREGATE:
        infoFlags2 |= HEF2containsNewDataset;
        if (queryChild(0) && (queryChild(0)->getOperator() == no_null))
            infoFlags2 |= HEF2constant;
        // don't percolate aliases beyond their subqueries at the moment.
        infoFlags &= ~(HEFcontainsAliasLocally|HEFthrowscalar|HEFcontainsDatasetAliasLocally);
        //a dataset fail, now becomes a scalar fail
        if (infoFlags & HEFthrowds)
            infoFlags = (infoFlags &~HEFthrowds)|HEFthrowscalar;
        break;
    case no_select:
        if (!isNewSelector(this))
        {
            IHqlExpression * left = queryChild(0);
            node_operator lOp = left->getOperator();
            infoFlags = (infoFlags & HEFretainedByActiveSelect);
            infoFlags |= HEFcontainsActiveDataset;
            switch (lOp)
            {
            case no_self:
            case no_left:
            case no_right:
                break;
            default:
                infoFlags |= HEFcontainsActiveNonSelector;
                break;
            }
        }
        else
        {
            infoFlags &= ~(HEFcontextDependentException|HEFcontainsActiveDataset|HEFcontainsAliasLocally|HEFcontainsDatasetAliasLocally|HEFthrowscalar);       // don't percolate aliases beyond their subqueries at the moment.
            infoFlags |= (queryChild(0)->getInfoFlags() & (HEFcontextDependentException|HEFcontainsActiveDataset));
            if (infoFlags & HEFthrowds)
                infoFlags = (infoFlags &~HEFthrowds)|HEFthrowscalar;
            infoFlags2 |= HEF2containsNewDataset;
        }
        break;
    case no_filepos:
    case no_file_logicalname:
    case no_joined:
        infoFlags = (infoFlags & HEFretainedByActiveSelect);
        infoFlags |= HEFcontainsActiveDataset|HEFcontainsActiveNonSelector;
        break;
    case no_offsetof:
    case no_activerow:
    case no_sizeof:
        infoFlags = (infoFlags & HEFretainedByActiveSelect);
        infoFlags |= HEFcontainsActiveDataset|HEFcontainsActiveNonSelector;
        break;
    case no_translated:
        //could possibly make this dependent on adding an attribute to the expression.
        infoFlags |= HEFtranslated;
        break;
    case no_transform:
    case no_newtransform:
        {
            IHqlExpression * record = queryRecord();
            if (record)
            {
                infoFlags |= (record->getInfoFlags() & HEFalwaysInherit);
                infoFlags2 |= (record->getInfoFlags2() & HEF2alwaysInherit);
            }
        }
        break;
    case no_null:
        infoFlags2 |= HEF2constant;
        break;
    case no_attr:
        infoFlags = (infoFlags & (HEFhousekeeping|HEFalwaysInherit));
        infoFlags2 |= HEF2constant;
        break;
    case no_newxmlparse:
    case no_xmlparse:
        //clear flag unless set in the dataset.
        if (!(queryChild(0)->getInfoFlags() & HEFcontainsXmlText))
            infoFlags &= ~HEFcontainsXmlText;
        break;
    case no_newparse:
    case no_parse:
        if (!(queryChild(0)->getInfoFlags() & HEFcontainsNlpText))
            infoFlags &= ~HEFcontainsNlpText;
        break;
    case no_assign:
        infoFlags = (infoFlags & ~HEFassigninheritFlags) | (queryChild(1)->getInfoFlags() & HEFassigninheritFlags);
        infoFlags2 = (infoFlags2 & ~HEF2assigninheritFlags) | (queryChild(1)->getInfoFlags2() & HEF2assigninheritFlags);
        infoFlags &= ~HEFcontainsDataset;
        assertex(queryChild(0)->isDictionary() == queryChild(1)->isDictionary());
        break;
    case no_internalselect:
        assertex(!queryChild(3)->isAttribute());
        break;
    case no_delayedselect:
    case no_libraryselect:
    case no_unboundselect:
        assertex(!queryChild(3)->isAttribute());
        //kill any flag derived from selecting pure virtual members
        infoFlags &= ~HEFcontextDependentException;
        break;
    case no_attr_link:
    case no_attr_expr:
        if (queryName() == onFailAtom)
            infoFlags &= ~HEFonFailDependent;
        infoFlags &= ~(HEFthrowscalar|HEFthrowds|HEFoldthrows);
        break;
    case no_clustersize:
        //wrong, but improves the generated code
        infoFlags |= HEFvolatile;
        break;
    case no_type:
        {
            HqlExprArray kids;
            IHqlScope * scope = queryScope();
            if (scope)
            {
                IHqlExpression * scopeExpr = queryExpression(scope);
                if (scopeExpr)
                {
                    infoFlags |= (scopeExpr->getInfoFlags() & HEFalwaysInherit);
                    infoFlags2 |= (scopeExpr->getInfoFlags2() & HEF2alwaysInherit);
                }
            }
            break;
        }
    case no_thisnode:
        infoFlags |= HEFcontainsThisNode;
        break;
    case no_getgraphresult:
        if (hasAttribute(_streaming_Atom))
            infoFlags2 |= HEF2mustHoist;
        break;
    case no_getgraphloopresult:
        infoFlags2 |= HEF2mustHoist;
        break;
    case no_alias:
        if (queryAttribute(globalAtom))
            infoFlags2 &= ~HEF2containsNonGlobalAlias;
        else
            infoFlags2 |= HEF2containsNonGlobalAlias;
        break;
    case no_param:
        infoFlags |= HEFgraphDependent;     // Need something better
        infoFlags2 &= ~HEF2workflow;
        break;
    case no_failure:
        infoFlags &= ~HEFonFailDependent;
        break;
    case no_externalcall:
        if (constant())
        {
            IHqlExpression * body = queryExternalDefinition()->queryChild(0);
            assertex(body);
            if (!body->hasAttribute(pureAtom))
                infoFlags2 &= ~HEF2constant;
        }
        break;
    case no_call:
        {
            IHqlExpression * funcdef = queryBody()->queryFunctionDefinition();
            IHqlExpression * body = funcdef->queryChild(0);
            if ((funcdef->getOperator() == no_funcdef) && (body->getOperator() == no_outofline))
            {
                infoFlags2 |= HEF2containsCall;
                IHqlExpression * bodycode = body->queryChild(0);
                if (bodycode->getOperator() == no_embedbody)
                {
                    if (bodycode->queryAttribute(actionAtom))
                        infoFlags |= HEFvolatile;
                }
            }
            else
                infoFlags2 |= (HEF2containsCall|HEF2containsDelayedCall);
            break;
        }
    case no_workunit_dataset:
    case no_getresult:
        {
            if (false && matchesConstantValue(queryAttributeChild(this, sequenceAtom, 0), ResultSequenceOnce))
                infoFlags |= HEFvolatile;
            break;
        }
    case no_embedbody:
        {
            if (queryAttribute(actionAtom))
                infoFlags |= HEFvolatile;
            break;
        }
    }

#ifdef VERIFY_EXPR_INTEGRITY
switch (op)
    {
    case no_select:
#ifdef CHECK_RECORD_CONSISTENCY
        {
            //Paranoid check to ensure that illegal no_selects aren't created (e.g., when dataset has link counted child, but field of select isn't)
            IHqlExpression * field = queryChild(1);
            IHqlExpression * lhsRecord = queryChild(0)->queryRecord();
            if (lhsRecord && lhsRecord->numChildren() && field->getOperator() == no_field)
            {
                OwnedHqlExpr resolved = lhsRecord->querySimpleScope()->lookupSymbol(field->queryId());
                if (resolved && resolved != field)
                    EclIR::dump_ir(resolved.get(), field);
                assertex(!resolved || resolved == field);
            }
        }
#endif
        break;
    case no_translated:
        assertex(queryUnqualifiedType(queryType()) == queryUnqualifiedType(queryChild(0)->queryType()));
        break;
    case no_transform:
    case no_newtransform:
        {
//          assertex(op != no_newtransform || !queryChildOperator(no_assignall, this));
            assertex(queryType() && queryType()->getTypeCode() == type_transform);
        }
        break;
    case no_assign:
#ifdef CHECK_RECORD_CONSISTENCY
        assertex(queryChild(0)->getOperator() != no_assign);
        assertex(queryChild(1)->getOperator() != no_assign);
        {
            IHqlExpression * lhsRecord = queryChild(0)->queryRecord();
            IHqlExpression * rhsRecord = queryChild(1)->queryRecord();
            if (lhsRecord && rhsRecord)
            {
                //This condition can be broken inside a transform that changes the types of fields.
                //It could possibly be avoided by more selective calls to transform.
                assertex(recordTypesMatch(lhsRecord, rhsRecord));
            }
        }
#endif
#ifdef PARANOID
        assertex(queryChild(1)->getOperator() != no_field);
#endif
        break;
#ifdef PARANOID
    case no_field:
        assertex(!queryChild(0) || queryChild(0)->getOperator() != no_field);
        break;
#endif
    case no_self:
        assertex(queryType());
        break;
    case no_getresult:
        {
            assertex(!isAction());
            break;
        }
    case no_and:
    case no_or:
        assertex(queryChild(1));
        break;
    case no_getgraphresult:
        assertex(!isAction());
        break;
    case no_getgraphloopresult:
        assertex(!isAction());
        break;
    case no_in:
    case no_notin:
    case no_eq:
    case no_ne:
    case no_ge:
    case no_gt:
    case no_le:
    case no_lt:
        assertex(queryType()->getTypeCode() == type_boolean);
        break;
    case no_funcdef:
        {
            assertex(queryType()->getTypeCode() == type_function && queryChild(1) && queryChild(1)->getOperator() == no_sortlist);
            assertex(queryType()->queryChildType() == queryChild(0)->queryType());
            break;
        }
    case no_setresult:
    case no_extractresult:
        {
            IHqlExpression * child = queryChild(0);
            //Don't do setresult, outputs completely in thor because thor can't do the necessary setresults.  Yet.
            assertex(!(child->getOperator() == no_thor && child->queryType()->getTypeCode() != type_void));
            break;
        }
    case no_implicitcast:
        assertex(queryType()->getTypeCode() != type_function);
        assertex(queryChild(0)->queryType()->getTypeCode() != type_function);
        break;
    case no_newsoapcall:
    case no_soapcall:
        {
            IHqlExpression * onFail = queryAttribute(onFailAtom);
            if (onFail)
            {
                IHqlExpression * transform = onFail->queryChild(0);
                if (transform->getOperator() != no_skip)
                    assertex(recordTypesMatch(transform, this));
            }
            break;
        }
    case no_indict:
        assertex(queryChild(1)->isDictionary());
        break;
    case no_countdict:
        assertex(queryChild(0)->isDictionary());
        break;
    }

#ifdef _DEBUG
    if (op == no_select)
    {
        IHqlExpression * ds = queryChild(0);
        if ((ds->getOperator() == no_select) && !ds->isDataset())
        {
            assertex(!hasAttribute(newAtom));
            if (hasAttribute(newAtom) && isNewSelector(ds))
            {
                IHqlExpression * root = queryDatasetCursor(ds);
                if (root->isDataset())
                    throwUnexpected();
            }
        }
    }
#endif

#if 0
    //Useful code for detecting when types get messed up for comparisons
    if (op == no_eq || op == no_ne)
    {
        IHqlExpression * left = queryChild(0);
        IHqlExpression * right = queryChild(1);
        ITypeInfo * leftType = left->queryType()->queryPromotedType();
        ITypeInfo * rightType = right->queryType()->queryPromotedType();
        if (!areTypesComparable(leftType,rightType))
            left = right;
    }
#endif
#endif      // _DEBUG

    ITypeInfo * thisType = queryType();
    if (thisType)
    {
        type_t tc = thisType->getTypeCode();
        switch (tc)
        {
        case type_alien:
        case type_scope:
            {
                IHqlExpression * typeExpr = queryExpression(thisType);
                if (typeExpr)
                {
                    infoFlags |= (typeExpr->getInfoFlags() & HEFalwaysInherit);
                    infoFlags2 |= (typeExpr->getInfoFlags2() & HEF2alwaysInherit);
                }
                break;
            }
        case type_dictionary:
        case type_groupedtable:
        case type_table:
            {
                //Not strictly true for nested counters..
                infoFlags &= ~HEFcontainsCounter;
            }
            //fall through
        case type_row:
        case type_transform:
            {
                IHqlExpression * record = queryRecord();
                if (record)
                {
                    infoFlags |= (record->getInfoFlags() & HEFalwaysInherit);
                    infoFlags2 |= (record->getInfoFlags2() & HEF2alwaysInherit);
                }

                switch (op)
                {
                case no_fail: case no_assert: case no_assert_ds: case no_externalcall: case no_libraryscopeinstance:
                case no_call:
                    break;
                case no_transform: case no_newtransform:
                    infoFlags &= ~HEFoldthrows;
                    break;
                default:
                    infoFlags &= ~HEFthrowscalar;
                    infoFlags &= ~HEFoldthrows;
                    if (tc == type_row)
                        infoFlags &= ~HEFthrowds;
                    break;
                }
                break;
            }
        case type_void:
            if (op != no_assign)
                infoFlags &= ~HEFthrowds;
            break;
        }
    }

#ifdef CHECK_SELSEQ_CONSISTENCY
    unsigned uidCount = 0;
    ForEachChild(i, this)
    {
        IHqlExpression * cur = queryChild(i);
        if (cur->isAttribute() && cur->queryName() == _uid_Atom)
            uidCount++;
    }

    switch (uidCount)
    {
    case 0:
        assertex(!definesColumnList(this));
        break;
    case 1:
        if (queryRecord())
            assertex(definesColumnList(this));
        break;
    default:
        throwUnexpected();
    }

    /*
    switch (getChildDatasetType(this))
    {
    case childdataset_left:
    case childdataset_leftright:
    case childdataset_same_left_right:
    case childdataset_top_left_right:
    case childdataset_datasetleft:
    case childdataset_nway_left_right:
        assertex(op == no_keydiff || querySelSeq(this));
        break;
    }
    */
#endif
}


void CHqlExpression::onAppendOperand(IHqlExpression & child, unsigned whichOperand)
{
    //MORE: All methods that use flags updated here need to be overridden in CHqlNamedExpr()
    bool updateFlags = true;
    unsigned childFlags = child.getInfoFlags();
    unsigned childFlags2 = child.getInfoFlags2();
    node_operator childOp = child.getOperator();
    switch (op)
    {
    case no_keyindex:
    case no_newkeyindex:
    case no_joined:
        if (whichOperand == 0)
            updateFlags = false;
        break;
    case no_activerow:
        updateFlags = false;
        break;
    case no_sizeof:
    case no_offsetof:
    case no_param:          // don't inherit attributes of default values in the function body.
    case no_nameof:
        updateFlags = false;
        break;
    case no_limit:
    case no_keyedlimit:
        if (whichOperand > 1)
            childFlags &= ~(HEFthrowscalar|HEFthrowds);
        break;
#ifdef _DEBUG
    case no_transform:
        {
            switch (childOp)
            {
            case no_assign:
            case no_assignall:
            case no_attr:
            case no_attr_link:
            case no_attr_expr:
            case no_alias_scope:
            case no_skip:
            case no_assert:
                break;
            default:
                UNIMPLEMENTED;
            }
            break;
        }
    case no_record:
        {
            switch (childOp)
            {
            case no_field: 
            case no_ifblock:
            case no_record:
            case no_attr:
            case no_attr_expr:
            case no_attr_link:
                break;
            default:
                UNIMPLEMENTED;
            }
            break;
        }
#endif
    }

    switch (childOp)
    {
    case no_transform:
    case no_newtransform:
        childFlags &= ~(HEFtransformDependent|HEFcontainsSkip|HEFthrowscalar|HEFthrowds);
        break;
    }

    if (updateFlags)
    {

        //These are cleared if not set in the child.
        infoFlags &= (childFlags | ~(HEFintersectionFlags));

        //These are set if set in the child
        infoFlags |= (childFlags & HEFunionFlags);

        infoFlags2 &= (childFlags2 | ~(HEF2intersectionFlags));
        infoFlags2 |= (childFlags2 & HEF2unionFlags);
    }
    else
    {
        infoFlags |= (childFlags & HEFalwaysInherit);
        infoFlags2 |= (childFlags2 & HEF2alwaysInherit);
    }
#ifdef _DEBUG
    //This should never occur on legal code, but can occur on illegal, so only check in debug mode.
    if (childOp == no_field)
        assertex(op == no_record || op == no_select || op == no_comma || op == no_attr || op == no_attr_expr || op == no_indirect);
#endif
}

CHqlExpression::~CHqlExpression()
{
//  DBGLOG("%lx: Destroy", (unsigned)(IHqlExpression *)this);
    delete attributes;
}

IHqlScope * CHqlExpression::queryScope() 
{
    //better, especially in cascaded error situations..
    if (op == no_compound)
        return queryChild(1)->queryScope();
    return NULL; 
}

IHqlSimpleScope * CHqlExpression::querySimpleScope() 
{ 
    if (op == no_compound)
        return queryChild(1)->querySimpleScope();
    return NULL; 
}

#define HASHFIELD(p) hashcode = hashc((unsigned char *) &p, sizeof(p), hashcode)

void CHqlExpression::setInitialHash(unsigned typeHash)
{
    hashcode = op+typeHash;
    unsigned kids = operands.ordinality();
    hashcode = hashc((const unsigned char *)operands.getArray(), kids * sizeof(IHqlExpression *), hashcode);
}

void CHqlExpression::sethash()
{
    // In 64-bit, just use bottom 32-bits of the ptr for the hash
    setInitialHash((unsigned) (memsize_t) queryType());
}

unsigned CHqlExpression::getCachedEclCRC()
{
    if (cachedCRC)
        return cachedCRC;
    
    unsigned crc = op;
    switch (op)
    {
    case no_record:
    case no_type:
        break;
    case no_self:
        //ignore new record argument
        cachedCRC = crc;
        return crc;
    case no_selfref:
        crc = no_self;
        break;
    case no_assertconstant:
    case no_assertconcrete:
        return queryChild(0)->getCachedEclCRC();
    case no_sortlist:
        //backward compatibility
        break;
    case no_attr_expr:
        {
            const IAtom * name = queryBody()->queryName();
            //Horrible backward compatibility "fix" for record crcs - they need to remain the same otherwise files
            //will be incompatible.
            if (name == maxLengthAtom || name == xpathAtom || name == cardinalityAtom || name == caseAtom || name == maxCountAtom || name == choosenAtom || name == maxSizeAtom || name == namedAtom || name == rangeAtom || name == xmlDefaultAtom || name == virtualAtom)
                crc = no_attr;
            //fallthrough
        }
    default:
        {
            ITypeInfo * thisType = queryType();
            if (thisType && this != queryExpression(thisType))
            {
                ITypeInfo * hashType = thisType;
                switch (hashType->getTypeCode())
                {
                case type_transform:
                    hashType = hashType->queryChildType();
                    break;
                case type_row:
                    //Backward compatibility
                    if (op == no_field)
                        hashType = hashType->queryChildType();
                    break;
                }
                unsigned typeCRC = hashType->getCrc();
                crc = hashc((const byte *)&typeCRC, sizeof(typeCRC), crc);
            }
            break;
        }
    }

    unsigned numChildrenToHash = numChildren();
    switch (op)
    {
    case no_constant:
        crc = queryValue()->getHash(crc);
        break;
    case no_field:
        {
            IAtom * name = queryName();
            if (name)
            {
                const char * nameText = name->str();
                if ((nameText[0] != '_') || (nameText[1] != '_'))
                    crc = hashnc((const byte *)nameText, strlen(nameText), crc);
            }
            IHqlExpression * record = queryRecord();
            if (record)
                crc ^= record->getCachedEclCRC();
            break;
        }
    case no_call:
        crc ^= queryBody()->queryFunctionDefinition()->getCachedEclCRC();
        break;
    case no_externalcall:
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        {
            const IAtom * name = queryBody()->queryName();
            if (name == _uid_Atom)
                return 0;
            const char * nameText = name->str();
            crc = hashnc((const byte *)nameText, strlen(nameText), crc);
            break;
        }
    case no_libraryscopeinstance:
        {
            IHqlExpression * scopeFunc = queryDefinition();
            IHqlExpression * moduleExpr = scopeFunc->queryChild(0); 
            crc ^= moduleExpr->getCachedEclCRC();
            break;
        }

    case no_libraryscope:
    case no_virtualscope:
        {
            //include information about symbols in the library scope crc, so that if the interface changes it forces a rebuild.
            HqlExprArray symbols;
            queryScope()->getSymbols(symbols);
            symbols.sort(compareSymbolsByName);
            ForEachItemIn(i, symbols)
            {
                IHqlExpression & cur = symbols.item(i);
                if (cur.isExported())
                {
                    unsigned crc2 = symbols.item(i).getCachedEclCRC();
                    crc = hashc((const byte *)&crc2, sizeof(crc2), crc);
                }
            }
            break;
        }
    }

    for (unsigned idx=0; idx < numChildrenToHash; idx++)
    {
        unsigned childCRC = queryChild(idx)->getCachedEclCRC();
        if (childCRC)
            crc = hashc((const byte *)&childCRC, sizeof(childCRC), crc);
    }
    cachedCRC = crc;
    return crc;
}


bool CHqlExpression::equals(const IHqlExpression & other) const
{
    if (!isAlive()) return false;
#ifndef CONSISTENCY_CHECK
    if (this == &other)
        return true;
#endif
    if (other.isAnnotation())
        return false;
    if (op != other.getOperator())
        return false;
    switch (op)
    {
    case no_record:
    case no_type:
    case no_scope:
    case no_service:
    case no_virtualscope:
    case no_concretescope:
    case no_libraryscope:
    case no_libraryscopeinstance:
        break;
    default:
        if (queryType() != other.queryType())
            return false;
        break;
    }
    unsigned kids = other.numChildren();
    if (kids != numChildren())
        return false;
    for (unsigned kid = 0; kid < kids; kid++)
    {
        if (queryChild(kid) != other.queryChild(kid))
            return false;
    }
    return true;
}

IHqlExpression *CHqlExpression::closeExpr()
{
    assertex(!isExprClosed());   // closeExpr() shouldn't be called twice
    updateFlagsAfterOperands();
    sethash();
    return commonUpExpression();
}

IHqlScope * closeScope(IHqlScope * scope)
{
    IHqlExpression * expr = queryExpression(scope);
    return expr->closeExpr()->queryScope();
}

bool getAttribute(IHqlExpression * expr, IAtom * propname, StringBuffer &ret)
{
    IHqlExpression* match = expr->queryAttribute(propname);
    if (match)
    {
        IHqlExpression * value = match->queryChild(0);
        if (value)
            value->queryValue()->getStringValue(ret);
        return true;
    }
    return false;
}

IHqlExpression *CHqlExpression::queryAttribute(IAtom * propname) const
{
    unsigned kids = numChildren();
    for (unsigned i = 0; i < kids; i++)
    {
        IHqlExpression *kid = queryChild(i);
        if (kid->isAttribute() && kid->queryName()==propname)
            return kid;
    }
    return NULL;
}

unsigned CHqlExpression::getSymbolFlags() const
{
    throwUnexpected();
    return 0;
}

IHqlExpression *CHqlExpression::queryChild(unsigned idx) const
{
    if (operands.isItem(idx))
        return &operands.item(idx);
    else
        return NULL;
}

unsigned CHqlExpression::numChildren() const
{
    return operands.length();
}

IHqlExpression *CHqlExpression::addOperand(IHqlExpression * child)
{
    //Forward scopes are never commoned up, and are shared as a side-effect of keeping references to their owner
    assertex (!IsShared() || (op == no_forwardscope));
    assertex (!isExprClosed());
    doAppendOperand(*child);
    return this;
}

inline bool matchesTypeCode(ITypeInfo * type, type_t search)
{
    loop
    {
        if (!type)
            return false;

        type_t tc = type->getTypeCode();
        if (tc == search)
            return true;
        if (tc != type_function)
            return false;
        type = type->queryChildType();
    }
}


bool CHqlExpression::isBoolean() 
{
    return matchesTypeCode(queryType(), type_boolean);
}

bool CHqlExpression::isConstant() 
{
    return constant();
}

bool CHqlExpression::isDataset() 
{
    ITypeInfo * cur = queryType();
    loop
    {
        if (!cur)
            return false;

        switch(cur->getTypeCode())
        {
        case type_groupedtable:
        case type_table:
            return true;
        case type_function:
            cur = cur->queryChildType();
            break;
        default:
            return false;
        }
    }
}

bool CHqlExpression::isDictionary()
{
    return matchesTypeCode(queryType(), type_dictionary);
}

bool CHqlExpression::isDatarow() 
{
    return matchesTypeCode(queryType(), type_row);
}

bool CHqlExpression:: isFunction() 
{ 
    ITypeInfo * thisType = queryType();
    return thisType && thisType->getTypeCode() == type_function;
} 

bool CHqlExpression::isMacro() 
{ 
    switch (op)
    {
    case no_macro:
        return true;
    case no_funcdef:
        return queryChild(0)->isMacro();
    }
    return false;
} 

bool CHqlExpression::isRecord() 
{
    return matchesTypeCode(queryType(), type_record);
}
 
bool CHqlExpression::isAction() 
{
    return matchesTypeCode(queryType(), type_void);
}

bool CHqlExpression::isTransform() 
{
    return matchesTypeCode(queryType(), type_transform);
}

bool CHqlExpression::isScope() 
{
    return matchesTypeCode(queryType(), type_scope);
}

bool CHqlExpression::isField() 
{
    return op == no_field;
}

bool CHqlExpression::isType() 
{
    switch(op)
    {
    case no_type:
        return true;
    case no_funcdef:
        return queryChild(0)->isType();
    default:
        return false;
    }
}

bool CHqlExpression::isList() 
{
    return matchesTypeCode(queryType(), type_set);
}

bool CHqlExpression::isAggregate()
{
    //This is only used for HOLe processing - I'm not sure how much sense it really makes.
    switch(op)
    {
    case NO_AGGREGATE:
    case NO_AGGREGATEGROUP:
    case no_distribution:


    case no_selectnth:
    case no_evaluate:
        return true;
    case no_select:
        return (queryChild(0)->getOperator() == no_selectnth);
    default:
        return false;
    }
}

StringBuffer &CHqlExpression::toString(StringBuffer &ret)
{
#ifdef TRACE_THIS
    ret.appendf("[%lx]", this);
#endif
    ret.appendf("%s", getOpString(op));
    return ret;
}


void CHqlExpression::unwindList(HqlExprArray &dst, node_operator u_op)
{
    if (op==u_op)
    {
        ForEachChild(idx, this)
            queryChild(idx)->unwindList(dst, u_op);
    }
    else
    {
        Link();
        dst.append((IHqlExpression &)*this);
    }
}

ITypeInfo *CHqlExpression::queryRecordType()
{
    return ::queryRecordType(queryType());
}

IHqlExpression *CHqlExpression::queryRecord()
{
    ITypeInfo *t = queryRecordType();
    if (t)
        return queryExpression(t);
    return NULL;
}

//== Commoning up code.... ==
void CHqlExpression::Link(void) const
{ 
#ifdef GATHER_LINK_STATS
    numLinks++;
    if (insideCreate)
        numCreateLinks++;
#endif
    Parent::Link();
    CHECK_EXPR_SEQID(1);
}

bool CHqlExpression::Release(void) const
{ 
#ifdef GATHER_LINK_STATS
    numReleases++;
    if (insideCreate)
        numCreateReleases++;
#endif
    CHECK_EXPR_SEQID(2);
    return Parent::Release();
}


void CHqlExpression::beforeDispose()
{
    CHECK_EXPR_SEQID(3);
#ifdef CONSISTENCY_CHECK
    if (hashcode)
    {
        unsigned oldhash = hashcode;
        sethash();
        assertex(hashcode == oldhash);
    }
    assertex(equals(*this));
#endif
    if (infoFlags & HEFobserved)
    {
        CriticalBlock block(*exprCacheCS);
        if (infoFlags & HEFobserved)
            exprCache->removeExact(this);
    }
    assertex(!(infoFlags & HEFobserved));
}


unsigned CHqlExpression::getHash() const                
{ 
    return hashcode; 
}


void CHqlExpression::addObserver(IObserver & observer)
{
    assertex(!(infoFlags & HEFobserved));
    assert(&observer == exprCache);
    infoFlags |= HEFobserved;
}

void CHqlExpression::removeObserver(IObserver & observer)
{
    assertex((infoFlags & HEFobserved));
    assert(&observer == exprCache);
    infoFlags &= ~HEFobserved;
}


IHqlExpression * CHqlExpression::commonUpExpression()
{
    switch (op)
    {
        //I'm still not completely convinced that commoning up parameters doesn't cause problems.
        //e.g. if a parameter from one function is passed into another.  Don't common up for the moment....
        //And parameter index must include all parameters in enclosing scopes as well.
    case no_uncommoned_comma:
        return this;
    case no_service:
        return this;
    case no_privatescope:
    case no_mergedscope:
        return this;
    case no_remotescope:
        if (isAnnotation())
            return this;
        throwUnexpectedOp(op);
    }

    IHqlExpression * match;
    {
        CriticalBlock block(*exprCacheCS);
        match = exprCache->addOrFind(*this);
#ifndef GATHER_COMMON_STATS
        if (match == this)
            return this;
#endif
        match->Link();
        if (!static_cast<CHqlExpression *>(match)->isAlive())
        {
            exprCache->replace(*this);
#ifndef GATHER_COMMON_STATS
            return this;
#endif
            Link();
            match = this;
        }
    }

#ifdef GATHER_COMMON_STATS
    node_operator statOp = op;

    if (isAnnotation())
    {
        annotate_kind  kind = getAnnotationKind();
        commonUpAnnCount[kind]++;
        if (match != this)
            commonUpAnnClash[kind]++;
    }
    else
    {
        commonUpCount[statOp]++;
        if (match != this)
            commonUpClash[statOp]++;
    }
#endif
    Release();
    return match;
}

IHqlExpression * CHqlExpression::calcNormalizedSelector() const
{
    IHqlExpression * left = &operands.item(0);
    IHqlExpression * normalizedLeft = left->queryNormalizedSelector();
    if ((normalizedLeft != left) || ((operands.ordinality() > 2) && hasAttribute(newAtom)))
    {
        HqlExprArray args;
        appendArray(args, operands);
        args.replace(*LINK(normalizedLeft), 0);
        removeAttribute(args, newAtom);
        return doCreateSelectExpr(args);
    }
    return NULL;
}

void displayHqlCacheStats()
{
#if 0
    static HqlExprCopyArray prev;
    DBGLOG("CachedItems = %d", exprCache->count());
    exprCache->dumpStats();
    JavaHashIteratorOf<IHqlExpression> iter(*exprCache, false);
    ForEach(iter)
    {
        IHqlExpression & ret = iter.query();
        if (!prev.contains(ret))
        {
            StringBuffer s;
            processedTreeToECL(&ret, s);
            DBGLOG("%p: %s", &ret, s.str());
        }
    }

    prev.kill();
    ForEach(iter)
    {
        prev.append(iter.query());
    }
#endif
}
//--------------------------------------------------------------------------------------------------------------

CHqlExpressionWithType::CHqlExpressionWithType(node_operator _op, ITypeInfo * _type, HqlExprArray & _ownedOperands)
: CHqlExpressionWithTables(_op)
{
    type = _type;
    setOperands(_ownedOperands); // after type is initialized
}

CHqlExpressionWithType::~CHqlExpressionWithType()
{
    ::Release(type);
}


ITypeInfo *CHqlExpressionWithType::queryType() const
{
    return type;
}

ITypeInfo *CHqlExpressionWithType::getType()
{
    ::Link(type);
    return type;
}

CHqlExpression *CHqlExpressionWithType::makeExpression(node_operator _op, ITypeInfo *_type, HqlExprArray &_ownedOperands)
{
    CHqlExpression *e = new CHqlExpressionWithType(_op, _type, _ownedOperands);
    return (CHqlExpression *)e->closeExpr();
}

CHqlExpression *CHqlExpressionWithType::makeExpression(node_operator _op, ITypeInfo *_type, ...)
{
    unsigned numArgs = 0;
    va_list argscount;
    va_start(argscount, _type);
    for (;;)
    {
        IHqlExpression *parm = va_arg(argscount, IHqlExpression *);
        if (!parm)
            break;
        numArgs++;
    }
    va_end(argscount);

    HqlExprArray operands;
    if (numArgs)
    {
        va_list args;
        va_start(args, _type);
        operands.ensure(numArgs);
        for (;;)
        {
            IHqlExpression *parm = va_arg(args, IHqlExpression *);
            if (!parm)
                break;
#ifdef _DEBUG
            assertex(QUERYINTERFACE(parm, IHqlExpression));
#endif
            operands.append(*parm);
        }
        va_end(args);
    }
    return makeExpression(_op, _type, operands);
}

IHqlExpression *CHqlExpressionWithType::clone(HqlExprArray &newkids)
{
    if ((newkids.ordinality() == 0) && (operands.ordinality() == 0))
        return LINK(this);

    ITypeInfo * newType = NULL;
    switch (op)
    {
    case no_outofline:
        return createWrapper(op, newkids);
    case no_embedbody:
        {
            if (queryType()->getTypeCode() == type_transform)
            {
                IHqlExpression & newRecord = newkids.item(1);
                if (&newRecord != queryChild(1))
                    newType = makeTransformType(LINK(newRecord.queryType()));
            }
            break;
        }
    }

    if (!newType)
        newType = LINK(type);
    return CHqlExpressionWithType::makeExpression(op, newType, newkids);
}

//--------------------------------------------------------------------------------------------------------------

CHqlNamedExpression::CHqlNamedExpression(node_operator _op, ITypeInfo *_type, IIdAtom * _id, ...) : CHqlExpressionWithType(_op, _type)
{
    id = _id;

    va_list args;
    va_start(args, _id);
    for (;;)
    {
        IHqlExpression *parm = va_arg(args, IHqlExpression *);
        if (!parm)
            break;
#ifdef _DEBUG
        assertex(QUERYINTERFACE(parm, IHqlExpression));
#endif
        doAppendOperand(*parm);
    }
    va_end(args);
}

CHqlNamedExpression::CHqlNamedExpression(node_operator _op, ITypeInfo *_type, IIdAtom * _id, HqlExprArray &_ownedOperands) : CHqlExpressionWithType(_op, _type, _ownedOperands)
{
    id = _id;
}


IHqlExpression *CHqlNamedExpression::clone(HqlExprArray &newkids)
{
    if (op == no_funcdef)
        return createFunctionDefinition(id, newkids);
    return (new CHqlNamedExpression(op, getType(), id, newkids))->closeExpr();
}

bool CHqlNamedExpression::equals(const IHqlExpression & r) const
{
    if (CHqlExpression::equals(r))
    {
        if (queryName() == r.queryName())
            return true;
    }
    return false;
}


void CHqlNamedExpression::sethash()
{
    CHqlExpression::sethash();
    HASHFIELD(id);
}

IHqlExpression *createNamedValue(node_operator op, ITypeInfo *type, IIdAtom * id, HqlExprArray & args)
{
    return (new CHqlNamedExpression(op, type, id, args))->closeExpr();
}

IHqlExpression *createId(IIdAtom * id)
{
    HqlExprArray args;
    return createNamedValue(no_id, makeNullType(), id, args);
}

//--------------------------------------------------------------------------------------------------------------

inline void addUniqueTable(HqlExprCopyArray & array, IHqlExpression * ds)
{
    if (array.find(*ds) == NotFound)
        array.append(*ds);
}

inline void addHiddenTable(HqlExprCopyArray & array, IHqlExpression * ds, IHqlExpression * selSeq)
{
#if defined(GATHER_HIDDEN_SELECTORS)
    if (array.find(*ds) == NotFound)
        array.append(*ds);
#endif
}

inline void addActiveTable(HqlExprCopyArray & array, IHqlExpression * ds)
{
    //left.subfield  should be reduced the base cursor
    ds = queryDatasetCursor(ds);

//  This test is valid once the tree is normalized, but now this can be called on a parse tree.
//  assertex(ds == ds->queryNormalizedSelector());          
    node_operator dsOp = ds->getOperator();
    if (dsOp != no_self && dsOp != no_selfref)
        addUniqueTable(array, ds->queryNormalizedSelector());
}


//---------------------------------------------------------------------------------------------------------------------

CUsedTables::CUsedTables()
{
    tables.single = NULL;
    numTables = 0;
    numActiveTables = 0;
}

CUsedTables::~CUsedTables()
{
    if (numTables > 1)
        delete [] tables.multi;
}


bool CUsedTables::usesSelector(IHqlExpression * selector) const
{
    if (numTables > 1)
    {
        for (unsigned i=0; i < numActiveTables; i++)
        {
            if (tables.multi[i] == selector)
                return true;
        }
        return false;
    }

    //Following works if numTables == 0
    return (selector == tables.single);
}

void CUsedTables::gatherTablesUsed(CUsedTablesBuilder & used) const
{
    if (numTables == 0)
        return;
    if (numTables == 1)
    {
        if (numActiveTables == 1)
            used.addActiveTable(tables.single);
        else
            used.addNewTable(tables.single);
    }
    else
    {
        for (unsigned i1=0; i1 < numActiveTables; i1++)
            used.addActiveTable(tables.multi[i1]);
        for (unsigned i2=numActiveTables; i2 < numTables; i2++)
            used.addNewTable(tables.multi[i2]);
    }
}

void CUsedTables::gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope) const
{
    if (numTables == 0)
        return;
    if (numTables == 1)
    {
        if (numActiveTables == 1)
        {
            if (inScope)
                addUniqueTable(*inScope, tables.single);
        }
        else
        {
            if (newScope)
                addUniqueTable(*newScope, tables.single);
        }
    }
    else
    {
        if (inScope)
        {
            for (unsigned i1=0; i1 < numActiveTables; i1++)
                addUniqueTable(*inScope, tables.multi[i1]);
        }
        if (newScope)
        {
            for (unsigned i2=numActiveTables; i2 < numTables; i2++)
                addUniqueTable(*newScope, tables.multi[i2]);
        }
    }
}


void CUsedTables::set(HqlExprCopyArray & activeTables, HqlExprCopyArray & newTables)
{
    numActiveTables = activeTables.ordinality();
    numTables = numActiveTables + newTables.ordinality();
    if (numTables == 1)
    {
        if (numActiveTables == 1)
        {
            tables.single = &activeTables.item(0);
        }
        else
        {
            tables.single = &newTables.item(0);
        }
    }
    else if (numTables != 0)
    {
        IHqlExpression * * multi = new IHqlExpression * [numTables];
        for (unsigned i1=0; i1 < numActiveTables; i1++)
            multi[i1] = &activeTables.item(i1);
        for (unsigned i2=numActiveTables; i2 < numTables; i2++)
            multi[i2] = &newTables.item(i2-numActiveTables);
        tables.multi = multi;
    }
}

void CUsedTables::setActiveTable(IHqlExpression * expr)
{
    tables.single = expr;
    numTables = 1;
    numActiveTables = 1;
}

//---------------------------------------------------------------------------------------------------------------------

void CUsedTablesBuilder::addNewTable(IHqlExpression * expr)
{
    addUniqueTable(newScopeTables, expr);
}

void CUsedTablesBuilder::addHiddenTable(IHqlExpression * expr, IHqlExpression * selSeq)
{
    ::addHiddenTable(newScopeTables, expr, selSeq);
}

void CUsedTablesBuilder::addActiveTable(IHqlExpression * expr)
{
    ::addActiveTable(inScopeTables, expr);
}

void CUsedTablesBuilder::cleanupProduction()
{
    ForEachItemInRev(i, inScopeTables)
    {
        switch (inScopeTables.item(i).getOperator())
        {
        case no_matchattr:
        case no_matchrow:
            inScopeTables.remove(i);
            break;
        }
    }
}

void CUsedTablesBuilder::removeParent(IHqlExpression * expr)
{
    IHqlExpression * sel = expr->queryNormalizedSelector();
    removeActive(sel);

    loop
    {
        IHqlExpression * root = queryRoot(expr);
        if (!root || root->getOperator() != no_select)
            break;
        if (!root->hasAttribute(newAtom))
            break;
        expr = root->queryChild(0);
        removeActive(expr->queryNormalizedSelector());
    }
}

void CUsedTablesBuilder::removeActiveRecords()
{
    ForEachItemInRev(i, inScopeTables)
    {
        if (inScopeTables.item(i).isRecord())
            inScopeTables.remove(i);
    }
}

void CUsedTablesBuilder::removeRows(IHqlExpression * expr, IHqlExpression * left, IHqlExpression * right)
{
    node_operator rowsSide = queryHasRows(expr);
    if (rowsSide == no_none)
        return;

    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);
    switch (rowsSide)
    {
    case no_left:
        {
            OwnedHqlExpr rowsExpr = createDataset(no_rows, LINK(left), LINK(rowsid));
            inScopeTables.zap(*rowsExpr);
            break;
        }
    case no_right:
        {
            OwnedHqlExpr rowsExpr = createDataset(no_rows, LINK(right), LINK(rowsid));
            inScopeTables.zap(*rowsExpr);
            break;
        }
    default:
        throwUnexpectedOp(rowsSide);
    }
}

//---------------------------------------------------------------------------------------------------------------------

//Don't need to check if already visited, because the information is cached in the expression itself.
void CHqlExpressionWithTables::cacheChildrenTablesUsed(CUsedTablesBuilder & used, unsigned from, unsigned to)
{
    for (unsigned i=from; i < to; i++)
        queryChild(i)->gatherTablesUsed(used);
}


void CHqlExpressionWithTables::cacheInheritChildTablesUsed(IHqlExpression * ds, CUsedTablesBuilder & used, const HqlExprCopyArray & childInScopeTables)
{
    //The argument to the operator is a new table, don't inherit grandchildren
    used.addNewTable(ds);

    //Any datasets in that are referenced by the child are included, but not including
    //the dataset itself.
    IHqlExpression * normalizedDs = ds->queryNormalizedSelector();
    ForEachItemIn(idx, childInScopeTables)
    {
        IHqlExpression & cur = childInScopeTables.item(idx);
        if (&cur != normalizedDs)
            used.addActiveTable(&cur);
    }
}

void CHqlExpressionWithTables::cacheTableUseage(CUsedTablesBuilder & used, IHqlExpression * expr)
{
#ifdef GATHER_HIDDEN_SELECTORS
    expr->gatherTablesUsed(used);
#else
    if (expr->getOperator() == no_activerow)
        used.addActiveTable(expr->queryChild(0));
    else
    {
        HqlExprCopyArray childInScopeTables;
        expr->gatherTablesUsed(NULL, &childInScopeTables);

        //The argument to the operator is a new table, don't inherit grandchildren
        cacheInheritChildTablesUsed(expr, used, childInScopeTables);
    }
#endif
}

void CHqlExpressionWithTables::cachePotentialTablesUsed(CUsedTablesBuilder & used)
{
    ForEachChild(i, this)
    {
        IHqlExpression * cur = queryChild(i);
        if (cur->isDataset())
            cacheTableUseage(used, cur);
        else
            cur->gatherTablesUsed(used);
    }
}


void CHqlExpressionWithTables::cacheTablesProcessChildScope(CUsedTablesBuilder & used, bool ignoreInputs)
{
    unsigned max = numChildren();
    switch (getChildDatasetType(this))
    {
    case childdataset_none: 
        cacheChildrenTablesUsed(used, 0, max);
        break;
    case childdataset_many_noscope:
        if (ignoreInputs)
        {
            unsigned first = getFirstActivityArgument(this);
            unsigned last = first + getNumActivityArguments(this);
            cacheChildrenTablesUsed(used, 0, first);
            cacheChildrenTablesUsed(used, last, max);
        }
        else
            cacheChildrenTablesUsed(used, 0, max);
        break;
    case childdataset_dataset_noscope:
        if (ignoreInputs)
            cacheChildrenTablesUsed(used, 1, max);
        else
            cacheChildrenTablesUsed(used, 0, max);
        break;
    case childdataset_if:
        if (ignoreInputs)
            cacheChildrenTablesUsed(used, 0, 1);
        else
            cacheChildrenTablesUsed(used, 0, max);
        break;
    case childdataset_case:
    case childdataset_map:
        //Assume the worst
        cacheChildrenTablesUsed(used, 0, max);
        break;
    case childdataset_many:
        {
            //can now have sorted() attribute which is dependent on the no_activetable element.
            unsigned firstAttr = getNumChildTables(this);
            cacheChildrenTablesUsed(used, firstAttr, max);
            used.removeActive(queryActiveTableSelector());
            if (!ignoreInputs)
                cacheChildrenTablesUsed(used, 0, firstAttr);
            break;
        }
    case childdataset_dataset:
        {
            IHqlExpression * ds = queryChild(0);
            cacheChildrenTablesUsed(used, 1, max);
            used.removeParent(ds);
            if (!ignoreInputs)
                cacheChildrenTablesUsed(used, 0, 1);
        }
        break;
    case childdataset_datasetleft:
        {
            IHqlExpression * ds = queryChild(0);
            IHqlExpression * selSeq = querySelSeq(this);
            OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
            cacheChildrenTablesUsed(used, 1, max);
            used.removeActive(left);
            used.removeParent(ds);
            used.removeRows(this, left, NULL);
            switch (op)
            {
            case no_parse:
            case no_newparse:
                used.cleanupProduction();
                used.removeActive(queryNlpParsePseudoTable());
                break;
            case no_xmlparse:
            case no_newxmlparse:
                used.removeActive(queryXmlParsePseudoTable());
                break;
            }
            if (!ignoreInputs)
                cacheChildrenTablesUsed(used, 0, 1);
#ifdef GATHER_HIDDEN_SELECTORS
            used.addHiddenTable(left, selSeq);
#endif
            break;
        }
    case childdataset_left:
        { 
            IHqlExpression * ds = queryChild(0);
            IHqlExpression * selSeq = querySelSeq(this);
            OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
            cacheChildrenTablesUsed(used, 1, max);
            used.removeActive(left);
            used.removeRows(this, left, NULL);

            if (!ignoreInputs)
                cacheChildrenTablesUsed(used, 0, 1);
#ifdef GATHER_HIDDEN_SELECTORS
            used.addHiddenTable(left, selSeq);
#endif
            break;
        }
    case childdataset_same_left_right:
    case childdataset_nway_left_right:
        {
            IHqlExpression * ds = queryChild(0);
            IHqlExpression * selSeq = querySelSeq(this);
            OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
            OwnedHqlExpr right = createSelector(no_right, ds, selSeq);
            cacheChildrenTablesUsed(used, 1, max);
            used.removeActive(left);
            used.removeActive(right);
            used.removeRows(this, left, right);

            if (!ignoreInputs)
                cacheChildrenTablesUsed(used, 0, 1);
#ifdef GATHER_HIDDEN_SELECTORS
            used.addHiddenTable(left, selSeq);
            used.addHiddenTable(right, selSeq);
#endif
            break;
        }
    case childdataset_top_left_right:
        {
            IHqlExpression * ds = queryChild(0);
            IHqlExpression * selSeq = querySelSeq(this);
            OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
            OwnedHqlExpr right = createSelector(no_right, ds, selSeq);
            cacheChildrenTablesUsed(used, 1, max);
            used.removeParent(ds);
            used.removeActive(left);
            used.removeActive(right);
            used.removeRows(this, left, right);
            cacheChildrenTablesUsed(used, 0, 1);
#ifdef GATHER_HIDDEN_SELECTORS
            used.addHiddenTable(left, selSeq);
            used.addHiddenTable(right, selSeq);
#endif
            break;
        }
    case childdataset_leftright: 
        {
            IHqlExpression * leftDs = queryChild(0);
            IHqlExpression * rightDs = queryChild(1);
            IHqlExpression * selSeq = querySelSeq(this);
            OwnedHqlExpr left = createSelector(no_left, leftDs, selSeq);
            OwnedHqlExpr right = createSelector(no_right, rightDs, selSeq);
            cacheChildrenTablesUsed(used, 2, max);
            used.removeActive(right);
            used.removeRows(this, left, right);
            if (op == no_normalize)
            {
                //two datasets form of normalize is weird because right dataset is based on left
                cacheChildrenTablesUsed(used, 1, 2);
                used.removeActive(left);
                if (!ignoreInputs)
                    cacheChildrenTablesUsed(used, 0, 1);
            }
            else
            {
                used.removeActive(left);
                if (!ignoreInputs)
                    cacheChildrenTablesUsed(used, 0, 2);
            }
#ifdef GATHER_HIDDEN_SELECTORS
            used.addHiddenTable(left, selSeq);
            used.addHiddenTable(right, selSeq);
#endif
            break;
        }
        break;
    case childdataset_evaluate:
        //handled elsewhere...
    default:
        UNIMPLEMENTED;
    }
}


void CHqlExpressionWithTables::calcTablesUsed(CUsedTablesBuilder & used, bool ignoreInputs)
{
    switch (op)
    {
    case no_attr:
    case no_attr_link:
    case no_keyed:
    case no_colon:
    case no_cluster:
    case no_nameof:
    case no_translated:
    case no_constant:
        break;
    case no_select:
        {
            IHqlExpression * ds = queryChild(0);
            if (isSelectRootAndActive())
            {
                used.addActiveTable(ds);
            }
            else
            {
                ds->gatherTablesUsed(used);
            }
            break;
        }
    case no_activerow:
        used.addActiveTable(queryChild(0));
        break;
    case no_rows:
    case no_rowset:
        //MORE: This is a bit strange!
        used.addActiveTable(queryChild(0));
        break;
    case no_left:
    case no_right:
        used.addActiveTable(this);
        break;
    case no_counter:
        //NB: Counter is added as a pseudo table, because it is too hard to keep track of nested counters otherwise
        used.addActiveTable(this);
        break;
    case no_filepos:
    case no_file_logicalname:
        used.addActiveTable(queryChild(0));
        break;
    case NO_AGGREGATE:
    case no_createset:
        {
#ifdef GATHER_HIDDEN_SELECTORS
            cachePotentialTablesUsed(used);
            used.removeParent(queryChild(0));
#else
            HqlExprCopyArray childInScopeTables;
            ForEachChild(idx, this)
                queryChild(idx)->gatherTablesUsed(NULL, &childInScopeTables);

            //The argument to the operator is a new table, don't inherit grandchildren
            IHqlExpression * ds = queryChild(0);
            cacheInheritChildTablesUsed(ds, used, childInScopeTables);
#endif
        }
        break;
    case no_sizeof:
        cachePotentialTablesUsed(used);
        used.removeActive(queryActiveTableSelector());
        used.removeActiveRecords();
        break;
    case no_externalcall:
    case no_rowvalue:
    case no_offsetof:
    case no_eq:
    case no_ne:
    case no_lt:
    case no_le:
    case no_gt:
    case no_ge:
    case no_order:
    case no_assign:
    case no_call:
    case no_libraryscopeinstance:
        //MORE: Should check this doesn't make the comparison invalid.
        cachePotentialTablesUsed(used);
        break;
    case no_keyindex:
    case no_newkeyindex:
        cacheChildrenTablesUsed(used, 1, numChildren());
        used.removeParent(queryChild(0));
        //Distributed attribute might contain references to the no_activetable
        used.removeActive(queryActiveTableSelector());
        break;
    case no_evaluate:
        cacheTableUseage(used, queryChild(0));
        queryChild(1)->gatherTablesUsed(used);
        break;
    case no_table:
        {
            cacheChildrenTablesUsed(used, 0, numChildren());
            IHqlExpression * parent = queryChild(3);
            if (parent)
                used.removeParent(parent);
            break;
        }
    case no_pat_production:
        {
            cacheChildrenTablesUsed(used, 0, numChildren());
            used.cleanupProduction();
            break;
        }
    default:
        {
            ITypeInfo * thisType = queryType();
            unsigned max = numChildren();
            if (max)
            {
                if (thisType)
                {
                    switch (thisType->getTypeCode())
                    {
                    case type_void:
                    case type_dictionary:
                    case type_table:
                    case type_groupedtable:
                    case type_row:
                    case type_transform:
                        {
                            cacheTablesProcessChildScope(used, ignoreInputs);
                            IHqlExpression * counter = queryAttribute(_countProject_Atom);
                            if (counter)
                                used.removeActive(counter->queryChild(0));
                            break;
                        }
                    default:
                        cacheChildrenTablesUsed(used, 0, max);
                        break;
                    }
                }
                else
                    cacheChildrenTablesUsed(used, 0, max);
            }
            break;
        }
    }

    switch (op)
    {
    case no_xmltext:
    case no_xmlunicode:
    case no_xmlproject:
        used.addActiveTable(queryXmlParsePseudoTable());
        break;
    case no_matched:
    case no_matchtext:
    case no_matchunicode:
    case no_matchlength:
    case no_matchposition:
    case no_matchrow:
    case no_matchutf8:
    case no_matchattr:
        used.addActiveTable(queryNlpParsePseudoTable());
        break;
    case no_externalcall:
        {
            IHqlExpression * def = queryExternalDefinition()->queryChild(0);
            if (def->hasAttribute(userMatchFunctionAtom))
                used.addActiveTable(queryNlpParsePseudoTable());
            break;
        }
    }
}

void CHqlExpressionWithTables::cacheTablesUsed()
{
    if (!(infoFlags & HEFgatheredNew))
    {
        //NB: This is not thread safe!  Should be protected with a cs 
        //but actually want it to be more efficient than that - don't want to call a cs at each level.
        //So need an ensureCached() function surrounding it that is cs protected.

        //Special case some common operators that can avoid going through the general code
        bool specialCased = false;
        if (false)
        switch (op)
        {
        case no_attr:
        case no_attr_link:
        case no_keyed:
        case no_colon:
        case no_cluster:
        case no_nameof:
        case no_translated:
        case no_constant:
            break;
        case no_select:
            {
                IHqlExpression * ds = queryChild(0);
                if (isSelectRootAndActive())
                {
                    usedTables.setActiveTable(ds);
                }
                else
                {
                    //MORE: ds->gatherTablesUsed(usedTables);
                    //which could ideally clone
                    specialCased = false;
                }
                break;
            }
        case no_activerow:
            usedTables.setActiveTable(queryChild(0));
            break;
        case no_rows:
        case no_rowset:
            //MORE: This is a bit strange!
            usedTables.setActiveTable(queryChild(0));
            break;
        case no_left:
        case no_right:
            usedTables.setActiveTable(this);
            break;
        case no_counter:
            //NB: Counter is added as a pseudo table, because it is too hard to keep track of nested counters otherwise
            usedTables.setActiveTable(this);
            break;
        case no_filepos:
        case no_file_logicalname:
            usedTables.setActiveTable(queryChild(0));
            break;
        default:
            specialCased = false;
            break;
        }

        if (!specialCased)
        {
            CUsedTablesBuilder used;
            calcTablesUsed(used, false);
            used.set(usedTables);
        }
        infoFlags |= HEFgatheredNew;
    }
}

bool CHqlExpressionWithTables::isIndependentOfScope()
{
    cacheTablesUsed();
    return usedTables.isIndependentOfScope();
}

bool CHqlExpressionWithTables::isIndependentOfScopeIgnoringInputs()
{
    CUsedTablesBuilder used;
    //MORE: We could try using a flag set by cacheTablesUsed() instead
    calcTablesUsed(used, true);
    return used.isIndependentOfScope();
}

bool CHqlExpressionWithTables::usesSelector(IHqlExpression * selector)
{
    cacheTablesUsed();
    return usedTables.usesSelector(selector);
}

void CHqlExpressionWithTables::gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope)
{
    cacheTablesUsed();
    usedTables.gatherTablesUsed(newScope, inScope);
}

void CHqlExpressionWithTables::gatherTablesUsed(CUsedTablesBuilder & used)
{
    cacheTablesUsed();
    usedTables.gatherTablesUsed(used);
}

//==============================================================================================================

CHqlSelectBaseExpression::CHqlSelectBaseExpression()
: CHqlExpression(no_select)
{
}

ITypeInfo *CHqlSelectBaseExpression::queryType() const
{
    dbgassertex(operands.ordinality()>=2);
    return operands.item(1).queryType();
}

ITypeInfo *CHqlSelectBaseExpression::getType()
{
    dbgassertex(operands.ordinality()>=2);
    return operands.item(1).getType();
}


void CHqlSelectBaseExpression::setOperands(IHqlExpression * left, IHqlExpression * right, IHqlExpression * attr)
{
    //Need to be very careful about the order that this is done in, since queryType() depends on operand2
    unsigned max = attr ? 3 : 2;
    operands.ensure(max);
    operands.append(*left);
    operands.append(*right);
    if (attr)
        operands.append(*attr);
    //Now the operands are added we can call the functions to update the flags
    for (unsigned i=0; i < max; i++)
        onAppendOperand(operands.item(i), i);

}

void CHqlSelectBaseExpression::setOperands(HqlExprArray & _ownedOperands)
{
    //base setOperands() already processes things in the correct order
    CHqlExpression::setOperands(_ownedOperands);
}

IHqlExpression * CHqlSelectBaseExpression::clone(HqlExprArray &newkids)
{
    return createSelectExpr(newkids);
}

IHqlExpression * CHqlSelectBaseExpression::makeSelectExpression(IHqlExpression * left, IHqlExpression * right, IHqlExpression * attr)
{
#ifdef _DEBUG
    assertex(!right->isDataset());
    assertex(left->getOperator() != no_activerow);
#endif
    IHqlExpression * normalizedLeft = left->queryNormalizedSelector();
    bool needNormalize = (normalizedLeft != left) || (attr && attr->queryName() == newAtom);

    CHqlSelectBaseExpression * select;
    if (needNormalize)
        select = new CHqlSelectExpression;
    else
        select = new CHqlNormalizedSelectExpression;
    select->setOperands(left, right, attr);
    select->calcNormalized();
    return select->closeExpr();
}

IHqlExpression * CHqlSelectBaseExpression::makeSelectExpression(HqlExprArray & ownedOperands)
{
#ifdef _DEBUG
    assertex(!ownedOperands.item(1).isDataset());
    assertex(ownedOperands.item(0).getOperator() != no_activerow);
#endif
    IHqlExpression * left = &ownedOperands.item(0);
    IHqlExpression * normalizedLeft = left->queryNormalizedSelector();
    bool needNormalize = (normalizedLeft != left) || ((ownedOperands.ordinality() > 2) && ::hasAttribute(newAtom, ownedOperands));

    CHqlSelectBaseExpression * select;
    if (needNormalize)
        select = new CHqlSelectExpression;
    else
        select = new CHqlNormalizedSelectExpression;
    select->setOperands(ownedOperands);
    select->calcNormalized();
    return select->closeExpr();
}


bool CHqlSelectBaseExpression::isIndependentOfScope()
{
    if (isSelectRootAndActive())
    {
        return false;
    }
    else
    {
        IHqlExpression * ds = queryChild(0);
        return ds->isIndependentOfScope();
    }
}

bool CHqlSelectBaseExpression::isIndependentOfScopeIgnoringInputs()
{
    if (isSelectRootAndActive())
    {
        return false;
    }
    else
    {
        IHqlExpression * ds = queryChild(0);
        return ds->isIndependentOfScopeIgnoringInputs();
    }
}


bool CHqlSelectBaseExpression::usesSelector(IHqlExpression * selector)
{
    IHqlExpression * ds = queryChild(0);
    if (isSelectRootAndActive())
    {
        return (selector == ds);
    }
    else
    {
        return ds->usesSelector(selector);
    }
}

void CHqlSelectBaseExpression::gatherTablesUsed(CUsedTablesBuilder & used)
{
    IHqlExpression * ds = queryChild(0);
    if (isSelectRootAndActive())
    {
        used.addActiveTable(ds);
    }
    else
    {
        ds->gatherTablesUsed(used);
    }
}

void CHqlSelectBaseExpression::gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope)
{
    IHqlExpression * ds = queryChild(0);
    if (isSelectRootAndActive())
    {
        if (inScope)
            ::addActiveTable(*inScope, ds);
    }
    else
    {
        ds->gatherTablesUsed(newScope, inScope);
    }
}


//==============================================================================================================

IHqlExpression * CHqlNormalizedSelectExpression::queryNormalizedSelector(bool skipIndex)
{
    return this;
}

void CHqlNormalizedSelectExpression::calcNormalized()
{
#ifdef VERIFY_EXPR_INTEGRITY
    OwnedHqlExpr normalized = calcNormalizedSelector();
    assertex(!normalized);
#endif
}

IHqlExpression * CHqlSelectExpression::queryNormalizedSelector(bool skipIndex)
{
    if (normalized)
        return normalized;
    return this;
}

void CHqlSelectExpression::calcNormalized()
{
    normalized.setown(calcNormalizedSelector());
    assertex(normalized);
}

//==============================================================================================================

CHqlConstant::CHqlConstant(IValue *_val) : CHqlExpression(no_constant)
{
    val = _val;
    infoFlags |= (HEFhasunadorned|HEFgatheredNew);
}

CHqlConstant *CHqlConstant::makeConstant(IValue *_val) 
{
    CHqlConstant *e = new CHqlConstant(_val);
    return (CHqlConstant *) e->closeExpr();
}

bool CHqlConstant::equals(const IHqlExpression & other) const
{
    if (!isAlive()) return false;
    IValue * oval = other.queryValue();
    if (oval)
        if (val->queryType() == oval->queryType())
            if (oval->compare(val) == 0)
                return true;
    return false;
}

void CHqlConstant::sethash()
{
    CHqlExpression::sethash();
    hashcode = val->getHash(hashcode);
}

IHqlExpression *CHqlConstant::clone(HqlExprArray &newkids)
{
    assertex(newkids.ordinality() == 0);
    Link();
    return this;
}

CHqlConstant::~CHqlConstant()
{
    val->Release();
}

StringBuffer &CHqlConstant::toString(StringBuffer &ret)
{
    val->generateECL(ret);
    return ret;
}

ITypeInfo *CHqlConstant::queryType() const
{
    return val->queryType();
}

ITypeInfo *CHqlConstant::getType()
{
    return LINK(val->queryType());
}


//==============================================================================================================

CHqlField::CHqlField(IIdAtom * _id, ITypeInfo *_type, IHqlExpression *defvalue)
 : CHqlExpressionWithType(no_field, _type)
{
    appendOperands(defvalue, NULL);
    assertex(_id);
    id = _id;
    onCreateField();
}

CHqlField::CHqlField(IIdAtom * _id, ITypeInfo *_type, HqlExprArray &_ownedOperands)
: CHqlExpressionWithType(no_field, _type, _ownedOperands)
{
    assertex(_id);
    id = _id;
    onCreateField();
}

void CHqlField::onCreateField()
{
    bool hasLCA = hasAttribute(_linkCounted_Atom);
    ITypeInfo * newType = setLinkCountedAttr(type, hasLCA);
    type->Release();
    type = newType;

#ifdef _DEBUG
    if (hasLinkCountedModifier(type) != hasAttribute(_linkCounted_Atom))
        throwUnexpected();
#endif
#ifdef DEBUG_ON_CREATE
    if (queryName() == createIdAtom("imgLength"))
        PrintLog("Create field %s=%p", expr->queryName()->str(), expr);
#endif

    infoFlags &= ~(HEFfunctionOfGroupAggregate);
    infoFlags |= HEFcontextDependentException;

    assertex(type->getTypeCode() != type_record);

    IHqlExpression * typeExpr = NULL;
    switch (type->getTypeCode())
    {
    case type_alien:
        typeExpr = queryExpression(type);
        break;
    case type_row:
        break;
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        typeExpr = queryRecord();
#ifdef _DEBUG
        assertex(!recordRequiresLinkCount(typeExpr) || hasAttribute(_linkCounted_Atom));
#endif
        break;
    }

    if (typeExpr)
    {
        infoFlags |= (typeExpr->getInfoFlags() & HEFalwaysInherit);
        infoFlags2 |= (typeExpr->getInfoFlags2() & HEF2alwaysInherit);
    }
}



bool CHqlField::equals(const IHqlExpression & r) const
{
    if (CHqlExpression::equals(r))
    {
        const CHqlField *fr = QUERYINTERFACE(&r, const CHqlField);
        if (fr && fr->id==id)
            return true;
    }
    return false;
}

IHqlExpression *CHqlField::clone(HqlExprArray &newkids)
{
    CHqlField* e = new CHqlField(id, LINK(type), newkids);
    return e->closeExpr();
}

StringBuffer &CHqlField::toString(StringBuffer &ret)
{
    ret.append(id->str());
    return ret;
}

void CHqlField::sethash()
{
    CHqlExpression::sethash();
    HASHFIELD(id);
}


//==============================================================================================================

CHqlRow::CHqlRow(node_operator op, ITypeInfo * type, HqlExprArray & _ownedOperands) 
: CHqlExpressionWithType(op, type, _ownedOperands)
{
    switch (op)
    {
    case no_select: 
        if (!hasAttribute(newAtom))
        {
            normalized.setown(calcNormalizedSelector());
        }
        break;
    }

    switch (op)
    {
    case no_activetable:
    case no_self:
    case no_left:
    case no_right:
    case no_activerow:
    case no_top:
        break;
    default:
        infoFlags |= HEFcontainsDataset;
        break;
    }
}

CHqlRow *CHqlRow::makeRow(node_operator op, ITypeInfo * type, HqlExprArray & args)
{
    CHqlRow *e = new CHqlRow(op, type, args);
    return (CHqlRow *) e->closeExpr();
}

IHqlExpression * CHqlRow::clone(HqlExprArray &newkids)
{
    return createRow(op, newkids);
}

IAtom * CHqlRow::queryName() const
{
    switch (op)
    {
    case no_left: return leftAtom;
    case no_right: return rightAtom;
    case no_self: return selfAtom;
    case no_activetable: return activeAtom;
    case no_top: return topAtom;
    }
    return NULL;
}

IHqlExpression *CHqlRow::queryNormalizedSelector(bool skipIndex)
{
    if (!skipIndex || (op != no_selectnth))
        return normalized.get() ? normalized.get() : this;
    return queryChild(0)->queryNormalizedSelector(skipIndex);
}

IHqlSimpleScope *CHqlRow::querySimpleScope()
{
    return QUERYINTERFACE(queryUnqualifiedType(type->queryChildType()), IHqlSimpleScope);
}

IHqlDataset *CHqlRow::queryDataset()
{
    IHqlExpression * dataset = queryChild(0);
    return dataset ? dataset->queryDataset() : NULL;
}

//==============================================================================================================

static bool isMany(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return (expr->queryName() == manyAtom);
    case no_range:
        return isMany(expr->queryChild(1));
    case no_constant:
        return (expr->queryValue()->getIntValue() > 1);
    default:
        UNIMPLEMENTED;
    }
}

// Is this numeric expression evaluated to zero?
bool isZero(IHqlExpression * expr)
{
    IValue * value = expr->queryValue();
    if (value)
    {
        switch (value->queryType()->getTypeCode())
        {
        case type_real:
            return (value->getRealValue() == 0);
        case type_boolean:
        case type_int:
        case type_date:
        case type_bitfield:
        case type_swapint:
        case type_packedint:
            return (value->getIntValue() == 0);
        default:
            {
                Owned<IValue> zero = value->queryType()->castFrom(true, I64C(0));
                return value->compare(zero) == 0;
            }
        }
    }
    if (expr->getOperator() == no_translated)
        return isZero(expr->queryChild(0));
    return false;
}

// Return -1 is constant is negative, 1 if positive, 0 if zero
static int compareConstantZero(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_constant);
    IValue * value = expr->queryValue();
    // MORE: optimising this for int and real could save some cycles
    Owned<IValue> zero = value->queryType()->castFrom(true, I64C(0));
    return value->compare(zero);
}

// If it's at all possible (but not necessarily sure) that expr could be negative
// Default is true (maybe). Use isNegative for a clear answer
bool couldBeNegative(IHqlExpression * expr)
{
    ITypeInfo * type = expr->queryType();
    if (!isNumericType(type))
        return false;
    if (!type->isSigned())
        return false;
    if (isCast(expr) && castPreservesValueAndOrder(expr))
        return couldBeNegative(expr->queryChild(0));
    if (expr->getOperator() == no_constant)
        return (compareConstantZero(expr) == -1);

    // Default is a conservative maybe
    return true;
}

// If expr is negative for sure.
// Default is false (not sure). Use couldBeNegative for possibilities
bool isNegative(IHqlExpression * expr)
{
    if (expr->getOperator() == no_constant)
        return (compareConstantZero(expr) == -1);
    if (!couldBeNegative(expr))
        return false;
    if (expr->getOperator() == no_translated)
        return isNegative(expr->queryChild(0));

    // When unsure, say no
    return false;
}

bool isChildRelationOf(IHqlExpression * child, IHqlExpression * other)
{
    if (!other)
        return false;
    if (child->getOperator() != no_select)
    {
        IHqlDataset * otherDs = other->queryDataset();
        if (!otherDs) return false;
        other = queryExpression(otherDs->queryRootTable());
        IHqlDataset * childDs = child->queryDataset();
        if (!childDs) return false;
        child = queryExpression(childDs->queryRootTable());
        if (!other || !child)
            return false;
    }

    IHqlDataset * searchDs = child->queryDataset();;
    if (!searchDs)
        return false;
    IHqlExpression * search = searchDs->queryContainer();
    while (search)
    {
        if (search == other)
            return true;
        IHqlDataset * searchDs = search->queryDataset();
        if (!searchDs)
            return false;
        search = searchDs->queryContainer();
    }
    return false;
}


bool isInImplictScope(IHqlExpression * scope, IHqlExpression * dataset)
{
    if (!scope)
        return false;
    IHqlDataset * scopeDs = scope->queryDataset();
    IHqlDataset * datasetDs = dataset->queryDataset();
    if (!scopeDs || !datasetDs)
        return false;
    if (isChildRelationOf(scope, dataset))
        return true;
    return false;
}

//===========================================================================

CHqlDictionary *CHqlDictionary::makeDictionary(node_operator _op, ITypeInfo *type, HqlExprArray &_ownedOperands)
{
    CHqlDictionary *e = new CHqlDictionary(_op, type, _ownedOperands);
    return (CHqlDictionary *) e->closeExpr();
}

CHqlDictionary::CHqlDictionary(node_operator _op, ITypeInfo *_type, HqlExprArray &_ownedOperands)
: CHqlExpressionWithType(_op, _type, _ownedOperands)
{
    if (op == no_select)
        normalized.setown(calcNormalizedSelector());
}

CHqlDictionary::~CHqlDictionary()
{
}

IHqlExpression *CHqlDictionary::clone(HqlExprArray &newkids)
{
    return createDictionary(op, newkids);
}

IHqlExpression * CHqlDictionary::queryNormalizedSelector(bool skipIndex)
{
    if (!normalized)
        return this;
    if (!skipIndex)
        return normalized;
    return normalized->queryNormalizedSelector(skipIndex);
}



//===========================================================================

CHqlDataset *CHqlDataset::makeDataset(node_operator _op, ITypeInfo *type, HqlExprArray &_ownedOperands)
{
    CHqlDataset *e = new CHqlDataset(_op, type, _ownedOperands);
    return (CHqlDataset *) e->closeExpr();
}

IHqlSimpleScope *CHqlDataset::querySimpleScope()
{
    return QUERYINTERFACE(queryUnqualifiedType(queryRecordType()), IHqlSimpleScope);
}

IHqlExpression *CHqlDataset::clone(HqlExprArray &newkids)
{
    return createDataset(op, newkids);
}

IHqlDataset *CHqlDataset::queryTable()
{
    if (op == no_compound)
        return queryChild(1)->queryDataset()->queryTable();

    if (definesColumnList(this))
        return this;

    IHqlExpression* child = queryChild(0);
    assert(child);
    IHqlDataset*  dataset = child->queryDataset();
    if (dataset)
        return dataset->queryTable();

    StringBuffer s("queryDataset() return NULL for: ");
    s.append(getOpString(op));
    throw MakeStringExceptionDirect(2, s.str());
}

void CHqlDataset::sethash() 
{ 
    CHqlExpression::sethash(); 
}

//==============================================================================================================

CHqlDataset::CHqlDataset(node_operator _op, ITypeInfo *_type, HqlExprArray &_ownedOperands) 
: CHqlExpressionWithType(_op, _type, _ownedOperands)
{
    infoFlags &= ~(HEFfunctionOfGroupAggregate|HEFassertkeyed); // parent dataset should never have keyed attribute
    infoFlags &= ~(HEF2assertstepped);

    infoFlags |= HEFcontainsDataset;
    cacheParent();

    //MORE we may want some datasets (e.g., null, temptable?) to be table invariant, but it may
    //cause problems with datasets that are created on the fly.
}

CHqlDataset::~CHqlDataset()
{
    ::Release(container);
}

bool CHqlDataset::equals(const IHqlExpression & r) const
{
    if (CHqlExpression::equals(r))
    {
        const CHqlDataset & other = (const CHqlDataset &)r;
        //No need to check name - since it is purely derived from one of the arguments
        return true;
    }
    return false;
}

/* If a dataset is derived from a base dataset ds, and satisfying:
    1) It does not change the structure of ds
    2) It does not change any field value of a record (e.g. anything that is related to a transform)
   Then ds is the parent dataset of the derived dataset. 
   
   Note that the derived dataset can:
    1) has fewer records than ds (by filter, selection, sampling etc)
    2) has different order for records (by sorting)
    3) has fewer fields (by selecting fields, like TABLE)
*/
void CHqlDataset::cacheParent()
{
    container = NULL;
    rootTable = NULL;
    switch (op)
    {
    case no_aggregate:
    case no_newaggregate:
    case no_newusertable:
        if (isAggregateDataset(this))
        {
            rootTable = queryChild(0)->queryDataset()->queryRootTable();
            break;
        }
        //Fallthrough...
    case no_selectfields:
    case no_keyed:
    // change ordering
    case no_sort:
    case no_subsort:
    case no_sorted:
    case no_stepped:
    case no_cosort:
    case no_preload:
    case no_assertsorted:
    case no_assertgrouped:
    case no_assertdistributed:
    // grouping: can only select fields in groupby - which is checked in checkGrouping() 
    case no_group:
    case no_cogroup:
    case no_grouped:
    // distributing:
    case no_distribute:
    case no_distributed:
    case no_preservemeta:
    // fewer records
    case no_filter:
    case no_choosen:
    case no_choosesets:
    case no_selectnth:
    case no_dedup:
    case no_enth:
    case no_sample:
    case no_limit:
    case no_catchds:
//  case no_keyedlimit:
    case no_topn:
    case no_keyeddistribute:
        {
            IHqlExpression * inDs = queryChild(0);
            IHqlDataset* ds = inDs->queryDataset();
            if (ds)
            {
                IHqlDataset * parent = ds->queryTable();
                rootTable = parent->queryRootTable();
            }
            else
            {
                PrintLog("cacheParent->queryDataset get NULL for: %s", getOpString(inDs->getOperator()));
            }
        }
        break;

    case no_keyindex:
    case no_newkeyindex:
    case no_temptable:
    case no_inlinetable:
    case no_xmlproject:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
    case no_fail:
    case no_skip:
    case no_field:
    case no_httpcall:
    case no_soapcall:
    case no_newsoapcall:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_getresult:
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_activetable:
    case no_alias:
    case no_id2blob:
    case no_externalcall:
    case no_call:
    case no_rows:
    case no_rowsetindex:
    case no_rowsetrange:
    case no_internalselect:
    case no_delayedselect:
    case no_libraryselect:
    case no_purevirtual:
    case no_unboundselect:
    case no_libraryscopeinstance:
    case no_libraryinput:
        rootTable = this;
        break;
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
        rootTable = this;       //?
        break;
    case no_table:
        {
            rootTable = this;
            IHqlExpression * parentArg = queryChild(3);
            if (parentArg)
            {
                IHqlDataset * pDataset = parentArg->queryDataset();
                if (pDataset)
                {
                    IHqlDataset * parent = pDataset->queryTable();
                    if (parent)
                    {
                        container = ::queryExpression(parent->queryRootTable());
                        container->Link();
                    }
                }
            }
        }
        break;
    case no_combine:
    case no_combinegroup:
    case no_join:                   // default join to following the left hand side.
    case no_comma:
    case no_process:
    case no_related:
        rootTable = queryChild(0)->queryDataset()->queryRootTable();
        break;
    case no_compound:
    case no_fetch:
        rootTable = queryChild(1)->queryDataset()->queryRootTable();
        break;
    case no_select:
        {
            rootTable = this;
            normalized.setown(calcNormalizedSelector());
            IHqlExpression * ds = queryChild(0);
            container = LINK(queryDatasetCursor(ds)->queryNormalizedSelector(false));
#ifdef _DEBUG
            assertex(!hasAttribute(newAtom) || !isAlwaysActiveRow(ds));
#endif
            break;
        }
    case no_if:
        {
            IHqlDataset * rootLeft = queryChild(1)->queryDataset()->queryRootTable();
            IHqlExpression * right = queryRealChild(this, 2);
            if (right)
            {
                IHqlDataset * rootRight = right->queryDataset()->queryRootTable();
                if (rootLeft == rootRight)
                    rootTable = rootLeft;
            }
            break;
        }
    case no_addfiles:
    case no_chooseds:
    default:
        if (getNumChildTables(this) == 1)
        {
            IHqlDataset * childDataset = queryChild(0)->queryDataset();
            assertex(childDataset);
            rootTable = childDataset->queryRootTable();
        }
        break;
    }

    if (op != no_select)
    {
        IHqlDataset * table = queryTable();
        IHqlExpression * tableExpr = ::queryExpression(table);
        if (tableExpr != this)
        {
            normalized.set(tableExpr->queryNormalizedSelector());
        }
    }
}

bool CHqlDataset::isAggregate()
{
    return isAggregateDataset(this);
}

IHqlExpression * CHqlDataset::queryNormalizedSelector(bool skipIndex)
{
    if (!normalized)
        return this;
    if (!skipIndex)
        return normalized;
    return normalized->queryNormalizedSelector(skipIndex);
}


IHqlExpression * queryRoot(IHqlExpression * expr)
{
    while ((expr->getOperator() == no_select) && expr->isDatarow())
        expr = expr->queryChild(0);

    IHqlDataset * dataset = expr->queryDataset();
    if (!dataset)
        return NULL;
    return queryExpression(dataset->queryRootTable());
}

IHqlExpression * queryTable(IHqlExpression * dataset)
{
    IHqlDataset * ds = dataset->queryDataset();
    if (!ds)
        return NULL;
    return queryExpression(ds->queryTable());
}


node_operator queryTableMode(IHqlExpression * expr)
{
    if (!expr)
        return no_none;

    switch (expr->getOperator())
    {
    case no_table:
        {
            node_operator modeOp = expr->queryChild(2)->getOperator();
            if (modeOp == no_thor)
                return no_flat;
            return modeOp;
        }
    }
    return no_none;
}

//==============================================================================================================

CHqlRecord::CHqlRecord() : CHqlExpressionWithTables(no_record)
{
    thisAlignment = 0;
}

CHqlRecord::CHqlRecord(HqlExprArray &operands) : CHqlExpressionWithTables(no_record)
{
    setOperands(operands);
    thisAlignment = 0;
    insertSymbols(this);
}

bool CHqlRecord::equals(const IHqlExpression & r) const
{
    if (CHqlExpression::equals(r))
    {
        return true;
    }
    return false;
}

unsigned CHqlRecord::getAlignment() 
{ 
    if (!thisAlignment)
    {
        unsigned numFields = numChildren();
        for (unsigned i = 0; i < numFields; i++)
        {
            IHqlExpression *field = queryChild(i);
            ITypeInfo * type = field->queryType();
            if (type)
            {
                size32_t align = type->getAlignment();
                if (align > thisAlignment)
                    thisAlignment = align;
            }
        }
    }
    return thisAlignment; 
}

IHqlExpression *CHqlRecord::addOperand(IHqlExpression *field)
{
    CHqlExpression::addOperand(field);
    insertSymbols(field);
    return this;
}

void CHqlRecord::sethash()
{
    // Can't use base class as that includes type (== this) which would make it different every time
    setInitialHash(0);
    assertex(fields.count() != 0 || isEmptyRecord(this));
}

IHqlExpression * CHqlRecord::clone(HqlExprArray &newkids)
{
    CHqlRecord* e = new CHqlRecord(newkids);
    return e->closeExpr();
}

unsigned CHqlRecord::getCrc()
{
    return getExpressionCRC(this);
}

void CHqlRecord::insertSymbols(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_field:
        {
            IAtom * name = expr->queryName();
            if (name)
                fields.setValue(name, expr);
        }
        break;
    case no_ifblock:
        expr = expr->queryChild(1);
        //fall through:
    case no_record:
        {
            ForEachChild(idx, expr)
                insertSymbols(expr->queryChild(idx));
        }
        break;
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        break;
    default:
        assertex("Unknown expression type added to record");
        break;
    }
}

ITypeInfo * CHqlRecord::queryType() const
{
    return const_cast<CHqlRecord *>(this);
}

ITypeInfo * CHqlRecord::getType()
{
    CHqlRecord::Link();
    return this;
}

CHqlRecord::~CHqlRecord()
{
}

/* return: linked */
IHqlExpression *CHqlRecord::lookupSymbol(IIdAtom * fieldName)
{
    IHqlExpression *ret = fields.getValue(fieldName->lower());
    ::Link(ret);
    return ret;
}

/* does not affect linkage */
bool CHqlRecord::assignableFrom(ITypeInfo * source)
{
    switch(source->getTypeCode())
    {
    case type_groupedtable:
    case type_dictionary:
    case type_table:
    case type_row:
    case type_transform:
        return assignableFrom(source->queryChildType());

    case type_record:
        {
            if  (numChildren() == 0 || recordTypesMatch(source, this))
                return true;

            //Record inheritance.  If the first entry in the source record is also a record, then check if compatible.
            IHqlExpression * other = ::queryRecord(source);
            ForEachChild(i, other)
            {
                IHqlExpression * cur = other->queryChild(i);
                switch (cur->getOperator())
                {
                case no_ifblock:
                case no_field:
                    return false;
                case no_record:
                    return assignableFrom(cur->queryType());
                }
            }
            return false;
        }

    default:
        return false;
    }
}

StringBuffer &CHqlRecord::getECLType(StringBuffer & out)
{
    return out.append(queryTypeName());
}

//==============================================================================================================
CHqlAnnotation::CHqlAnnotation(IHqlExpression * _body)
: CHqlExpression(_body ? _body->getOperator() : no_nobody)
{
    body = _body;
    if (!body)
        body = LINK(cachedNoBody);
}

CHqlAnnotation::~CHqlAnnotation()
{
    ::Release(body);
}


void CHqlAnnotation::sethash()
{
    hashcode = 0;
    HASHFIELD(body);
}

bool CHqlAnnotation::equals(const IHqlExpression & other) const
{
    if (!isAlive()) return false;
    if (getAnnotationKind() != other.getAnnotationKind())
        return false;

    const CHqlAnnotation * cast = static_cast<const CHqlAnnotation *>(&other);
    if ((body != cast->body))
        return false;
    return true;
}

unsigned CHqlAnnotation::getInfoFlags() const 
{ 
    return body->getInfoFlags();
}

unsigned CHqlAnnotation::getInfoFlags2() const 
{ 
    return body->getInfoFlags2();
}

unsigned CHqlAnnotation::getSymbolFlags() const
{
    return body->getSymbolFlags();
}

bool CHqlAnnotation::isConstant()
{
    return body->isConstant();
}

bool CHqlAnnotation::isPure()
{
    return body->isPure();
}

bool CHqlAnnotation::isAttribute() const
{
    return body->isAttribute();
}

StringBuffer &CHqlAnnotation::toString(StringBuffer &s)
{
    return body->toString(s);
}

IHqlExpression *CHqlAnnotation::clone(HqlExprArray &newkids)
{
    OwnedHqlExpr newbody = body->clone(newkids);
    return cloneAnnotation(newbody);
}

IHqlExpression * CHqlAnnotation::cloneAllAnnotations(IHqlExpression * newbody)
{
    OwnedHqlExpr updatedBody = body->cloneAllAnnotations(newbody);
    return cloneAnnotation(updatedBody);
}


bool CHqlAnnotation::isIndependentOfScope()
{
    return body->isIndependentOfScope();
}

bool CHqlAnnotation::isIndependentOfScopeIgnoringInputs()
{
    return body->isIndependentOfScopeIgnoringInputs();
}

bool CHqlAnnotation::usesSelector(IHqlExpression * selector)
{
    return body->usesSelector(selector);
}

void CHqlAnnotation::gatherTablesUsed(CUsedTablesBuilder & used)
{
    body->gatherTablesUsed(used);
}

void CHqlAnnotation::gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope)
{
    body->gatherTablesUsed(newScope, inScope);
}

IHqlExpression *CHqlAnnotation::queryChild(unsigned idx) const
{
    return body->queryChild(idx);
}

IHqlExpression *CHqlAnnotation::queryAttribute(IAtom * propname) const
{
    return body->queryAttribute(propname);
}

IHqlExpression * CHqlAnnotation::queryAnnotationParameter(unsigned idx) const
{
    return CHqlExpression::queryChild(idx);
}

unsigned CHqlAnnotation::numChildren() const
{
    return body->numChildren();
}

unsigned int CHqlAnnotation::getCachedEclCRC()
{
    return body->getCachedEclCRC();
}

bool CHqlAnnotation::isExported() const
{
    return body->isExported();
}

StringBuffer & CHqlAnnotation::getTextBuf(StringBuffer & out)
{
    return body->getTextBuf(out);
}

IFileContents * CHqlAnnotation::queryDefinitionText() const
{
    return body->queryDefinitionText();
}


IHqlExpression * CHqlAnnotation::addOperand(IHqlExpression * expr)
{
    throwUnexpected();
    return body->addOperand(expr);
}

bool CHqlAnnotation::isFullyBound() const 
{
    return body->isFullyBound();
}

IIdAtom * CHqlAnnotation::queryFullContainerId() const
{
    return body->queryFullContainerId();
}

IHqlExpression * CHqlAnnotation::queryProperty(ExprPropKind kind)
{
    return body->queryProperty(kind);
}

IHqlExpression * CHqlAnnotation::queryNormalizedSelector(bool skipIndex)
{
    return body->queryNormalizedSelector(skipIndex);
}

IHqlExpression * CHqlAnnotation::queryExternalDefinition() const 
{
    return body->queryExternalDefinition();
}

IHqlExpression * CHqlAnnotation::queryFunctionDefinition() const 
{
    return body->queryFunctionDefinition();
}

IHqlSimpleScope * CHqlAnnotation::querySimpleScope()
{
    return body->querySimpleScope();
}

IHqlScope * CHqlAnnotation::queryScope()
{
    return body->queryScope();
}

IHqlDataset * CHqlAnnotation::queryDataset()
{
    return body->queryDataset();
}

unsigned __int64 CHqlAnnotation::querySequenceExtra()
{
    return body->querySequenceExtra();
}

IInterface * CHqlAnnotation::queryUnknownExtra()
{
    return body->queryUnknownExtra();
}

IValue * CHqlAnnotation::queryValue() const 
{
    return body->queryValue();
}

IHqlExpression * CHqlAnnotation::queryBody(bool singleLevel)
{
    //Not sure about the following...
    if (body == cachedNoBody)
        return NULL;
    if (singleLevel)
        return body;
    return body->queryBody(singleLevel);
}

IPropertyTree * CHqlAnnotation::getDocumentation() const
{
    return body->getDocumentation();
}

bool CHqlAnnotation::isGroupAggregateFunction()
{
    return body->isGroupAggregateFunction();
}

bool CHqlAnnotation::isMacro()
{
    return body->isMacro();
}

bool CHqlAnnotation::isType()
{
    return body->isType();
}

bool CHqlAnnotation::isScope()
{
    return body->isScope();
}

int CHqlAnnotation::getStartColumn() const
{
    return body->getStartColumn();
}

int CHqlAnnotation::getStartLine() const
{
    return body->getStartLine();
}

IAtom * CHqlAnnotation::queryName() const 
{
    return body->queryName();
}

IIdAtom * CHqlAnnotation::queryId() const
{
    return body->queryId();
}

ITypeInfo * CHqlAnnotation::queryType() const
{
    return body->queryType();
}

ITypeInfo * CHqlAnnotation::getType()
{
    return body->getType();
}



//==============================================================================================================

CHqlCachedBoundFunction::CHqlCachedBoundFunction(IHqlExpression *func, bool _forceOutOfLineExpansion)
: CHqlExpressionWithTables(no_bound_func)
{
    appendOperands(LINK(func), NULL);
    if (_forceOutOfLineExpansion)
        addOperand(createConstant(true));
}

ITypeInfo * CHqlCachedBoundFunction::queryType() const
{
    return nullType;
}

ITypeInfo * CHqlCachedBoundFunction::getType()
{
    return LINK(nullType);
}

IHqlExpression * CHqlCachedBoundFunction::clone(HqlExprArray &)
{
    throwUnexpected();
    return LINK(this);
}

#ifdef NEW_VIRTUAL_DATASETS
//Skeleton code to implement template functions using the standard binding mechanism (to behave more like c++ templates)
//However it would require moving lots of the semantic checking into the binding mechanism
//essentially parse to syntax tree, expand, semantically check.  Needless to say it is likely to be a lot of work.
static void doGatherAbstractSelects(HqlExprArray & selects, IHqlExpression * expr, IHqlExpression * selector)
{
    if (expr->queryTransformExtra())
        return;
    switch (expr->getOperator())
    {
    case no_attr:
        return;
    case no_select:
        if (expr->queryChild(0)->queryNormalizedSelector() == selector)
        {
            if (selects.find(*expr) == NotFound)
                selects.append(*LINK(expr));
            return;
        }
        break;
    }
    ForEachChild(i, expr)
        doGatherAbstractSelects(selects, expr->queryChild(i), selector);
    expr->setTransformExtraUnlinked(expr);
}

static void gatherAbstractSelects(HqlExprArray & selects, IHqlExpression * expr, IHqlExpression * selector)
{
    TransformMutexBlock procedure;
    doGatherAbstractSelects(selects, expr, selector);
}

static void associateBindMap(HqlExprArray & selects, IHqlExpression * formal, IHqlExpression * actual, IHqlExpression * mapping)
{
    IHqlSimpleScope * actualScope = actual->queryRecord()->querySimpleScope();
    HqlExprArray maps;
    mapping->unwindList(maps, no_comma);

    unsigned numMaps = maps.ordinality();
    for (unsigned i=0; i < numMaps; i+= 2)
    {
        IIdAtom * mapFrom = maps.item(i).queryName();
        IIdAtom * mapTo = maps.item(i+1).queryName();

        ForEachItemIn(j, selects)
        {
            IHqlExpression * selectFrom = &selects.item(j);
            if (selectFrom->queryChild(1)->queryName() == mapFrom)
            {
                OwnedHqlExpr to = actualScope->lookupSymbol(mapTo);
                if (!to)
                    throwError1(HQLERR_FieldInMapNotDataset, mapTo->str());

                OwnedHqlExpr selectTo = createSelectExpr(LINK(actual), LINK(to));
                if (selectFrom->queryTransformExtra())
                    throwError1(HQLERR_FieldAlreadyMapped, mapFrom->str());
                selectFrom->setTransformExtraOwned(selectTo.getClear());
            }
        }
    }
}

static void associateBindByName(HqlExprArray & selects, IHqlExpression * formal, IHqlExpression * actual)
{
    IHqlSimpleScope * actualScope = actual->queryRecord()->querySimpleScope();
    ForEachItemIn(i, selects)
    {
        IHqlExpression * selectFrom = &selects.item(i);
        IHqlExpression * curFormal = selectFrom->queryChild(1);
        if (!selectFrom->queryTransformExtra())
        {
            IIdAtom * name = curFormal->queryName();
            OwnedHqlExpr to = actualScope->lookupSymbol(name);
            if (!to)
            {
                if (isAbstractDataset(actual))
                    to.set(curFormal);
                else
                    throwError1(HQLERR_FileNotInDataset, name->str());
            }

            OwnedHqlExpr selectTo = createSelectExpr(LINK(actual), LINK(to));
            selectFrom->setTransformExtra(selectTo);
        }
    }
}
#endif

//---------------------------------------------------------------------------------------------------------------------

CHqlSymbolAnnotation::CHqlSymbolAnnotation(IIdAtom * _id, IIdAtom * _moduleId, IHqlExpression *_expr, IHqlExpression * _funcdef, unsigned _symbolFlags)
: CHqlAnnotation(_expr)
{
    id = _id;
    symbolFlags = _symbolFlags;
    moduleId = _moduleId;
    funcdef = _funcdef;
    if (funcdef && containsInternalSelect(funcdef))
        infoFlags |= HEFinternalSelect;
}

void CHqlSymbolAnnotation::sethash()
{
    hashcode = 0;
    HASHFIELD(id);
    HASHFIELD(body);
    HASHFIELD(moduleId);
}

CHqlSymbolAnnotation::~CHqlSymbolAnnotation()
{
    ::Release(funcdef);
}

bool CHqlSymbolAnnotation::equals(const IHqlExpression & other) const
{
    if (!CHqlAnnotation::equals(other))
        return false;

    //Must be a named symbol if got here
    if (id != other.queryId())
        return false;

    if ((symbolFlags != other.getSymbolFlags()) || (funcdef != other.queryFunctionDefinition()))
        return false;

    if (moduleId != other.queryFullContainerId())
        return false;

    if (op == no_nobody)
    {
        //no_nobody is currently used for attributes that have had their text read
        //but have not been parsed.  If so, we need to check the text matches.
        //There should almost certainly be a different representation for delayed expressions.
        if (!isSameText(queryDefinitionText(), other.queryDefinitionText()))
            return false;
    }
    return true;
}


unsigned CHqlSymbolAnnotation::getSymbolFlags() const
{
    return symbolFlags;
}

IHqlExpression * CHqlSymbolAnnotation::cloneAnnotation(IHqlExpression * newbody)
{
    if (body == newbody)
        return LINK(this);
    return cloneSymbol(id, newbody, funcdef, NULL);
}

IHqlExpression *CHqlSymbolAnnotation::queryFunctionDefinition() const
{
    return funcdef;
}

IHqlExpression *CHqlSymbolAnnotation::queryExpression()
{
    return this;
}


//---------------------------------------------------------------------------------------------------------------------

inline unsigned combineSymbolFlags(unsigned symbolFlags, bool exported, bool shared)
{
    return symbolFlags | (exported ? ob_exported : 0) | (shared ? ob_shared : 0);
}

CHqlSimpleSymbol::CHqlSimpleSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, IHqlExpression * _funcdef, unsigned _symbolFlags)
: CHqlSymbolAnnotation(_id, _module, _expr, _funcdef, _symbolFlags)
{
}

IHqlExpression *CHqlSimpleSymbol::makeSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, IHqlExpression * _funcdef, unsigned _flags)
{
    CHqlSimpleSymbol *e = new CHqlSimpleSymbol(_id, _module, _expr, _funcdef, _flags);
    return e->closeExpr();
}

IHqlExpression * CHqlSimpleSymbol::cloneSymbol(IIdAtom * optid, IHqlExpression * optnewbody, IHqlExpression * optnewfuncdef, HqlExprArray * optargs)
{
    assertex(!optargs || optargs->ordinality() == 0);
    IIdAtom * newid = optid ? optid : id;
    IHqlExpression * newbody = optnewbody ? optnewbody : body;
    IHqlExpression * newfuncdef = optnewfuncdef ? optnewfuncdef : funcdef;

    if (newid == id && newbody==body && newfuncdef==funcdef)
        return LINK(this);

    return makeSymbol(newid, moduleId, LINK(newbody), LINK(newfuncdef), symbolFlags);
}


//---------------------------------------------------------------------------------------------------------------------


CHqlNamedSymbol::CHqlNamedSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, bool _exported, bool _shared, unsigned _symbolFlags)
: CHqlSymbolAnnotation(_id, _module, _expr, NULL, combineSymbolFlags(_symbolFlags, _exported, _shared))
{
    startpos = 0;
    bodypos = 0;
    endpos = 0;
    startLine = 0;
    startColumn = 0;
}

CHqlNamedSymbol::CHqlNamedSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, IHqlExpression *_funcdef, bool _exported, bool _shared, unsigned _symbolFlags, IFileContents *_text, int _startLine, int _startColumn, int _startpos, int _bodypos, int _endpos)
: CHqlSymbolAnnotation(_id, _module, _expr, _funcdef, combineSymbolFlags(_symbolFlags, _exported, _shared))
{
    text.set(_text);
    startpos = _startpos;
    bodypos = _bodypos;
    endpos = _endpos;
    startLine = _startLine;
    startColumn = _startColumn;
}

CHqlNamedSymbol *CHqlNamedSymbol::makeSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, bool _exported, bool _shared, unsigned _flags)
{
    CHqlNamedSymbol *e = new CHqlNamedSymbol(_id, _module, _expr, _exported, _shared, _flags);
    return (CHqlNamedSymbol *) e->closeExpr();
}


CHqlNamedSymbol *CHqlNamedSymbol::makeSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, IHqlExpression *_funcdef, bool _exported, bool _shared, unsigned _flags, IFileContents *_text, int _startLine, int _startColumn, int _startpos, int _bodypos, int _endpos)
{
    CHqlNamedSymbol *e = new CHqlNamedSymbol(_id, _module, _expr, _funcdef, _exported, _shared, _flags, _text, _startLine, _startColumn, _startpos, _bodypos, _endpos);
    return (CHqlNamedSymbol *) e->closeExpr();
}

IHqlExpression * CHqlNamedSymbol::cloneSymbol(IIdAtom * optid, IHqlExpression * optnewbody, IHqlExpression * optnewfuncdef, HqlExprArray * optargs)
{
    IIdAtom * newid = optid ? optid : id;
    IHqlExpression * newbody = optnewbody ? optnewbody : body;
    IHqlExpression * newfuncdef = optnewfuncdef ? optnewfuncdef : funcdef;
    HqlExprArray * newoperands = optargs ? optargs : &operands;

    if (newid == id && newbody==body && newfuncdef==funcdef)
    {
        if (newoperands == &operands || arraysSame(*newoperands, operands))
            return LINK(this);
    }

    CHqlNamedSymbol * e = new CHqlNamedSymbol(newid, moduleId, LINK(newbody), LINK(newfuncdef), isExported(), isShared(), symbolFlags, text, startLine, startColumn, startpos, bodypos, endpos);
    //NB: do not all doAppendOpeand() because the parameters to a named symbol do not change it's attributes - e.g., whether pure.
    e->operands.ensure(newoperands->ordinality());
    ForEachItemIn(idx, *newoperands)
        e->operands.append(OLINK(newoperands->item(idx)));
    return e->closeExpr();
}

IFileContents * CHqlNamedSymbol::getBodyContents()
{
    return createFileContentsSubset(text, bodypos, getEndPos()-bodypos);
}

IFileContents * CHqlNamedSymbol::queryDefinitionText() const
{
    return text;
}

ISourcePath * CHqlNamedSymbol::querySourcePath() const
{
    if (text)
        return text->querySourcePath();
    return CHqlAnnotation::querySourcePath();
}

IHqlNamedAnnotation * queryNameAnnotation(IHqlExpression * expr)
{
    IHqlExpression * symbol = queryNamedSymbol(expr);
    if (!symbol)
        return NULL;
    return static_cast<IHqlNamedAnnotation *>(symbol->queryAnnotation());
}

//---------------------------------------------------------------------------------------------------------------------


bool isExported(IHqlExpression * expr)
{
    IHqlNamedAnnotation * symbol = queryNameAnnotation(expr);
    return (symbol && symbol->isExported());
}

bool isShared(IHqlExpression * expr)
{
    IHqlNamedAnnotation * symbol = queryNameAnnotation(expr);
    return (symbol && symbol->isShared());
}

bool isPublicSymbol(IHqlExpression * expr)
{
    IHqlNamedAnnotation * symbol = queryNameAnnotation(expr);
    return (symbol && symbol->isPublic());
}

bool isImport(IHqlExpression * expr)
{
    IHqlExpression * symbol = queryNamedSymbol(expr);
    return symbol && ((symbol->getSymbolFlags() & ob_import) != 0);
}

IECLError * queryAnnotatedWarning(const IHqlExpression * expr)
{
    assertex(expr->getAnnotationKind() == annotate_warning);
    const CHqlWarningAnnotation * cast = static_cast<const CHqlWarningAnnotation *>(expr);
    return cast->queryWarning();
}


//==============================================================================================================


CHqlAnnotationWithOperands::CHqlAnnotationWithOperands(IHqlExpression *_body, HqlExprArray & _args)
: CHqlAnnotation(_body)
{
    operands.ensure(_args.ordinality());
    ForEachItemIn(i, _args)
        operands.append(OLINK(_args.item(i)));
}

void CHqlAnnotationWithOperands::sethash()
{
//  CHqlExpression::sethash();
    setInitialHash(getAnnotationKind());            // hash the operands, not the body's
    HASHFIELD(body);
}


bool CHqlAnnotationWithOperands::equals(const IHqlExpression & other) const
{
    if (!CHqlAnnotation::equals(other))
        return false;
    const CHqlAnnotationWithOperands * cast = static_cast<const CHqlAnnotationWithOperands *>(&other);
    if (operands.ordinality() != cast->operands.ordinality())
        return false;
    ForEachItemIn(i, operands)
    {
        if (&operands.item(i) != &cast->operands.item(i))
            return false;
    }
    return true;
}



//==============================================================================================================


CHqlMetaAnnotation::CHqlMetaAnnotation(IHqlExpression *_body, HqlExprArray & _args)
: CHqlAnnotationWithOperands(_body, _args)
{
}

IHqlExpression * CHqlMetaAnnotation::cloneAnnotation(IHqlExpression * newbody)
{
    if (body == newbody)
        return LINK(this);
    return createMetaAnnotation(LINK(newbody), operands);
}

IHqlExpression * CHqlMetaAnnotation::createAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args)
{
    return (new CHqlMetaAnnotation(_ownedBody, _args))->closeExpr();
}
IHqlExpression * createMetaAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args)
{
    return CHqlMetaAnnotation::createAnnotation(_ownedBody, _args);
}



//==============================================================================================================


CHqlParseMetaAnnotation::CHqlParseMetaAnnotation(IHqlExpression *_body, HqlExprArray & _args)
: CHqlAnnotationWithOperands(_body, _args)
{
}

IHqlExpression * CHqlParseMetaAnnotation::cloneAnnotation(IHqlExpression * newbody)
{
    if (body == newbody)
        return LINK(this);
    return createParseMetaAnnotation(LINK(newbody), operands);
}

IHqlExpression * CHqlParseMetaAnnotation::createAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args)
{
    return (new CHqlParseMetaAnnotation(_ownedBody, _args))->closeExpr();
}
IHqlExpression * createParseMetaAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args)
{
    return CHqlParseMetaAnnotation::createAnnotation(_ownedBody, _args);
}


//==============================================================================================================


CHqlLocationAnnotation::CHqlLocationAnnotation(IHqlExpression *_body, ISourcePath * _sourcePath, int _lineno, int _column)
: CHqlAnnotation(_body), sourcePath(_sourcePath)
{
    lineno = _lineno;
    column = _column;
}

void CHqlLocationAnnotation::sethash()
{
//  CHqlExpression::sethash();
    setInitialHash(annotate_location);          // has the operands, not the body's
    HASHFIELD(body);
    HASHFIELD(sourcePath);
    HASHFIELD(lineno);
    HASHFIELD(column);
}


IHqlExpression * CHqlLocationAnnotation::cloneAnnotation(IHqlExpression * newbody)
{
    if (body == newbody)
        return LINK(this);
    return createLocationAnnotation(LINK(newbody), sourcePath, lineno, column);
}

bool CHqlLocationAnnotation::equals(const IHqlExpression & other) const
{
    if (!CHqlAnnotation::equals(other))
        return false;
    const CHqlLocationAnnotation * cast = static_cast<const CHqlLocationAnnotation *>(&other);
    if ((sourcePath != cast->sourcePath) || (lineno != cast->lineno) || (column != cast->column))
        return false;
    return true;
}

IHqlExpression * CHqlLocationAnnotation::createLocationAnnotation(IHqlExpression * _ownedBody, ISourcePath * _sourcePath, int _lineno, int _column)
{
    return (new CHqlLocationAnnotation(_ownedBody, _sourcePath, _lineno, _column))->closeExpr();
}
IHqlExpression * createLocationAnnotation(IHqlExpression * _ownedBody, const ECLlocation & _location)
{
    return createLocationAnnotation(_ownedBody, _location.sourcePath, _location.lineno, _location.column);
}

IHqlExpression * createLocationAnnotation(IHqlExpression * ownedBody, ISourcePath * sourcePath, int lineno, int column)
{
#ifdef _DEBUG
    assertex(lineno != 0xdddddddd && column != 0xdddddddd);
    assertex(lineno != 0xcdcdcdcd && column != 0xcdcdcdcd);
    assertex(lineno != 0xcccccccc && column != 0xcccccccc);
#endif
    return CHqlLocationAnnotation::createLocationAnnotation(ownedBody, sourcePath, lineno, column);
}


extern HQL_API bool okToAddAnnotation(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_field:
    case no_select:
    case no_param:
    case no_self:
    case no_selfref:
    case no_ifblock:
        return false;
    }
    ITypeInfo * type = expr->queryType();
    if (!type)
        return false;
    return true;
}

extern HQL_API bool okToAddLocation(IHqlExpression * expr)
{
#if defined(ANNOTATE_EXPR_POSITION) || defined(ANNOTATE_DATASET_POSITION)
    if (!okToAddAnnotation(expr))
        return false;
#endif

#if defined(ANNOTATE_EXPR_POSITION)
    return true;
#elif defined(ANNOTATE_DATASET_POSITION)
    ITypeInfo * type = expr->queryType();
    switch (type->getTypeCode())
    {
    case type_dictionary:
    case type_table:
    case type_groupedtable:
    case type_void:
        return true;
    }
    return false;
#else
    return false;
#endif
}

//==============================================================================================================


CHqlAnnotationExtraBase::CHqlAnnotationExtraBase(IHqlExpression *_body, IInterface * _ownedExtra)
: CHqlAnnotation(_body), extra(_ownedExtra)
{
}

void CHqlAnnotationExtraBase::sethash()
{
//  CHqlExpression::sethash();
    setInitialHash(getAnnotationKind());
    HASHFIELD(body);
    HASHFIELD(extra);
}


bool CHqlAnnotationExtraBase::equals(const IHqlExpression & other) const
{
    if (!CHqlAnnotation::equals(other))
        return false;
    const CHqlAnnotationExtraBase * cast = static_cast<const CHqlAnnotationExtraBase *>(&other);
    if (extra != cast->extra)
        return false;
    return true;
}


//==============================================================================================================

//equals could use the following instead...
//  if (!areMatchingPTrees(doc, cast->doc))
//      return false;

CHqlWarningAnnotation::CHqlWarningAnnotation(IHqlExpression *_body, IECLError * _ownedWarning)
: CHqlAnnotationExtraBase(_body, _ownedWarning)
{
}

IHqlExpression * CHqlWarningAnnotation::cloneAnnotation(IHqlExpression * newbody)
{
    if (body == newbody)
        return LINK(this);
    return createWarningAnnotation(LINK(newbody), LINK(queryWarning()));
}

IHqlExpression * CHqlWarningAnnotation::createWarningAnnotation(IHqlExpression * _ownedBody, IECLError * _ownedWarning)
{
    return (new CHqlWarningAnnotation(_ownedBody, _ownedWarning))->closeExpr();
}

IHqlExpression * createWarningAnnotation(IHqlExpression * _ownedBody, IECLError * _ownedWarning)
{
    return CHqlWarningAnnotation::createWarningAnnotation(_ownedBody, _ownedWarning);
}



//==============================================================================================================


CHqlJavadocAnnotation::CHqlJavadocAnnotation(IHqlExpression *_body, IPropertyTree * _ownedJavadoc)
: CHqlAnnotationExtraBase(_body, _ownedJavadoc)
{
}

IHqlExpression * CHqlJavadocAnnotation::cloneAnnotation(IHqlExpression * newbody)
{
    if (body == newbody)
        return LINK(this);
    return createJavadocAnnotation(LINK(newbody), LINK(queryDocumentation()));
}

IHqlExpression * CHqlJavadocAnnotation::createJavadocAnnotation(IHqlExpression * _ownedBody, IPropertyTree * _ownedJavadoc)
{
    return (new CHqlJavadocAnnotation(_ownedBody, _ownedJavadoc))->closeExpr();
}

IHqlExpression * createJavadocAnnotation(IHqlExpression * _ownedBody, IPropertyTree * _ownedJavadoc)
{
    return CHqlJavadocAnnotation::createJavadocAnnotation(_ownedBody, _ownedJavadoc);
}



//==============================================================================================================

CFileContents::CFileContents(IFile * _file, ISourcePath * _sourcePath) : file(_file), sourcePath(_sourcePath)
{
    delayedRead = false;
    if (!preloadFromFile())
        file.clear();
}

bool CFileContents::preloadFromFile()
{
    const char * filename = file->queryFilename();
    if (stdIoHandle(filename)<0)
    {
        delayedRead = true;
        return true;
    }

    //Read std input now to prevent blocking or other weird effects.
    Owned<IFileIO> io = file->openShared(IFOread, IFSHread);
    if (!io)
        throw MakeStringException(0, "Failed to open input '%s'", filename);

    MemoryBuffer mb;
    size32_t rd;
    size32_t sizeRead = 0;
    do {
        rd = io->read(sizeRead, STDIO_BUFFSIZE, mb.reserve(STDIO_BUFFSIZE+1)); // +1 as will need
        sizeRead += rd;
        mb.setLength(sizeRead);
    } while (rd);
    ensureUtf8(mb);
    setContentsOwn(mb);
    return true;
}

void CFileContents::ensureUtf8(MemoryBuffer & contents)
{
    //If looks like utf16 then convert it to utf8
    const void * zero = memchr(contents.bufferBase(), 0, contents.length());
    if (zero)
    {
        MemoryBuffer translated;
        if (convertToUtf8(translated, contents.length(), contents.bufferBase()))
            contents.swapWith(translated);
        else
            throw MakeStringException(1, "File %s doesn't appear to be UTF8", file->queryFilename());
    }
}

void CFileContents::ensureLoaded()
{
    if (!delayedRead)
        return;
    delayedRead = false;
    Owned<IFileIO> io = file->openShared(IFOread, IFSHread);
    if (!io)
        throw MakeStringException(1, "File %s could not be opened", file->queryFilename());

    offset_t size = io->size();
    if (size == (offset_t)-1)
        throw MakeStringException(1, "File %s could not be read", file->queryFilename());

    size32_t sizeToRead = (size32_t)size;
    if (sizeToRead != size)
        throw MakeStringException(1, "File %s is larger than 4Gb", file->queryFilename());

    MemoryBuffer buffer;
    buffer.ensureCapacity(sizeToRead+1);
    byte * contents = static_cast<byte *>(buffer.reserve(sizeToRead));
    size32_t sizeRead = io->read(0, sizeToRead, contents);
    ensureUtf8(buffer);
    setContentsOwn(buffer);

    if (sizeRead != sizeToRead)
        throw MakeStringException(1, "File %s only read %u of %u bytes", file->queryFilename(), sizeRead, sizeToRead);
}

CFileContents::CFileContents(const char *query, ISourcePath * _sourcePath) 
: sourcePath(_sourcePath)
{
    if (query)
        setContents(strlen(query), query);

    delayedRead = false;
}

CFileContents::CFileContents(unsigned len, const char *query, ISourcePath * _sourcePath) 
: sourcePath(_sourcePath)
{
    setContents(len, query);
    delayedRead = false;
}


void CFileContents::setContents(unsigned len, const char * query)
{
    void * buffer = fileContents.allocate(len+1);
    memcpy(buffer, query, len);
    ((byte *)buffer)[len] = '\0';
}

void CFileContents::setContentsOwn(MemoryBuffer & buffer)
{
    buffer.append((byte)0);
    buffer.truncate();
    size32_t len = buffer.length();
    fileContents.setOwn(len, buffer.detach());
}

IFileContents * createFileContentsFromText(unsigned len, const char * text, ISourcePath * sourcePath)
{
    return new CFileContents(len, text, sourcePath);
}

IFileContents * createFileContentsFromText(const char * text, ISourcePath * sourcePath)
{
    //MORE: Treatment of nulls?
    return new CFileContents(text, sourcePath);
}

IFileContents * createFileContentsFromFile(const char * filename, ISourcePath * sourcePath)
{
    Owned<IFile> file = createIFile(filename);
    return new CFileContents(file, sourcePath);
}

IFileContents * createFileContents(IFile * file, ISourcePath * sourcePath)
{
    return new CFileContents(file, sourcePath);
}

class CFileContentsSubset : public CInterfaceOf<IFileContents>
{
public:
    CFileContentsSubset(IFileContents * _contents, size32_t _offset, size32_t _len)
        : contents(_contents), offset(_offset), len(_len)
    {
    }

    virtual IFile * queryFile() { return contents->queryFile(); }
    virtual ISourcePath * querySourcePath() { return contents->querySourcePath(); }
    virtual const char *getText() { return contents->getText() + offset; }
    virtual size32_t length() { return len; }

protected:
    Linked<IFileContents> contents;
    size32_t offset;
    size32_t len;
};

extern HQL_API IFileContents * createFileContentsSubset(IFileContents * contents, size32_t offset, size32_t len)
{
    if (!contents)
        return NULL;
    if ((offset == 0) && (len == contents->length()))
        return LINK(contents);
    return new CFileContentsSubset(contents, offset, len);
}

//==============================================================================================================

CHqlScope::CHqlScope(node_operator _op, IIdAtom * _id, const char * _fullName)
: CHqlExpressionWithType(_op, NULL), id(_id), fullName(_fullName)
{
    containerId = NULL;
    type = this;
    initContainer();
}

CHqlScope::CHqlScope(IHqlScope* scope)
: CHqlExpressionWithType(no_scope, NULL)
{
    id = scope->queryId();
    containerId = NULL;
    fullName.set(scope->queryFullName());
    CHqlScope* s = QUERYINTERFACE(scope, CHqlScope);
    if (s && s->text)
        text.set(s->text);
    type = this;
    initContainer();
}

CHqlScope::CHqlScope(node_operator _op) 
: CHqlExpressionWithType(_op, NULL)
{
    id = NULL;
    containerId = NULL;
    type = this;
}

CHqlScope::~CHqlScope()
{
    if (type == this)
        type = NULL;
}

void CHqlScope::initContainer()
{
    if (fullName)
    {
        const char * dot = strrchr(fullName, '.');
        if (dot)
            containerId = createIdAtom(fullName, dot-fullName);
    }
}

bool CHqlScope::assignableFrom(ITypeInfo * source)
{
    if (source == this)
        return true;
    if (!source)
        return false;

    IHqlScope * scope = ::queryScope(source);
    if (scope)
    {
        if (queryConcreteScope() == scope)
            return true;

        return scope->hasBaseClass(this);
    }

    return false;
}

bool CHqlScope::hasBaseClass(IHqlExpression * searchBase)
{
    if (this == searchBase)
        return true;


    ForEachChild(i, this)
    {
        IHqlExpression * cur = queryChild(i);
        if (cur->queryScope() && cur->queryScope()->hasBaseClass(searchBase))
            return true;
    }
    return false;
}

void CHqlScope::sethash()
{
    switch (op)
    {
    case no_service:
        return;
    case no_scope:
    case no_virtualscope:
    case no_libraryscope:
    case no_libraryscopeinstance:
    case no_concretescope:
        break;
    case no_param:
    case no_privatescope:
    case no_forwardscope:
    case no_mergedscope:
        setInitialHash(0);
        return;
    default:
        throwUnexpectedOp(op);
    }
    setInitialHash(0);

    //MORE: Should symbols also be added as operands - would make more sense....
    SymbolTableIterator iter(symbols);
    HqlExprCopyArray sortedSymbols;
    sortedSymbols.ensure(symbols.count());
    for (iter.first(); iter.isValid(); iter.next()) 
    {
        IHqlExpression *cur = symbols.mapToValue(&iter.query());
        sortedSymbols.append(*cur);
    }
    sortedSymbols.sort(compareSymbolsByName);
    ForEachItemIn(i, sortedSymbols)
    {
        IHqlExpression *cur = &sortedSymbols.item(i);
        HASHFIELD(cur);
    }
}

bool CHqlScope::equals(const IHqlExpression &r) const
{
    return (this == &r);
}

unsigned CHqlScope::getCrc()
{
    return getExpressionCRC(this);
}

IHqlExpression *CHqlScope::clone(HqlExprArray &newkids)
{
    HqlExprArray syms;
    getSymbols(syms);
    return clone(newkids, syms)->queryExpression();
}

IHqlExpression * createFunctionDefinition(IIdAtom * id, IHqlExpression * value, IHqlExpression * parameters, IHqlExpression * defaults, IHqlExpression * attrs)
{
    HqlExprArray args;
    args.append(*value);
    args.append(*parameters);
    if (defaults)
        args.append(*defaults);
    if (attrs)
        attrs->unwindList(args, no_comma);
    return createFunctionDefinition(id, args);
}

IHqlExpression * createFunctionDefinition(IIdAtom * id, HqlExprArray & args)
{
    IHqlExpression * body = &args.item(0);
    IHqlExpression * formals = &args.item(1);
    IHqlExpression * defaults = args.isItem(2) ? &args.item(2) : NULL;

#if 1
    //This is a bit of a waste of time, but need to improve assignableFrom for a function type to ignore the uids.
    QuickExpressionReplacer defaultReplacer;
    HqlExprArray normalized;
    ForEachChild(i, formals)
    {
        IHqlExpression * formal = formals->queryChild(i);
        HqlExprArray attrs;
        unwindChildren(attrs, formal);
        IHqlExpression * normal = createParameter(formal->queryId(), UnadornedParameterIndex, formal->getType(), attrs);
        normalized.append(*normal);
        defaultReplacer.setMapping(formal->queryBody(), normal);
    }

    OwnedHqlExpr newFormals = formals->clone(normalized);
    OwnedHqlExpr newDefaults = defaults ? defaultReplacer.transform(defaults) : NULL;

    ITypeInfo * type = makeFunctionType(body->getType(), newFormals.getClear(), newDefaults.getClear());
#else
    ITypeInfo * type = makeFunctionType(body->getType(), LINK(formals), LINK(defaults));
#endif

    return createNamedValue(no_funcdef, type, id, args);
}



void CHqlScope::defineSymbol(IIdAtom * _id, IIdAtom * moduleName, IHqlExpression *value,
                             bool exported, bool shared, unsigned symbolFlags,
                             IFileContents *fc, int lineno, int column,
                             int _startpos, int _bodypos, int _endpos)
{
    if (!moduleName)
        moduleName = id;

    IHqlExpression * symbol = createSymbol(_id, moduleName, value, NULL, exported, shared, symbolFlags, fc, lineno, column, _startpos, _bodypos, _endpos);
    defineSymbol(symbol);
}

void CHqlScope::defineSymbol(IIdAtom * _id, IIdAtom * moduleName, IHqlExpression *value, bool exported, bool shared, unsigned symbolFlags)
{
    assertex(_id);
    if (!moduleName) moduleName = id;
    defineSymbol(createSymbol(_id, moduleName, value, exported, shared, symbolFlags));
}

void CHqlScope::defineSymbol(IHqlExpression * expr)
{
    assertex(expr->queryName());
    assertex(hasNamedSymbol(expr));
//  assertex(expr->queryBody() != expr && expr->queryName());
    symbols.setValue(expr->queryName(), expr);
    infoFlags |= (expr->getInfoFlags() & HEFalwaysInherit);
    infoFlags2 |= (expr->getInfoFlags2() & HEF2alwaysInherit);
    expr->Release();
}

void CHqlScope::removeSymbol(IIdAtom * _id)
{
    symbols.remove(_id->lower());
    //in general infoFlags needs recalculating - although currently I think this can only occur when a constant has been added
}


IHqlExpression *CHqlScope::lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
    OwnedHqlExpr ret = symbols.getLinkedValue(searchName->lower());

    if (!ret)
        return NULL; 
    
    if (!(ret->isExported() || (lookupFlags & LSFsharedOK)))
        return NULL;

    return ret.getClear();
}


int CHqlScope::getPropInt(IAtom * a, int def) const
{
    return def;
}

bool CHqlScope::getProp(IAtom * a, StringBuffer &ret) const
{ 
    return false;
}



void CHqlScope::getSymbols(HqlExprArray& exprs) const
{
    SymbolTable & localSymbols = const_cast<SymbolTable &>(symbols);
    SymbolTableIterator iter(localSymbols);
    for (iter.first(); iter.isValid(); iter.next()) 
    {
        IHqlExpression *cur = localSymbols.mapToValue(&iter.query());
        if (!cur)
        {
            IAtom * name = * static_cast<IAtom * const *>(iter.query().getKey());
            throw MakeStringException(ERR_INTERNAL_NOEXPR, "INTERNAL: getSymbol %s has no associated expression", name ? name->str() : "");
        }

        cur->Link();
        exprs.append(*cur);
    }
}

IHqlScope * CHqlScope::cloneAndClose(HqlExprArray & children, HqlExprArray & symbols)
{
    ForEachItemIn(i1, children)
        addOperand(LINK(&children.item(i1)));

    ForEachItemIn(i2, symbols)
        defineSymbol(&OLINK(symbols.item(i2)));

    return closeExpr()->queryScope();
}

void CHqlScope::throwRecursiveError(IIdAtom * searchName)
{
    StringBuffer filename;
    if (fullName)
        filename.append(fullName).append('.');
    filename.append(*searchName);

    StringBuffer msg("Definition of ");
    msg.append(*searchName).append(" contains a recursive dependency");
    throw createECLError(ERR_RECURSIVE_DEPENDENCY, msg.toCharArray(), filename, 0, 0, 1);
}

inline bool namesMatch(const char * lName, const char * rName)
{
    if (lName == rName)
        return true;
    if (!lName || !rName)
        return false;
    return strcmp(lName, rName) == 0;
}

static bool scopesEqual(const IHqlScope * l, const IHqlScope * r)
{
    HqlExprArray symL, symR;
    if (!l || !r)
        return l==r;

    if (l->queryName() != r->queryName())
        return false;
    if (!namesMatch(l->queryFullName(), r->queryFullName()))
        return false;

    l->getSymbols(symL);
    r->getSymbols(symR);
    if (symL.ordinality() != symR.ordinality())
        return false;
    ForEachItemIn(i, symL)
        if (symR.find(symL.item(i)) == NotFound)
            return false;
    return true;
}


//==============================================================================================================

CHqlRemoteScope::CHqlRemoteScope(IIdAtom * _name, const char * _fullName, IEclRepositoryCallback * _repository, IProperties* _props, IFileContents * _text, bool _lazy, IEclSource * _eclSource)
: CHqlScope(no_remotescope, _name, _fullName), eclSource(_eclSource)
{
    text.set(_text);
    ownerRepository = _repository;
    loadedAllSymbols = !_lazy;
    props = LINK(_props);
    // Could delay creating this - it would be more efficient
    resolved.setown(createPrivateScope());
}

CHqlRemoteScope::~CHqlRemoteScope()
{
    ::Release(props);
}


bool CHqlRemoteScope::isImplicit() const
{
    unsigned flags=getPropInt(flagsAtom, 0);
    return (flags & PLUGIN_IMPLICIT_MODULE) != 0;
}

bool CHqlRemoteScope::isPlugin() const
{
    unsigned flags=getPropInt(flagsAtom, 0);
    return (flags & PLUGIN_DLL_MODULE) != 0;
}

void CHqlRemoteScope::sethash()
{
    throwUnexpected();
    setInitialHash(0);
}

bool CHqlRemoteScope::equals(const IHqlExpression &r) const
{
    return (this == &r);
}

void CHqlRemoteScope::defineSymbol(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    if (!body || (body->getOperator() == no_nobody))
    {
        //Defining an attribute defined in an ecl file - save the text of a symbol for later parsing
        CHqlScope::defineSymbol(expr);
        return;
    }

    //Slightly ugly and could do with improving....
    //1) If text is specified for the remote scope then the whole text (rather than an individual symbol)
    //will be being parsed, so this definition also needs to be added to the symbols
    bool addToSymbols = false;
    if (text)
        addToSymbols = true;
    else
    {
        //remote scopes should possibly be held on a separate list, but that would require extra lookups in ::lookupSymbol
        //If this expression is defining a remote scope then it (currently) needs to be added to the symbols as well.
        //Don't add other symbols so we don't lose the original text (e.g., if syntax errors in dependent attributes)
        if (dynamic_cast<IHqlRemoteScope *>(body) != NULL)
            addToSymbols = true;
    }

    resolved->defineSymbol(expr);
    if (addToSymbols)
        CHqlScope::defineSymbol(LINK(expr));
}

void CHqlRemoteScope::ensureSymbolsDefined(HqlLookupContext & ctx)
{
    preloadSymbols(ctx, true);
}


void CHqlRemoteScope::preloadSymbols(HqlLookupContext & ctx, bool forceAll)
{
    CriticalBlock block(generalCS);
    if (!loadedAllSymbols)
    {
        if (text)
        {
            doParseScopeText(ctx);
        }
        else
        {
            repositoryLoadModule(ctx, forceAll);
        }
    }
}

void CHqlRemoteScope::doParseScopeText(HqlLookupContext & ctx)
{
    loadedAllSymbols = true;
    bool loadImplicit = true;
    parseModule(queryScope(), text, ctx, NULL, loadImplicit);
}

IHqlExpression *CHqlRemoteScope::clone(HqlExprArray &newkids)
{
    assertex(false);
    Link();
    return this;
}


IHqlExpression *CHqlRemoteScope::lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
//  PrintLog("lookupSymbol %s#%d", searchName->getAtomNamePtr(),version);
    preloadSymbols(ctx, false);
    OwnedHqlExpr resolvedSym = resolved->lookupSymbol(searchName, lookupFlags, ctx);
    if (resolvedSym && resolvedSym->getOperator() == no_processing)
    {
        if (lookupFlags & LSFrequired)
            throwRecursiveError(searchName);
        return NULL;
    }

    if (!(lookupFlags & LSFignoreBase))
    {
        if (resolvedSym)
        {
            ctx.noteExternalLookup(this, resolvedSym);
            return resolvedSym.getClear();
        }
    }

    OwnedHqlExpr ret = CHqlScope::lookupSymbol(searchName, LSFsharedOK|lookupFlags, ctx);
    if(!ret || !ret->hasText())
    { 
        OwnedHqlExpr symbol = repositoryLoadSymbol(searchName);
        if(!symbol)
            return NULL;

        assertex(symbol->isNamedSymbol());
        if (symbol->getOperator() != no_nobody)
        {
            //A nested remote scope...
            resolved->defineSymbol(LINK(symbol));
            return symbol.getClear();
        }

        ret.setown(symbol.getClear());
    }

    if ((lookupFlags & LSFignoreBase))
        return ret.getClear();

    StringBuffer filename;
    if (fullName)
        filename.append(fullName).append('.');
    filename.append(*searchName);

    IFileContents * contents = ret->queryDefinitionText();
    if (!contents || (contents->length() == 0))
    {
        StringBuffer msg("Definition for ");
        msg.append(*searchName).append(" contains no text");
        throw createECLError(ERR_EXPORT_OR_SHARE, msg.toCharArray(), filename, 0, 0, 1);
    }

    OwnedHqlExpr recursionGuard = createSymbol(searchName, LINK(processingMarker), ob_exported);
    resolved->defineSymbol(LINK(recursionGuard));

    unsigned prevErrors = ctx.numErrors();
    parseAttribute(this, contents, ctx, searchName);

    OwnedHqlExpr newSymbol = resolved->lookupSymbol(searchName, LSFsharedOK|lookupFlags, ctx);
    if (ctx.numErrors() != prevErrors)
    {
        //If there was an error processing the attribute then return unknown.
        //The caller will also spot the difference in the error count, so we won't get attributes
        //incorrectly defined.
        if (newSymbol)
            resolved->removeSymbol(searchName);
        return NULL;
    }

    if(!newSymbol || (newSymbol->getOperator() == no_processing))
    {
        if (newSymbol)
            resolved->removeSymbol(searchName);
        StringBuffer msg("Definition must contain EXPORT or SHARED value for ");
        msg.append(*searchName);
        throw createECLError(ERR_EXPORT_OR_SHARE, msg.toCharArray(), filename, 0, 0, 1);
    }

    //Preserve ob_sandbox etc. annotated on the original definition, but not on the parsed code.
    unsigned repositoryFlags=ret->getSymbolFlags();
    IHqlNamedAnnotation * symbol = queryNameAnnotation(newSymbol);
    assertex(symbol);
    symbol->setRepositoryFlags(repositoryFlags);

    if (repositoryFlags&ob_sandbox)
    {
        if (ctx.errs)
            ctx.errs->reportWarning(WRN_DEFINITION_SANDBOXED,"Definition is sandboxed",filename,0,0,0);
    }

    if (!(newSymbol->isExported() || (lookupFlags & LSFsharedOK)))
        return NULL;

    return newSymbol.getClear();
}

void CHqlRemoteScope::getSymbols(HqlExprArray& exprs) const
{
    //ensureSymbolsDefined should have been called before this function.  If not the symbols won't be present...

    //ugly ugly hack for getting plugin symbols.
    if (text && resolved)
        resolved->getSymbols(exprs);
    else
        CHqlScope::getSymbols(exprs);
}


IFileContents * CHqlRemoteScope::queryDefinitionText() const
{
    return text;
}

int CHqlRemoteScope::getPropInt(IAtom * a, int def) const
{
    if (props)
        return props->getPropInt(a->getAtomNamePtr(), def);
    else
        return def;
}

bool CHqlRemoteScope::getProp(IAtom * a, StringBuffer &ret) const
{ 
    return (props != NULL && props->getProp(a->getAtomNamePtr(), ret));
}



void CHqlRemoteScope::setProp(IAtom * a, int val)
{
    if (!props)
        props = createProperties();
    props->setProp(a->getAtomNamePtr(), val);
}

void CHqlRemoteScope::setProp(IAtom * a, const char * val)
{
    if (!props)
        props = createProperties();
    props->setProp(a->getAtomNamePtr(), val);
}


void CHqlRemoteScope::repositoryLoadModule(HqlLookupContext & ctx, bool forceAll)
{
    if (ownerRepository->loadModule(this, ctx.errs, forceAll))
    {
        assertex(!text);
        loadedAllSymbols = true;
    }
}

IHqlExpression * CHqlRemoteScope::repositoryLoadSymbol(IIdAtom * attrName)
{
    if (loadedAllSymbols)
        return NULL;

    OwnedHqlExpr symbol = ownerRepository->loadSymbol(this, attrName);
    if(!symbol || !symbol->hasText())
        return NULL;

    return symbol.getClear();
}


extern HQL_API void ensureSymbolsDefined(IHqlScope * scope, HqlLookupContext & ctx)
{
    scope->ensureSymbolsDefined(ctx);
}

extern HQL_API void ensureSymbolsDefined(IHqlExpression * scopeExpr, HqlLookupContext & ctx)
{
    IHqlScope * scope = scopeExpr->queryScope();
    if (scope)
        scope->ensureSymbolsDefined(ctx);
}

//==============================================================================================================

void exportSymbols(IPropertyTree* data, IHqlScope * scope, HqlLookupContext & ctx)
{
    scope->ensureSymbolsDefined(ctx); 

    data->setProp("@name", scope->queryFullName());
    unsigned access = scope->getPropInt(accessAtom, 3);

    HqlExprArray allSymbols;
    scope->getSymbols(allSymbols);

    ForEachItemIn(i, allSymbols)
    {
        IHqlExpression *cur = &allSymbols.item(i);
        if (cur && !isImport(cur) && (cur->getOperator() != no_remotescope))
        {
            IPropertyTree * attr=createPTree("Attribute", ipt_caseInsensitive);
            attr->setProp("@name", *cur->queryName());
            unsigned symbolFlags = cur->getSymbolFlags();
            if (cur->isExported())
                symbolFlags |= ob_exported;
            if(access >= cs_read)
                symbolFlags |= ob_showtext;
            attr->setPropInt("@flags", symbolFlags);
            attr->setPropInt("@version", (1<<18 | 1));
            attr->setPropInt("@latestVersion", (1<<18 | 1));
            attr->setPropInt("@access", access);

            if(cur->hasText())
            {
                StringBuffer attrText;
                cur->getTextBuf(attrText);
                attrText.clip();
                attr->setProp("Text",attrText.str());
            }
            data->addPropTree("Attribute",attr);
        }
    }
}



//==============================================================================================================

CHqlLocalScope::CHqlLocalScope(IHqlScope* scope)
: CHqlScope(scope)
{
}

CHqlLocalScope::CHqlLocalScope(node_operator _op, IIdAtom * _name, const char * _fullName)
: CHqlScope(_op, _name, _fullName)
{
}

void CHqlLocalScope::sethash()
{
    CHqlScope::sethash();
}

bool CHqlLocalScope::equals(const IHqlExpression &r) const
{
    if (!CHqlExpression::equals(r))
        return false;

    if (!scopesEqual(this, const_cast<IHqlExpression &>(r).queryScope()))
        return false;

    return true;
}

IHqlExpression *CHqlLocalScope::clone(HqlExprArray &newkids)
{
    HqlExprArray syms;
    getSymbols(syms);
    return clone(newkids, syms)->queryExpression();
}

IHqlScope * CHqlLocalScope::clone(HqlExprArray & children, HqlExprArray & symbols)
{
    CHqlScope * cloned = new CHqlLocalScope(op, id, fullName);
    return cloned->cloneAndClose(children, symbols);
}

//==============================================================================================================

/*

A merged scope is used to make two different scopes (e.g., from two difference sources) appear as one.
There are several complications (mainly caused by the legacy import semantics):

a definitions of plugins in earlier scopes take precedence
b Legacy import needs to get a list of all implicit modules
c You want to avoid parsing any attributes until actually required.
d You need to examine the resolved attributes from the scopes to determine if they are implicit
 *
Unfortunately b,c,d are mutually incompatible.  Possibly solutions might be
a) Don't support legacy any more.
   Nice idea, but not for a couple of years.
b) only gather implicit modules from the first repository.
   Unfortunately this means archives containing plugins not registered with eclcc no longer compile, which causes
   problems regression testing.w
c) Ensure all system plugins are parsed (so that (d) works, and allows the parsing short-circuiting to work).
   A bit ugly that you need to remember to do it, but has the advantage of ensuring plugins have no dependencies on
   anything else.

 */

void CHqlMergedScope::addScope(IHqlScope * scope)
{
    //This only supports real scopes - it can't be based on modile definitions that could be virtual.
    assertex(!scope->queryExpression()->queryAttribute(_virtualSeq_Atom));
    if (mergedScopes.ordinality())
    {
        const char * name0 = mergedScopes.item(0).queryFullName();
        const char * name1 = scope->queryFullName();
        assertex(name0 == name1 || (name0 && name1 && (stricmp(name0, name1) == 0)));
    }
    mergedScopes.append(*LINK(scope));
}

bool CHqlMergedScope::allBasesFullyBound() const
{
    ForEachItemIn(i, mergedScopes)
    {
        if (!mergedScopes.item(i).allBasesFullyBound())
            return false;
    }
    return true;
}

inline bool canMergeDefinition(IHqlExpression * expr)
{
    IHqlScope * scope = expr->queryScope();
    if (!scope || expr->hasText())
        return false;
    return true;
}

IHqlExpression * CHqlMergedScope::lookupSymbol(IIdAtom * searchId, unsigned lookupFlags, HqlLookupContext & ctx)
{
    CriticalBlock block(cs);
    OwnedHqlExpr resolved = CHqlScope::lookupSymbol(searchId, lookupFlags, ctx);
    if (resolved)
    {
        node_operator resolvedOp = resolved->getOperator();
        if (resolvedOp == no_merge_pending)
        {
            if (lookupFlags & LSFrequired)
                throwRecursiveError(searchId);
            return NULL;
        }
        if (resolvedOp == no_merge_nomatch)
            return NULL;
        if (resolvedOp != no_nobody)
            return resolved.getClear();
    }
    else
    {
        if (mergedAll)
            return NULL;
    }

    OwnedHqlExpr recursionGuard = createSymbol(searchId, LINK(mergePendingMarker), ob_exported);
    defineSymbol(LINK(recursionGuard));

    OwnedHqlExpr previousMatch;
    Owned<CHqlMergedScope> mergeScope;
    unsigned prevErrors = ctx.numErrors();
    unsigned symbolFlags = 0;
    ForEachItemIn(i, mergedScopes)
    {
        OwnedHqlExpr matched = mergedScopes.item(i).lookupSymbol(searchId, lookupFlags, ctx);
        if (matched)
        {
            if (!canMergeDefinition(matched))
            {
                if (!previousMatch)
                    previousMatch.setown(matched.getClear());
                break;
            }

            if (previousMatch)
            {
                IHqlScope * previousScope = previousMatch->queryScope();
                mergeScope.setown(new CHqlMergedScope(searchId, previousScope->queryFullName()));
                mergeScope->addScope(previousScope);
            }

            //Not so sure about this....
            symbolFlags |= matched->getSymbolFlags();
            if (mergeScope)
            {
                IHqlScope * scope = matched->queryScope();
                mergeScope->addScope(scope);
            }
            else
                previousMatch.setown(matched.getClear());
        }
        else
        {
            //Prevent cascaded errors in attributes which shouldn't have been reachable (e.g., syntax checking)
            if (prevErrors != ctx.numErrors())
                break;
        }
    }
    if (mergeScope)
    {
        OwnedHqlExpr newScope = mergeScope.getClear()->closeExpr();
        IHqlExpression * symbol = createSymbol(searchId, id, LINK(newScope), true, false, symbolFlags);
        defineSymbol(symbol);
        return LINK(symbol);
    }
    if (previousMatch)
    {
        defineSymbol(LINK(previousMatch));
        return previousMatch.getClear();
    }
    //Indicate that no match was found to save work next time.
    defineSymbol(createSymbol(searchId, LINK(mergeNoMatchMarker), ob_exported));
    return NULL;
}


//This function is only likely to be called when gathering implicit modules for the legacy option
void CHqlMergedScope::ensureSymbolsDefined(HqlLookupContext & ctx)
{
    CriticalBlock block(cs);
    if (mergedAll)
        return;

    ForEachItemIn(iScope, mergedScopes)
    {
        IHqlScope & cur = mergedScopes.item(iScope);
        cur.ensureSymbolsDefined(ctx);

        HqlExprArray scopeSymbols;
        cur.getSymbols(scopeSymbols);

        //Generate a list of symbols from all the merged scopes
        //If the symbol has already been resolved then store that resolved definition
        //If a definition can be merged with one from a previous one insert a named symbol with no body as a placeholder.
        ForEachItemIn(iSym, scopeSymbols)
        {
            IHqlExpression & cur = scopeSymbols.item(iSym);
            IIdAtom * curName = cur.queryId();

            OwnedHqlExpr prev = symbols.getLinkedValue(curName->lower());
            if (prev)
            {
                //Unusual - check that this hasn't already been resolved by a call to lookupSymbol()
                //otherwise recreating a merged scope can cause problems.
                if ((prev->getOperator() != no_mergedscope) && (prev != &cur))
                {
                    if (canMergeDefinition(prev))
                    {
                        //no_nobody means it need to be resolve - either because multiple matches, or because
                        //a child scope hasn't resolved it yet.
                        if (prev->getOperator() != no_nobody)
                        {
                            IHqlExpression * newSymbol = createSymbol(curName, id, NULL, true, false, 0);
                            defineSymbol(newSymbol);
                        }
                    }
                }
            }
            else
                defineSymbol(LINK(&cur));
        }
    }

    mergedAll = true;
}

bool CHqlMergedScope::isImplicit() const
{
    //If implicit only the first scope will count.
    if (mergedScopes.ordinality())
        return mergedScopes.item(0).isImplicit();
    return false;
}

bool CHqlMergedScope::isPlugin() const
{
    //If a plugin only the first scope will count.
    if (mergedScopes.ordinality())
        return mergedScopes.item(0).isPlugin();
    return false;
}




//==============================================================================================================

CHqlLibraryInstance::CHqlLibraryInstance(IHqlExpression * _funcdef, HqlExprArray &parms) : CHqlScope(no_libraryscopeinstance), scopeFunction(_funcdef)
{
    assertex(scopeFunction->getOperator() == no_funcdef);
    libraryScope = scopeFunction->queryChild(0)->queryScope();
    ForEachItemIn(i, parms)
        addOperand(&parms.item(i));
    parms.kill(true);
}

IHqlExpression * CHqlLibraryInstance::clone(HqlExprArray & children)
{
    return createLibraryInstance(LINK(scopeFunction), children);
}

IHqlScope * CHqlLibraryInstance::clone(HqlExprArray & children, HqlExprArray & symbols)
{
    throwUnexpected();
    return clone(children)->queryScope();
}


IHqlExpression *CHqlLibraryInstance::lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
    OwnedHqlExpr ret = libraryScope->lookupSymbol(searchName, lookupFlags, ctx);

    if (!ret)
        return NULL;

    if (!ret->isExported())
        return ret.getClear();

    //Not sure about following:
    if ((lookupFlags & LSFignoreBase))
        return ret.getClear();
    
    return createDelayedReference(no_libraryselect, this, ret, lookupFlags, ctx);
}

IHqlExpression * createLibraryInstance(IHqlExpression * scopeFunction, HqlExprArray &operands)
{
    return (new CHqlLibraryInstance(scopeFunction, operands))->closeExpr();
}

bool CHqlLibraryInstance::equals(const IHqlExpression & other) const
{
    if (!CHqlExpression::equals(other))
        return false;
    if (queryExternalDefinition() != other.queryExternalDefinition())
        return false;
    return true;
}

void CHqlLibraryInstance::sethash()
{
    CHqlScope::sethash();
    HASHFIELD(scopeFunction);
}

//==============================================================================================================

//Each function instance needs to have a unique set of parameters
static IHqlExpression * cloneFunction(IHqlExpression * expr)
{
    if (expr->getOperator() != no_funcdef)
        return LINK(expr);

    IHqlExpression * formals = expr->queryChild(1);
    if (formals->numChildren() == 0)
        return LINK(expr);

    QuickExpressionReplacer replacer;
    ForEachChild(i, formals)
    {
        IHqlExpression * formal = formals->queryChild(i);
        unsigned seq = (unsigned)formal->querySequenceExtra();
        HqlExprArray attrs;
        unwindChildren(attrs, formal);
        OwnedHqlExpr newFormal = createParameter(formal->queryId(), seq, formal->getType(), attrs);
        assertex(formal != newFormal || seq == UnadornedParameterIndex);
        replacer.setMapping(formal, newFormal);
    }
    return replacer.transform(expr);
}


IHqlExpression * createDelayedReference(node_operator op, IHqlExpression * moduleMarker, IHqlExpression * attr, unsigned lookupFlags, HqlLookupContext & ctx)
{
    IHqlExpression * record = queryOriginalRecord(attr);
    if (!record)
        record = queryNullRecord();

    HqlExprArray args;
    args.append(*LINK(record));
    args.append(*LINK(moduleMarker));
    args.append(*LINK(attr));
    args.append(*createOpenNamedValue(no_attrname, makeVoidType(), attr->queryId())->closeExpr());
    if (lookupFlags & LSFignoreBase)
        args.append(*createAttribute(ignoreBaseAtom));
    if (isGrouped(attr))
        args.append(*createAttribute(groupedAtom));

    OwnedHqlExpr ret;
    if (attr->isFunction())
        ret.setown(createValue(op, attr->getType(), args));
    else if (attr->isDataset())
        ret.setown(createDataset(op, args));
    else if (attr->isDatarow())
        ret.setown(createRow(op, args));
    else
        ret.setown(createValue(op, attr->getType(), args));

    if (attr->isScope())
    {
        if (attr->getOperator() != no_funcdef)
            ret.setown(createDelayedScope(ret.getClear()));
    }

    return attr->cloneAllAnnotations(ret);
}

//---------------------------------------------------------------------------------------------------------------------

class VirtualSymbolReplacer : public QuickHqlTransformer
{
public:
    VirtualSymbolReplacer(HqlTransformerInfo & _info, IErrorReceiver * _errors, IHqlExpression * _searchModule)
    : QuickHqlTransformer(_info, _errors)
    { 
        visited.setown(createAttribute(alreadyVisitedAtom));
        searchModule = _searchModule;
    }

    virtual IHqlExpression * transform(IHqlExpression * expr)
    {
#ifdef TRANSFORM_STATS
        stats.beginTransform();
#endif
        IHqlExpression * match = static_cast<IHqlExpression *>(expr->queryTransformExtra());
        if (match)
        {
            if (match == visited)
            {
                IIdAtom * id = expr->queryId();
                const char * idText = id ? id->str() : "";
                ECLlocation loc(searchModule->queryAttribute(_location_Atom));
                reportError(errors, HQLERR_CycleWithModuleDefinition, loc, HQLERR_CycleWithModuleDefinition_Text, idText);
            }

#ifdef TRANSFORM_STATS
            stats.endMatchTransform(expr, match);
#endif
            return LINK(match);
        }

        IHqlExpression * ret;
        if (containsInternalSelect(expr))
        {
            expr->setTransformExtraUnlinked(visited);
            ret = createTransformed(expr);
        }
        else
            ret = LINK(expr);

#ifdef TRANSFORM_STATS
        stats.endNewTransform(expr, ret);
#endif

        expr->setTransformExtra(ret);
        return ret;
    }

    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr)
    {
        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_unboundselect:
            throwUnexpected();
        case no_internalselect:
            if (expr->queryChild(1) == searchModule)
            {
                IIdAtom * id = expr->queryChild(3)->queryId();
                return getVirtualReplacement(id);
            }
            break;
        }

        return QuickHqlTransformer::createTransformedBody(expr);
    }

protected:
    virtual IHqlExpression * getVirtualReplacement(IIdAtom * id) = 0;

protected:
    OwnedHqlExpr visited;
    IHqlExpression * searchModule;
};


//---------------------------------------------------------------------------------------------------------------------

static HqlTransformerInfo concreteVirtualSymbolReplacerInfo("ConcreteVirtualSymbolReplacer");
class ConcreteVirtualSymbolReplacer : public VirtualSymbolReplacer
{
public:
    ConcreteVirtualSymbolReplacer(IErrorReceiver * _errors, IHqlExpression * _searchModule, SymbolTable & _symbols)
    : VirtualSymbolReplacer(concreteVirtualSymbolReplacerInfo, _errors, _searchModule), symbols(_symbols)
    {
    }

    virtual IHqlExpression * getVirtualReplacement(IIdAtom * name)
    {
        OwnedHqlExpr value = symbols.getLinkedValue(name->lower());
        return transform(value);
    }

protected:
    SymbolTable & symbols;
};


//---------------------------------------------------------------------------------------------------------------------

static HqlTransformerInfo parameterVirtualSymbolReplacerInfo("ParameterVirtualSymbolReplacer");
class ParameterVirtualSymbolReplacer : public VirtualSymbolReplacer
{
public:
    ParameterVirtualSymbolReplacer(HqlLookupContext & _ctx, IHqlScope * _paramScope, IHqlExpression * _virtualAttribute, unsigned _lookupFlags)
    : VirtualSymbolReplacer(concreteVirtualSymbolReplacerInfo, _ctx.errs, _virtualAttribute),
      ctx(_ctx), paramScope(_paramScope), lookupFlags(_lookupFlags)
    {
    }

    virtual IHqlExpression * getVirtualReplacement(IIdAtom * id)
    {
        return paramScope->lookupSymbol(id, lookupFlags, ctx);
    }

protected:
    HqlLookupContext & ctx;
    IHqlScope * paramScope;
    unsigned lookupFlags;
};


//---------------------------------------------------------------------------------------------------------------------

/*
Modules, members, scopes and virtual members.

ECL allows some modules to be "virtual"
- MODULE,VIRTUAL and INTERFACE indicate that every member within that module is virtual
- EXPORT VIRTUAL and SHARED VIRTUAL before a definition mark that definition as virtual
- If an EXPORTED of SHARED symbol is VIRTUAL in a base module it is virtual in this module.

Each module that contains any virtuals has a unique virtualSeq attribute associated with it.
While a virtual module is being defined it is marked as incomplete:
* When defining a member of a virtual class, references to other members of the class need to be delayed.  (The might
  be redefined in this module, or in a module that inherits from this one.)
  This is done by adding an expression no_internalselect(virtual-attribute, <resolved-member>, flags, ...)
  - <resolved> is used for the name and for the type - its value is not used.
* Whenever a member is selected from an incomplete module it must be a reference from one of its own members, so
  lookupSymbol() returns an expression in this form.
* Some types of expressions (e.g., records) must currently be returned as no_record and can't be delayed.  In this case
  the value is returned (along with no_internalselects for members it references).

When a module is completed *in the parser* the following happens:
* Any definitions that haven't been overridden are extracted from the base modules.
  o If the base module is unbound then create a no_unboundselect as a delayed reference.
  o If the base definition is non-virtual, inherit the same definition (add test!)
  o Otherwise use the definition from the base with the base module's virtual uid replaced with references
    to the current module's virtual uid. [definition could also be unbound or undefined]
  o If the definition is defined in multiple base classes ambiguously then complain.
    (An alternative would be to convert it to a no_purevirtual definition and allow it to be overridden later.)
  o If some definitions are no_unboundselect then add a $hasUnknownBase internal attribute.
  - NOTE: Issue HPCC-9325 proposes changing the way inherited members are processed.

When a module expression is closed:
* If all bases are bound:
  o If the module has a $hasUnknownBase attribute then re-resolve all no_unboundselect definitions, and remove the attribute.

Parameters:
* A parameter needs to be projected to the type of the argument to avoid symbols in a derived module from clashing
  with those defined inside the function.  (Stops the interface becoming polluted with new names).
* Could get syntax errors from incompatible definitions (e.g., derived from parameter) and derived argument.

The concrete module is created on demand if the module is not abstract.

When a value is retrieved from a virtual module the following happens:
* If being accessed from a derived module just return the definition
* If has a concrete value then return it.
* If it doesn't have any unbound bases then it is an error accessing an abstract class.
* If the type of the value cannot be virtual
  o return the value with all no_internalselects and no_unboundselects recursively expanded as no_delayedselects.
* Create no_assertconcrete node for the current scope. - may not be needed.
  Create a no_delayedselect(<asserted-concrete>, <resolved-member>, flags, ...)

Delayedselects/internalselects
* Create a <op>(scope, <resolved-member>, flags, ...)
* If results is a module wrap it in a no_delayedscope
* If it is a function.....then it will create a CHqlDelayedCall

The following opcodes are used

no_internalselect - a reference to another member within the same module definition
no_unboundselect - a reference to a member in a base module definition that is currently unbound.
no_delayedselect - a (delayed) reference to a member in another module.
no_purevirtual - a member that has no associated definition.

*/

bool canBeDelayed(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    //The fewer entries in here the better
    case no_enum:
    case no_typedef:
    case no_macro:
    case no_record:             // LEFT has problems because queryRecord() is the unbound funcdef
    case no_keyindex:           // BUILD() and other functions a reliant on this being expanded out
    case no_newkeyindex:
        return false;
    case no_funcdef:
        return canBeDelayed(expr->queryChild(0));
    }
    return true;
}

bool canBeVirtual(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
    case no_enum:
    case no_typedef:
    case no_type:
    case no_macro:
        return false;
    case no_virtualscope:
    case no_delayedscope:
    case no_call:
        return true;
    case no_concretescope:
    case no_libraryscope:
    case no_libraryscopeinstance:
    case no_forwardscope:
        return false;
    case no_scope:
        throwUnexpected();
    case no_funcdef:
        return canBeVirtual(expr->queryChild(0));
    }
    assertex(!expr->isScope());
    return true;
}

CHqlVirtualScope::CHqlVirtualScope(IIdAtom * _name, const char * _fullName)
: CHqlScope(no_virtualscope, _name, _fullName)
{
    isAbstract = false;
    complete = false;
    containsVirtual = false;
    allVirtual = false;
    fullyBoundBase =true;
}

IHqlExpression *CHqlVirtualScope::addOperand(IHqlExpression * arg)
{
    if (arg->isAttribute())
    {
        IAtom * name = arg->queryName();
        if (name == _virtualSeq_Atom)
        {
            if (arg->querySequenceExtra() == 0)
            {
                //create a virtual attribute with a unique id.
                ensureVirtualSeq();
                arg->Release();
                return this;
            }
            assertex(!hasAttribute(_virtualSeq_Atom));
            containsVirtual = true;
        }
        else if (name == interfaceAtom || name == virtualAtom)
        {
            ensureVirtualSeq();
            isAbstract = (name == interfaceAtom);
            allVirtual = true;
        }
    }
    else if (arg->isScope())
    {
        if (arg->hasAttribute(_virtualSeq_Atom))
            ensureVirtualSeq();
        else if (arg->getOperator() == no_param)
            ensureVirtualSeq();    // yuk- needs another member function.

        if (!areAllBasesFullyBound(arg))
            fullyBoundBase = false;
    }
    return CHqlScope::addOperand(arg);
}


static UniqueSequenceCounter virtualSequence;
void CHqlVirtualScope::ensureVirtualSeq()
{
    if (!containsVirtual)
    {
        CHqlScope::addOperand(createSequence(no_attr, makeNullType(), _virtualSeq_Atom, virtualSequence.next()));
        containsVirtual = true;
    }
}

void CHqlVirtualScope::sethash()
{
    complete = true;
    CHqlScope::sethash();
}

bool CHqlVirtualScope::equals(const IHqlExpression &r) const
{
    if (!CHqlExpression::equals(r))
        return false;

    if (!scopesEqual(this, const_cast<IHqlExpression &>(r).queryScope()))
        return false;

    return true;
}

IHqlExpression *CHqlVirtualScope::clone(HqlExprArray &newkids)
{
    assertex(false);
    Link();
    return this;
}

IHqlScope * CHqlVirtualScope::clone(HqlExprArray & children, HqlExprArray & symbols)
{
    CHqlScope * cloned = new CHqlVirtualScope(id, fullName);
    return cloned->cloneAndClose(children, symbols);
}

extern HQL_API bool isVirtualSymbol(IHqlExpression * expr)
{
    IHqlNamedAnnotation * symbol = static_cast<IHqlNamedAnnotation *>(expr->queryAnnotation());
    if (symbol && symbol->getAnnotationKind() == annotate_symbol)
        return symbol->isVirtual();
    return false;
}

void CHqlVirtualScope::defineSymbol(IHqlExpression * expr)
{
#ifdef ALL_MODULE_ATTRS_VIRTUAL
    ensureVirtual();            // remove if we want it to be conditional on an attribute
#endif
    if (isPureVirtual(expr))
    {
        isAbstract = true;
        ensureVirtualSeq();        // a little bit too late... but probably good enough. should be an error if not virtual.
    }
    else
    {
        if (isVirtualSymbol(expr))
        {
            ensureVirtualSeq();
        }
        else
        {
            //MORE: If this is virtual in the base class then it is an error if virtual flag not set
        }
    }

    CHqlScope::defineSymbol(expr);
}

bool CHqlVirtualScope::queryForceSymbolVirtual(IIdAtom * searchName, HqlLookupContext & ctx)
{
    if (allVirtual)
        return true;

    IHqlExpression * definitionModule = NULL;
    OwnedHqlExpr match = lookupBaseSymbol(definitionModule, searchName, LSFsharedOK, ctx);
    if (match)
    {
        IHqlNamedAnnotation * symbol = static_cast<IHqlNamedAnnotation *>(match->queryAnnotation());
        assertex(symbol && symbol->getAnnotationKind() == annotate_symbol);
        return symbol->isVirtual();
    }
    return false;
}

IHqlExpression * CHqlVirtualScope::lookupBaseSymbol(IHqlExpression * & definitionModule, IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
    ForEachChild(i, this)
    {
        IHqlExpression * child = queryChild(i);
        IHqlScope * base = child->queryScope();
        if (base)
        {
            IHqlExpression * match = base->lookupSymbol(searchName, lookupFlags|LSFfromderived, ctx);
            if (match)
            {
                definitionModule = child;
                return match;
            }
        }
    }
    return NULL;
}

IHqlExpression *CHqlVirtualScope::lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
    //Are we just trying to find out what the definition in this scope is?
    if (lookupFlags & LSFignoreBase)
        return CHqlScope::lookupSymbol(searchName, lookupFlags, ctx);

    //The scope is complete=>this is a reference from outside the scope
    if (complete)
    {
        //NOTE: If the members are virtual, then all that is significant is whether a match exists or not.
        if (concrete && !(lookupFlags & LSFfromderived))
            return concrete->lookupSymbol(searchName, lookupFlags, ctx);

        //The class is not concrete...
        //1. It is based on a parameter, and not complete because the parameter hasn't been substituted.
        //2. A reference from a derived module accessing the base definition.
        //3. An illegal access to a member of an abstract class
        OwnedHqlExpr match = CHqlScope::lookupSymbol(searchName, lookupFlags, ctx);
        if (!match)
            return NULL;

        if (!containsVirtual || (lookupFlags & LSFfromderived))
            return match.getClear();

        if (!isVirtualSymbol(match))
        {
            //Select from a parameter where the item is not defined in the parameter's scope => error
            if (match->getOperator() == no_unboundselect)
                throwError1(HQLERR_MemberXContainsVirtualRef, searchName->str());

            if (containsInternalSelect(match))
            {
                //All internal references need to be recursively replaced with no_delayedselect
                //e.g., if fields used to provide default values for records.
                ParameterVirtualSymbolReplacer replacer(ctx, this, queryAttribute(_virtualSeq_Atom), LSFsharedOK);
                return replacer.transform(match);
            }
            return match.getClear();
        }
        assertex(canBeVirtual(match));

        match.setown(createDelayedReference(no_delayedselect, this, match, lookupFlags, ctx));

        //module.x can be used for accessing a member of a base module - even if it is abstract
        return match.getClear();
    }

    //check to see if it is defined in the current module
    OwnedHqlExpr match = CHqlScope::lookupSymbol(searchName, lookupFlags, ctx);
    IHqlExpression * definitionModule = this;
    if (!match)
        match.setown(lookupBaseSymbol(definitionModule, searchName, lookupFlags, ctx));

    if (match)
    {
        if (!containsVirtual)
            return match.getClear();

        if (!isVirtualSymbol(match))
        {
            //Select from a parameter where the item is not defined in the parameter's scope => error
            node_operator matchOp = match->getOperator();
            if (matchOp == no_unboundselect || matchOp == no_purevirtual)
                throwError1(HQLERR_MemberXContainsVirtualRef, searchName->str());

            if (containsInternalSelect(match))
            {
                //throwError1(HQLERR_MemberXContainsVirtualRef, searchName->str());
                //All internal references need to be recursively replaced with no_internalselect
                //e.g., if fields used to provide default values for records.
                ParameterVirtualSymbolReplacer replacer(ctx, this, definitionModule->queryAttribute(_virtualSeq_Atom), LSFsharedOK|LSFfromderived);
                return replacer.transform(match);
            }
            return match.getClear();
        }
        assertex(canBeVirtual(match));

        //References to "virtual" members are always represented with a delayed reference,
        //so that the correct definition will be used.
        return createDelayedReference(no_internalselect, queryAttribute(_virtualSeq_Atom), match, lookupFlags, ctx);
    }

    return NULL;
}

IHqlExpression * CHqlVirtualScope::closeExpr()
{
    if (containsVirtual && !isAbstract && fullyBoundBase)
    {
        resolveUnboundSymbols();
        concrete.setown(deriveConcreteScope());
        if (!containsInternalSelect(concrete->queryExpression()))
            infoFlags &= ~HEFinternalSelect;
    }

    return CHqlScope::closeExpr();
}

IHqlScope * CHqlVirtualScope::deriveConcreteScope()
{
    Owned<IHqlScope> scope = createConcreteScope();
    IHqlExpression * scopeExpr = scope->queryExpression();

    ForEachChild(i, this)
        scopeExpr->addOperand(LINK(queryChild(i)));

    //begin scope
    {
        ConcreteVirtualSymbolReplacer replacer(NULL, queryAttribute(_virtualSeq_Atom), symbols);
        SymbolTableIterator iter(symbols);
        ForEach(iter)
        {
            IHqlExpression *cur = symbols.mapToValue(&iter.query());
            OwnedHqlExpr mapped = replacer.transform(cur);
            OwnedHqlExpr rebound = cloneFunction(mapped);
            scope->defineSymbol(rebound.getClear());
        }
    }

    return scope.getClear()->queryExpression()->closeExpr()->queryScope();
}


void CHqlVirtualScope::resolveUnboundSymbols()
{
    IHqlExpression * virtualAttr = queryAttribute(_virtualSeq_Atom);
    Owned<IErrorReceiver> errorReporter = createThrowingErrorReceiver();
    HqlDummyLookupContext localCtx(errorReporter);
    SymbolTableIterator iter(symbols);
    HqlExprArray defines;
    ForEach(iter)
    {
        IHqlExpression *cur = symbols.mapToValue(&iter.query());
        if (cur->getOperator() == no_unboundselect)
        {
            IIdAtom * searchName = cur->queryId();
            OwnedHqlExpr match;
            ForEachChild(i, this)
            {
                IHqlExpression * child = queryChild(i);
                IHqlScope * base = child->queryScope();
                if (base)
                {
                    OwnedHqlExpr resolved = base->lookupSymbol(searchName, LSFsharedOK|LSFfromderived, localCtx);
                    if (resolved)
                    {
                        //Select from a parameter where the item is not defined in the parameter's scope => error
                        node_operator resolvedOp = resolved->getOperator();
                        if (resolvedOp == no_unboundselect || resolvedOp == no_purevirtual)
                            throwError1(HQLERR_MemberXContainsVirtualRef, searchName->str());

                        match.setown(quickFullReplaceExpression(resolved, child->queryAttribute(_virtualSeq_Atom), virtualAttr));
                        break;
                    }
                }
            }

            assertex(match);
            defines.append(*match.getClear());
        }
    }

    //Define the symbols after the iterator so the hash table isn't invalidated
    ForEachItemIn(i, defines)
        defineSymbol(LINK(&defines.item(i)));
}


//==============================================================================================================

class CHqlForwardScope : public CHqlVirtualScope, public IHasUnlinkedOwnerReference
{
public:
    CHqlForwardScope(IHqlScope * _parentScope, HqlGramCtx * _parentCtx, HqlParseContext & parseCtx);
    ~CHqlForwardScope()
    {
        assertex(!parentScope);
    }
    IMPLEMENT_IINTERFACE_USING(CHqlVirtualScope)

    virtual void clearOwner()
    {
        if (parentScope)
        {
            resolvedAll = true;
            parentCtx.clear();
            parentScope = NULL;
        }
    }

    virtual void defineSymbol(IHqlExpression * expr);
    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual IHqlScope * queryResolvedScope(HqlLookupContext * context);

protected:
    IHqlScope * parentScope;
    Owned<HqlGramCtx> parentCtx;
    Owned<IHqlScope> resolved;
    bool resolvedAll;
};

//error if !fullBoundBase || isVirtual
CHqlForwardScope::CHqlForwardScope(IHqlScope * _parentScope, HqlGramCtx * _parentCtx, HqlParseContext & parseCtx)
: CHqlVirtualScope(NULL, NULL), parentScope(_parentScope), parentCtx(_parentCtx)
{
    //Need to register this foward reference otherwise circular reference means it will never get deleted.
    parseCtx.addForwardReference(parentScope, this);
    op = no_forwardscope; 
    assertex(parentScope);
    //Until we've resolved the contents we have no idea if it is fully bound!
    if (parentCtx->hasAnyActiveParameters())
        infoFlags |= HEFunbound;

    resolved.setown(createVirtualScope(NULL, NULL));
    IHqlExpression * resolvedScopeExpr = resolved->queryExpression();
    ForEachChild(i, this)
        resolvedScopeExpr->addOperand(LINK(queryChild(i)));
    resolvedAll = false;
}


void CHqlForwardScope::defineSymbol(IHqlExpression * expr)
{
    if (expr->queryBody())
        resolved->defineSymbol(expr);
    else
        CHqlVirtualScope::defineSymbol(expr);
}

IHqlExpression *CHqlForwardScope::lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
    OwnedHqlExpr resolvedSym = resolved->lookupSymbol(searchName, lookupFlags, ctx);
    if (resolvedSym && resolvedSym->getOperator() == no_processing)
        return NULL;

    if (!(lookupFlags & LSFignoreBase))
    {
        if (resolvedSym || resolvedAll)
            return resolvedSym.getClear();
    }

    OwnedHqlExpr ret = CHqlScope::lookupSymbol(searchName, lookupFlags, ctx);
    if (!ret || (lookupFlags & LSFignoreBase))
        return ret.getClear();

    //MORE: If we had multi-threaded parsing this might cause issues.
    if (!parentScope)
        return NULL;

    IHqlExpression * processingSymbol = createSymbol(searchName, LINK(processingMarker), ob_exported);
    resolved->defineSymbol(processingSymbol);

    bool ok = parseForwardModuleMember(*parentCtx, this, ret, ctx);
    OwnedHqlExpr newSymbol = resolved->lookupSymbol(searchName, lookupFlags, ctx);

    if(!ok || !newSymbol || (newSymbol == processingSymbol))
    {
        IHqlNamedAnnotation * oldSymbol = queryNameAnnotation(ret);
        assertex(oldSymbol);
        resolved->removeSymbol(searchName);
        const char * filename = parentCtx->sourcePath->str();
        StringBuffer msg("Definition must contain EXPORT or SHARED value for ");
        msg.append(*searchName);
        throw createECLError(ERR_EXPORT_OR_SHARE, msg.toCharArray(), filename, oldSymbol->getStartLine(), oldSymbol->getStartColumn(), 1);
    }

    if (!(newSymbol->isExported() || (lookupFlags & LSFsharedOK)))
        return NULL;

    return newSymbol.getClear();
}

IHqlScope * CHqlForwardScope::queryResolvedScope(HqlLookupContext * context)
{ 
    if (!resolvedAll)
    {
        //Generally we should have a lookup context passed in so the archive is updated correctly
        //But currently painful in one context, so allow it to be omitted.
        Owned<IErrorReceiver> errorReporter = createThrowingErrorReceiver();
        HqlDummyLookupContext localCtx(errorReporter);
        HqlLookupContext * activeContext = context ? context : &localCtx;
        HqlExprArray syms;
        getSymbols(syms);
        syms.sort(compareSymbolsByName);            // Make errors consistent
        ForEachItemIn(i, syms)
        {
            IIdAtom * cur = syms.item(i).queryId();
            ::Release(lookupSymbol(cur, LSFsharedOK, *activeContext));
            //Could have been fully resolved while looking up a symbol!
            if (resolvedAll)
                return resolved;
        }
        if (!resolvedAll)
        {
            resolved.setown(closeScope(resolved.getClear()));
            resolvedAll = true;
        }
    }
    return resolved;
}


void addForwardDefinition(IHqlScope * scope, IIdAtom * symbolName, IIdAtom * moduleName, IFileContents * contents, unsigned symbolFlags, bool isExported, unsigned startLine, unsigned startColumn)
{
    IHqlExpression * cur = createSymbol(symbolName, moduleName, NULL, NULL,
                            isExported, !isExported, symbolFlags,
                            contents, startLine, startColumn, 0, 0, 0);

    scope->defineSymbol(cur);
}



//==============================================================================================================

CHqlMultiParentScope::CHqlMultiParentScope(IIdAtom * _name, ...)
 : CHqlScope(no_privatescope, _name, _name->str())
{
    va_list args;
    va_start(args, _name);
    for (;;)
    {
        IHqlScope *parent = va_arg(args, IHqlScope*);
        if (!parent)
            break;

        //This should only be used for parser scopes (which are real and non virtual).
        IHqlExpression * parentExpr = parent->queryExpression();
        assertex(!parentExpr->hasAttribute(_virtualSeq_Atom));
        parents.append(*parent);
    }
    va_end(args);
}

IHqlExpression *CHqlMultiParentScope::lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
    IHqlExpression *ret = CHqlScope::lookupSymbol(searchName, lookupFlags, ctx);
    if (ret)
        return ret;

    unsigned numChildren = parents.length();
    for (unsigned i=0; i<numChildren; i++)
    {
        ret = ((IHqlScope&)parents.item(i)).lookupSymbol(searchName, lookupFlags, ctx);
        if (ret)
            return ret;
    }
    return NULL;
}

//==============================================================================================================
CHqlContextScope::CHqlContextScope(IHqlScope* _scope) : CHqlScope(no_privatescope) 
{   
    CHqlContextScope* scope = QUERYINTERFACE(_scope, CHqlContextScope);
    assertex(scope);
    SymbolTableIterator it(scope->defined);
    
    //StringBuffer debug("   Context:");
    for(it.first();it.isValid();it.next())
    {
        IMapping* name = &it.query();       
        IHqlExpression * value = scope->defined.mapToValue(name);
        
        if(value)
        {
            IIdAtom * valueId = value->queryId();
            //debug.appendf(" %s",name->str());
            defined.setValue(valueId->lower(),value);
        }
    }
    //PrintLog(debug.str());
}

//==============================================================================================================

static UniqueSequenceCounter parameterSequence;
CHqlParameter::CHqlParameter(IIdAtom * _id, unsigned _idx, ITypeInfo *_type)
 : CHqlExpressionWithType(no_param, _type)
{
    id = _id;
    idx = _idx;
    infoFlags |= HEFunbound;
    uid = (idx == UnadornedParameterIndex) ? 0 : parameterSequence.next();
}

CHqlParameter::~CHqlParameter()
{
}

bool CHqlParameter::equals(const IHqlExpression & _other) const
{
    if (!CHqlExpression::equals(_other))
        return false;
    const CHqlParameter & other = static_cast<const CHqlParameter &>(_other);
    if (uid != other.uid)
        return false;
    if ((id != other.id) || (idx != other.idx))
        return false;
    return true;
}

IHqlExpression * CHqlParameter::makeParameter(IIdAtom * _id, unsigned _idx, ITypeInfo *_type, HqlExprArray & _attrs)
{
    IHqlExpression * e;
    type_t tc = _type->getTypeCode();
    switch (tc)
    {
    case type_dictionary:
        e = new CHqlDictionaryParameter(_id, _idx, _type);
        break;
    case type_table:
    case type_groupedtable:
        e = new CHqlDatasetParameter(_id, _idx, _type);
        break;
    case type_scope:
        e = new CHqlScopeParameter(_id, _idx, _type);
        break;
    default:
        e = new CHqlParameter(_id, _idx, _type);
        break;
    }
    ForEachItemIn(i, _attrs)
        e->addOperand(LINK(&_attrs.item(i)));
    return e->closeExpr();
}

IHqlSimpleScope *CHqlParameter::querySimpleScope()
{
    ITypeInfo * recordType = ::queryRecordType(type);
    if (!recordType)
        return NULL;
    return QUERYINTERFACE(queryUnqualifiedType(recordType), IHqlSimpleScope);
}

void CHqlParameter::sethash()
{
    CHqlExpression::sethash();
    HASHFIELD(uid);
    HASHFIELD(id);
    HASHFIELD(idx);
}

IHqlExpression *CHqlParameter::clone(HqlExprArray &newkids)
{
    return makeParameter(id, idx, LINK(type), newkids);
}

StringBuffer &CHqlParameter::toString(StringBuffer &ret)
{
    ret.append('%');
    ret.append(id->str());
    ret.append('-');
    ret.append(idx);
    return ret;
}

//==============================================================================================================

CHqlScopeParameter::CHqlScopeParameter(IIdAtom * _id, unsigned _idx, ITypeInfo *_type)
 : CHqlScope(no_param, _id, _id->str())
{
    type = _type;
    idx = _idx;
    typeScope = ::queryScope(type);
    infoFlags |= HEFunbound;
    uid = (idx == UnadornedParameterIndex) ? 0 : parameterSequence.next();
    if (!hasAttribute(_virtualSeq_Atom))
        addOperand(createSequence(no_attr, makeNullType(), _virtualSeq_Atom, virtualSequence.next()));
}

bool CHqlScopeParameter::assignableFrom(ITypeInfo * source) 
{ 
    return type->assignableFrom(source);
}


bool CHqlScopeParameter::equals(const IHqlExpression & _other) const
{
    if (!CHqlExpression::equals(_other))
        return false;
    const CHqlScopeParameter & other = static_cast<const CHqlScopeParameter &>(_other);
    if (uid != other.uid)
        return false;
    if ((id != other.id) || (idx != other.idx))
        return false;
    return (this == &_other);
}

IHqlExpression *CHqlScopeParameter::clone(HqlExprArray &newkids)
{
    throwUnexpected();
    return createParameter(id, idx, LINK(type), newkids);
}

IHqlScope * CHqlScopeParameter::clone(HqlExprArray & children, HqlExprArray & symbols)
{
    throwUnexpected(); 
}

void CHqlScopeParameter::sethash()
{
    CHqlScope::sethash();
    HASHFIELD(uid);
    HASHFIELD(id);
    HASHFIELD(idx);
}

StringBuffer &CHqlScopeParameter::toString(StringBuffer &ret)
{
    ret.append('%');
    ret.append(id->str());
    ret.append('-');
    ret.append(idx);
    return ret;
}

IHqlExpression * CHqlScopeParameter::lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
    OwnedHqlExpr match = typeScope->lookupSymbol(searchName, lookupFlags|LSFfromderived, ctx);
    if (!match)
        return NULL;
    if (lookupFlags & (LSFignoreBase|LSFfromderived))
        return match.getClear();

    if (!canBeVirtual(match))
    {
        if (containsInternalSelect(match))
        {
            IHqlExpression * typeScopeExpr = ::queryExpression(typeScope);
            ParameterVirtualSymbolReplacer replacer(ctx, this, typeScopeExpr->queryAttribute(_virtualSeq_Atom), LSFsharedOK);
            match.setown(replacer.transform(match));
        }

        return match.getClear();
    }

    return createDelayedReference(no_delayedselect, this, match, lookupFlags, ctx);
}

//==============================================================================================================

CHqlDelayedScope::CHqlDelayedScope(HqlExprArray &_ownedOperands)
 : CHqlExpressionWithTables(no_delayedscope)
{
    setOperands(_ownedOperands); // after type is initialized
    type = queryChild(0)->queryType();

    ITypeInfo * scopeType = type;
    if (scopeType->getTypeCode() == type_function)
        scopeType = scopeType->queryChildType();

    typeScope = ::queryScope(scopeType);
    assertex(typeScope);

    if (!hasAttribute(_virtualSeq_Atom))
        addOperand(createSequence(no_attr, makeNullType(), _virtualSeq_Atom, virtualSequence.next()));
}

bool CHqlDelayedScope::assignableFrom(ITypeInfo * source)
{
    return type->assignableFrom(source);
}

bool CHqlDelayedScope::equals(const IHqlExpression & _other) const
{
    if (!CHqlExpressionWithTables::equals(_other))
        return false;
    return true;
}

IHqlExpression *CHqlDelayedScope::clone(HqlExprArray &newkids)
{
    return createDelayedScope(newkids);
}

IHqlScope * CHqlDelayedScope::clone(HqlExprArray & children, HqlExprArray & symbols)
{
    throwUnexpected();
}

IHqlExpression * CHqlDelayedScope::lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
    IHqlScope * scope = queryChild(0)->queryScope();
    if (scope)
        return scope->lookupSymbol(searchName, lookupFlags, ctx);

    OwnedHqlExpr match = typeScope->lookupSymbol(searchName, lookupFlags, ctx);
    if (!match)
        return NULL;

    if (lookupFlags & LSFignoreBase)
        return match.getClear();

    if (!canBeVirtual(match))
        return match.getClear();

    return createDelayedReference(no_delayedselect, this, match, lookupFlags, ctx);
}


void CHqlDelayedScope::ensureSymbolsDefined(HqlLookupContext & ctx)
{
}

void CHqlDelayedScope::getSymbols(HqlExprArray& exprs) const
{
    typeScope->getSymbols(exprs);
}

ITypeInfo * CHqlDelayedScope::queryType() const
{
    return type;
}

ITypeInfo * CHqlDelayedScope::getType()
{
    return LINK(type);
}

bool CHqlDelayedScope::hasBaseClass(IHqlExpression * searchBase)
{
    return typeScope->hasBaseClass(searchBase);
}

IHqlScope * CHqlDelayedScope::queryConcreteScope()
{
    return NULL;
}

IHqlScope * CHqlDelayedScope::queryResolvedScope(HqlLookupContext * context)
{
    return this;
}

IHqlExpression * createDelayedScope(HqlExprArray &newkids)
{
    CHqlDelayedScope * scope = new CHqlDelayedScope(newkids);
    return scope->closeExpr();
}

IHqlExpression * createDelayedScope(IHqlExpression * expr)
{
    HqlExprArray args;
    args.append(*expr);
    return createDelayedScope(args);
}

//==============================================================================================================
CHqlVariable::CHqlVariable(node_operator _op, const char * _name, ITypeInfo * _type) : CHqlExpressionWithType(_op, _type)
{
#ifdef SEARCH_NAME1
    if (strcmp(_name, SEARCH_NAME1) == 0)
        debugMatchedName();
#endif
#ifdef SEARCH_NAME2
    if (strcmp(_name, SEARCH_NAME2) == 0)
        debugMatchedName();
#endif
    name.set(_name);
    infoFlags |= HEFtranslated;
    infoFlags |= HEFhasunadorned;
}

CHqlVariable *CHqlVariable::makeVariable(node_operator op, const char * name, ITypeInfo * type) 
{
    CHqlVariable *e = new CHqlVariable(op, name, type);
    return (CHqlVariable *) e->closeExpr();
}

bool CHqlVariable::equals(const IHqlExpression & r) const
{
    if (CHqlExpression::equals(r))
    {
        assertex(QUERYINTERFACE(&r, const CHqlVariable) == (const CHqlVariable *) &r);
        const CHqlVariable *c = (const CHqlVariable *) &r;
        return strcmp(name, c->name) == 0;
    }
    return false;
}

void CHqlVariable::sethash()
{
    CHqlExpression::sethash();
    hashcode = hashc((unsigned char *) name.get(), name.length(), hashcode);
}

IHqlExpression *CHqlVariable::clone(HqlExprArray &newkids)
{
    assertex(newkids.ordinality() == 0);  // No operands so there can't be any difference!
    Link();
    return this;
}

StringBuffer &CHqlVariable::toString(StringBuffer &ret)
{
    return ret.append(name);
}

//==============================================================================================================

CHqlAttribute::CHqlAttribute(node_operator _op, IAtom * _name) : CHqlExpressionWithTables(_op)
{
    name = _name;
}

CHqlAttribute *CHqlAttribute::makeAttribute(node_operator op, IAtom * name)
{
    CHqlAttribute* e = new CHqlAttribute(op, name);
    return e;
}

bool CHqlAttribute::equals(const IHqlExpression &r) const
{
    if (CHqlExpression::equals(r))
        return name==r.queryName();
    return false;
}

void CHqlAttribute::sethash()
{
    CHqlExpression::sethash();
    hashcode = hashc((unsigned char *) &name, sizeof(name), hashcode);
}

IHqlExpression *CHqlAttribute::clone(HqlExprArray & args)
{
    return createAttribute(op, name, args);
}

StringBuffer &CHqlAttribute::toString(StringBuffer &ret)
{
    ret.append(name);
    
    unsigned kids = numChildren();
    if (kids)
    {
        ret.append('(');
        for (unsigned i=0; i<kids; i++)
        {
            if (i>0)
                ret.append(',');
            queryChild(i)->toString(ret);
        }
        ret.append(')');
    }

    return ret;
}

ITypeInfo * CHqlAttribute::queryType() const
{
    return nullType;
}

ITypeInfo * CHqlAttribute::getType()
{
    return LINK(nullType);
}

//==============================================================================================================

CHqlUnknown::CHqlUnknown(node_operator _op, ITypeInfo * _type, IAtom * _name, IInterface * _extra) : CHqlExpressionWithType(_op, _type)
{
    name = _name;
    extra.setown(_extra);
}

CHqlUnknown *CHqlUnknown::makeUnknown(node_operator _op, ITypeInfo * _type, IAtom * _name, IInterface * _extra)
{
    if (!_type) _type = makeVoidType();
    return new CHqlUnknown(_op, _type, _name, _extra);
}

bool CHqlUnknown::equals(const IHqlExpression &r) const
{
    if (CHqlExpression::equals(r) && name==r.queryName())
    {
        const CHqlUnknown * other = dynamic_cast<const CHqlUnknown *>(&r);
        if (other && (extra == other->extra))
            return true;
    }
    return false;
}

IInterface * CHqlUnknown::queryUnknownExtra()
{
    return extra;
}


void CHqlUnknown::sethash()
{
    CHqlExpression::sethash();
    hashcode = hashc((unsigned char *) &name, sizeof(name), hashcode);
    hashcode = hashc((unsigned char *) &extra, sizeof(extra), hashcode);
}

IHqlExpression *CHqlUnknown::clone(HqlExprArray &newkids)
{
    assertex(newkids.ordinality() == 0);
    return createUnknown(op, type, name, extra.getLink());
}

StringBuffer &CHqlUnknown::toString(StringBuffer &ret)
{
    return ret.append(name);
}

//==============================================================================================================

CHqlSequence::CHqlSequence(node_operator _op, ITypeInfo * _type, IAtom * _name, unsigned __int64 _seq) : CHqlExpressionWithType(_op, _type)
{
    infoFlags |= HEFhasunadorned;
    name = _name;
    seq = _seq;
}

CHqlSequence *CHqlSequence::makeSequence(node_operator _op, ITypeInfo * _type, IAtom * _name, unsigned __int64 _seq)
{
    if (!_type) _type = makeNullType();
    return new CHqlSequence(_op, _type, _name, _seq);
}

bool CHqlSequence::equals(const IHqlExpression &r) const
{
    if (CHqlExpression::equals(r) && name==r.queryName())
    {
        const CHqlSequence * other = dynamic_cast<const CHqlSequence *>(&r);
        if (other && (seq == other->seq))
            return true;
    }
    return false;
}

void CHqlSequence::sethash()
{
    CHqlExpression::sethash();
    hashcode = hashc((unsigned char *) &name, sizeof(name), hashcode);
    hashcode = hashc((unsigned char *) &seq, sizeof(seq), hashcode);
}

IHqlExpression *CHqlSequence::clone(HqlExprArray &newkids)
{
    assertex(newkids.ordinality() == 0);
    return LINK(this);
}

StringBuffer &CHqlSequence::toString(StringBuffer &ret)
{
    return ret.append(name);
}

//==============================================================================================================

CHqlExternal::CHqlExternal(IIdAtom * _id, ITypeInfo *_type, HqlExprArray &_ownedOperands) : CHqlExpressionWithType(no_external, _type, _ownedOperands)
{
    id = _id;
}

bool CHqlExternal::equals(const IHqlExpression &r) const
{
    return (this == &r);
}

CHqlExternal *CHqlExternal::makeExternalReference(IIdAtom * _id, ITypeInfo *_type, HqlExprArray &_ownedOperands)
{
    return new CHqlExternal(_id, _type, _ownedOperands);
}

//==============================================================================================================

CHqlExternalCall::CHqlExternalCall(IHqlExpression * _funcdef, ITypeInfo * _type, HqlExprArray &_ownedOperands) : CHqlExpressionWithType(no_externalcall, _type, _ownedOperands), funcdef(_funcdef)
{
    IHqlExpression * def = funcdef->queryChild(0);
    //Once aren't really pure, but are as far as the code generator is concerned.  Split into more flags if it becomes an issue.
    if (!def->hasAttribute(pureAtom) && !def->hasAttribute(onceAtom))
    {
        infoFlags |= (HEFvolatile);
    }
    
    if (def->hasAttribute(actionAtom) || (type && type->getTypeCode() == type_void))
        infoFlags |= HEFaction;

    if (def->hasAttribute(userMatchFunctionAtom))
    {
        infoFlags |= HEFcontainsNlpText;
    }

    if (def->hasAttribute(contextSensitiveAtom))
        infoFlags |= HEFcontextDependentException;

    if (def->hasAttribute(ctxmethodAtom) || def->hasAttribute(gctxmethodAtom) || def->hasAttribute(globalContextAtom) || def->hasAttribute(contextAtom))
        infoFlags |= HEFaccessRuntimeContext;
}

bool CHqlExternalCall::equals(const IHqlExpression & other) const
{
    if (this == &other)
        return true;
    if (!CHqlExpression::equals(other))
        return false;
    if (queryExternalDefinition() != other.queryExternalDefinition())
        return false;
    return true;
}

IHqlExpression * CHqlExternalCall::queryExternalDefinition() const
{
    return funcdef;
}

void CHqlExternalCall::sethash()
{
    CHqlExpression::sethash();
    HASHFIELD(funcdef);
}

IHqlExpression *CHqlExternalCall::clone(HqlExprArray &newkids)
{
    if ((newkids.ordinality() == 0) && (operands.ordinality() == 0))
        return LINK(this);
    return makeExternalCall(LINK(funcdef), LINK(type), newkids);
}


IHqlExpression * CHqlExternalCall::makeExternalCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &_ownedOperands)
{
    CHqlExternalCall * ret;
    switch (type->getTypeCode())
    {
    case type_table:
    case type_groupedtable:
        ret = new CHqlExternalDatasetCall(_funcdef, type, _ownedOperands);
        break;
    default:
        ret = new CHqlExternalCall(_funcdef, type, _ownedOperands);
        break;
    }
    return ret->closeExpr();
}


IHqlExpression *CHqlExternalDatasetCall::clone(HqlExprArray &newkids)
{
    return makeExternalCall(LINK(funcdef), LINK(type), newkids);
}


//==============================================================================================================

CHqlDelayedCall::CHqlDelayedCall(IHqlExpression * _param, ITypeInfo * _type, HqlExprArray &_ownedOperands) : CHqlExpressionWithType(no_call, _type, _ownedOperands), param(_param)
{
    infoFlags |= (param->getInfoFlags() & HEFalwaysInherit);
    infoFlags2 |= (param->getInfoFlags2() & HEF2alwaysInherit);
}

bool CHqlDelayedCall::equals(const IHqlExpression & _other) const
{
    if (!CHqlExpression::equals(_other))
        return false;

    const CHqlDelayedCall & other = static_cast<const CHqlDelayedCall &>(_other);
    if (param != other.param)
        return false;
    return true;
}

void CHqlDelayedCall::sethash()
{
    CHqlExpression::sethash();
    HASHFIELD(param);
}

IHqlExpression *CHqlDelayedCall::clone(HqlExprArray &newkids)
{
    return makeDelayedCall(LINK(param), newkids);
}


IHqlExpression * CHqlDelayedCall::makeDelayedCall(IHqlExpression * _funcdef, HqlExprArray &operands)
{
    ITypeInfo * returnType = _funcdef->queryType()->queryChildType();
    CHqlDelayedCall * ret;
    switch (returnType->getTypeCode())
    {
    case type_table:
    case type_groupedtable:
        ret = new CHqlDelayedDatasetCall(_funcdef, LINK(returnType), operands);
        break;
    case type_scope:
        ret = new CHqlDelayedScopeCall(_funcdef, LINK(returnType), operands);
        break;
    default:
        ret = new CHqlDelayedCall(_funcdef, LINK(returnType), operands);
        break;
    }
    return ret->closeExpr();
}


//==============================================================================================================

CHqlDelayedScopeCall::CHqlDelayedScopeCall(IHqlExpression * _param, ITypeInfo * type, HqlExprArray &parms) 
: CHqlDelayedCall(_param, type, parms) 
{
    typeScope = ::queryScope(type); // no need to link
    if (!hasAttribute(_virtualSeq_Atom))
        addOperand(createSequence(no_attr, makeNullType(), _virtualSeq_Atom, virtualSequence.next()));
}

IHqlExpression * CHqlDelayedScopeCall::lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx)
{
    OwnedHqlExpr match = typeScope->lookupSymbol(searchName, lookupFlags, ctx);
    if (!match)
        return NULL;
    if (lookupFlags & LSFignoreBase)
        return match.getClear();

    return createDelayedReference(no_delayedselect, this, match, lookupFlags, ctx);
}

void CHqlDelayedScopeCall::getSymbols(HqlExprArray& exprs) const
{
    typeScope->getSymbols(exprs);
}

IHqlScope * CHqlDelayedScopeCall::queryConcreteScope()
{
    return NULL;
}

IAtom * CHqlDelayedScopeCall::queryName() const
{
    return typeScope->queryName();
}

IIdAtom * CHqlDelayedScopeCall::queryId() const
{
    return typeScope->queryId();
}

bool CHqlDelayedScopeCall::hasBaseClass(IHqlExpression * searchBase)
{
    return typeScope->hasBaseClass(searchBase);
}

//==============================================================================================================

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4355 )
#endif
/* In parm: scope is linked */
CHqlAlienType::CHqlAlienType(IIdAtom * _id, IHqlScope *_scope, IHqlExpression * _funcdef) : CHqlExpressionWithTables(no_type)
{
    id = _id;
    scope = _scope;
    funcdef = _funcdef;

    if (!funcdef)
        funcdef = this;

    IHqlExpression * load = queryLoadFunction();
    IHqlExpression * store = queryStoreFunction();
    
    assertex(load->getOperator() == no_funcdef && load->queryChild(1)->numChildren()==1);
    assertex(store->getOperator() == no_funcdef && store->queryChild(1)->numChildren()==1);

    IHqlExpression * loadParam = load->queryChild(1)->queryChild(0);
    logical= load->queryType()->queryChildType();
    physical = loadParam->queryType();
    
}
#ifdef _MSC_VER
#pragma warning( pop )
#endif

CHqlAlienType::~CHqlAlienType()
{
    ::Release(scope);
    if (funcdef != this)
        ::Release(funcdef);
}

ITypeInfo * CHqlAlienType::queryType() const
{
    return const_cast<CHqlAlienType *>(this);
}

ITypeInfo * CHqlAlienType::getType()
{
    CHqlAlienType::Link();
    return this;
}

bool CHqlAlienType::equals(const IHqlExpression &r) const
{
    if (!CHqlExpression::equals(r))
        return false;
    //hack
    return scopesEqual(scope, const_cast<IHqlExpression &>(r).queryScope());
}


void CHqlAlienType::sethash()
{
    // Can't use base class as that includes type (== this) which would make it different every time
    setInitialHash(0);
}

unsigned CHqlAlienType::getCardinality()            
{ 
    //MORE?
    unsigned pcard = physical->getCardinality();
    unsigned lcard = logical->getCardinality();
    if (pcard < lcard)
        return pcard;
    else
        return lcard;
}

IHqlExpression *CHqlAlienType::clone(HqlExprArray &newkids)
{
    IHqlExpression * ret = new CHqlAlienType(id, LINK(scope), LINK(funcdef));
    ForEachItemIn(idx2, newkids)
        ret->addOperand(&OLINK(newkids.item(idx2)));
    return ret->closeExpr();
}



unsigned CHqlAlienType::getCrc()
{
    return getExpressionCRC(this);
}

unsigned CHqlAlienType::getMaxSize()
{
    unsigned size = physical->getSize();
    if (size != UNKNOWN_LENGTH)
        return size;

    OwnedHqlExpr maxSize = lookupSymbol(maxSizeId);
    if (!maxSize)
        maxSize.setown(lookupSymbol(maxLengthId));
    if (maxSize)
    {
        OwnedHqlExpr folded = foldHqlExpression(maxSize);
        if (folded->queryValue())
            return (unsigned)folded->queryValue()->getIntValue();
    }
    return UNKNOWN_LENGTH;
}

IHqlExpression *CHqlAlienType::queryFunctionDefinition() const
{
    return funcdef;
}


IHqlExpression * CHqlAlienType::queryLoadFunction()
{
    return queryMemberFunc(loadId);
}

IHqlExpression * CHqlAlienType::queryLengthFunction()
{
    OwnedHqlExpr func = lookupSymbol(physicalLengthId);
    assertex(func);
    return func;
}

IHqlExpression * CHqlAlienType::queryStoreFunction()
{
    return queryMemberFunc(storeId);
}

IHqlExpression * CHqlAlienType::queryFunction(IIdAtom * id)
{
    OwnedHqlExpr func = lookupSymbol(id);
    if (func)
        assertex(func->getOperator() == no_funcdef);
    return func;
}

IHqlExpression * CHqlAlienType::queryMemberFunc(IIdAtom * search)
{
    OwnedHqlExpr func = lookupSymbol(search);
    assertex(func);
    assertex(func->getOperator() == no_funcdef);
    return func;
}


IHqlExpression *CHqlAlienType::lookupSymbol(IIdAtom * searchName)
{
    HqlDummyLookupContext ctx(NULL);
    return scope->lookupSymbol(searchName, LSFpublic, ctx);
}

size32_t CHqlAlienType::getSize() 
{ 
    return physical->getSize(); 
    //return getMaxSize();      // would this be better?, and should the value be cached?
}

/* in parm _scope: linked */
extern IHqlExpression *createAlienType(IIdAtom * _id, IHqlScope *_scope)
{
    assertex(_scope);
    return new CHqlAlienType(_id, _scope, NULL);
}

extern IHqlExpression *createAlienType(IIdAtom * id, IHqlScope * scope, HqlExprArray &newkids, IHqlExpression * funcdef)
{
    assertex(scope);
//  assertex(!funcdef);     // I'm not sure what value this has...
    IHqlExpression * ret = new CHqlAlienType(id, scope, funcdef);
    ForEachItemIn(idx2, newkids)
        ret->addOperand(&OLINK(newkids.item(idx2)));
    return ret->closeExpr();
}



//==============================================================================================================

/* In parm: scope is linked */
CHqlEnumType::CHqlEnumType(ITypeInfo * _type, IHqlScope *_scope) : CHqlExpressionWithType(no_enum, _type)
{
    scope = _scope;
    IHqlExpression * scopeExpr = queryExpression(scope);
    infoFlags |= (scopeExpr->getInfoFlags() & HEFalwaysInherit);
    infoFlags2 |= (scopeExpr->getInfoFlags2() & HEF2alwaysInherit);
}

CHqlEnumType::~CHqlEnumType()
{
    ::Release(scope);
}


bool CHqlEnumType::equals(const IHqlExpression &r) const
{
    if (!CHqlExpression::equals(r))
        return false;

    return scopesEqual(scope, const_cast<IHqlExpression &>(r).queryScope());
}


void CHqlEnumType::sethash()
{
    // Can't use base class as that includes type (== this) which would make it different every time
    CHqlExpression::sethash();
    IHqlExpression * scopeExpr = queryExpression(scope);
    if (scopeExpr)
        hashcode ^= scopeExpr->getHash();
}


IHqlExpression *CHqlEnumType::clone(HqlExprArray &newkids)
{
    assertex(newkids.ordinality() == 0);
    return LINK(this);
}


/* in parm _scope: linked */
extern IHqlExpression *createEnumType(ITypeInfo * _type, IHqlScope *_scope)
{
    if (!_scope) _scope = createScope();
    IHqlExpression * ret = new CHqlEnumType(_type, _scope);
    return ret->closeExpr();
}

//==============================================================================================================

void CHqlTemplateFunctionContext::sethash() 
{ 
    CHqlExpression::sethash(); 
    hashcode = hashc((unsigned char *) context, sizeof(context), hashcode); 
}

//==============================================================================================================

extern IHqlExpression *createParameter(IIdAtom * id, unsigned idx, ITypeInfo *type, HqlExprArray & attrs)
{
    return CHqlParameter::makeParameter(id, idx, type, attrs);
}

extern IHqlExpression *createValue(node_operator op)
{
    return CHqlExpressionWithType::makeExpression(op, NULL, NULL);
}
extern IHqlExpression *createValue(node_operator op, ITypeInfo *type)
{
    return CHqlExpressionWithType::makeExpression(op, type, NULL);
}
extern IHqlExpression *createOpenValue(node_operator op, ITypeInfo *type)
{
#if defined(_DEBUG)
    //reports calling the wrong function if enabled.... some examples can't be fixed yet...
    if (type)
    {
        switch (op)
        {
        case no_funcdef:
        case no_translated:
            break;
        default:
            switch(type->getTypeCode())
            {
                // MORE - is this right???
            case type_groupedtable:
            case type_table:
                assertex(!"createDataset should be called instead");
            }
        }
    }
#endif
    return new CHqlExpressionWithType(op, type);
}

extern IHqlExpression *createOpenNamedValue(node_operator op, ITypeInfo *type, IIdAtom * id)
{
#if defined(_DEBUG)
    //reports calling the wrong function if enabled.... some examples can't be fixed yet...
    if (type)
    {
        switch (op)
        {
        case no_funcdef:
        case no_translated:
            break;
        default:
            switch(type->getTypeCode())
            {
                // MORE - is this right???
            case type_groupedtable:
            case type_table:
                assertex(!"createDataset should be called instead");
            }
        }
    }
#endif
    return new CHqlNamedExpression(op, type, id, NULL);
}

extern IHqlExpression *createValue(node_operator op, IHqlExpression *p1, IHqlExpression *p2)
{
    return CHqlExpressionWithType::makeExpression(op, p1->getType(), p1, p2, NULL);
}

extern IHqlExpression *createValue(node_operator op, ITypeInfo *type, IHqlExpression *p1)
{
    return CHqlExpressionWithType::makeExpression(op, type, p1, NULL);
}
extern IHqlExpression *createValue(node_operator op, ITypeInfo *type, IHqlExpression *p1, IHqlExpression *p2)
{
    return CHqlExpressionWithType::makeExpression(op, type, p1, p2, NULL);
}
extern IHqlExpression *createValue(node_operator op, ITypeInfo *type, IHqlExpression *p1, IHqlExpression *p2, IHqlExpression *p3)
{
    return CHqlExpressionWithType::makeExpression(op, type, p1, p2, p3, NULL);
}

extern IHqlExpression *createValue(node_operator op, ITypeInfo *type, IHqlExpression *p1, IHqlExpression *p2, IHqlExpression *p3, IHqlExpression *p4)
{
    return CHqlExpressionWithType::makeExpression(op, type, p1, p2, p3, p4, NULL);
}

extern IHqlExpression *createValueF(node_operator op, ITypeInfo *type, ...)
{
    HqlExprArray children;
    va_list args;
    va_start(args, type);
    for (;;)
    {
        IHqlExpression *parm = va_arg(args, IHqlExpression *);
        if (!parm)
            break;
#ifdef _DEBUG
        assertex(QUERYINTERFACE(parm, IHqlExpression));
#endif
        if (parm->getOperator() == no_comma)
        {
            parm->unwindList(children, no_comma);
            parm->Release();
        }
        else
            children.append(*parm);
    }
    va_end(args);
    return CHqlExpressionWithType::makeExpression(op, type, children);
}

extern HQL_API IHqlExpression *createValue(node_operator op, ITypeInfo * type, unsigned num, IHqlExpression * * args)
{
    IHqlExpression * expr = createOpenValue(op, type);
    unsigned index;
    for (index = 0; index < num; index++)
        expr->addOperand(args[index]);
    return expr->closeExpr();
}

extern HQL_API IHqlExpression * createValueFromCommaList(node_operator op, ITypeInfo * type, IHqlExpression * argsExpr)
{
    HqlExprArray args;
    if (argsExpr)
    {
        argsExpr->unwindList(args, no_comma);
        argsExpr->Release();
    }
    return createValue(op, type, args);
}

extern HQL_API IHqlExpression * createValueSafe(node_operator op, ITypeInfo * type, const HqlExprArray & args)
{
    ForEachItemIn(idx, args)
        args.item(idx).Link();
    HqlExprArray & castArgs = const_cast<HqlExprArray &>(args);
    IHqlExpression * * exprList = static_cast<IHqlExpression * *>(castArgs.getArray());
    return createValue(op, type, args.ordinality(), exprList);
}

extern HQL_API IHqlExpression * createValueSafe(node_operator op, ITypeInfo * type, const HqlExprArray & args, unsigned from, unsigned max)
{
    assertex(from <= args.ordinality() && max <= args.ordinality() && from <= max);
    for (unsigned idx=from; idx < max; idx++)
        args.item(idx).Link();
    HqlExprArray & castArgs = const_cast<HqlExprArray &>(args);
    IHqlExpression * * exprList = static_cast<IHqlExpression * *>(castArgs.getArray());
    return createValue(op, type, max-from, exprList + from);
}

extern IHqlExpression *createBoolExpr(node_operator op, IHqlExpression *p1)
{
    return CHqlExpressionWithType::makeExpression(op, makeBoolType(), p1, NULL);
}

extern IHqlExpression *createBoolExpr(node_operator op, IHqlExpression *p1, IHqlExpression *p2)
{
    return CHqlExpressionWithType::makeExpression(op, makeBoolType(), p1, p2, NULL);
}

extern IHqlExpression *createBoolExpr(node_operator op, IHqlExpression *p1, IHqlExpression *p2, IHqlExpression *p3)
{
    return CHqlExpressionWithType::makeExpression(op, makeBoolType(), p1, p2, p3, NULL);
}

extern IHqlExpression *createField(IIdAtom *id, ITypeInfo *type, HqlExprArray & _ownedOperands)
{
    IHqlExpression * field = new CHqlField(id, type, _ownedOperands);
    return field->closeExpr();
}

extern IHqlExpression *createField(IIdAtom *id, ITypeInfo *type, IHqlExpression *defaultValue, IHqlExpression * attrs)
{
    HqlExprArray args;
    if (defaultValue)
        args.append(*defaultValue);
    if (attrs)
    {
        attrs->unwindList(args, no_comma);
        attrs->Release();
    }
    IHqlExpression* expr = new CHqlField(id, type, args);
    return expr->closeExpr();
}

extern HQL_API IHqlExpression *createQuoted(const char * text, ITypeInfo *type)
{
    return CHqlVariable::makeVariable(no_quoted, text, type);
}

extern HQL_API IHqlExpression *createVariable(const char * name, ITypeInfo *type)
{
    return CHqlVariable::makeVariable(no_variable, name, type);
}

IHqlExpression *createAttribute(node_operator op, IAtom * name, IHqlExpression * value, IHqlExpression *value2, IHqlExpression * value3)
{
    assertex(name);
    IHqlExpression * ret = CHqlAttribute::makeAttribute(op, name);
    if (value)
    {
        ret->addOperand(value);
        if (value2)
            ret->addOperand(value2);
        if (value3)
            ret->addOperand(value3);
    }
    return ret->closeExpr();
}

IHqlExpression *createAttribute(node_operator op, IAtom * name, HqlExprArray & args)
{
    IHqlExpression * ret = CHqlAttribute::makeAttribute(op, name);
    ForEachItemIn(idx, args)
        ret->addOperand(&OLINK(args.item(idx)));
    return ret->closeExpr();
}

extern HQL_API IHqlExpression *createAttribute(IAtom * name, IHqlExpression * value, IHqlExpression *value2, IHqlExpression * value3)
{
    return createAttribute(no_attr, name, value, value2, value3);
}

extern HQL_API IHqlExpression *createAttribute(IAtom * name, HqlExprArray & args)
{
    return createAttribute(no_attr, name, args);
}

extern HQL_API IHqlExpression *createExprAttribute(IAtom * name, IHqlExpression * value, IHqlExpression *value2, IHqlExpression * value3)
{
    return createAttribute(no_attr_expr, name, value, value2, value3);
}

extern HQL_API IHqlExpression *createExprAttribute(IAtom * name, HqlExprArray & args)
{
    return createAttribute(no_attr_expr, name, args);
}

extern HQL_API IHqlExpression *createLinkAttribute(IAtom * name, IHqlExpression * value, IHqlExpression *value2, IHqlExpression * value3)
{
    return createAttribute(no_attr_link, name, value, value2, value3);
}

extern HQL_API IHqlExpression *createLinkAttribute(IAtom * name, HqlExprArray & args)
{
    return createAttribute(no_attr_link, name, args);
}

extern HQL_API IHqlExpression *createUnknown(node_operator op, ITypeInfo * type, IAtom * name, IInterface * extra)
{
    IHqlExpression * ret = CHqlUnknown::makeUnknown(op, type, name, extra);
    return ret->closeExpr();
}

extern HQL_API IHqlExpression *createSequence(node_operator op, ITypeInfo * type, IAtom * name, unsigned __int64 seq)
{
    IHqlExpression * ret = CHqlSequence::makeSequence(op, type, name, seq);
    return ret->closeExpr();
}

IHqlExpression *createSymbol(IIdAtom * id, IHqlExpression *expr, unsigned exportFlags)
{
    return CHqlSimpleSymbol::makeSymbol(id, NULL, expr, NULL, exportFlags);
}

IHqlExpression * createSymbol(IIdAtom * id, IIdAtom * moduleName, IHqlExpression * expr, bool exported, bool shared, unsigned symbolFlags)
{
    return CHqlSimpleSymbol::makeSymbol(id, moduleName, expr, NULL, combineSymbolFlags(symbolFlags, exported, shared));
}

IHqlExpression * createSymbol(IIdAtom * _id, IIdAtom * moduleName, IHqlExpression *expr, IHqlExpression * funcdef,
                             bool exported, bool shared, unsigned symbolFlags,
                             IFileContents *fc, int lineno, int column,
                             int _startpos, int _bodypos, int _endpos)
{
    return CHqlNamedSymbol::makeSymbol(_id, moduleName, expr, funcdef,
                             exported, shared, symbolFlags,
                             fc, lineno, column, _startpos, _bodypos, _endpos);
}


IHqlExpression *createDataset(node_operator op, IHqlExpression *dataset, IHqlExpression *list)
{
    HqlExprArray parms;
    parms.append(*dataset);
    if (list)
    {
        list->unwindList(parms, no_comma);
        list->Release();
    }
    return createDataset(op, parms);
}

IHqlExpression *createDataset(node_operator op, IHqlExpression *dataset)
{
    HqlExprArray parms;
    parms.append(*dataset);
    return createDataset(op, parms);
}


extern IHqlExpression *createDatasetF(node_operator op, ...)
{
    HqlExprArray children;
    va_list args;
    va_start(args, op);
    for (;;)
    {
        IHqlExpression *parm = va_arg(args, IHqlExpression *);
        if (!parm)
            break;
#ifdef _DEBUG
        assertex(QUERYINTERFACE(parm, IHqlExpression));
#endif
        if (parm->getOperator() == no_comma)
        {
            parm->unwindList(children, no_comma);
            parm->Release();
        }
        else
            children.append(*parm);
    }
    va_end(args);
    return createDataset(op, children);
}

IHqlExpression *createDictionary(node_operator op, HqlExprArray & parms)
{
#ifdef GATHER_LINK_STATS
    insideCreate++;
#endif
    Owned<ITypeInfo> type = NULL;

    switch (op)
    {
    case no_createdictionary:
        type.setown(makeDictionaryType(makeRowType(createRecordType(&parms.item(0)))));
        break;
    case no_select:
    case no_mapto:
        type.set(parms.item(1).queryType());
        break;
    case no_addfiles:
        type.set(parms.item(0).queryType());  // It's an error if they don't all match, caught elsewhere (?)
        break;
    case no_if:
        type.set(parms.item(1).queryType());  // It's an error if they don't match, caught elsewhere
        break;
    case no_chooseds:
        type.set(parms.item(1).queryType());  // It's an error if they don't match, caught elsewhere
        break;
    case no_case:
        //following is wrong, but they get removed pretty quickly so I don't really care
        type.set(parms.item(1).queryType());
        break;
    case no_map:
        //following is wrong, but they get removed pretty quickly so I don't really care
        type.set(parms.item(0).queryType());
        break;
    case no_null:
    case no_fail:
    case no_anon:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getresult:
    {
        IHqlExpression * record = &parms.item(0);
        IHqlExpression * metadata = queryAttribute(_metadata_Atom, parms);
        bool linkCounted = (queryAttribute(_linkCounted_Atom, parms) || recordRequiresLinkCount(record));
        if (!metadata)
        {
            ITypeInfo * recordType = createRecordType(record);
            assertex(recordType->getTypeCode() == type_record);
            ITypeInfo * rowType = makeRowType(recordType);
            type.setown(makeDictionaryType(rowType));
        }
        else
            UNIMPLEMENTED_XY("Type calculation for dictionary operator", getOpString(op));

        if (linkCounted)
            type.setown(setLinkCountedAttr(type, true));
        break;
    }
    case no_serialize:
        {
            assertex(parms.ordinality() >= 2);
            IHqlExpression & form = parms.item(1);
            assertex(form.isAttribute());
            assertex(form.queryName() != diskAtom);  //It should be a dataset instead...
            type.setown(getSerializedForm(parms.item(0).queryType(), form.queryName()));
            break;
        }
    case no_deserialize:
        {
            assertex(parms.ordinality() >= 3);
            IHqlExpression & record = parms.item(1);
            IHqlExpression & form = parms.item(2);
            assertex(form.isAttribute());
            ITypeInfo * recordType = record.queryType();
            OwnedITypeInfo rowType = makeRowType(LINK(recordType));
            assertex(record.getOperator() == no_record);
            type.setown(makeDictionaryType(LINK(rowType)));
            type.setown(setLinkCountedAttr(type, true));

    #ifdef _DEBUG
            OwnedITypeInfo serializedType = getSerializedForm(type, form.queryName());
            assertex(recordTypesMatch(serializedType, parms.item(0).queryType()));
    #endif
            break;
        }
    case no_nofold:
    case no_nohoist:
    case no_thor:
    case no_nothor:
    case no_alias:
    case no_translated:
    case no_catch:
    case no_colon:
    case no_globalscope:
    case no_thisnode:
        type.set(parms.item(0).queryType());
        break;
    default:
        UNIMPLEMENTED_XY("Type calculation for dictionary operator", getOpString(op));
        break;
    }

    IHqlExpression * ret = CHqlDictionary::makeDictionary(op, type.getClear(), parms);
#ifdef GATHER_LINK_STATS
    insideCreate--;
#endif
    return ret;
}

IHqlExpression *createDictionary(node_operator op, IHqlExpression *dictionary, IHqlExpression *list)
{
    HqlExprArray parms;
    parms.append(*dictionary);
    if (list)
    {
        list->unwindList(parms, no_comma);
        list->Release();
    }
    return createDictionary(op, parms);
}

IHqlExpression *createDictionary(node_operator op, IHqlExpression *dictionary)
{
    HqlExprArray parms;
    parms.append(*dictionary);
    return createDictionary(op, parms);
}

IHqlExpression * createAliasOwn(IHqlExpression * expr, IHqlExpression * attr)
{
    if (expr->getOperator() == no_alias)
        return expr;
    if (expr->isDataset())
        return createDataset(no_alias, expr, attr);
    if (expr->isDatarow())
        return createRow(no_alias, expr, attr);
    return createValueF(no_alias, expr->getType(), expr, attr, NULL);   // unwinds no_comma
}


IHqlExpression * createTypedValue(node_operator op, ITypeInfo * type, HqlExprArray & args)
{
    switch (type->getTypeCode())
    {
    case type_groupedtable:
    case type_table:
        return createDataset(op, args);
    case type_dictionary:
        return createDictionary(op, args);
    case type_row:
        return createRow(op, args);
    default:
        return createValue(op, LINK(type), args);
    }
}


//---------------------------------------------------------------------------

/*
Calls and parameter binding.

This has various complications including:
* Expanding nested attributes.
* Delayed expansion of calls passed in as parameters.
* Interfaces which ensure all instances have the same prototype.

Originally this was done so that the function call was inlined as soon as the call was encountered.
- Ensure all parameters in nested definitions have unique expressions so parent parameters aren't
  substituted with child parameters.

Now it is done as follows:
- When a call is created a no_call node is created (or no_externalcall/no_libraryscopeinstance)
- Later the tree of calls is expanded.
- It is impossible to guarantee that all parameters in the expression tree are unique (because of interfaces if nothing else)
  so can't use a single transform.
=> Transform a call at a time
=> After a call is transformed have an option to transform the body in a nested transform.
*/

bool isExternalFunction(IHqlExpression * funcdef)
{
    if (funcdef->getOperator() == no_funcdef)
    {
        IHqlExpression * body = funcdef->queryChild(0);
        return (body->getOperator() == no_external);
    }
    return false;
}


bool isEmbedFunction(IHqlExpression * funcdef)
{
    IHqlExpression * body = funcdef->queryChild(0);
    if (body->getOperator() != no_outofline)
        return false;
    return body->queryChild(0)->getOperator() == no_embedbody;
}


bool isEmbedCall(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_call);
    return isEmbedFunction(expr->queryBody()->queryFunctionDefinition());
}


inline bool isExternalMethodDefinition(IHqlExpression * funcdef)
{
    if (funcdef->getOperator() == no_funcdef)
    {
        IHqlExpression * body = funcdef->queryChild(0);
        if ((body->getOperator() == no_external) && 
            (body->hasAttribute(methodAtom) || body->hasAttribute(omethodAtom)))
            return true;
    }
    return false;
}


inline IHqlExpression * createCallExpression(IHqlExpression * funcdef, HqlExprArray & resolvedActuals)
{
    IHqlExpression * body = funcdef->queryBody(true);
    if (funcdef != body)
    {
        if (funcdef->getAnnotationKind() != annotate_symbol)
        {
            OwnedHqlExpr call = createCallExpression(body, resolvedActuals);
            return funcdef->cloneAnnotation(call);
        }

        //Slightly nasty - need to save the parameters away because create may kill them
        HqlExprArray clonedActuals;
        appendArray(clonedActuals, resolvedActuals);

        OwnedHqlExpr call = createCallExpression(body, resolvedActuals);
        //Don't bother adding symbols for calls to external functions
        if (call->getOperator() == no_externalcall)
            return LINK(call);

        IHqlNamedAnnotation * annotation = static_cast<IHqlNamedAnnotation *>(funcdef->queryAnnotation());
        return annotation->cloneSymbol(NULL, call, funcdef, &clonedActuals);
    }

    if (funcdef->getOperator() == no_funcdef)
    {
        IHqlExpression * funcBody = funcdef->queryChild(0);
        switch (funcBody->getOperator())
        {
        case no_external:
            return CHqlExternalCall::makeExternalCall(LINK(funcdef), funcBody->getType(), resolvedActuals);
        case no_libraryscope:
            return createLibraryInstance(LINK(funcdef), resolvedActuals);
        }
    }
    return CHqlDelayedCall::makeDelayedCall(LINK(funcdef), resolvedActuals);
}


struct CallExpansionContext
{
    CallExpansionContext() 
    { 
        errors = NULL; 
        functionCache = NULL; 
        expandNestedCalls = true; 
        forceOutOfLineExpansion = false; 
    }

    inline bool expandFunction(IHqlExpression * funcdef)
    {
        if (funcdef->getOperator() == no_funcdef)
        {
            //A no_funcdef means we either have an outofline function, or we can expand the body
            IHqlExpression * body = funcdef->queryChild(0);
            switch (body->getOperator())
            {
            case no_outofline:
                return forceOutOfLineExpansion;
            }
            return true;
        }
        return false;
    }
    inline bool expandFunctionCall(IHqlExpression * call)
    {
        return expandFunction(call->queryBody()->queryFunctionDefinition());
    }

    IErrorReceiver * errors;
    HqlExprArray * functionCache;
    bool expandNestedCalls;
    bool forceOutOfLineExpansion;
};

extern IHqlExpression * expandFunctionCall(CallExpansionContext & ctx, IHqlExpression * call);


static HqlTransformerInfo parameterBindTransformerInfo("ParameterBindTransformer");
class ParameterBindTransformer : public QuickHqlTransformer
{
public:
    ParameterBindTransformer(CallExpansionContext & _ctx) 
    : QuickHqlTransformer(parameterBindTransformerInfo, _ctx.errors), 
      ctx(_ctx)
    {
    }

    IHqlExpression * createExpandedCall(IHqlExpression * call);
    IHqlExpression * createBound(IHqlExpression *func, const HqlExprArray &actuals);

protected:
    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        //MORE: This test needs to be extended?
        if (expr->isFullyBound() && !containsCall(expr, ctx.forceOutOfLineExpansion))
            return LINK(expr);

        return QuickHqlTransformer::createTransformed(expr);
    }

    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr)
    {
        node_operator op = expr->getOperator();

        //Parameters are being used a lot to select between two items in inside a function/module
        //so much better if we trim the tree earlier....
        switch (op)
        {
        case no_if:
            {
                IHqlExpression * cond  = expr->queryChild(0);
                OwnedHqlExpr newcond = transform(cond);
                if (newcond->isConstant())
                    newcond.setown(foldHqlExpression(newcond));
                IValue * value = newcond->queryValue();
                if (value && !expr->isAction())
                {
                    unsigned branch = value->getBoolValue() ? 1 : 2;
                    IHqlExpression * arg = expr->queryChild(branch);
                    if (arg)
                        return transform(arg);
                }
                break;
            }
        case no_map:
            {
                //This could strip leading failing matches, but for the moment only do that if we know
                //which branch matches.
                ForEachChild(i, expr)
                {
                    IHqlExpression * cur = expr->queryChild(i);
                    if (cur->getOperator() == no_mapto)
                    {
                        OwnedHqlExpr newcond = transform(cur->queryChild(0));

                        if (newcond->isConstant())
                            newcond.setown(foldHqlExpression(newcond));
                        IValue * value = newcond->queryValue();
                        if (!value)
                            break;

                        if (value->getBoolValue())
                            return transform(cur->queryChild(1));
                    }
                    else if (!cur->isAttribute())
                        return transform(cur);
                }
                break;
            }
        case no_case:
            {
                IHqlExpression * search = expr->queryChild(0);
                if (!search->isConstant())
                    break;
                OwnedHqlExpr folded = foldHqlExpression(search);
                IValue * searchValue = folded->queryValue();
                if (!searchValue)
                    break;
                ITypeInfo * searchType = searchValue->queryType();
                ForEachChildFrom(i, expr, 1)
                {
                    IHqlExpression * cur = expr->queryChild(i);
                    if (cur->getOperator() == no_mapto)
                    {
                        OwnedHqlExpr newcond = transform(cur->queryChild(0));

                        if (newcond->isConstant())
                            newcond.setown(foldHqlExpression(newcond));
                        IValue * compareValue = newcond->queryValue();
                        if (!compareValue)
                            break;

                        if (searchType != newcond->queryType())
                        {
                            Owned<ITypeInfo> type = ::getPromotedECLCompareType(searchType, newcond->queryType());
                            OwnedHqlExpr castSearch = ensureExprType(folded, type);
                            OwnedHqlExpr castCompare = ensureExprType(newcond, type);
                            IValue * castSearchValue = castSearch->queryValue();
                            IValue * castCompareValue = castCompare->queryValue();
                            if (!castSearchValue || !castCompareValue)
                                break;

                            if (castSearchValue->compare(castCompareValue) == 0)
                                return transform(cur->queryChild(1));
                        }
                        else
                        {
                            if (searchValue->compare(compareValue) == 0)
                                return transform(cur->queryChild(1));
                        }
                    }
                    else if (!cur->isAttribute())
                        return transform(cur);
                }
                break;
            }
        case no_delayedselect:
            {
                IHqlExpression * oldModule = expr->queryChild(1);
                OwnedHqlExpr newModule = transform(oldModule);
                if (oldModule != newModule)
                {
                    IIdAtom * selectedName = expr->queryChild(3)->queryId();
                    newModule.setown(checkCreateConcreteModule(ctx.errors, newModule, newModule->queryAttribute(_location_Atom)));

                    HqlDummyLookupContext dummyctx(ctx.errors);
                    IHqlScope * newScope = newModule->queryScope();
                    if (newScope)
                        return newScope->lookupSymbol(selectedName, makeLookupFlags(true, expr->hasAttribute(ignoreBaseAtom), false), dummyctx);
                    return ::replaceChild(expr, 1, newModule);
                }
                break;
            }
        }

        OwnedHqlExpr transformed = QuickHqlTransformer::createTransformedBody(expr);
        if ((op == no_call) && ctx.expandNestedCalls)
        {
            if (ctx.expandFunctionCall(transformed))
                return expandFunctionCall(ctx, transformed);
        }
        return transformed.getClear();
    }

protected:
    IHqlExpression * createBoundBody(IHqlExpression *func, const HqlExprArray &actuals);
    IHqlExpression * createExpandedCall(IHqlExpression *funcdef, const HqlExprArray & resolvedActuals);

protected:
    CallExpansionContext & ctx;
};


IHqlExpression * ParameterBindTransformer::createBoundBody(IHqlExpression *funcdef, const HqlExprArray &actuals)
{
    //The following can be created from (undocumented) conditional statements, and possibly conditional modules once implemented
    //As far as I know these conditional items can only be present the first time a function is bound.
    node_operator op = funcdef->getOperator();
    switch (op)
    {
    case no_if:
        {
            IHqlExpression * cond = funcdef->queryChild(0);
            OwnedHqlExpr left = createBoundBody(funcdef->queryChild(1), actuals);
            OwnedHqlExpr right = createBoundBody(funcdef->queryChild(2), actuals);
            return createIf(LINK(cond), left.getClear(), right.getClear());
        }
    case no_mapto:
        {
            OwnedHqlExpr mapped = createBoundBody(funcdef->queryChild(1), actuals);
            return createValue(no_mapto, mapped->getType(), LINK(funcdef->queryChild(0)), LINK(mapped));
        }
    case no_map:
        {
            HqlExprArray args;
            ForEachChild(i, funcdef)
                args.append(*createBoundBody(funcdef->queryChild(i), actuals));
            return createTypedValue(no_map, args.item(0).queryType(), args);
        }
    }

    OwnedHqlExpr newFuncdef;
    if (op == no_funcdef)
    {
        //A no_funcdef means we either have an outofline function, or we can expand the body
        IHqlExpression * body = funcdef->queryChild(0);
        switch (body->getOperator())
        {
        case no_outofline:
            {
                if (ctx.forceOutOfLineExpansion)
                    return transform(body->queryChild(0));

                HqlExprArray args;
                args.append(*LINK(body));
                args.append(*LINK(funcdef->queryChild(1)));
                newFuncdef.setown(completeTransform(funcdef, args));
                break;
            }
        default:
            return transform(body);
        }
    }
    else
        newFuncdef.setown(transform(funcdef));

    HqlExprArray clonedActuals;
    appendArray(clonedActuals, actuals);

    return createCallExpression(newFuncdef, clonedActuals);
}


IHqlExpression * ParameterBindTransformer::createExpandedCall(IHqlExpression * call)
{
    HqlExprArray actuals;
    unwindChildren(actuals, call);
    while (actuals.ordinality() && actuals.tos().isAttribute())
        actuals.pop();

    return createExpandedCall(call->queryBody()->queryFunctionDefinition(), actuals);
}

IHqlExpression * ParameterBindTransformer::createExpandedCall(IHqlExpression *funcdef, const HqlExprArray & resolvedActuals)
{
    assertex(funcdef->getOperator() == no_funcdef);
    IHqlExpression * formals = funcdef->queryChild(1);
    assertex(formals->numChildren() == resolvedActuals.length());
    ForEachItemIn(i, resolvedActuals)
    {
        IHqlExpression * formal = formals->queryChild(i);
        IHqlExpression * actual = &resolvedActuals.item(i);
        formal->queryBody()->setTransformExtra(actual);
    }

    return createBoundBody(funcdef, resolvedActuals);
}


extern HQL_API bool isKey(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_keyindex:
    case no_newkeyindex:
        return true;
    case no_call:
        {
            IHqlExpression * funcdef = expr->queryBody()->queryFunctionDefinition();
            if (funcdef->getOperator() == no_funcdef)
                return isKey(funcdef->queryChild(0));
            return false;
        }
    default:
        return false;
    }
}


//-------------------------------------------------------------------------------------

static HqlTransformerInfo callExpandTransformerInfo("CallExpandTransformer");
class CallExpandTransformer : public QuickHqlTransformer
{
public:
    CallExpandTransformer(CallExpansionContext & _ctx) 
    : QuickHqlTransformer(callExpandTransformerInfo, _ctx.errors), 
      ctx(_ctx)
    {
    }

protected:
    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr)
    {
        if (!containsCall(expr, ctx.forceOutOfLineExpansion))
            return LINK(expr);

        OwnedHqlExpr transformed = QuickHqlTransformer::createTransformedBody(expr);
        if (transformed->getOperator() == no_call)
        {
            IHqlExpression * funcdef = transformed->queryBody()->queryFunctionDefinition();
            return queryExpandFunctionCall(funcdef, transformed);
        }
        return transformed.getClear();
    }

    IHqlExpression * queryExpandFunctionCall(IHqlExpression *funcdef, IHqlExpression * call)
    {
        //The following can be created from (undocumented) conditional statements, and possibly conditional modules once implemented
        //As far as I know these conditional items can only be present the first time a function is bound.
        node_operator op = funcdef->getOperator();
        switch (op)
        {
        case no_if:
            {
                IHqlExpression * cond = funcdef->queryChild(0);
                OwnedHqlExpr left = queryExpandFunctionCall(funcdef->queryChild(1), call);
                OwnedHqlExpr right = queryExpandFunctionCall(funcdef->queryChild(2), call);
                return createIf(LINK(cond), left.getClear(), right.getClear());
            }
        case no_mapto:
            {
                OwnedHqlExpr mapped = queryExpandFunctionCall(funcdef->queryChild(1), call);
                return createValue(no_mapto, mapped->getType(), LINK(funcdef->queryChild(0)), LINK(mapped));
            }
        case no_map:
            {
                HqlExprArray args;
                ForEachChild(i, funcdef)
                    args.append(*queryExpandFunctionCall(funcdef->queryChild(i), call));
                return createTypedValue(no_map, args.item(0).queryType(), args);
            }
        }

        IHqlExpression * oldFuncdef = call->queryBody()->queryFunctionDefinition();
        OwnedHqlExpr newCall;
        if (funcdef != oldFuncdef)
        {
            HqlExprArray actuals;
            unwindChildren(actuals, call);
            newCall.setown(createCallExpression(funcdef, actuals));
        }
        else
            newCall.set(call);

        if (ctx.expandFunction(funcdef))
            return expandFunctionCall(ctx, newCall);
        return newCall.getClear();
    }



protected:
    CallExpansionContext & ctx;
};



//-------------------------------------------------------------------------------------

static void createAssignAll(HqlExprArray & assigns, IHqlExpression * self, IHqlExpression * left, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            createAssignAll(assigns, self, left, cur);
            break;
        case no_ifblock:
            createAssignAll(assigns, self, left, cur->queryChild(1));
            break;
        case no_field:
            {
                OwnedHqlExpr target = createSelectExpr(LINK(self), LINK(cur));
                OwnedHqlExpr field = lookupNewSelectedField(left, cur);;
                OwnedHqlExpr source = createSelectExpr(LINK(left), field.getClear());
                assigns.append(*createAssign(target.getClear(), source.getClear()));
                break;
            }
        }
    }
}

static void normalizeCallParameters(HqlExprArray & resolvedActuals, IHqlExpression *funcdef, const HqlExprArray &actuals)
{
    assertex(funcdef->isFunction());

    //This is the root function called for binding a function.
    //First associate each of the parameters with their values...
    ITypeInfo * funcType = funcdef->queryType();
    IHqlExpression * formals = queryFunctionParameters(funcType);
    IHqlExpression * defaults = queryFunctionDefaults(funcdef);

    unsigned maxFormal = formals->numChildren();
    unsigned curActual = 0;
    if (isExternalMethodDefinition(funcdef))
        resolvedActuals.append(OLINK(actuals.item(curActual++)));

    QuickExpressionReplacer defaultReplacer;
    for (unsigned i = 0; i < maxFormal; i++)
    {
        //NOTE: For functional parameters, formal is a no_funcdef, not a no_param.  I suspect something better should be implemented..
        IHqlExpression * formal = formals->queryChild(i);
        LinkedHqlExpr actual;
        if (actuals.isItem(curActual))
            actual.set(&actuals.item(curActual++));
        if (!actual || actual->getOperator()==no_omitted || actual->isAttribute())
        {
            actual.set(queryDefaultValue(defaults, i));
            if (!actual)
                actual.setown(createNullExpr(formal));
            else if (actual->getOperator() == no_sequence)
                actual.setown(createSequenceExpr());

            //implicit parameters added to out of line function definitions, may reference the function parameters (e..g, global(myParameter * 2)
            //so they need substituting first.  May also occur if defaults are based on other parameter values.
            //MORE: Should probably lazily create the mapping etc since 95% of the time it won't be used
            if (!actual->isFullyBound() && !actual->isFunction())
                actual.setown(defaultReplacer.transform(actual));
        }

        ITypeInfo * type = formal->queryType();
        if (type)
        {
            switch (type->getTypeCode())
            {
            case type_record:
                // MORE - should be more exact?  Don't add a cast when binding parameters into a transform...
                break;
            case type_table:
            case type_groupedtable:
                if (isAbstractDataset(formal))
                {
#ifdef NEW_VIRTUAL_DATASETS
                    HqlExprArray abstractSelects;
                    gatherAbstractSelects(abstractSelects, funcdef->queryChild(0), formal);

                    if (actual->getOperator()==no_fieldmap)
                    {
                        LinkedHqlExpr map = actual->queryChild(1);
                        actual.set(actual->queryChild(0));
                        associateBindMap(abstractSelects, formal, actual, map);
                    }

                    associateBindByName(abstractSelects, formal, actual);
#else
                    if (actual->getOperator()==no_fieldmap)
                        actual.set(actual->queryChild(0));
#endif
                }
                else
                {
                    OwnedHqlExpr actualRecord = getUnadornedRecordOrField(actual->queryRecord());
                    IHqlExpression * formalRecord = ::queryOriginalRecord(type);
                    OwnedHqlExpr normalFormalRecord = getUnadornedRecordOrField(formalRecord);
                    if (actualRecord && normalFormalRecord && normalFormalRecord->numChildren() && (normalFormalRecord->queryBody() != actualRecord->queryBody()))
                    {
                        //If the actual dataset is derived from the input dataset, then insert a project so types remain correct
                        //otherwise x+y will change meaning.
                        OwnedHqlExpr seqAttr = createSelectorSequence();
                        OwnedHqlExpr self = createSelector(no_self, formalRecord, NULL);
                        OwnedHqlExpr left = createSelector(no_left, actual, seqAttr);
                        HqlExprArray assigns;
                        createAssignAll(assigns, self, left, formalRecord);
                        OwnedHqlExpr transform = createValue(no_transform, makeTransformType(formalRecord->getType()), assigns);
                        actual.setown(createDataset(no_hqlproject, actual.getClear(), createComma(transform.getClear(), LINK(seqAttr))));
                    }
                }
                break;
            case type_dictionary:
                // MORE - needs some code
                // For now, never cast
                break;
            case type_row:
            case type_transform:
            case type_function:
                break;
            case type_unicode:
                if ((type->getSize() == UNKNOWN_LENGTH) && (actual->queryType()->getTypeCode() == type_varunicode))
                    break;
                actual.setown(ensureExprType(actual, type));
                break;
#if 0
            case type_string:
                if (type->getSize() == UNKNOWN_LENGTH)
                {
                    ITypeInfo * actualType = actual->queryType();
                    if ((actualType->getTypeCode() == type_varstring) && (actualType->queryCharset() == type->queryCharset())))
                        break;
                }
                actual.setown(ensureExprType(actual, type));
                break;
#endif
            default:
                if (type != actual->queryType())
                    actual.setown(ensureExprType(actual, type));
                break;
            }
        }
    
        defaultReplacer.setMapping(formal->queryBody(), actual);
        resolvedActuals.append(*LINK(actual));
    }
    while (actuals.isItem(curActual))
    {
        IHqlExpression & actual = actuals.item(curActual++);
        if (actual.isAttribute())
            resolvedActuals.append(OLINK(actual));
    }
}

static IHqlExpression * createNormalizedCall(IHqlExpression *funcdef, const HqlExprArray &actuals)
{
    HqlExprArray resolvedActuals;
    normalizeCallParameters(resolvedActuals, funcdef, actuals);

    return createCallExpression(funcdef, resolvedActuals);
}


inline IHqlExpression * expandFunctionalCallBody(CallExpansionContext & ctx, IHqlExpression * call)
{
    ParameterBindTransformer binder(ctx);
    return binder.createExpandedCall(call);
}

static IHqlExpression * cachedExpandFunctionCallBody(CallExpansionContext & ctx, IHqlExpression * call)
{
    if (ctx.functionCache)
    {
        CHqlCachedBoundFunction *cache2 = new CHqlCachedBoundFunction(call, ctx.forceOutOfLineExpansion);
        Owned<CHqlCachedBoundFunction> cache = static_cast<CHqlCachedBoundFunction *>(cache2->closeExpr());
        if (cache->bound)
            return LINK(cache->bound);

        IHqlExpression *ret = expandFunctionalCallBody(ctx, call);

        cache->bound.set(ret);
        ctx.functionCache->append(*cache.getClear());
        return ret;
    }
    return expandFunctionalCallBody(ctx, call);
}

/* in parms: func NOT linked by caller */
static IHqlExpression * expandFunctionCallPreserveAnnotation(CallExpansionContext & ctx, IHqlExpression * call)
{
    IHqlExpression * body = call->queryBody(true);
    if (call == body)
    {
        if (call->getOperator() != no_call)
            return LINK(call);

        return cachedExpandFunctionCallBody(ctx, call);
    }

    OwnedHqlExpr bound = expandFunctionCallPreserveAnnotation(ctx, body);
    //Strip symbol annotations from calls to external functions - since they just complicate the tree
    //unnecessarily
    if (call->getAnnotationKind() == annotate_symbol)
    {
        if (call->getOperator() == no_externalcall)
            return LINK(bound);
    }

    if (bound == body)
        return LINK(call);
    return call->cloneAnnotation(bound);
}


extern IHqlExpression * createBoundFunction(IErrorReceiver * errors, IHqlExpression *func, HqlExprArray &actuals, HqlExprArray * functionCache, bool forceExpansion)
{
    CallExpansionContext ctx;
    ctx.errors = errors;
    ctx.functionCache = functionCache;
    OwnedHqlExpr call = createNormalizedCall(func, actuals);
    if (func->getOperator() != no_funcdef)
        return call.getClear();

    if (!forceExpansion && canBeDelayed(func))
        return call.getClear();

    return expandFunctionCallPreserveAnnotation(ctx, call);
}

IHqlExpression * expandFunctionCall(CallExpansionContext & ctx, IHqlExpression * call)
{
    return expandFunctionCallPreserveAnnotation(ctx, call);
}


IHqlExpression * expandOutOfLineFunctionCall(IHqlExpression * expr)
{
    HqlExprArray functionCache;
    CallExpansionContext ctx;
    ctx.functionCache = &functionCache;
    ctx.forceOutOfLineExpansion = true;
    assertex(expr->getOperator() == no_call);
    if (ctx.expandFunctionCall(expr))
        return expandFunctionCallPreserveAnnotation(ctx, expr);
    return LINK(expr);
}


void expandDelayedFunctionCalls(IErrorReceiver * errors, HqlExprArray & exprs)
{
    HqlExprArray functionCache;
    CallExpansionContext ctx;
    ctx.functionCache = &functionCache;
    ctx.errors = errors;
    CallExpandTransformer binder(ctx);

    HqlExprArray target;
    binder.transformArray(exprs, target);
    replaceArray(exprs, target);
}


extern IHqlExpression * createReboundFunction(IHqlExpression *func, HqlExprArray &actuals)
{
    return createCallExpression(func, actuals);
}


//---------------------------------------------------------------------------------------------------------------

static void checkConsistent(IHqlExpression* e)
{
    //Check there are no datasets in the expression other than no_activetable
    if (!e) return;
    OwnedHqlExpr e1 = replaceSelector(e, NULL, querySelfReference());
    OwnedHqlExpr e2 = replaceSelector(e, queryActiveTableSelector(), querySelfReference());
    if (e1 != e2)
    {
        DBGLOG("Paranoia: type information wasn't correctly normalized");
        e1.setown(replaceSelector(e, NULL, querySelfReference()));
    }
}

//NOTE: The type information - e.g., distribution, grouping, sorting cannot include the dataset of the primary file
//because for a no_newusertable etc. that would result in a circular reference.
//so all references to fields in the primary file have no_activetable as the selector.
IHqlExpression *createDataset(node_operator op, HqlExprArray & parms)
{
#ifdef GATHER_LINK_STATS
    insideCreate++;
#endif

    switch (op)
    {
    case no_select:
        //This can occur in very unusual situations...
        if ((parms.ordinality() > 2) && isAlwaysActiveRow(&parms.item(0)))
            removeAttribute(parms, newAtom);
        break;
    case no_denormalize:
        {
            IHqlExpression * transform = &parms.item(3);
            assertex(recordTypesMatch(parms.item(0).queryRecordType(), transform->queryRecordType()));
            break;
        }
    case no_rollup:
        {
            IHqlExpression * transform = &parms.item(2);
            assertRecordTypesMatch(parms.item(0).queryRecordType(), transform->queryRecordType());
            break;
        }
    case no_iterate:
        {
            IHqlExpression * transform = &parms.item(1);
            assertRecordTypesMatch(parms.item(0).queryRecordType(), transform->queryRecordType());
            break;
        }
     }

#if 0
    //Some debug code for adding a pseudo attribute to all datasets - to help find all places that wouldn't allow hints to
    //be added without causing problems.  no_filter, no_if, no_case, no_map still have issues.
    if (!queryAttribute(_metadata_Atom, parms) &&
        (op != no_select) && (op != no_translated) && (op != no_colon))
        parms.append(*createAttribute(_metadata_Atom));
#endif

    Owned<ITypeInfo> type = calculateDatasetType(op, parms);

    IHqlExpression * ret = CHqlDataset::makeDataset(op, type.getClear(), parms);

#ifdef GATHER_LINK_STATS
    insideCreate--;
#endif
    return ret;
}

extern IHqlExpression *createNewDataset(IHqlExpression *name, IHqlExpression *recorddef, IHqlExpression *mode, IHqlExpression *parent, IHqlExpression *joinCondition, IHqlExpression * options)
{
    HqlExprArray args;
    args.append(*name);
    if (recorddef)
        args.append(*recorddef);
    if (mode)
        args.append(*mode);
    if (parent)
        args.append(*parent);
    if (joinCondition)
        args.append(*joinCondition);
    if (options)
    {
        options->unwindList(args, no_comma);
        options->Release();
    }
    return createDataset(no_table, args);
}

extern IHqlExpression *createRow(node_operator op, IHqlExpression *dataset, IHqlExpression *rowNum)
{
    HqlExprArray args;
    if (dataset)
        args.append(*dataset);
    if (rowNum)
    {
        rowNum->unwindList(args, no_comma);
        rowNum->Release();
    }
    return createRow(op, args);
}

extern IHqlExpression *createRowF(node_operator op, ...)
{
    HqlExprArray children;
    va_list args;
    va_start(args, op);
    for (;;)
    {
        IHqlExpression *parm = va_arg(args, IHqlExpression *);
        if (!parm)
            break;
#ifdef _DEBUG
        assertex(QUERYINTERFACE(parm, IHqlExpression));
#endif
        if (parm->getOperator() == no_comma)
        {
            parm->unwindList(children, no_comma);
            parm->Release();
        }
        else
            children.append(*parm);
    }
    va_end(args);
    return createRow(op, children);
}


extern IHqlExpression *createRow(node_operator op, HqlExprArray & args)
{
    ITypeInfo * type = NULL;
    switch (op)
    {
    case no_soapcall:
        {
            IHqlExpression & record = args.item(3);
            type = makeRowType(record.getType());
            break;
        }
    case no_soapcall_ds:
    case no_newsoapcall:
        {
            IHqlExpression & record = args.item(4);
            type = makeRowType(record.getType());
            break;
        }
    case no_newsoapcall_ds:
        {
            IHqlExpression & record = args.item(5);
            type = makeRowType(record.getType());
            break;
        }
    case no_select:
        {
            IHqlExpression & field = args.item(1);
            ITypeInfo * fieldType = field.queryType();
            type_t fieldTC = fieldType->getTypeCode();
            assertex(fieldTC == type_row);
            type = LINK(fieldType);
            break;
        }
    case no_embedbody:
    case no_id2blob:
    case no_temprow:
    case no_projectrow:         // arg(1) is actually a transform
        {
            IHqlExpression & record = args.item(1);
            type = makeRowType(LINK(record.queryRecordType()));
            if (queryAttribute(_linkCounted_Atom, args))
                type = makeAttributeModifier(type, getLinkCountedAttr());
            break;
        }
    case no_typetransfer:
    case no_createrow:
    case no_fromxml:
        {
            IHqlExpression & transform = args.item(0);
            type = makeRowType(LINK(transform.queryRecordType()));
            break;
        }
    case no_compound:
    case no_case:
    case no_mapto:
        type = args.item(1).getType();
        break;
    case no_translated:
        type = args.item(0).getType();
        break;
    case no_if:
        type = args.item(1).getType();
        //can be null if the 
        if (!type)
            type = args.item(2).getType();
        break;
    case no_serialize:
        {
            assertex(args.ordinality() >= 2);
            IHqlExpression & form = args.item(1);
            assertex(form.isAttribute());
            type = getSerializedForm(args.item(0).queryType(), form.queryName());
            break;
        }
    case no_deserialize:
        {
            assertex(args.ordinality() >= 3);
            IHqlExpression & record = args.item(1);
            IHqlExpression & form = args.item(2);
            assertex(form.isAttribute());
            assertex(record.getOperator() == no_record);
            ITypeInfo * recordType = record.queryType();
            type = makeAttributeModifier(makeRowType(LINK(recordType)), getLinkCountedAttr());

#ifdef _DEBUG
            OwnedITypeInfo serializedType = getSerializedForm(type, form.queryName());
            ITypeInfo * childType = args.item(0).queryType();
            assertex(recordTypesMatch(serializedType, childType));
#endif
            break;
        }
    case no_getresult:
    case no_getgraphresult:
        {
            IHqlExpression * record = &args.item(0);
            type = makeRowType(record->getType());
            if (recordRequiresLinkCount(record))
                type = makeAttributeModifier(type, getLinkCountedAttr());
            break;
        }
    case no_readspill:
        {
            IHqlExpression * record = queryOriginalRecord(&args.item(0));
            type = makeRowType(record->getType());
            if (recordRequiresLinkCount(record))
                type = makeAttributeModifier(type, getLinkCountedAttr());
            break;
        }
    default:
        {
            IHqlExpression * dataset = &args.item(0);
            assertex(dataset);
            ITypeInfo * datasetType = dataset->queryType();
            if (datasetType)
            {
                if (datasetType->getTypeCode() == type_row)
                    type = LINK(datasetType);
                else
                    type = makeRowType(LINK(dataset->queryRecordType()));
            }
            break;
        }
    }

    if (!type)
        type = makeRowType(NULL);

    return CHqlRow::makeRow(op, type, args);
}

extern IHqlExpression *createList(node_operator op, ITypeInfo *type, IHqlExpression *list)
{
    HqlExprArray parms;
    if (list)
    {
        list->unwindList(parms, no_comma);
        list->Release();
    }
    return CHqlExpressionWithType::makeExpression(op, type, parms);
}

extern IHqlExpression *createBinaryList(node_operator op, HqlExprArray & args)
{
    unsigned numArgs = args.ordinality();
    assertex(numArgs != 0);
    IHqlExpression * ret = &OLINK(args.item(0));
    for (unsigned idx = 1; idx < numArgs; idx++)
        ret = createValue(op, ret->getType(), ret, &OLINK(args.item(idx)));
    return ret;
}

extern IHqlExpression *createLeftBinaryList(node_operator op, HqlExprArray & args)
{
    unsigned numArgs = args.ordinality();
    assertex(numArgs != 0);
    IHqlExpression * ret = &OLINK(args.item(numArgs-1));
    for (unsigned idx = numArgs-1; idx-- != 0; )
        ret = createValue(op, ret->getType(), &OLINK(args.item(idx)), ret);
    return ret;
}

extern IHqlExpression *createRecord()
{
    return new CHqlRecord();
}

extern IHqlExpression *createRecord(const HqlExprArray & fields)
{
    CHqlRecord * record = new CHqlRecord();
    ForEachItemIn(idx, fields)
    {
        IHqlExpression & cur = fields.item(idx);
        record->addOperand(LINK(&cur));
    }
    return record->closeExpr();
}

extern IHqlExpression * createAssign(IHqlExpression * expr1, IHqlExpression * expr2)
{
    return createValue(no_assign, makeVoidType(), expr1, expr2);
}

extern IHqlExpression *createConstant(bool constant)
{
    if (constant)
        return LINK(constantTrue);
    return LINK(constantFalse);
}

extern IHqlExpression *createConstant(__int64 constant)
{
    //return CHqlConstant::makeConstant(createIntValue(constant, 8, true));
    // NOTE - we do not support large uint64 consts properly....
    bool sign;
    unsigned size;
    if (constant >= 0)
    {
        if (constant < 256)
        {
            size = 1;
            sign = constant < 128;
        }
        else if (constant < 65536)
        {
            size = 2;
            sign = constant < 32768;
        }
        else if (constant < I64C(0x100000000))
        {
            size = 4;
            sign = constant < 0x80000000;
        }
        else
        {
            size = 8;
            sign = true;
        }
    }
    else
    {
        sign = true;
        if (constant >= -128)
            size = 1;
        else if (constant >= -32768)
            size = 2;
        else if (constant >= (signed __int32) 0x80000000)
            size = 4;
        else
            size = 8;
    }
    return CHqlConstant::makeConstant(createIntValue(constant, size, sign));
}

extern IHqlExpression *createConstant(__int64 constant, ITypeInfo * type)
{
    return createConstant(createIntValue(constant, type));
}

extern IHqlExpression *createConstant(double constant)
{
    return CHqlConstant::makeConstant(createRealValue(constant, DEFAULT_REAL_SIZE));
}

extern IHqlExpression *createConstant(const char *constant)
{
    return CHqlConstant::makeConstant(createStringValue(constant, strlen(constant)));
}

extern IHqlExpression *createConstant(IValue * constant)
{
    return CHqlConstant::makeConstant(constant);
}

//This is called by the code generator when it needs to make an explicit call to an internal function, with arguments already translated.
extern IHqlExpression * createTranslatedExternalCall(IErrorReceiver * errors, IHqlExpression *func, HqlExprArray &actuals)
{
    assertex(func->getOperator() == no_funcdef);
    IHqlExpression * binder = func->queryChild(0);
    assertex(binder->getOperator() == no_external);
    return CHqlExternalCall::makeExternalCall(LINK(func), binder->getType(), actuals);
}


extern IHqlExpression *createExternalReference(IIdAtom * id, ITypeInfo *_type, IHqlExpression *props)
{
    HqlExprArray attributes;
    if (props)
    {
        props->unwindList(attributes, no_comma);
        props->Release();
    }
    return CHqlExternal::makeExternalReference(id, _type, attributes)->closeExpr();
}

extern IHqlExpression *createExternalReference(IIdAtom * id, ITypeInfo *_type, HqlExprArray & attributes)
{
    return CHqlExternal::makeExternalReference(id, _type, attributes)->closeExpr();
}

IHqlExpression * createExternalFuncdefFromInternal(IHqlExpression * funcdef)
{
    IHqlExpression * body = funcdef->queryChild(0);
    HqlExprArray attrs;
    unwindChildren(attrs, body, 1);

    if (body->isPure())
        attrs.append(*createAttribute(pureAtom));
    if (body->getInfoFlags() & HEFaction)
        attrs.append(*createAttribute(actionAtom));
    if (body->getInfoFlags() & HEFcontextDependentException)
        attrs.append(*createAttribute(contextSensitiveAtom));

    ITypeInfo * returnType = funcdef->queryType()->queryChildType();
    OwnedHqlExpr externalExpr = createExternalReference(funcdef->queryId(), LINK(returnType), attrs);
    return replaceChild(funcdef, 0, externalExpr);
}

extern IHqlExpression* createValue(node_operator op, HqlExprArray& operands) {
    return CHqlExpressionWithType::makeExpression(op, NULL, operands);
}

extern IHqlExpression* createValue(node_operator op, ITypeInfo *_type, HqlExprArray& operands) {
    return CHqlExpressionWithType::makeExpression(op, _type, operands);
}

extern IHqlExpression *createValue(node_operator op, IHqlExpression *p1) {
    return CHqlExpressionWithType::makeExpression(op, NULL, p1, NULL);
}

extern IHqlExpression* createConstant(int ival) {
    return CHqlConstant::makeConstant(createIntValue(ival, DEFAULT_INT_SIZE, true));
}

extern IHqlExpression* createBoolExpr(node_operator op, HqlExprArray& operands) {
    return CHqlExpressionWithType::makeExpression(op, makeBoolType(), operands);
}

extern IHqlExpression *createWrapper(node_operator op, IHqlExpression * e)
{
    ITypeInfo * type = e->queryType();
    if (type)
    {
        switch (type->getTypeCode())
        {
        case type_row:
            return createRow(op, e);
        case type_table:
        case type_groupedtable:
            return createDataset(op, e, NULL);
        case type_dictionary:
            return createDictionary(op, e, NULL);
        }
    }
    return createValue(op, LINK(type), e);
}


IHqlExpression *createWrapper(node_operator op, ITypeInfo * type, HqlExprArray & args)
{
    if (type)
    {
        switch (type->getTypeCode())
        {
        case type_row:
            return createRow(op, args);
        case type_dictionary:
            return createDictionary(op, args);
        case type_table:
        case type_groupedtable:
            return createDataset(op, args);
        }
    }
    return createValue(op, LINK(type), args);
}

extern HQL_API IHqlExpression *createWrapper(node_operator op, IHqlExpression * expr, IHqlExpression * arg)
{
    HqlExprArray args;
    args.append(*expr);
    if (arg)
    {
        arg->unwindList(args, no_comma);
        arg->Release();
    }
    return createWrapper(op, expr->queryType(), args);
}


IHqlExpression *createWrappedExpr(IHqlExpression * expr, node_operator op, HqlExprArray & args)
{
    args.add(*LINK(expr), 0);
    return createWrapper(op, expr->queryType(), args);
}


IHqlExpression *createWrapper(node_operator op, HqlExprArray & args)
{
    return createWrapper(op, args.item(0).queryType(), args);
}


extern IHqlExpression *createDatasetFromRow(IHqlExpression * ownedRow)
{
    //The follow generates better code, but causes problems with several examples in the ln regression suite
    //So disabled until can investigate more
    if (false && isSelectFirstRow(ownedRow))
    {
        IHqlExpression * childDs = ownedRow->queryChild(0);
        if (hasSingleRow(childDs))
        {
            OwnedHqlExpr releaseRow = ownedRow;
            return LINK(childDs);
        }
    }

    return createDataset(no_datasetfromrow, ownedRow); //, createUniqueId());
}


inline IHqlExpression * normalizeSelectLhs(IHqlExpression * lhs, bool & isNew)
{
    loop
    {
        switch (lhs->getOperator())
        {
        case no_newrow:
            assertex(!isNew);
            isNew = true;
            lhs = lhs->queryChild(0);
            break;  // round the loop again
        case no_activerow:
            isNew = false;
            lhs = lhs->queryChild(0);
            break;  // round the loop again
        case no_left:
        case no_right:
        case no_top:
        case no_activetable:
        case no_self:
        case no_selfref:
            isNew = false;
            return lhs;
        case no_select:
            if (isNew && isAlwaysActiveRow(lhs))
                isNew = false;
            return lhs;
        case no_selectnth:
        case no_createrow:
            assertex(isAlwaysNewRow(lhs));
            //Ensure selects of these expressions always have new set - it saves problems when an
            //unnormalized tree is walked or transformed.
            //IF/PROJECTROW are not includes since they could be folded to an active selector
            isNew = true;
            return lhs;
        default:
            return lhs;
        }
    }
}

inline void checkRhsSelect(IHqlExpression * rhs)
{
#ifdef _DEBUG
    node_operator rhsOp = rhs->getOperator();
    assertex(rhsOp == no_field || rhsOp == no_ifblock || rhsOp == no_indirect);
#endif
}

extern IHqlExpression * createSelectExpr(IHqlExpression * _lhs, IHqlExpression * rhs, bool _isNew)
{
    OwnedHqlExpr lhs = _lhs;
    bool isNew = _isNew;
    IHqlExpression * normalLhs = normalizeSelectLhs(lhs, isNew);
    IHqlExpression * newAttr = isNew ? newSelectAttrExpr : NULL;

    checkRhsSelect(rhs);

    type_t t = rhs->queryType()->getTypeCode();
    if (t == type_table || t == type_groupedtable)
        return createDataset(no_select, LINK(normalLhs), createComma(rhs, LINK(newAttr)));
    if (t == type_dictionary)
        return createDictionary(no_select, LINK(normalLhs), createComma(rhs, LINK(newAttr)));
    if (t == type_row)
        return createRow(no_select, LINK(normalLhs), createComma(rhs, LINK(newAttr)));

    return CHqlSelectBaseExpression::makeSelectExpression(LINK(normalLhs), rhs, LINK(newAttr));
}

static IHqlExpression * doCreateSelectExpr(HqlExprArray & args)
{
    IHqlExpression * rhs = &args.item(1);
    checkRhsSelect(rhs);

    type_t t = rhs->queryType()->getTypeCode();
    if (t == type_table || t == type_groupedtable)
        return createDataset(no_select, args);
    if (t == type_dictionary)
        return createDictionary(no_select, args);
    if (t == type_row)
        return createRow(no_select, args);

    return CHqlSelectBaseExpression::makeSelectExpression(args);
}

extern IHqlExpression * createSelectExpr(HqlExprArray & args)
{
    IHqlExpression * lhs = &args.item(0);
    bool isNew = false;
    for (unsigned i=2; i < args.ordinality(); i++)
    {
        if (args.item(i).queryName() == newAtom)
        {
            isNew = true;
            args.remove(i);
            break;
        }
    }

    IHqlExpression * normalLhs = normalizeSelectLhs(lhs, isNew);
    if (lhs != normalLhs)
        args.replace(*LINK(normalLhs), 0);
    if (isNew)
        args.append(*LINK(newSelectAttrExpr));

    return doCreateSelectExpr(args);
}

IHqlExpression * ensureDataset(IHqlExpression * expr)
{
    if (expr->isDataset())
        return LINK(expr);

    if (expr->isDatarow())
        return createDatasetFromRow(LINK(expr));

    throwUnexpected();
}


extern bool isAlwaysNewRow(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_createrow:
    case no_selectnth:
        return true;
    }
    return false;
}

extern bool isAlwaysActiveRow(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_activerow:
    case no_left:
    case no_right:
    case no_top:
    case no_activetable:
    case no_self:
    case no_selfref:
        return true;
    case no_select:
        if (expr->isDataset())
            return false;
        return isAlwaysActiveRow(expr->queryChild(0));
    }
    return false;
}

IHqlExpression * ensureActiveRow(IHqlExpression * expr)
{
    if (isAlwaysActiveRow(expr))
        return LINK(expr);
    return createRow(no_activerow, LINK(expr->queryNormalizedSelector()));
}

IHqlExpression * ensureSerialized(IHqlExpression * expr, IAtom * serialForm)
{
    ITypeInfo * type = expr->queryType();
    Owned<ITypeInfo> serialType = getSerializedForm(type, serialForm);
    if (type == serialType)
        return LINK(expr);

    HqlExprArray args;
    args.append(*LINK(expr));
    args.append(*createAttribute(serialForm));
    return createWrapper(no_serialize, serialType, args);
}

IHqlExpression * ensureDeserialized(IHqlExpression * expr, ITypeInfo * type, IAtom * serialForm)
{
    assertex(type->getTypeCode() != type_record);
    Owned<ITypeInfo> serialType = getSerializedForm(type, serialForm);
    if (queryUnqualifiedType(type) == queryUnqualifiedType(serialType))
        return LINK(expr);

    assertRecordTypesMatch(expr->queryType(), serialType);

    HqlExprArray args;
    args.append(*LINK(expr));
    args.append(*LINK(queryOriginalRecord(type)));
    args.append(*createAttribute(serialForm));
    //MORE: I may prefer to create a project instead of a serialize...
    return createWrapper(no_deserialize, type, args);
}

bool isDummySerializeDeserialize(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    if ((op != no_serialize) && (op != no_deserialize))
        return false;

    IHqlExpression * child = expr->queryChild(0);
    node_operator childOp = child->getOperator();
    if ((childOp != no_serialize) && (childOp != no_deserialize))
        return false;

    if (op == childOp)
        return false;

    //MORE:? Need to check the serialization form?
    if (expr->queryType() != child->queryChild(0)->queryType())
        return false;

    return true;
}


bool isRedundantGlobalScope(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_globalscope);
    IHqlExpression * child = expr->queryChild(0);
    if (child->getOperator() != no_globalscope)
        return false;
    if (expr->hasAttribute(optAtom) && !child->hasAttribute(optAtom))
        return false;
    return true;
}

bool isIndependentOfScope(IHqlExpression * expr)
{
    return expr->isIndependentOfScope();
}

bool canEvaluateInScope(const HqlExprCopyArray & activeScopes, const HqlExprCopyArray & required)
{
    ForEachItemIn(i, required)
    {
        if (!activeScopes.contains(required.item(i)))
            return false;
    }
    return true;
}

    
bool canEvaluateInScope(const HqlExprCopyArray & activeScopes, IHqlExpression * expr)
{
    HqlExprCopyArray scopesUsed;
    expr->gatherTablesUsed(NULL, &scopesUsed);
    return canEvaluateInScope(activeScopes, scopesUsed);
}


bool exprReferencesDataset(IHqlExpression * expr, IHqlExpression * dataset)
{
    return expr->usesSelector(dataset->queryNormalizedSelector());
}

void gatherChildTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope, IHqlExpression * expr, unsigned firstChild)
{
    unsigned max = expr->numChildren();
    for (unsigned i=firstChild; i < max; i++)
        expr->queryChild(i)->gatherTablesUsed(newScope, inScope);
}

extern IHqlScope *createService()
{
    return new CHqlLocalScope(no_service, NULL, NULL);
}

extern IHqlScope *createScope()
{
    return new CHqlLocalScope(no_scope, NULL, NULL);
}

extern IHqlScope *createPrivateScope()
{
    return new CHqlLocalScope(no_privatescope, NULL, NULL);
}

extern IHqlScope *createPrivateScope(IHqlScope * scope)
{
    return new CHqlLocalScope(no_privatescope, scope->queryId(), scope->queryFullName());
}

extern IHqlScope* createScope(IHqlScope* scope)
{
    return new CHqlLocalScope(scope);
}

extern IHqlScope* createVirtualScope()
{
    return new CHqlVirtualScope(NULL, NULL);
}

extern IHqlScope* createForwardScope(IHqlScope * parentScope, HqlGramCtx * parentCtx, HqlParseContext & parseCtx)
{
    return new CHqlForwardScope(parentScope, parentCtx, parseCtx);
}

extern IHqlScope* createConcreteScope()
{
    return new CHqlLocalScope(no_concretescope, NULL, NULL);
}

extern IHqlScope* createVirtualScope(IIdAtom * id, const char * fullName)
{
    return new CHqlVirtualScope(id, fullName);
}

extern IHqlScope* createLibraryScope()
{
    return new CHqlLocalScope(no_libraryscope, NULL, NULL);
}

extern IHqlRemoteScope *createRemoteScope(IIdAtom * id, const char * fullName, IEclRepositoryCallback *ds, IProperties* props, IFileContents * text, bool lazy, IEclSource * eclSource)
{
    assertex(fullName || !id);
    return new CHqlRemoteScope(id, fullName, ds, props, text, lazy, eclSource);
}

IHqlExpression * populateScopeAndClose(IHqlScope * scope, const HqlExprArray & children, const HqlExprArray & symbols)
{
    IHqlExpression * scopeExpr = queryExpression(scope);
    ForEachItemIn(i1, children)
        scopeExpr->addOperand(LINK(&children.item(i1)));

    ForEachItemIn(i2, symbols)
        scope->defineSymbol(&OLINK(symbols.item(i2)));

    return scopeExpr->closeExpr();
}


extern HQL_API IHqlExpression* createTemplateFunctionContext(IHqlExpression* expr, IHqlScope* scope)
{
    IHqlExpression* e = new CHqlTemplateFunctionContext(expr, scope);
    return e->closeExpr();
}

extern IHqlExpression* createFieldMap(IHqlExpression* ds, IHqlExpression* map)
{
    return createDataset(no_fieldmap, ds, map);
}

extern IHqlExpression * createCompound(IHqlExpression * expr1, IHqlExpression * expr2)
{
    if (!expr1)
        return expr2;
    if (!expr2)
        return expr1;
    assertex(expr1->queryType()->getTypeCode() == type_void);
    if (!expr2->isFunction())
    {
        if (expr2->isDataset())
            return createDataset(no_compound, expr1, expr2);
        if (expr2->isDatarow())
            return createRow(no_compound, expr1, expr2);
    }
    return createValue(no_compound, expr2->getType(), expr1, expr2);
}

extern IHqlExpression * createCompound(const HqlExprArray & actions)
{
    IHqlExpression * expr = NULL;
    ForEachItemInRev(idx, actions)
        expr = createCompound(&OLINK(actions.item(idx)), expr);
    return expr;
}

extern IHqlExpression * createActionList(node_operator op, const HqlExprArray & actions)
{
    switch (actions.ordinality())
    {
    case 0:
        return createValue(no_null, makeVoidType());
    case 1:
        return LINK(&actions.item(0));
    }
    return createValueSafe(op, makeVoidType(), actions);
}

extern IHqlExpression * createActionList(const HqlExprArray & actions)
{
    return createActionList(no_actionlist, actions);
}

extern void ensureActions(HqlExprArray & actions, unsigned first, unsigned last)
{
    for (unsigned i=first; i < last; i++)
    {
        IHqlExpression & cur = actions.item(i);
        if (!cur.isAction())
            actions.replace(*createValue(no_evaluate_stmt, makeVoidType(), LINK(&cur)), i);
    }
}

extern void ensureActions(HqlExprArray & actions)
{
    ensureActions(actions, 0, actions.ordinality());
}

extern IHqlExpression * createActionList(node_operator op, const HqlExprArray & actions, unsigned from, unsigned to)
{
    switch (to-from)
    {
    case 0:
        return createValue(no_null, makeVoidType());
    case 1:
        return LINK(&actions.item(from));
    }
    return createValueSafe(op, makeVoidType(), actions, from, to);
}

extern IHqlExpression * createActionList(const HqlExprArray & actions, unsigned from, unsigned to)
{
    return createActionList(no_actionlist, actions, from, to);
}

extern IHqlExpression * createCompound(node_operator op, const HqlExprArray & actions)
{
    if (op == no_compound)
        return createCompound(actions);
    else
        return createActionList(actions);
}

extern IHqlExpression * createComma(IHqlExpression * expr1, IHqlExpression * expr2)
{
    if (!expr1)
        return expr2;
    if (!expr2)
        return expr1;
    return createValue(no_comma, expr1->getType(), expr1, expr2);
}

extern IHqlExpression * createComma(IHqlExpression * expr1, IHqlExpression * expr2, IHqlExpression * expr3)
{
    return createComma(expr1, createComma(expr2, expr3));
}


extern IHqlExpression * createComma(IHqlExpression * expr1, IHqlExpression * expr2, IHqlExpression * expr3, IHqlExpression * expr4)
{
    return createComma(expr1, createComma(expr2, createComma(expr3, expr4)));
}


static IHqlExpression * createComma(const HqlExprArray & exprs, unsigned first, unsigned last)
{
    if (first +1 == last)
        return &OLINK(exprs.item(first));
    unsigned mid = (first+last)/2;
    return createComma(createComma(exprs, first, mid), createComma(exprs, mid, last));
}


extern IHqlExpression * createComma(const HqlExprArray & exprs)
{
    //Create a balanced tree instead of unbalanced...
    unsigned max = exprs.ordinality();
    if (max == 0)
        return NULL;
    else if (max == 1)
        return &OLINK(exprs.item(0));
    return createComma(exprs, 0, max);
}


IHqlExpression * createBalanced(node_operator op, ITypeInfo * type, const HqlExprArray & exprs, unsigned first, unsigned last)
{
    if (first +1 == last)
        return &OLINK(exprs.item(first));
    unsigned mid = (first+last)/2;
    IHqlExpression * left = createBalanced(op, type, exprs, first, mid);
    IHqlExpression * right = createBalanced(op, type, exprs, mid, last);
    return createValue(op, LINK(type), left, right);
}


extern IHqlExpression * createBalanced(node_operator op, ITypeInfo * type, const HqlExprArray & exprs)
{
    //Create a balanced tree instead of unbalanced...
    unsigned max = exprs.ordinality();
    if (max == 0)
        return NULL;
    else if (max == 1)
        return &OLINK(exprs.item(0));
    return createBalanced(op, type, exprs, 0, max);
}


extern IHqlExpression * createUnbalanced(node_operator op, ITypeInfo * type, const HqlExprArray & exprs)
{
    unsigned max = exprs.ordinality();
    if (max == 0)
        return NULL;
    LinkedHqlExpr ret = &exprs.item(0);
    for (unsigned i=1; i < max; i++)
        ret.setown(createValue(op, LINK(type), ret.getClear(), LINK(&exprs.item(i))));
    return ret.getClear();
}


IHqlExpression * extendConditionOwn(node_operator op, IHqlExpression * l, IHqlExpression * r)
{
    if (!r) return l;
    IValue * rvalue = r->queryValue();
    if (rvalue)
    {
        bool val = rvalue->getBoolValue();
        switch (op)
        {
        case no_or:
            //x or true = true, x or false = x
            if (val)
            {
                ::Release(l);
                return r;
            }
            else
            {
                r->Release();
                return l;
            }
            break;
        case no_and:
            //x and true = x, x and false = false
            if (val)
            {
                r->Release();
                return l;
            }
            else
            {
                ::Release(l);
                return r;
            }
            break;
        }
    }
    if (!l) return r;
    return createValue(op, l->getType(), l, r);
}


extern IValue * createNullValue(ITypeInfo * type)
{
    IValue * null;
    switch (type->getTypeCode())
    {
    case type_alien:
        {
            IHqlAlienTypeInfo * alien = queryAlienType(type);
            type = alien->queryPhysicalType();
            null = blank->castTo(type);
        }
        break;
    case type_bitfield:
        null = createNullValue(type->queryPromotedType());
        break;
    default:
        null = blank->castTo(type);
        break;
    }
    assertex(null);
    return null;
}

extern HQL_API IHqlExpression * createNullScope()
{
    return queryExpression(createScope())->closeExpr();
}

extern HQL_API IHqlExpression * createNullExpr(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_set:
        {
            ITypeInfo * childType = type->queryChildType();
            if (childType && isDatasetType(childType))
                return createValue(no_datasetlist, LINK(type));
            return createValue(no_list, LINK(type));
        }
    case type_groupedtable:
        {
            ITypeInfo * recordType = queryRecordType(type);
            IHqlExpression * record = queryExpression(recordType);
            return createDataset(no_null, LINK(record), createGroupedAttribute(NULL));
        }
    case type_table:
        {
            ITypeInfo * recordType = queryRecordType(type);
            IHqlExpression * record = queryExpression(recordType);
            IHqlExpression * attr = queryAttribute(type, _linkCounted_Atom);
            return createDataset(no_null, LINK(record), LINK(attr));
        }
    case type_dictionary:
        {
            ITypeInfo * recordType = queryRecordType(type);
            IHqlExpression * record = queryExpression(recordType);
            IHqlExpression * attr = queryAttribute(type, _linkCounted_Atom);
            return createDictionary(no_null, LINK(record), LINK(attr));
        }
    case type_row:
#if 0
        {
            OwnedHqlExpr nullTransform = createClearTransform(queryOriginalRecord(type));
            return createRow(no_createrow, LINK(nullTransform));
        }
#endif
    case type_record:
        return createRow(no_null, LINK(queryRecord(type)));
    case type_int:
        {
            unsigned size = type->getSize();
            unsigned isSigned = type->isSigned() ? 0 : 1;

            CriticalBlock block(*nullIntCS);
            IHqlExpression * null = nullIntValue[size][isSigned];
            if (!null)
            {
                null = createConstant(blank->castTo(type));
                nullIntValue[size][isSigned] = null;
            }
            return LINK(null);
        }
    case type_void:
        return createValue(no_null, makeVoidType());
    case type_scope:
        return createNullScope();
    case type_sortlist:
        return createValue(no_sortlist, LINK(type));
    case type_transform:
        return createNullTransform(queryRecord(type));
    default:
        return createConstant(createNullValue(type));
    }
}

extern HQL_API IHqlExpression * createNullExpr(IHqlExpression * expr)
{
    if (expr->getOperator()==no_select)
        return createNullExpr(expr->queryChild(1));
    IHqlExpression * defaultValue = queryAttributeChild(expr, defaultAtom, 0);
    if (defaultValue)
        return LINK(defaultValue);
    return createNullExpr(expr->queryType());
}


extern HQL_API IHqlExpression * createPureVirtual(ITypeInfo * type)
{
    if (type)
    {
        switch (type->getTypeCode())
        {
        case type_table:
            return createDataset(no_purevirtual, LINK(queryOriginalRecord(type)));
        case type_groupedtable:
            return createDataset(no_purevirtual, LINK(queryOriginalRecord(type)), createAttribute(groupedAtom));
        case type_dictionary:
            return createDictionary(no_purevirtual, LINK(queryOriginalRecord(type)), createAttribute(groupedAtom));
        case type_row:
        case type_record:
            return createRow(no_purevirtual, LINK(queryOriginalRecord(type)));
        case type_scope:
            //Could turn this into an interface - needed if we support nested modules
            throwUnexpected();
        default:
            return createValue(no_purevirtual, LINK(type));
        }
    }
    else
        return createValue(no_purevirtual, makeIntType(8, true));
}

extern HQL_API bool isNullExpr(IHqlExpression * expr, IHqlExpression * field)
{
    ITypeInfo * exprType = expr->queryType();
    ITypeInfo * fieldType = field->queryType();
    if (exprType->getTypeCode() != fieldType->getTypeCode())
        return false;

    IValue * value = expr->queryValue();
    switch (fieldType->getTypeCode())
    {
    case type_boolean:
    case type_int:
    case type_swapint:
    case type_date:
    case type_enumerated:
    case type_bitfield:
    case type_data:
    case type_string:
    case type_varstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
    case type_qstring:
    case type_decimal:
    case type_real:
    case type_alien:
    case type_set:
        {
            if (!value)
                return false;
            OwnedHqlExpr null = createNullExpr(field);
            OwnedHqlExpr castValue = ensureExprType(expr, fieldType);
            return null->queryBody() == castValue->queryBody();
        }
    case type_row:
    case type_table:
    case type_groupedtable:
        return (expr->getOperator() == no_null);
    default:
        return false;
    }
}

extern HQL_API IHqlExpression * createConstantOne()
{
    return LINK(cachedOne);
}

extern HQL_API IHqlExpression * createLocalAttribute()
{
    return LINK(cachedLocalAttribute);
}

IHqlExpression * cloneOrLink(IHqlExpression * expr, HqlExprArray & children)
{
    unsigned prevNum = expr->numChildren();
    unsigned newNum = children.ordinality();
    if (prevNum != newNum)
        return expr->clone(children);

    ForEachItemIn(idx, children)
    {
        if (&children.item(idx) != expr->queryChild(idx))
            return expr->clone(children);
    }
    return LINK(expr);
}

IHqlExpression * createCompareExpr(node_operator op, IHqlExpression * l, IHqlExpression * r)
{
    Owned<ITypeInfo> type = ::getPromotedECLCompareType(l->queryType(), r->queryType());
    IHqlExpression * ret = createValue(op, makeBoolType(), ensureExprType(l, type), ensureExprType(r, type));
    l->Release();
    r->Release();
    return ret;
}

IHqlExpression * createNullDataset(IHqlExpression * ds)
{
    IHqlExpression * attr = isGrouped(ds) ? createGroupedAttribute(NULL) : NULL;
    return createDataset(no_null, LINK(ds->queryRecord()), attr);
}

static IHqlExpression * doAttachWorkflowOwn(IHqlExpression * value, IHqlExpression * workflow)
{
    if (!value->isFunction())
    {
        if (value->isDataset())
            return createDataset(no_colon, value, LINK(workflow));
        
        if (value->queryType()->getTypeCode() == type_row)
            return createRow(no_colon, value, LINK(workflow));

        if (value->isDictionary())
            return createDictionary(no_colon, value, LINK(workflow));
    }

    //If a string value is stored, its type is a string of unknown length 
    //(because a different length string might be stored...)
    Linked<ITypeInfo> type = value->queryType();
#ifdef STORED_CAN_CHANGE_LENGTH
    switch (type->getTypeCode())
    {
    case type_qstring:
    case type_varstring:
    case type_string:
    case type_data:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        type.setown(getStretchedType(UNKNOWN_LENGTH, type));
        break;
    }
#endif
    return createValueF(no_colon, type.getClear(), value, LINK(workflow), NULL);
}


void processSectionPseudoWorkflow(SharedHqlExpr & expr, IHqlExpression * workflow, const HqlExprCopyArray * allActiveParameters)
{
    //Section attributes are implemented by adding a sectionAnnotation to all datasets within a section that aren't 
    //included in the list of active parameters, or the datasets provided as parameters to the section
    HqlSectionAnnotator annotator(workflow);
    if (allActiveParameters)
    {
        ForEachItemIn(i1, *allActiveParameters)
        {
            IHqlExpression & cur = allActiveParameters->item(i1);
            if (!cur.isFunction() && cur.isDataset())
                annotator.noteInput(&cur);
        }
    }
    ForEachChildFrom(i2, workflow, 1)
    {
        IHqlExpression * cur = workflow->queryChild(i2);
        if (!cur->isFunction() && cur->isDataset())
            annotator.noteInput(cur);
    }

    OwnedHqlExpr value = annotator.transform(expr);
    expr.set(value);
}


static IHqlExpression * processPseudoWorkflow(SharedHqlExpr & expr, HqlExprArray & meta, IHqlExpression * workflow, const HqlExprCopyArray * allActiveParameters)
{
    switch (workflow->getOperator())
    {
    case no_comma:
        {
            IHqlExpression * left = workflow->queryChild(0);
            IHqlExpression * right = workflow->queryChild(1);
            OwnedHqlExpr newLeft = processPseudoWorkflow(expr, meta, left, allActiveParameters);
            OwnedHqlExpr newRight = processPseudoWorkflow(expr, meta, right, allActiveParameters);
            if ((left != newLeft) || (right != newRight))
                return createComma(newLeft.getClear(), newRight.getClear());
            return LINK(workflow);
        }
    case no_attr:
        {
            IAtom * name = workflow->queryName();
            if (name == sectionAtom)
            {
                processSectionPseudoWorkflow(expr, workflow, allActiveParameters);
                return NULL;
            }
            else if ((name == deprecatedAtom) || (name == onWarningAtom))
            {
                meta.append(*LINK(workflow));
                return NULL;
            }
            break;
        }
    }
    return LINK(workflow);
}

IHqlExpression * attachWorkflowOwn(HqlExprArray & meta, IHqlExpression * _value, IHqlExpression * workflow, const HqlExprCopyArray * allActiveParameters)
{
    if (!workflow)
        return _value;

    //For patterns, we only allow define(x), and we want it inserted inside the no_pat_instance.
    //Really the latter should be added after the workflow....  Also convert define to the non workflow form to
    //aid processing later.
    OwnedHqlExpr value = _value;
    if (value->getOperator() == no_pat_instance)
    {
        assertex(workflow->isAttribute() && workflow->queryName() == defineAtom);
        IHqlExpression * pattern = value->queryChild(0);
        HqlExprArray args;
        args.append(*createValue(no_define, pattern->getType(), LINK(pattern), LINK(workflow->queryChild(0))));
        unwindChildren(args, value, 1);
        return value->clone(args);
    }

    if (value->getOperator() == no_outofline)
    {
        HqlExprArray args;
        args.append(*attachWorkflowOwn(meta, LINK(value->queryChild(0)), workflow, allActiveParameters));
        unwindChildren(args, value, 1);
        return value->clone(args);
    }

    OwnedHqlExpr newWorkflow = processPseudoWorkflow(value, meta, workflow, allActiveParameters);
    if (newWorkflow)
        value.setown(doAttachWorkflowOwn(value.getClear(), newWorkflow));
    return value.getClear();
}


//==============================================================================================================

static bool queryOriginalName(ITypeInfo* type, StringBuffer& s)
{
    ITypeInfo * original = queryModifier(type, typemod_original);
    if (original)
    {
        IHqlExpression * expr = (IHqlExpression *)original->queryModifierExtra();
        if (expr->queryName())
        {
            s.append(expr->queryName());
            return true;
        }
    }
    return false;
}


void PrintLogExprTree(IHqlExpression *expr, const char *caption, bool full)
{
    StringBuffer s;
    toECL(expr, s, full);
    s.append('\n');
    if (caption)
        PrintLog(caption);
    else if (full)
        s.insert(0, "\n");
    PrintLogDirect(s.str());
}

//========This will go to IExpression implementations ======================================================================================================

static unsigned exportRecord(IPropertyTree *dataNode, IHqlExpression * record, unsigned & offset, bool flatten);
static unsigned exportRecord(IPropertyTree *dataNode, IHqlExpression * record, bool flatten);

static inline bool isdigit_or_underbar(unsigned char c)
{
    return isdigit(c) || c=='_';
}

unsigned exportField(IPropertyTree *table, IHqlExpression *field, unsigned & offset, bool flatten)
{
    if (field->isAttribute())
        return 0;
    IHqlExpression *defValue = field->queryChild(0);
    IPropertyTree *f = createPTree("Field", ipt_caseInsensitive);
    ITypeInfo * type = field->queryType();
    if (type->getTypeCode() == type_row)
        type = type->queryChildType();

    f->setProp("@label", field->queryName()->getAtomNamePtr());
    f->setProp("@name", field->queryName()->getAtomNamePtr());
    f->setPropInt("@position", offset++);

    if (defValue && !defValue->isAttribute() && defValue->getOperator()!=no_field && defValue->getOperator() != no_select)
    {
        StringBuffer ecl;
        toECL(defValue, ecl, false);
        while (ecl.length())
        {
            unsigned char c = ecl.charAt(ecl.length()-1);
            if (isspace(c) || c==';')
                ecl.setLength(ecl.length()-1);
            else
                break;
        }
        f->setProp("Expression", ecl.str());
    }
    f->setPropInt("@rawtype", getClarionResultType(type));
    StringBuffer typeName;
    if (!queryOriginalName(type, typeName))
        type->getECLType(typeName);
    f->setProp("@ecltype", typeName.str());
    while (isdigit_or_underbar((unsigned char)typeName.charAt(typeName.length()-1)))
        typeName.remove(typeName.length()-1, 1);
    f->setProp("@type", typeName.str());
    f = table->addPropTree(f->queryName(), f);

    unsigned thisSize = type->getSize();
    IHqlExpression * record = queryRecord(type);
    if (record)
    {
        switch (type->getTypeCode())
        {
        case type_record:
        case type_row:
            f->setPropBool("@isRecord", true);
            break;
        case type_table:
        case type_groupedtable:
            f->setPropBool("@isDataset", true);
            break;
        }
        if (flatten)
        {
            thisSize = exportRecord(table, record, offset, flatten);

            IPropertyTree *end = createPTree("Field", ipt_caseInsensitive);
            end->setPropBool("@isEnd", true);
            end->setProp("@name", field->queryName()->getAtomNamePtr());
            table->addPropTree(end->queryName(), end);
        }
        else
            thisSize = exportRecord(f, record, flatten);
    }
    f->setPropInt("@size", thisSize);
    return thisSize;
}

static unsigned exportRecord(IPropertyTree *dataNode, IHqlExpression * record, unsigned & offset, bool flatten)
{
    //MORE: Nested
    unsigned size = 0;
    ForEachChild(idx, record)
    {
        IHqlExpression * cur = record->queryChild(idx);
        unsigned thisSize = 0;
        switch (cur->getOperator())
        {
        case no_field:
            thisSize = exportField(dataNode, cur, offset, flatten);
            break;
        case no_record:
            thisSize = exportRecord(dataNode, cur, offset, flatten);
            break;
        case no_ifblock:
            {
                if (flatten)
                {
                    exportRecord(dataNode, cur->queryChild(1), offset, flatten);
                }
                else
                {
                    IPropertyTree * ifblock = dataNode->addPropTree("IfBlock", createPTree("IfBlock", ipt_caseInsensitive));
                    ifblock->setPropInt("@position", idx);
                    exportRecord(ifblock, cur->queryChild(1), flatten);
                    size = UNKNOWN_LENGTH;
                }
                break;
            }
        }
        if (size != UNKNOWN_LENGTH)
        {
            if (thisSize != UNKNOWN_LENGTH)
                size += thisSize;
            else
                size = UNKNOWN_LENGTH;
        }
    }
    return size;
}

static unsigned exportRecord(IPropertyTree *dataNode, IHqlExpression * record, bool flatten)
{
    unsigned offset = 0;
    return exportRecord(dataNode, record, offset, flatten);
}


StringBuffer &getExportName(IHqlExpression *table, StringBuffer &s)
{
    StringBuffer tname;
    if (table->getOperator()==no_table)
    {
        IHqlExpression *mode = table->queryChild(2);
        StringBuffer lc;
        s.append(mode->toString(lc).toLowerCase());
        IValue *name = table->queryChild(0)->queryValue();  // MORE - should probably try to fold
        name->getStringValue(tname);
    }
    else
    {
        s.append("logical");
        tname.append(table->queryName());
    }
    if (tname.length())
    {
        tname.toLowerCase();
        tname.setCharAt(0, (char)(tname.charAt(0) - 0x20));
    }
    return s.append(tname);
}

void exportMap(IPropertyTree *dataNode, IHqlExpression *destTable, IHqlExpression *sourceTable)
{

    IPropertyTree *maps = dataNode->queryPropTree("./Map");
    if (!maps)
    {
        maps = createPTree("Map", ipt_caseInsensitive);
        dataNode->addPropTree("Map", maps);
    }
    IPropertyTree *map = createPTree("MapTables", ipt_caseInsensitive);
    StringBuffer name;
    map->setProp("@destTable", getExportName(destTable, name).str());
    map->setProp("@sourceTable", getExportName(sourceTable, name.clear()).str());
    maps->addPropTree("MapTables", map);
}

void exportData(IPropertyTree *data, IHqlExpression *table, bool flatten)
{
    IPropertyTree *tt = NULL;
    switch (table->getOperator())
    {
    case no_table:
        {
            IHqlExpression *mode = table->queryChild(2);
            StringBuffer prefix;
            mode->toString(prefix);
            prefix.toLowerCase();
            prefix.setCharAt(0, (char)(prefix.charAt(0) - 0x20));
            tt = createPTree(StringBuffer(prefix).append("Table").str(), ipt_caseInsensitive);
            StringBuffer name;
            tt->setProp("@name", getExportName(table, name).str());
            tt->setProp("@exported", table->isExported() ? "true" : "false");
            unsigned size = exportRecord(tt, table->queryChild(1), flatten);
            if (mode->getOperator()==no_flat)
                tt->setPropInt("@recordLength", size);
            data->addPropTree(tt->queryName(), tt);
            break;
        }
    case no_usertable:
        {
            IHqlExpression *filter = NULL;
            IHqlExpression *base = table->queryChild(0);
            if (base->getOperator()==no_filter)
            {
                // MORE - filter can have multiple kids. We will lose all but the first
                filter = base->queryChild(1);
                base = base->queryChild(0);
            }
            exportMap(data, table, base);
            exportData(data, base);
            tt = createPTree("LogicalTable", ipt_caseInsensitive);
            StringBuffer name;
            tt->setProp("@name", getExportName(table, name).str());
            tt->setProp("@exported", table->isExported() ? "true" : "false");
            exportRecord(tt, table->queryChild(1), flatten);
            if (filter)
            {
                StringBuffer ecl;
                toECL(filter, ecl, false);
                while (ecl.length())
                {
                    unsigned char c = ecl.charAt(ecl.length()-1);
                    if (isspace(c) || c==';')
                        ecl.setLength(ecl.length()-1);
                    else
                        break;
                }
                tt->setProp("Filter", ecl.str());
            }
            data->addPropTree(tt->queryName(), tt);
            break;
        }
    case no_record:
        {
            exportRecord(data, table, flatten);
            break;
        }
    case no_select:
        {
            unsigned offset = 0;
            exportField(data, table->queryChild(1), offset, flatten);
            break;
        }
    case no_field:
        {
            unsigned offset = 0;
            exportField(data, table, offset, flatten);
            break;
        }
    default:
        UNIMPLEMENTED;
        break;
    }
}

//---------------------------------------------------------------------------

static bool exprContainsCounter(RecursionChecker & checker, IHqlExpression * expr, IHqlExpression * counter)
{
    if (checker.alreadyVisited(expr))
        return false;
    checker.setVisited(expr);
    switch (expr->getOperator())
    {
    case no_field:
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return false;
    case no_counter:
        return (expr == counter);
    case no_select:
        {
            IHqlExpression * ds = expr->queryChild(0);
            if (isNewSelector(expr))
                return exprContainsCounter(checker, ds, counter);
            return false;
        }
    }
    ForEachChild(idx, expr)
        if (exprContainsCounter(checker, expr->queryChild(idx), counter))
            return true;
    return false;
}

bool transformContainsCounter(IHqlExpression * transform, IHqlExpression * counter)
{
    RecursionChecker checker;

    return exprContainsCounter(checker, transform, counter);
}

//==============================================================================================================

static IHqlExpression * applyInstantEclLimit(IHqlExpression * expr, unsigned limit)
{
    if (expr->getOperator() == no_selectfields)
    {
        //Legacy artefact.  no_selectfield should be removed.
        HqlExprArray args;
        args.append(*applyInstantEclLimit(expr->queryChild(0), limit));
        unwindChildren(args, expr, 1);
        args.append(*createUniqueId());
        return expr->clone(args);
    }
    
    if (expr->isDataset())
    {
        if (limit && (expr->getOperator() != no_choosen))
            return createDataset(no_choosen, LINK(expr), createConstant((__int64) limit));
    }

    return LINK(expr);
}

static IHqlExpression *doInstantEclTransformation(IHqlExpression * subquery, unsigned limit)
{
    if (subquery->getOperator()==no_output && !queryRealChild(subquery, 1) && !subquery->hasAttribute(allAtom))
    {
        IHqlExpression * ds = subquery->queryChild(0);
        OwnedHqlExpr limitedDs = applyInstantEclLimit(ds, limit);
        return replaceChild(subquery, 0, limitedDs);
    }
    return LINK(subquery);
}


static IHqlExpression * walkInstantEclTransformations(IHqlExpression * expr, unsigned limit)
{
    switch (expr->getOperator())
    {
    case no_compound:
    case no_comma:
    case no_sequential:
    case no_parallel:
    case no_orderedactionlist:
    case no_actionlist:
    case no_if:
    case no_case:
    case no_map:
    case no_colon:
    case no_chooseds:
        {
            HqlExprArray args;
            ForEachChild(i, expr)
                args.append(*walkInstantEclTransformations(expr->queryChild(i), limit));
            return expr->clone(args);
        }
    case no_output:
        return doInstantEclTransformation(expr, limit);
    }
    return LINK(expr);
}

static IHqlExpression * walkRootInstantEclTransformations(IHqlExpression * expr, unsigned limit)
{
    switch (expr->getOperator())
    {
    case no_compound:
    case no_comma:
        {
            HqlExprArray args;
            ForEachChild(i, expr)
                args.append(*walkRootInstantEclTransformations(expr->queryChild(i), limit));
            return expr->clone(args);
        }
    }
    if (expr->isDataset())
        return applyInstantEclLimit(expr, limit);
    return walkInstantEclTransformations(expr, limit);
}

extern HQL_API IHqlExpression *doInstantEclTransformations(IHqlExpression *qquery, unsigned limit)
{
    try
    {
        return walkRootInstantEclTransformations(qquery, limit);
    }
    catch (...)
    {
        PrintLog("InstantECL transformations - exception caught");
    }
    return LINK(qquery);
}

//==============================================================================================================

//MAKEValueArray(byte, ByteArray);
typedef UnsignedArray DepthArray;

struct TransformTrackingInfo
{
public:
    void lock();
    void unlock();

public:
    transformdepth_t curTransformDepth;
    PointerArray transformStack;
    UnsignedArray transformStackMark;

    DepthArray depthStack;
};

static TransformTrackingInfo transformExtraState[NUM_PARALLEL_TRANSFORMS+1];
static bool isActiveMask[NUM_PARALLEL_TRANSFORMS+1];

#if NUM_PARALLEL_TRANSFORMS==1
unsigned threadActiveExtraIndex=1;
#else
#ifdef _WIN32
__declspec(thread) unsigned threadActiveExtraIndex;
#else
__thread unsigned threadActiveExtraIndex;
#endif
#endif

unsigned queryCurrentTransformDepth()
{
    //only valid if called within a transform
    const TransformTrackingInfo * state = &transformExtraState[threadActiveExtraIndex];
    return state->curTransformDepth;
}

IInterface * CHqlExpression::queryTransformExtra() 
{ 
    const TransformTrackingInfo * state = &transformExtraState[threadActiveExtraIndex];
    const transformdepth_t curTransformDepth = state->curTransformDepth;
    const unsigned extraIndex = threadActiveExtraIndex-1;
#if NUM_PARALLEL_TRANSFORMS == 1
    assertex(extraIndex == 0);
#endif
    assertex(curTransformDepth);
    if (TRANSFORM_DEPTH(transformDepth[extraIndex]) == curTransformDepth)
        return transformExtra[extraIndex]; 
    return NULL;
}

void CHqlExpression::resetTransformExtra(IInterface * x, unsigned depth)
{ 
    const unsigned extraIndex = threadActiveExtraIndex-1;
#if NUM_PARALLEL_TRANSFORMS == 1
    assertex(extraIndex == 0);
#endif
    RELEASE_TRANSFORM_EXTRA(transformDepth[extraIndex], transformExtra[extraIndex]);
    transformDepth[extraIndex] = depth;
    transformExtra[extraIndex] = x;
}

//The following is extacted from the function definition below.  Using a macro because I'm not convinced how well it gets inlined.
#define DO_SET_TRANSFORM_EXTRA(x, depthMask)                                                                                    \
{                                                                                                                               \
    TransformTrackingInfo * state = &transformExtraState[threadActiveExtraIndex];                                               \
    const transformdepth_t curTransformDepth = state->curTransformDepth;                                                        \
    const unsigned extraIndex = threadActiveExtraIndex-1;                                                                       \
    assertex(curTransformDepth);                                                                                                \
    const transformdepth_t exprDepth = transformDepth[extraIndex];                                                              \
    if (TRANSFORM_DEPTH(exprDepth) == curTransformDepth)                                                                        \
    {                                                                                                                           \
        RELEASE_TRANSFORM_EXTRA(exprDepth, transformExtra[extraIndex]);                                                         \
    }                                                                                                                           \
    else                                                                                                                        \
    {                                                                                                                           \
        unsigned saveDepth = exprDepth;                                                                                         \
        if (exprDepth)                                                                                                          \
        {                                                                                                                       \
            IInterface * saveValue = transformExtra[extraIndex];                                                                \
            if (saveValue == this)                                                                                              \
                saveDepth |= TRANSFORM_DEPTH_SAVE_MATCH_EXPR;                                                                   \
            else                                                                                                                \
                state->transformStack.append(saveValue);                                                                        \
        }                                                                                                                       \
        state->transformStack.append(LINK(this));                                                                               \
        if (curTransformDepth > 1)                                                                                              \
            state->depthStack.append(saveDepth);                                                                                \
    }                                                                                                                           \
                                                                                                                                \
    const transformdepth_t newDepth = (curTransformDepth | depthMask);                                                          \
    if (exprDepth != newDepth)                                                                                                  \
        transformDepth[extraIndex] = newDepth;                                                                                  \
    transformExtra[extraIndex] = x;                                                                                             \
}


void CHqlExpression::doSetTransformExtra(IInterface * x, unsigned depthMask)
{ 
#ifdef GATHER_LINK_STATS
    numSetExtra++;
    if (x == this)
        numSetExtraSame++;
    if (depthMask & TRANSFORM_DEPTH_NOLINK)
        numSetExtraUnlinked++;
#endif
    TransformTrackingInfo * state = &transformExtraState[threadActiveExtraIndex];
    const transformdepth_t curTransformDepth = state->curTransformDepth;
    const unsigned extraIndex = threadActiveExtraIndex-1;
#if NUM_PARALLEL_TRANSFORMS == 1
    assertex(extraIndex == 0);
#endif
    assertex(curTransformDepth);
    const transformdepth_t exprDepth = transformDepth[extraIndex];
    if (TRANSFORM_DEPTH(exprDepth) == curTransformDepth)
    {
        RELEASE_TRANSFORM_EXTRA(exprDepth, transformExtra[extraIndex]);
    }
    else
    {
#ifdef GATHER_LINK_STATS
        if (curTransformDepth > 1)
            numNestedExtra++;
#endif
        unsigned saveDepth = exprDepth;
        if (exprDepth)
        {
            IInterface * saveValue = transformExtra[extraIndex];
            if (saveValue == this)
                saveDepth |= TRANSFORM_DEPTH_SAVE_MATCH_EXPR;
            else
                state->transformStack.append(saveValue);
        }
        state->transformStack.append(LINK(this));               // if expr was created inside a transformer, then it may disappear before unlock is called()
        if (curTransformDepth > 1)                          // don't save depth if it can only be 0
            state->depthStack.append(saveDepth);
    }

    const transformdepth_t newDepth = (curTransformDepth | depthMask);
    if (exprDepth != newDepth)
        transformDepth[extraIndex] = newDepth;
    transformExtra[extraIndex] = x;
}


void CHqlExpression::setTransformExtra(IInterface * x)
{ 
    unsigned depthMask;
    if (x != this)
    {
        ::Link(x);
        depthMask = 0;
    }
    else
        depthMask = TRANSFORM_DEPTH_NOLINK;
    DO_SET_TRANSFORM_EXTRA(x, depthMask);
}


void CHqlExpression::setTransformExtraOwned(IInterface * x)
{ 
    DO_SET_TRANSFORM_EXTRA(x, 0);
}


void CHqlExpression::setTransformExtraUnlinked(IInterface * x)
{ 
    DO_SET_TRANSFORM_EXTRA(x, TRANSFORM_DEPTH_NOLINK);
}


void TransformTrackingInfo::lock()
{
#ifdef GATHER_LINK_STATS
    numLocks++;
    if (curTransformDepth)
        numNestedLocks++;
    if (curTransformDepth>=maxNestedLocks)
        maxNestedLocks = curTransformDepth+1;
#endif
    curTransformDepth++;
    assertex((curTransformDepth & TRANSFORM_DEPTH_MASK) != 0);          // 
    transformStackMark.append(transformStack.ordinality());
}

void TransformTrackingInfo::unlock()
{
    unsigned transformStackLevel = transformStackMark.pop();
    while (transformStack.ordinality() > transformStackLevel)
    {
        CHqlExpression * expr = (CHqlExpression *)transformStack.pop();
        unsigned oldDepth = 0;
        IInterface * extra = NULL;
        if (curTransformDepth > 1)
        {
            oldDepth = depthStack.pop();
            if (oldDepth & TRANSFORM_DEPTH_SAVE_MATCH_EXPR)
                extra = expr;
            else if (oldDepth)
                extra = (IInterface *)transformStack.pop();
        }
        expr->resetTransformExtra(extra, oldDepth);
        expr->Release();
    }
    curTransformDepth--;
}

static void ensureThreadExtraIndex()
{
    {
        CriticalBlock block(*transformCS);
        if (threadActiveExtraIndex != 0)
            return;
    }
    transformSemaphore->wait();
    {
        CriticalBlock block(*transformCS);
        assertex(threadActiveExtraIndex == 0);      // something seriously wrong...
        for (unsigned i=1; i <= NUM_PARALLEL_TRANSFORMS; i++)
        {
            if (!isActiveMask[i])
            {
                threadActiveExtraIndex = i;
                isActiveMask[i] = true;
                return;
            }
        }
        throwUnexpected();
    }
}

static void releaseExtraIndex()
{
    CriticalBlock block(*transformCS);
    isActiveMask[threadActiveExtraIndex] = false;
    threadActiveExtraIndex = 0;
    transformSemaphore->signal();
}

extern HQL_API void lockTransformMutex()
{
#if NUM_PARALLEL_TRANSFORMS==1
    assertex(transformMutex);
    transformMutex->lock();
#else
    assertex(transformCS);
    ensureThreadExtraIndex();
#endif
    transformExtraState[threadActiveExtraIndex].lock();
}

extern HQL_API void unlockTransformMutex()
{
    assertex(threadActiveExtraIndex);
    TransformTrackingInfo * state = &transformExtraState[threadActiveExtraIndex];
    state->unlock();
#if NUM_PARALLEL_TRANSFORMS==1
    transformMutex->unlock();
#else
    if (state->curTransformDepth == 0)
        releaseExtraIndex();
#endif
}

//==============================================================================================================

unsigned getExpressionCRC(IHqlExpression * expr)
{
    CriticalBlock procedure(*crcCS);
    return expr->getCachedEclCRC();
}

// ================ improve error message ======================

HQL_API StringBuffer& getFriendlyTypeStr(IHqlExpression* e, StringBuffer& s)
{
    assertex(e);
    ITypeInfo *type = e->queryType();

    return getFriendlyTypeStr(type,s);
}

HQL_API StringBuffer& getFriendlyTypeStr(ITypeInfo* type, StringBuffer& s)
{
    if (!type)
        return s.append("<unknown>");
    
    switch(type->getTypeCode())
    {
    case type_int:
        s.append("Integer");
        break;
    case type_real:
        s.append("Real");
        break;
    case type_boolean:
        s.append("Boolean");
        break;
    case type_string:
        s.append("String");
        break;
    case type_data:
        s.append("Data");
        break;
    
    case type_set:
        {
            s.append("Set of ");
            ITypeInfo* chdType = type->queryChildType();
            getFriendlyTypeStr(chdType, s);
            break;
        }

    case type_groupedtable:
        {
            s.append("Grouped ");
            ITypeInfo* chdType = type->queryChildType();
            if (chdType)
                getFriendlyTypeStr(chdType, s);
            else
                s.append("Table");
            break;
        }
    case type_table:
        {
            s.append("Table of ");
            ITypeInfo * chdType = queryRecordType(type);
            if (chdType)
            {
                if (!queryOriginalName(chdType, s))
                    chdType->getECLType(s);
            }
            else
                s.append("<unknown>");
            break;
        }
    case type_dictionary:
        {
            s.append(type->queryTypeName()).append(" of ");
            ITypeInfo * chdType = queryRecordType(type);
            if (chdType)
            {
                if (!queryOriginalName(chdType, s))
                    chdType->getECLType(s);
            }
            else
                s.append("<unknown>");
            break;
        }
    case type_record:
        s.append("Record ");
        if (!queryOriginalName(type, s))
            type->getECLType(s);
        break;

    case type_transform:
        s.append("Transform ");
        if (!queryOriginalName(type, s))
            type->getECLType(s);
        break;

    case type_row:
        {
            s.append("row");
            ITypeInfo * childType = type->queryChildType();
            if (childType)
            {
                s.append(" of ");
                if (!queryOriginalName(type, s) && !queryOriginalName(childType, s))
                    childType->getECLType(s);
            }
            break;
        }

    case type_void:
        s.append("<void>");
        break;

    default:
        type->getECLType(s);
    }

    return s;
}


//==============================================================================================================

static HqlTransformerInfo virtualAttributeRemoverInfo("VirtualAttributeRemover");
class VirtualAttributeRemover : public QuickHqlTransformer
{
public:
    VirtualAttributeRemover() : QuickHqlTransformer(virtualAttributeRemoverInfo, NULL) {}

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        case no_field:
            {
                if (expr->hasAttribute(virtualAtom))
                {
                    OwnedHqlExpr cleaned = removeAttribute(expr, virtualAtom);
                    return QuickHqlTransformer::createTransformed(cleaned);
                }
                break;
            }
        }
        return QuickHqlTransformer::createTransformed(expr);
    }
};

static bool removeVirtualAttributes(HqlExprArray & fields, IHqlExpression * cur, HqlMapTransformer & transformer)
{
    switch (cur->getOperator())
    {
    case no_field:
        {
            ITypeInfo * type = cur->queryType();
            Linked<ITypeInfo> targetType = type;
            switch (type->getTypeCode())
            {
            case type_groupedtable:
            case type_table:
            case type_row:
                {
                    IHqlExpression * fieldRecord = cur->queryRecord();
                    HqlExprArray subFields;
                    if (removeVirtualAttributes(subFields, fieldRecord, transformer))
                    {
                        OwnedHqlExpr newRecord = fieldRecord->clone(subFields);

                        switch (type->getTypeCode())
                        {
                        case type_groupedtable:
                            targetType.setown(makeGroupedTableType(makeTableType(makeRowType(newRecord->getType()))));
                            break;
                        case type_table:
                            targetType.setown(makeTableType(makeRowType(newRecord->getType())));
                            break;
                        case type_row:
                            targetType.set(makeRowType(newRecord->queryType()));
                            break;
                        case type_record:
                            throwUnexpected();
                            break;
                        }
                    }
                    break;
                }
            }

            LinkedHqlExpr newField = cur;
            if ((type != targetType) || cur->hasAttribute(virtualAtom))
            {
                HqlExprArray args;
                unwindChildren(args, cur);
                removeAttribute(args, virtualAtom);
                newField.setown(createField(cur->queryId(), targetType.getLink(), args));
            }
            fields.append(*LINK(newField));
            transformer.setMapping(cur, newField);
            return (newField != cur);
        }
    case no_ifblock:
        {
            HqlExprArray subfields;
            IHqlExpression * condition = cur->queryChild(0);
            OwnedHqlExpr mappedCondition = transformer.transformRoot(condition);
            if (removeVirtualAttributes(subfields, cur->queryChild(1), transformer) || (condition != mappedCondition))
            {
                fields.append(*createValue(no_ifblock, makeNullType(), LINK(mappedCondition), createRecord(subfields)));
                return true;
            }
            fields.append(*LINK(cur));
            return false;
        }
    case no_record:
        {
            bool changed = false;
            ForEachChild(idx, cur)
                if (removeVirtualAttributes(fields, cur->queryChild(idx), transformer))
                    changed = true;
            return changed;
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        fields.append(*LINK(cur));
        return false;
    }
    UNIMPLEMENTED;
}

IHqlExpression * removeVirtualAttributes(IHqlExpression * record)
{
    assertex(record->getOperator() == no_record);
    VirtualAttributeRemover transformer;
    return transformer.transform(record);
}

IHqlExpression * extractChildren(IHqlExpression * value)
{
    HqlExprArray children;
    unwindChildren(children, value);
    return createComma(children);
}

IHqlExpression * queryDatasetCursor(IHqlExpression * ds)
{
    while ((ds->getOperator() == no_select) && !ds->isDataset() && !ds->isDictionary())
        ds = ds->queryChild(0);
    return ds;
}

IHqlExpression * querySelectorDataset(IHqlExpression * expr, bool & isNew)
{
    assertex(expr->getOperator() == no_select);
    isNew = expr->hasAttribute(newAtom);
    IHqlExpression * ds = expr->queryChild(0);
    while ((ds->getOperator() == no_select) && !ds->isDataset() && !ds->isDictionary())
    {
        if (ds->hasAttribute(newAtom))
            isNew = true;
        assertex(ds->isDatarow());
        ds = ds->queryChild(0);
    }
    return ds;
}

bool isNewSelector(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_select);
    if (expr->hasAttribute(newAtom))
        return true;
    IHqlExpression * ds = expr->queryChild(0);
    while ((ds->getOperator() == no_select) && !ds->isDataset() && !ds->isDictionary())
    {
        if (ds->hasAttribute(newAtom))
            return true;
        assertex(ds->isDatarow());
        ds = ds->queryChild(0);
    }
    return false;
}

bool isTargetSelector(IHqlExpression * expr)
{
    while (expr->getOperator() == no_select)
        expr = expr->queryChild(0);
    return (expr->getOperator() == no_self);
}


IHqlExpression * replaceSelectorDataset(IHqlExpression * expr, IHqlExpression * newDataset)
{
    assertex(expr->getOperator() == no_select);
    IHqlExpression * ds = expr->queryChild(0);
    if ((ds->getOperator() == no_select) && !ds->isDataset() && !ds->isDictionary())
    {
        OwnedHqlExpr newSelector = replaceSelectorDataset(ds, newDataset);
        return replaceChild(expr, 0, newSelector);
    }
    else
        return replaceChild(expr, 0, newDataset);
}

IHqlExpression * querySkipDatasetMeta(IHqlExpression * dataset)
{
    loop
    {
        switch (dataset->getOperator())
        {
        case no_sorted:
        case no_grouped:
        case no_distributed:
        case no_preservemeta:
        case no_stepped:
            break;
        default:
            return dataset;
        }
        dataset = dataset->queryChild(0);
    }
}

//==============================================================================================================

static ITypeInfo * doGetSimplifiedType(ITypeInfo * type, bool isConditional, bool isSerialized, IAtom * serialForm)
{
    ITypeInfo * promoted = type->queryPromotedType();
    switch (type->getTypeCode())
    {
    case type_boolean:
    case type_int:
    case type_real:
    case type_decimal:
    case type_swapint:
    case type_packedint:
        return LINK(type);
    case type_data:
    case type_string:
    case type_varstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
    case type_qstring:
        if (isConditional)
            return getStretchedType(UNKNOWN_LENGTH, type);
        return LINK(promoted);
    case type_date:
    case type_enumerated:
        return makeIntType(promoted->getSize(), false);
    case type_bitfield:
        return LINK(promoted);
    case type_alien:
        return getSimplifiedType(promoted, isConditional, isSerialized, serialForm);
    case type_row:
    case type_dictionary:
    case type_table:
    case type_groupedtable:
    case type_transform:
        if (isSerialized)
            return getSerializedForm(type, serialForm);
        //should possibly remove some weird options
        return LINK(type);
    case type_set:
        {
            ITypeInfo * childType = type->queryChildType();
            Owned<ITypeInfo> simpleChildType = getSimplifiedType(childType, false, isSerialized, serialForm);
            return makeSetType(simpleChildType.getClear());
        }
    }
    UNIMPLEMENTED;  //i.e. doesn't make any sense....
}

ITypeInfo * getSimplifiedType(ITypeInfo * type, bool isConditional, bool isSerialized, IAtom * serialForm)
{
    Owned<ITypeInfo> newType = doGetSimplifiedType(type, isConditional, isSerialized, serialForm);
    if (isSameBasicType(newType, type))
        return LINK(type);

    newType.setown(cloneModifiers(type, newType));
    //add a maxlength qualifier, and preserve any maxlength qualifiers in the source.
    return newType.getClear();
}


static void simplifyFileViewRecordTypes(HqlExprArray & fields, IHqlExpression * cur, bool isConditional, bool & needsTransform, bool isKey, unsigned & count)
{
    switch (cur->getOperator())
    {
    case no_field:
        {
            bool forceSimplify = false;
            if (isKey)
            {
                if (cur->hasAttribute(blobAtom))
                    forceSimplify = true;
            }
            else
            {
                //MORE: Really not so sure about this...
                if (cur->hasAttribute(virtualAtom))
                    return;
            }

            ITypeInfo * type = cur->queryType();
            Owned<ITypeInfo> targetType;
            HqlExprArray attrs;
            switch (type->getTypeCode())
            {
            case type_groupedtable:
                throwError(HQLERR_NoBrowseGroupChild);
            case type_table:
                {
                    bool childNeedsTransform = false;
                    IHqlExpression * fieldRecord = cur->queryRecord();
                    HqlExprArray subFields;
                    simplifyFileViewRecordTypes(subFields, fieldRecord, isConditional, childNeedsTransform, isKey, count);
                    if (childNeedsTransform || hasLinkCountedModifier(type))
                    {
                        OwnedHqlExpr newRecord = fieldRecord->clone(subFields);
                        targetType.setown(makeTableType(makeRowType(newRecord->getType())));
                    }
                    else
                        targetType.set(type);

                    IHqlExpression * maxCountAttr = cur->queryAttribute(maxCountAtom);
                    IHqlExpression * countAttr = cur->queryAttribute(countAtom);
                    if (countAttr || cur->hasAttribute(sizeofAtom))
                        forceSimplify = true;

                    if (maxCountAttr)
                        attrs.append(*LINK(maxCountAttr));
                    if (countAttr && !maxCountAttr && countAttr->queryChild(0)->queryValue())
                        attrs.append(*createAttribute(maxCountAtom, LINK(countAttr->queryChild(0))));
                    break;
                }
            case type_set:
                targetType.setown(getSimplifiedType(type, false, true, diskAtom));
                break;
            case type_row:
                {
                    bool childNeedsTransform = false;
                    IHqlExpression * fieldRecord = cur->queryRecord();
                    HqlExprArray subFields;
                    simplifyFileViewRecordTypes(subFields, fieldRecord, isConditional, childNeedsTransform, isKey, count);
                    if (childNeedsTransform)
                    {
                        OwnedHqlExpr newRecord = fieldRecord->clone(subFields);
                        targetType.setown(replaceChildType(type, newRecord->queryType()));
                    }
                    else
                        targetType.set(type);
                    break;
                }
                return;
            default:
                targetType.setown(getSimplifiedType(type, isConditional, true, diskAtom));
                break;
            }

            LinkedHqlExpr newField = cur;
            if (forceSimplify || type != targetType)
            {
                needsTransform = true;
                //MORE xmldefault, default
                inheritAttribute(attrs, cur, xpathAtom);
                inheritAttribute(attrs, cur, xmlDefaultAtom);
                inheritAttribute(attrs, cur, defaultAtom);
                newField.setown(createField(cur->queryId(), targetType.getLink(), attrs));
            }
            fields.append(*LINK(newField));
            break;
        }
    case no_ifblock:
        {
            //Remove them for the moment...
            //Gets complicated if they are kept, because the test condition will need testing
            needsTransform = true;
            HqlExprArray subfields;
            StringBuffer name;
            name.append("unnamed").append(++count);
            subfields.append(*createField(createIdAtom(name), makeBoolType(), NULL, createAttribute(__ifblockAtom)));
            simplifyFileViewRecordTypes(subfields, cur->queryChild(1), true, needsTransform, isKey, count);
//          fields.append(*createValue(no_ifblock, makeVoidType(), createValue(no_not, makeBoolType(), createConstant(false)), createRecord(subfields)));
            fields.append(*createValue(no_ifblock, makeNullType(), createConstant(true), createRecord(subfields)));
            break;
        }
    case no_record:
        {
            ForEachChild(idx, cur)
            {
                IHqlExpression * next = cur->queryChild(idx);
                if (next->getOperator() == no_record)
                {
                    HqlExprArray subfields;
                    simplifyFileViewRecordTypes(subfields, next, isConditional, needsTransform, isKey, count);
                    fields.append(*cloneOrLink(cur, subfields));
                }
                else
                    simplifyFileViewRecordTypes(fields, next, isConditional, needsTransform, isKey, count);
            }
            break;
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        fields.append(*LINK(cur));
        break;
    }
}

static IHqlExpression * simplifyFileViewRecordTypes(IHqlExpression * record, bool & needsTransform, bool isKey)
{
    assertex(record->getOperator() == no_record);
    HqlExprArray fields;
    unsigned count = 0;
    simplifyFileViewRecordTypes(fields, record, false, needsTransform, isKey, count);
    return cloneOrLink(record, fields);
}

extern HQL_API IHqlExpression * getFileViewerRecord(IHqlExpression * record, bool isKey)
{
    bool needsTransform = false;
    try
    {
        OwnedHqlExpr newRecord = simplifyFileViewRecordTypes(record, needsTransform, isKey);
        if (needsTransform)
            return newRecord.getClear();
        return NULL;
    }
    catch (IException * e)
    {
        LOG(MCwarning, unknownJob, e);
        e->Release();
    }
    return NULL;
}


static void getSimplifiedAssigns(HqlExprArray & assigns, IHqlExpression * tgt, IHqlExpression * src, IHqlExpression * targetSelector, IHqlExpression * sourceSelector, unsigned targetDelta)
{
    switch (src->getOperator())
    {
    case no_field:
        {
            //MORE: Really not so sure about this...
            if (src->hasAttribute(virtualAtom) || src->hasAttribute(__ifblockAtom))
                return;
            OwnedHqlExpr srcSelect = sourceSelector ? createSelectExpr(LINK(sourceSelector), LINK(src)) : LINK(src);
            OwnedHqlExpr tgtSelect = createSelectExpr(LINK(targetSelector), LINK(tgt));

            ITypeInfo * type = src->queryType();
            type_t tc = type->getTypeCode();
            if (tc == type_row)
            {
                IHqlExpression * srcRecord = src->queryRecord();
                if (false && isSimplifiedRecord(srcRecord, false))
                    assigns.append(*createAssign(LINK(tgtSelect), LINK(srcSelect)));
                else
                {
                    OwnedHqlExpr seq = createSelectorSequence();
                    OwnedHqlExpr leftChildSelector = createSelector(no_left, srcSelect, seq);
                    OwnedHqlExpr tform = getSimplifiedTransform(tgt->queryRecord(), src->queryRecord(), leftChildSelector);
                    OwnedHqlExpr pj = createRow(no_projectrow, LINK(srcSelect), createComma(tform.getClear(), LINK(seq)));
                    assigns.append(*createAssign(LINK(tgtSelect), LINK(pj)));
                }
            }
            else if (((tc == type_table) || (tc == type_groupedtable)) && (srcSelect->queryType() != tgtSelect->queryType()))
            {
                //self.target := project(srcSelect, transform(...));
                IHqlExpression * srcRecord = src->queryRecord();
                OwnedHqlExpr seq = createSelectorSequence();
                OwnedHqlExpr srcSelector = createSelector(no_left, srcSelect, seq);
                OwnedHqlExpr transform = getSimplifiedTransform(tgt->queryRecord(), srcRecord, srcSelector);
                OwnedHqlExpr project = createDataset(no_hqlproject, LINK(srcSelect), createComma(LINK(transform), LINK(seq)));
                assigns.append(*createAssign(LINK(tgtSelect), LINK(project)));
            }
            else
                assigns.append(*createAssign(LINK(tgtSelect), LINK(srcSelect)));
            break;
        }
    case no_ifblock:
        {
            IHqlExpression * specialField = tgt->queryChild(1)->queryChild(0);
            OwnedHqlExpr tgtSelect = createSelectExpr(LINK(targetSelector), LINK(tgt));
            IHqlExpression * self = querySelfReference();
            OwnedHqlExpr cond = replaceSelector(src->queryChild(0), self, sourceSelector);
            assigns.append(*createAssign(createSelectExpr(LINK(targetSelector), LINK(specialField)), cond.getClear()));
            getSimplifiedAssigns(assigns, tgt->queryChild(1), src->queryChild(1), targetSelector, sourceSelector, 1);
            break;
        }
    case no_record:
        {
            ForEachChild(idx, src)
                getSimplifiedAssigns(assigns, tgt->queryChild(idx+targetDelta), src->queryChild(idx), targetSelector, sourceSelector, 0);
            break;
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        break;
    default:
        UNIMPLEMENTED;
    }
}

extern HQL_API IHqlExpression * getSimplifiedTransform(IHqlExpression * tgt, IHqlExpression * src, IHqlExpression * sourceSelector)
{
    HqlExprArray assigns;
    OwnedHqlExpr self = getSelf(tgt);
    getSimplifiedAssigns(assigns, tgt, src, self, sourceSelector, 0);
    return createValue(no_newtransform, makeTransformType(tgt->getType()), assigns);
}


extern HQL_API bool isSimplifiedRecord(IHqlExpression * expr, bool isKey)
{
    OwnedHqlExpr simplified = getFileViewerRecord(expr, isKey);
    return !simplified || (expr == simplified);
}


void unwindChildren(HqlExprArray & children, IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    children.ensure(max);
    for (unsigned idx=0; idx < max; idx++)
    {
        IHqlExpression * child = expr->queryChild(idx);
        children.append(*LINK(child));
    }
}


void unwindChildren(HqlExprArray & children, const IHqlExpression * expr, unsigned first)
{
    unsigned max = expr->numChildren();
    children.ensure(max-first);
    for (unsigned idx=first; idx < max; idx++)
    {
        IHqlExpression * child = expr->queryChild(idx);
        children.append(*LINK(child));
    }
}


void unwindChildren(HqlExprArray & children, const IHqlExpression * expr, unsigned first, unsigned max)
{
    children.ensure(max-first);
    for (unsigned idx=first; idx < max; idx++)
    {
        IHqlExpression * child = expr->queryChild(idx);
        children.append(*LINK(child));
    }
}


void unwindChildren(HqlExprCopyArray & children, const IHqlExpression * expr, unsigned first)
{
    unsigned max = expr->numChildren();
    children.ensure(max-first);
    for (unsigned idx=first; idx < max; idx++)
    {
        IHqlExpression * child = expr->queryChild(idx);
        children.append(*child);
    }
}


void unwindRealChildren(HqlExprArray & children, const IHqlExpression * expr, unsigned first)
{
    unsigned max = expr->numChildren();
    children.ensure(max-first);
    for (unsigned idx=first; idx < max; idx++)
    {
        IHqlExpression * child = expr->queryChild(idx);
        if (!child->isAttribute())
            children.append(*LINK(child));
    }
}


void unwindAttributes(HqlExprArray & children, const IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    for (unsigned idx=0; idx < max; idx++)
    {
        IHqlExpression * child = expr->queryChild(idx);
        if (child->isAttribute())
        {
            //Subsequent calls to ensure are quick no-ops
            children.ensure(max - idx);
            children.append(*LINK(child));
        }
    }
}


void unwindList(HqlExprArray &dst, IHqlExpression * expr, node_operator op)
{
    while (expr->getOperator() == op)
    {
        unsigned _max = expr->numChildren();
        if (_max == 0)
            return;
        unsigned max = _max-1;
        for (unsigned i=0; i < max; i++)
            unwindList(dst, expr->queryChild(i), op);
        expr = expr->queryChild(max);
    }
    dst.append(*LINK(expr));
}


void unwindCopyList(HqlExprCopyArray &dst, IHqlExpression * expr, node_operator op)
{
    while (expr->getOperator() == op)
    {
        unsigned _max = expr->numChildren();
        if (_max == 0)
            return;
        unsigned max = _max-1;
        for (unsigned i=0; i < max; i++)
            unwindCopyList(dst, expr->queryChild(i), op);
        expr = expr->queryChild(max);
    }
    dst.append(*expr);
}


void unwindCommaCompound(HqlExprArray & target, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_compound:
    case no_comma:
    case no_actionlist:
        {
            ForEachChild(idx, expr)
                unwindCommaCompound(target, expr->queryChild(idx));
            break;
        }
    default:
        target.append(*LINK(expr));
        break;
    }
}


void unwindRecordAsSelects(HqlExprArray & children, IHqlExpression * record, IHqlExpression * ds, unsigned limit)
{
    ForEachChild(idx, record)
    {
        if (idx == limit)
            return;

        IHqlExpression * cur = record->queryChild(idx);
        switch (cur->getOperator())
        {
        case no_field:
            {
                OwnedHqlExpr selected = createSelectExpr(LINK(ds), LINK(cur));
                IHqlExpression * fieldRecord = cur->queryRecord();
                if (fieldRecord && !cur->isDataset())
                    unwindRecordAsSelects(children, fieldRecord, selected);
                else
                    children.append(*selected.getClear());
                break;
            }
        case no_ifblock:
            unwindRecordAsSelects(children, cur->queryChild(1), ds);
            break;
        case no_record:
            unwindRecordAsSelects(children, cur, ds);
            break;
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            break;
        default:
            UNIMPLEMENTED;
        }
    }
}


void unwindAttribute(HqlExprArray & args, IHqlExpression * expr, IAtom * name)
{
    IHqlExpression * attr = expr->queryAttribute(name);
    if (attr)
        args.append(*LINK(attr));
}


unsigned unwoundCount(IHqlExpression * expr, node_operator op)
{
    if (!expr)
        return 0;

    unsigned count = 0;
    // comma almost always needs head recursion
    loop
    {
        if (expr->getOperator() != op)
            return count+1;
        ForEachChildFrom(idx, expr, 1)
            count += unwoundCount(expr->queryChild(idx), op);
        expr = expr->queryChild(0);
    }
}


IHqlExpression * queryChildOperator(node_operator op, IHqlExpression * expr)
{
    ForEachChild(idx, expr)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        if (cur->getOperator() == op)
            return cur;
    }
    return NULL;
}

bool isKeyedJoin(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    if ((op == no_join) || (op == no_joincount) || (op == no_denormalize) || (op == no_denormalizegroup))
    {
        if (expr->hasAttribute(allAtom) || expr->hasAttribute(lookupAtom) || expr->hasAttribute(smartAtom))
            return false;
        if (expr->hasAttribute(keyedAtom) || containsAssertKeyed(expr->queryChild(2)))
            return true;
        //Keyed joins only support INNER/LEFT.  Default to a normal join for other join types.
        if (!isInnerJoin(expr) && !isLeftJoin(expr))
            return false;
        if (isKey(expr->queryChild(1)))
            return true;
    }
    return false;
}

static bool cachedJoinSortOrdersMatch(IHqlExpression * left, IHqlExpression * right);
static bool doJoinSortOrdersMatch(IHqlExpression * left, IHqlExpression * right)
{
    node_operator leftOp = left->getOperator();
    node_operator rightOp = right->getOperator();
    if (leftOp != rightOp)
    {
        if (((leftOp == no_left) && (rightOp == no_right)) || ((leftOp == no_right) && (rightOp == no_left)))
            return true;
        return false;
    }
    if (left == right)
        return true;
    if (leftOp == no_field)
        return false;

    unsigned numLeft = left->numChildren();
    unsigned numRight = right->numChildren();
    if (numLeft != numRight)
        return false;
    if ((numLeft == 0) && (left != right))
        return false;
    for (unsigned idx=0; idx<numLeft; idx++)
        if (!cachedJoinSortOrdersMatch(left->queryChild(idx), right->queryChild(idx)))
            return false;
    return true;
}

static bool cachedJoinSortOrdersMatch(IHqlExpression * left, IHqlExpression * right)
{
    IHqlExpression * matched = static_cast<IHqlExpression *>(left->queryTransformExtra());
    if (matched)
        return matched == right;
    bool ret = doJoinSortOrdersMatch(left, right);
    if (ret)
        left->setTransformExtra(right);
    return ret;
}

static bool joinSortOrdersMatch(IHqlExpression * left, IHqlExpression * right)
{
    TransformMutexBlock block;
    return cachedJoinSortOrdersMatch(left, right);
}

static bool joinSortOrdersMatch(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_and:
        return joinSortOrdersMatch(expr->queryChild(0)) && joinSortOrdersMatch(expr->queryChild(1));
    case no_eq:
        return joinSortOrdersMatch(expr->queryChild(0), expr->queryChild(1));
    }
    return true;
}

bool isSelfJoin(IHqlExpression * expr)
{
    if (expr->getOperator() != no_join)
        return false;

    IHqlExpression * datasetL = expr->queryChild(0);
    IHqlExpression * datasetR = expr->queryChild(1);
    if (datasetL->queryBody() != datasetR->queryBody())
        return false;

    if (expr->hasAttribute(allAtom) || expr->hasAttribute(lookupAtom) || expr->hasAttribute(smartAtom))
        return false;

    if (expr->queryChild(2)->isConstant())
        return false;

    if (!joinSortOrdersMatch(expr->queryChild(2)))
        return false;

    //Check this isn't going to generate a between join - if it is that takes precedence.  A bit arbitrary
    //when one is more efficient.
    JoinSortInfo joinInfo;
    joinInfo.findJoinSortOrders(expr, true);
    if ((joinInfo.slidingMatches.ordinality() != 0) && (joinInfo.queryLeftReq().ordinality() == joinInfo.slidingMatches.ordinality()))
        return false;
    return true;
}

IHqlExpression * queryJoinRhs(IHqlExpression * expr)
{
    if (expr->getOperator() == no_selfjoin)
        return expr->queryChild(0);
    return expr->queryChild(1);
}

bool isInnerJoin(IHqlExpression * expr)
{
    return queryJoinKind(expr) == innerAtom;
}


IAtom * queryJoinKind(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    for (unsigned i=4; i < max; i++)
    {
        IHqlExpression * cur = expr->queryChild(i);
        switch (cur->getOperator())
        {
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            {
                IAtom * name = cur->queryName();
                //Only allow inner joins
                if ((name == innerAtom) || (name == leftouterAtom) || (name == rightouterAtom) ||
                    (name == fullouterAtom) || (name == leftonlyAtom) ||
                    (name == rightonlyAtom) || (name == fullonlyAtom))
                    return name;
                break;
            }
        }
    }
    return innerAtom;
}


bool isFullJoin(IHqlExpression * expr)
{
    return expr->hasAttribute(fullouterAtom) || expr->hasAttribute(fullonlyAtom);
}


bool isLeftJoin(IHqlExpression * expr)
{
    return expr->hasAttribute(leftouterAtom) || expr->hasAttribute(leftonlyAtom);
}


bool isRightJoin(IHqlExpression * expr)
{
    return expr->hasAttribute(rightouterAtom) || expr->hasAttribute(rightonlyAtom);
}


bool isSimpleInnerJoin(IHqlExpression * expr)
{
    return isInnerJoin(expr) && !isLimitedJoin(expr);
}


bool isLimitedJoin(IHqlExpression * expr)
{
    return expr->hasAttribute(keepAtom) || expr->hasAttribute(rowLimitAtom) || expr->hasAttribute(atmostAtom);
}

bool filterIsKeyed(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    for (unsigned i=1; i < max; i++)
        if (containsAssertKeyed(expr->queryChild(i)))
            return true;
    return false;
}


bool filterIsUnkeyed(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    for (unsigned i=1; i < max; i++)
        if (!containsAssertKeyed(expr->queryChild(i)))
            return true;
    return false;
}


bool canEvaluateGlobally(IHqlExpression * expr)
{
    if (isContextDependent(expr))
        return false;
    if (!isIndependentOfScope(expr))
        return false;
    return true;
}



bool preservesValue(ITypeInfo * after, IHqlExpression * expr)
{
    ITypeInfo * before = expr->queryType();
    if (preservesValue(after, before))
        return true;

    //Special case casting a constant to a different length of the same type
    //e.g., string/integer.  It may preserve value in this special case.
    //don't allow int->string because it isn't symetric
    IValue * value = expr->queryValue();
    if (!value || (before->getTypeCode() != after->getTypeCode()))
        return false;

    OwnedIValue castValue = value->castTo(after);
    if (!castValue)
        return false;

    OwnedIValue recastValue = castValue->castTo(before);
    if (!recastValue)
        return false;

    return (recastValue->compare(value) == 0);
}

static const unsigned UNLIMITED_REPEAT = (unsigned)-1;

unsigned getRepeatMin(IHqlExpression * expr)
{
    IHqlExpression * minExpr = queryRealChild(expr, 1);
    if (minExpr)
        return (unsigned)minExpr->queryValue()->getIntValue();
    return 0;
}

unsigned getRepeatMax(IHqlExpression * expr)
{
    IHqlExpression * minExpr = queryRealChild(expr, 1);
    IHqlExpression * maxExpr = queryRealChild(expr, 2);
    if (minExpr && maxExpr)
    {
        if (maxExpr->getOperator() == no_any)
            return UNLIMITED_REPEAT;
        return (unsigned)maxExpr->queryValue()->getIntValue();
    }
    if (minExpr)
        return (unsigned)minExpr->queryValue()->getIntValue();
    return (unsigned)UNLIMITED_REPEAT;  // no limit...
}

bool isStandardRepeat(IHqlExpression * expr)
{
    if (expr->hasAttribute(minimalAtom)) return false;

    unsigned min = getRepeatMin(expr);
    unsigned max = getRepeatMax(expr);
    if ((min == 0) && (max == 1)) return true;
    if ((min == 0) && (max == UNLIMITED_REPEAT)) return true;
    if ((min == 1) && (max == UNLIMITED_REPEAT)) return true;
    return false;
}

IHqlExpression * queryOnlyField(IHqlExpression * record)
{
    IHqlExpression * ret = NULL;
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            if (ret)
                return NULL;
            ret = cur;
            break;
        }
    }
    return ret;
}


bool isPureActivity(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    for (unsigned i = getNumChildTables(expr); i < max; i++)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (!cur->isPure() || containsSkip(cur))
            return false;
    }
    return true;
}

bool isPureActivityIgnoringSkip(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    const unsigned mask = HEFimpure & ~(HEFcontainsSkip);
    for (unsigned i = getNumChildTables(expr); i < max; i++)
    {
        if (expr->queryChild(i)->getInfoFlags() & mask)
            return false;
    }
    return true;
}

bool assignsContainSkip(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_newtransform:
    case no_transform:
    case no_assignall:
        {
            ForEachChild(i, expr)
            {
                if (assignsContainSkip(expr->queryChild(i)))
                    return true;
            }
            return false;
        }
    case no_assign:
        return containsSkip(expr->queryChild(1));
    case no_alias_scope:
        return assignsContainSkip(expr->queryChild(0));
    default:
        return false;
    }
}

extern HQL_API bool isKnownTransform(IHqlExpression * transform)
{
    switch (transform->getOperator())
    {
    case no_transform:
    case no_newtransform:
        return true;
    case no_alias_scope:
        return isKnownTransform(transform->queryChild(0));
    }
    return false;
}

extern HQL_API bool hasUnknownTransform(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_aggregate:
        if (expr->hasAttribute(mergeTransformAtom))
            return true;
        break;
    case no_inlinetable:
        {
            IHqlExpression * transforms = expr->queryChild(0);
            ForEachChild(i, transforms)
                if (!isKnownTransform(transforms->queryChild(i)))
                    return true;
            return false;
        }
    }
    return !isKnownTransform(queryNewColumnProvider(expr));
}

bool isContextDependent(IHqlExpression * expr, bool ignoreFailures, bool ignoreGraph)
{ 
    unsigned flags = expr->getInfoFlags();
    unsigned mask = HEFcontextDependent;
    if (ignoreFailures)
        mask = HEFcontextDependentNoThrow;
    if (ignoreGraph)
        mask &= ~HEFgraphDependent;

    if ((flags & mask) == 0)
        return false;
    return true;
}


bool isPureCanSkip(IHqlExpression * expr)
{
    return (expr->getInfoFlags() & (HEFvolatile|HEFaction|HEFthrowscalar|HEFthrowds)) == 0; 
}

bool hasSideEffects(IHqlExpression * expr)
{
    return (expr->getInfoFlags() & (HEFthrowscalar|HEFthrowds)) != 0; 
}

bool isPureVirtual(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_purevirtual:
        return true;
    case no_funcdef:
        return isPureVirtual(expr->queryChild(0));
    default:
        return false;
    }
}

bool transformHasSkipAttr(IHqlExpression * transform)
{
    ForEachChild(i, transform)
    {
        IHqlExpression * cur = transform->queryChild(i);
        if (cur->getOperator() == no_skip)
            return true;
    }
    return false;
}


bool isPureInlineDataset(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_inlinetable);
    IHqlExpression * values = expr->queryChild(0);
    ForEachChild(i, values)
    {
        IHqlExpression * transform = values->queryChild(i);
        if (!transform->isPure() || containsSkip(transform))
            return false;
    }
    return true;
}


IHqlExpression * getActiveTableSelector()
{
    return LINK(cachedActiveTableExpr);
}

IHqlExpression * queryActiveTableSelector()
{
    return cachedActiveTableExpr;
}

IHqlExpression * getSelf(IHqlExpression * ds)
{
    return createSelector(no_self, ds, NULL);
}

IHqlExpression * querySelfReference()
{
    return cachedSelfReferenceExpr;
}

IHqlExpression * queryNullRecord()
{
    return cachedNullRecord;
}

IHqlExpression * queryNullRowRecord()
{
    return cachedNullRowRecord;
}

IHqlExpression * createNullDataset()
{
    return createDataset(no_null, LINK(queryNullRecord()));
}

IHqlExpression * createNullDictionary()
{
    return createDictionary(no_null, LINK(queryNullRecord()));
}

bool removeAttribute(HqlExprArray & args, IAtom * name)
{
    bool removed = false;
    ForEachItemInRev(idx, args)
    {
        IHqlExpression & cur = args.item(idx);
        if (cur.isAttribute() && (cur.queryName() == name))
        {
            args.remove(idx);
            removed = true;
        }
    }
    return removed;
}

void removeAttributes(HqlExprArray & args)
{
    ForEachItemInRev(idx, args)
    {
        IHqlExpression & cur = args.item(idx);
        if (cur.isAttribute())
            args.remove(idx);
    }
}

IHqlExpression * queryRecord(ITypeInfo * type)
{
    ITypeInfo * recordType = queryRecordType(type);
    if (recordType)
        return queryExpression(recordType);
    return NULL;
}

//NB: An ifblock is counted as a single payload field.
unsigned numPayloadFields(IHqlExpression * index)
{
    IHqlExpression * payloadAttr = index->queryAttribute(_payload_Atom);
    if (payloadAttr)
        return (unsigned)getIntValue(payloadAttr->queryChild(0));
    if (getBoolAttribute(index, filepositionAtom, true))
        return 1;
    return 0;
}

unsigned numKeyedFields(IHqlExpression * index)
{
    return getFlatFieldCount(index->queryRecord())-numPayloadFields(index);
}

unsigned firstPayloadField(IHqlExpression * index)
{
    return firstPayloadField(index->queryRecord(), numPayloadFields(index));
}

unsigned firstPayloadField(IHqlExpression * record, unsigned cnt)
{
    unsigned max = record->numChildren();
    if (cnt == 0)
        return max;
    while (max--)
    {
        IHqlExpression * cur = record->queryChild(max);
        switch (cur->getOperator())
        {
        case no_field:
        case no_record:
        case no_ifblock:
            if (--cnt == 0)
                return max;
            break;
        }
    }
    return 0;
}



IHqlExpression * createSelector(node_operator op, IHqlExpression * ds, IHqlExpression * seq)
{
//  OwnedHqlExpr record = getUnadornedExpr(ds->queryRecord());
    IHqlExpression * dsRecord = ds->queryRecord();
    assertex(dsRecord);
    IHqlExpression * record = dsRecord->queryBody();

    switch (op)
    {
    case no_left:
    case no_right:
    case no_top:
        assertex(seq && seq->isAttribute());
        return createRow(op, LINK(record), LINK(seq));
    case no_self:
        {
            //seq is set when generating code unique target selectors
            return createRow(op, LINK(record), LINK(seq));
        }
    case no_none:
        return LINK(ds);
    case no_activetable:
        return LINK(cachedActiveTableExpr);
    default:
        return createValue(op);
    }
}

static UniqueSequenceCounter uidSequence;
IHqlExpression * createUniqueId()
{
    unsigned __int64 uid = uidSequence.next();
    return createSequence(no_attr, NULL, _uid_Atom, uid);
}

static UniqueSequenceCounter counterSequence;
IHqlExpression * createCounter()
{
    return createSequence(no_counter, makeIntType(8, false), NULL, counterSequence.next());
}

static UniqueSequenceCounter selectorSequence;
IHqlExpression * createUniqueSelectorSequence()
{
    unsigned __int64 seq = selectorSequence.next();
    return createSequence(no_attr, NULL, _selectorSequence_Atom, seq);
}


IHqlExpression * createSelectorSequence(unsigned __int64 seq)
{
    return createSequence(no_attr, NULL, _selectorSequence_Atom, seq);
}


static UniqueSequenceCounter rowsidSequence;
IHqlExpression * createUniqueRowsId()
{
    unsigned __int64 seq = rowsidSequence.next();
    return createSequence(no_attr, NULL, _rowsid_Atom, seq);
}

static UniqueSequenceCounter sequenceExprSequence;
IHqlExpression * createSequenceExpr()
{
    unsigned __int64 seq = sequenceExprSequence.next();
    return createSequence(no_sequence, makeIntType(sizeof(size32_t), false), NULL, seq);
}

IHqlExpression * createSelectorSequence()
{
#ifdef USE_SELSEQ_UID
    return createUniqueSelectorSequence();
#else
    return LINK(defaultSelectorSequenceExpr);
#endif
}


IHqlExpression * createDummySelectorSequence()
{
    return LINK(defaultSelectorSequenceExpr);
}

IHqlExpression * queryNewSelectAttrExpr()
{
    return newSelectAttrExpr;
}

//Follow inheritance structure when getting property value.
IHqlExpression * queryRecordAttribute(IHqlExpression * record, IAtom * name)
{
    IHqlExpression * match = record->queryAttribute(name);
    if (match)
        return match;
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            return queryRecordAttribute(cur, name);
        case no_field:
        case no_ifblock:
            return NULL;
        }
    }
    return NULL;
}


bool isSortDistribution(IHqlExpression * distribution)
{
    return (distribution && distribution->isAttribute() && (distribution->queryName() == sortedAtom));
}


bool isChooseNAllLimit(IHqlExpression * limit)
{
    IValue * value = limit->queryValue();
    return (value && (value->getIntValue() == CHOOSEN_ALL_LIMIT));
}


bool activityMustBeCompound(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_keyedlimit:
        return true;
    case no_filter:
        return filterIsKeyed(expr);
    case no_newaggregate:
    case no_newusertable:
    case no_aggregate:
    case no_usertable:
    case no_hqlproject:
        return expr->hasAttribute(keyedAtom);
    }
    return false;
}

bool isSequentialActionList(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_orderedactionlist:
    case no_sequential:
        return true;
    }
    return false;
}

bool isSelectFirstRow(IHqlExpression * expr)
{
    if (expr->getOperator() != no_selectnth)
        return false;
    IValue * value = expr->queryChild(1)->queryValue();
    return value && (value->getIntValue() == 1);
}

bool includeChildInDependents(IHqlExpression * expr, unsigned which)
{
    switch (expr->getOperator())
    {
    case no_keyindex:
    case no_newkeyindex:
        return (which != 0);
    case no_table:
        return which <= 2;
    }
    return true;
}

IHqlExpression * queryExpression(ITypeInfo * type)
{
    if (!type) return NULL;
    ITypeInfo * indirect = queryModifier(type, typemod_indirect);
    if (indirect)
        return static_cast<IHqlExpression *>(indirect->queryModifierExtra());
    return QUERYINTERFACE(queryUnqualifiedType(type), IHqlExpression);
}

IHqlExpression * queryExpression(IHqlDataset * ds)
{
    if (!ds) return NULL;
    return ds->queryExpression();
}

IHqlScope * queryScope(ITypeInfo * type)
{
    if (!type) return NULL;
    return QUERYINTERFACE(queryUnqualifiedType(type), IHqlScope);
}

IHqlRemoteScope * queryRemoteScope(IHqlScope * scope)
{
    return dynamic_cast<IHqlRemoteScope *>(scope);
}

IHqlAlienTypeInfo * queryAlienType(ITypeInfo * type)
{
    return QUERYINTERFACE(queryUnqualifiedType(type), IHqlAlienTypeInfo);
}

bool isSameUnqualifiedType(ITypeInfo * l, ITypeInfo * r)
{
    return queryUnqualifiedType(l) == queryUnqualifiedType(r);
}

bool isSameFullyUnqualifiedType(ITypeInfo * l, ITypeInfo * r)
{
    Owned<ITypeInfo> ul = getFullyUnqualifiedType(l);
    Owned<ITypeInfo> ur = getFullyUnqualifiedType(r);
    return ul == ur;
}

bool recordTypesMatch(ITypeInfo * left, ITypeInfo * right)
{
    if (!left || !right)
        return (left == right);

    if (queryUnqualifiedType(queryRecordType(left)) == queryUnqualifiedType(queryRecordType(right)))
        return true;

    IHqlExpression * leftRecord = queryRecord(left);
    IHqlExpression * rightRecord = queryRecord(right);
    if (!leftRecord || !rightRecord)
        return leftRecord == rightRecord;

    if (leftRecord->hasAttribute(abstractAtom) || rightRecord->hasAttribute(abstractAtom))
        return true;

    //This test compares the real record structure, ignoring names etc.   The code above just short-cicuits this test.
    OwnedHqlExpr unadornedLeft = getUnadornedRecordOrField(leftRecord);
    OwnedHqlExpr unadornedRight = getUnadornedRecordOrField(rightRecord);
    if (unadornedLeft == unadornedRight)
        return true;

#ifdef _DEBUG
    debugFindFirstDifference(unadornedLeft, unadornedRight);
#endif

    return false;
}

void assertRecordTypesMatch(ITypeInfo * left, ITypeInfo * right)
{
    if (recordTypesMatch(left, right))
        return;

    EclIR::dbglogIR(2, left, right);
    throwUnexpected();
}


void assertRecordTypesMatch(IHqlExpression * left, IHqlExpression * right)
{
    assertRecordTypesMatch(left->queryRecordType(), right->queryRecordType());
}

bool recordTypesMatch(IHqlExpression * left, IHqlExpression * right)
{
    return recordTypesMatch(left->queryRecordType(), right->queryRecordType());
}


bool recordTypesMatchIgnorePayload(IHqlExpression *left, IHqlExpression *right)
{
    OwnedHqlExpr simpleLeft = removeAttribute(left->queryRecord(), _payload_Atom);
    OwnedHqlExpr simpleRight = removeAttribute(right->queryRecord(), _payload_Atom);
    return recordTypesMatch(simpleLeft->queryType(), simpleRight->queryType());
}


IHqlExpression * queryTransformSingleAssign(IHqlExpression * transform)
{
    if ((transform->numChildren() == 0) || queryRealChild(transform, 1))
        return NULL;

    IHqlExpression * assign = transform->queryChild(0);
    if (assign->getOperator() == no_assignall)
    {
        if (assign->numChildren() != 1)
            return NULL;
        assign = assign->queryChild(0);
    }
    if (assign->getOperator() != no_assign)
        return NULL;
    return assign;
}


IHqlExpression * convertToSimpleAggregate(IHqlExpression * expr)
{
    if ((expr->getOperator() == no_select) && expr->queryAttribute(newAtom))
    {
        expr = expr->queryChild(0);
        if (!isSelectFirstRow(expr))
            return NULL;
        expr = expr->queryChild(0);
    }

    if (expr->getOperator() == no_compound_childaggregate)
        expr = expr->queryChild(0);
    if (expr->getOperator() != no_newaggregate)
        return NULL;
    if (datasetHasGroupBy(expr))
        return NULL;
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * assign = queryTransformSingleAssign(transform);
    if (!assign)
        return NULL;
    IHqlExpression * lhs = assign->queryChild(0);
    IHqlExpression * rhs = assign->queryChild(1);
    node_operator newop;
    unsigned numArgs = 0;
    switch (rhs->getOperator())
    {
    case no_avegroup:       newop = no_ave; numArgs = 1; break;
    case no_countgroup:     newop = no_count; break;
    case no_mingroup:       newop = no_min; numArgs = 1; break;
    case no_maxgroup:       newop = no_max; numArgs = 1; break;
    case no_sumgroup:       newop = no_sum; numArgs = 1; break;
    case no_existsgroup:    newop = no_exists; break;
    default: 
        return NULL;
    }

    if ((numArgs != 0) && (lhs->queryType() != rhs->queryType()))
        return NULL;

    IHqlExpression * ds = expr->queryChild(0);
    loop
    {
        node_operator op = ds->getOperator();
        if ((op != no_sorted) && (op != no_distributed) && (op != no_preservemeta) && (op != no_alias_scope))
            break;
        ds = ds->queryChild(0);
    }

    ::Link(ds);
    IHqlExpression * filter = queryRealChild(rhs, numArgs);
    if (filter)
        ds = createDataset(no_filter, ds, LINK(filter));
    HqlExprArray args;
    args.append(*ds);
    for (unsigned i=0; i < numArgs; i++)
        args.append(*LINK(rhs->queryChild(i)));

    IHqlExpression * keyed = rhs->queryAttribute(keyedAtom);
    if (keyed)
        args.append(*LINK(keyed));
    OwnedHqlExpr aggregate = createValue(newop, rhs->getType(), args);
    return ensureExprType(aggregate, lhs->queryType());
}

IHqlExpression * queryAggregateFilter(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_countgroup:
    case no_existsgroup:
        return queryRealChild(expr, 0);
    case no_sumgroup:
    case no_vargroup:
    case no_covargroup:
    case no_corrgroup:
    case no_maxgroup:
    case no_mingroup:
    case no_avegroup:
        return queryRealChild(expr, 1);
    }
    throwUnexpected();
}

node_operator querySingleAggregate(IHqlExpression * expr, bool canFilterArg, bool canBeGrouped, bool canCast)
{
    //This needs to only matche examples suported by the function above (with canBeGrouped set to false).
    switch (expr->getOperator())
    {
    case no_compound_childaggregate:
    case no_compound_diskaggregate:
    case no_compound_indexaggregate:
        expr = expr->queryChild(0);
        break;
    }
    if (expr->getOperator() != no_newaggregate)
        return no_none;
    if (!canBeGrouped && (datasetHasGroupBy(expr) || isGrouped(expr->queryChild(0))))
        return no_none;
    IHqlExpression * transform = expr->queryChild(2);
    if (!canBeGrouped && (transform->numChildren() != 1))
        return no_none;
    node_operator matchedOp = no_none;
    ForEachChild(i, transform)
    {
        IHqlExpression * assign = transform->queryChild(i);
        if (assign->getOperator() != no_assign)
            return no_none;
        IHqlExpression * rhs = assign->queryChild(1);
        node_operator curOp = rhs->getOperator();
        switch (curOp)
        {
        case NO_AGGREGATEGROUP:
            if (matchedOp != no_none)
                return no_none;
            break;
        default:
            if (!canBeGrouped)
                return no_none;
            //A non aggregate, so iterate again.
            continue;
        }
        if (assign->queryChild(0)->queryType() != rhs->queryType())
        {
            if (!canCast)
                return no_none;
            switch (curOp)
            {
            case no_existsgroup:
            case no_countgroup:
                break;
            default:
                return no_none;
            }
        }

        if (!canFilterArg && queryAggregateFilter(rhs))
            return no_none;

        matchedOp = curOp;
    }
    return matchedOp;
}

node_operator querySimpleAggregate(IHqlExpression * expr, bool canFilterArg, bool canCast)
{
    //This needs to match convertToSimpleAggregate() above.
    return querySingleAggregate(expr, canFilterArg, false, canCast);
}

bool isSimpleCountAggregate(IHqlExpression * expr, bool canFilterArg)
{
    return querySimpleAggregate(expr, canFilterArg, true) == no_countgroup;
}

bool isSimpleCountExistsAggregate(IHqlExpression * expr, bool canFilterArg, bool canCast)
{
    node_operator op = querySimpleAggregate(expr, canFilterArg, canCast);
    return (op == no_countgroup) || (op == no_existsgroup);
}

bool isKeyedCountAggregate(IHqlExpression * aggregate)
{
    IHqlExpression * transform = aggregate->queryChild(2);
    IHqlExpression * assign = transform->queryChild(0);
    if (!assign || assign->getOperator() != no_assign)
        return false;
    IHqlExpression * count = assign->queryChild(1);
    if (count->getOperator() != no_countgroup)
        return false;
    return count->hasAttribute(keyedAtom);
}


static bool getBoolAttributeValue(IHqlExpression * attr)
{
    IHqlExpression * value = attr->queryChild(0);
    //No argument implies true
    if (!value)
        return true;

    //If it is a constant return it.
    if (value->queryValue())
        return getBoolValue(value, true);

    //Not a constant => fold the expression
    OwnedHqlExpr folded = foldHqlExpression(value);
    if (folded->queryValue())
        return getBoolValue(folded, true);

    throwError1(HQLERR_PropertyArgumentNotConstant, attr->queryName()->str());
}

bool getBoolAttribute(IHqlExpression * expr, IAtom * name, bool dft)
{
    if (!expr)
        return dft;
    IHqlExpression * attr = expr->queryAttribute(name);
    if (!attr)
        return dft;
    return getBoolAttributeValue(attr);
}

bool getBoolAttributeInList(IHqlExpression * expr, IAtom * search, bool dft)
{
    IHqlExpression * match = queryAttributeInList(search, expr);
    if (!match)
        return dft;
    return getBoolAttributeValue(match);
}

IHqlExpression * queryOriginalRecord(IHqlExpression * expr)
{
    return queryOriginalRecord(expr->queryType());
}

IHqlExpression * queryOriginalTypeExpression(ITypeInfo * t)
{
    loop
    {
        typemod_t modifier = t->queryModifier();
        if (modifier == typemod_none)
            break;

        if (modifier == typemod_original)
        {
            IHqlExpression * originalExpr = static_cast<IHqlExpression *>(t->queryModifierExtra());
            if (originalExpr->queryType()->getTypeCode() == t->getTypeCode())
                return originalExpr;
        }

        t = t->queryTypeBase();
    }
    return queryExpression(t);
}

IHqlExpression * queryOriginalRecord(ITypeInfo * t)
{
    t = queryRecordType(t);
    if (!t)
        return NULL;

    loop
    {
        typemod_t modifier = t->queryModifier();
        if (modifier == typemod_none)
            return queryExpression(t);

        if (modifier == typemod_original)
        {
            IHqlExpression * originalExpr = static_cast<IHqlExpression *>(t->queryModifierExtra());
            if (originalExpr->getOperator() == no_record)
                return originalExpr;
        }

        t = t->queryTypeBase();
    }
    return queryExpression(t);
}

ITypeInfo * createRecordType(IHqlExpression * record)
{
    if (record->getOperator() != no_record)
        return LINK(record->queryRecordType());
    if (record->queryBody(true) == record)
        return record->getType();
    return makeOriginalModifier(record->getType(), LINK(record)); 
}


ITypeInfo * getSumAggType(ITypeInfo * argType)
{
    type_t tc = argType->getTypeCode();
    switch (tc)
    {
    case type_packedint:
    case type_swapint:
    case type_bitfield:
        return makeIntType(8, true);
    case type_int:
        if (argType->getSize() < 8)
            return makeIntType(8, true);
        return LINK(argType);
    case type_real:
        return makeRealType(8);
    case type_decimal:
        {
            //A guess is to add 12 more digits 
            unsigned oldDigits = argType->getDigits();
            if (oldDigits == UNKNOWN_LENGTH)
                return LINK(argType);
            unsigned oldPrecision = argType->getPrecision();
            unsigned newDigits = argType->getDigits()+12;
            if (newDigits - oldPrecision > MAX_DECIMAL_LEADING)
                newDigits = MAX_DECIMAL_LEADING + oldPrecision;
            return makeDecimalType(newDigits, oldPrecision, argType->isSigned());
        }
    default:
        return LINK(argType);
    }
}

ITypeInfo * getSumAggType(IHqlExpression * arg)
{
    return getSumAggType(arg->queryType());
}


//Return a base attribute ignoring any delayed evaluation, so we can find out the type of the base object
//and get an approximation to the definition when it is required.
IHqlExpression * queryNonDelayedBaseAttribute(IHqlExpression * expr)
{
    if (!expr)
        return NULL;

    loop
    {
        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_call:
        case no_libraryscopeinstance:
            expr = expr->queryDefinition();
            break;
        case no_funcdef:
            expr = expr->queryChild(0);
            break;
        case no_delayedselect:
        case no_internalselect:
        case no_libraryselect:
        case no_unboundselect:
            expr = expr->queryChild(2);
            break;
        default:
            return expr;
        }
    }
}

extern bool areAllBasesFullyBound(IHqlExpression * module)
{
    if (module->getOperator() == no_param)
        return false;
    ForEachChild(i, module)
    {
        IHqlExpression * cur = module->queryChild(i);
        if (!cur->isAttribute() && !areAllBasesFullyBound(cur))
            return false;
    }
    return true;
}


bool isUpdatedConditionally(IHqlExpression * expr)
{
    IHqlExpression * updateAttr = expr->queryAttribute(updateAtom);
    return (updateAttr && !updateAttr->queryAttribute(alwaysAtom));
}

ITypeInfo * getTypedefType(IHqlExpression * expr) 
{ 
    //Don't create annotate with a typedef modifier if it doesn't add any information - e.g., maxlength
    if ((expr->getOperator() == no_typedef) && (expr->numChildren() == 0))
        return expr->getType();
    return makeOriginalModifier(expr->getType(), LINK(expr)); 
}

void extendAdd(SharedHqlExpr & value, IHqlExpression * expr)
{
    if (value)
        value.setown(createValue(no_add, value->getType(), LINK(value), LINK(expr)));
    else
        value.set(expr);
}

//Not certain of the best representation.  At the moment just take the record, and add an abstract flag
extern HQL_API IHqlExpression * createAbstractRecord(IHqlExpression * record)
{
    HqlExprArray args;
    if (record)
        unwindChildren(args, record);
    //Alteranative code which should also work, but may require changes to the existing (deprecated) virtual dataset code....
    //if (record)
    //  args.append(*LINK(record));
    args.append(*createAttribute(abstractAtom));
    return createRecord(args);
}

extern HQL_API IHqlExpression * createSortList(HqlExprArray & elements)
{
     return createValue(no_sortlist, makeSortListType(NULL), elements);
}

IHqlExpression * cloneFieldMangleName(IHqlExpression * field)
{
    StringBuffer newName;
    newName.append(field->queryId()->str()).append("_").append(getUniqueId());
    HqlExprArray children;
    unwindChildren(children, field);
    return createField(createIdAtom(newName), field->getType(), children);
}

//==============================================================================================================



static void safeLookupSymbol(HqlLookupContext & ctx, IHqlScope * modScope, IIdAtom * name)
{
    try
    {
        OwnedHqlExpr resolved = modScope->lookupSymbol(name, LSFpublic, ctx);
        if (!resolved || !resolved->isMacro())
            return;

        //Macros need special processing to expand their definitions.
        HqlLookupContext childContext(ctx);
        childContext.createDependencyEntry(modScope, name);

        OwnedHqlExpr expanded = expandMacroDefinition(resolved, childContext, false);
    }
    catch (IException * e)
    {
        e->Release();
    }
}

static void gatherAttributeDependencies(HqlLookupContext & ctx, IHqlScope * modScope)
{
    modScope->ensureSymbolsDefined(ctx);
    HqlExprArray symbols;
    modScope->getSymbols(symbols);
    symbols.sort(compareSymbolsByName);

    ForEachItemIn(i, symbols)
        safeLookupSymbol(ctx, modScope, symbols.item(i).queryId());
}

static void gatherAttributeDependencies(HqlLookupContext & ctx, const char * item)
{
    try
    {
        IIdAtom * moduleName = NULL;
        IIdAtom * attrName = NULL;
        const char * dot = strrchr(item, '.');
        if (dot)
        {
            moduleName = createIdAtom(item, dot-item);
            attrName = createIdAtom(dot+1);
        }
        else
            moduleName = createIdAtom(item);

        OwnedHqlExpr resolved = ctx.queryRepository()->queryRootScope()->lookupSymbol(moduleName, LSFpublic, ctx);
        if (resolved)
        {
            IHqlScope * scope = resolved->queryScope();
            if (scope)
            {
                if (attrName)
                    safeLookupSymbol(ctx, scope, attrName);
                else
                    gatherAttributeDependencies(ctx, scope);
            }
        }
    }
    catch (IException * e)
    {
        e->Release();
    }
}

extern HQL_API IPropertyTree * gatherAttributeDependencies(IEclRepository * dataServer, const char * items)
{
    HqlParseContext parseCtx(dataServer, NULL);
    parseCtx.nestedDependTree.setown(createPTree("Dependencies"));

    Owned<IErrorReceiver> errorHandler = createNullErrorReceiver();
    HqlLookupContext ctx(parseCtx, errorHandler);
    if (items && *items)
    {
        loop
        {
            const char * comma = strchr(items, ',');
            if (!comma)
                break;

            StringBuffer next;
            next.append(comma-items, items);
            gatherAttributeDependencies(ctx, next.str());
            items = comma+1;
        }
        gatherAttributeDependencies(ctx, items);
    }
    else
    {
        HqlScopeArray scopes;   
        getRootScopes(scopes, dataServer, ctx);
        scopes.sort(compareScopesByName);
        ForEachItemIn(i, scopes)
        {
            IHqlScope & cur = scopes.item(i);
            gatherAttributeDependencies(ctx, &cur);
        }
    }

    return parseCtx.nestedDependTree.getClear();
}


IIdAtom * queryPatternName(IHqlExpression * expr)
{
    if (expr->getOperator() == no_pat_instance)
        return expr->queryChild(1)->queryId();
    return NULL;
}

IHqlExpression * createGroupedAttribute(IHqlExpression * grouping)
{
    return createAttribute(groupedAtom, LINK(grouping));
}


IHqlExpression * createTypeTransfer(IHqlExpression * expr, ITypeInfo * _newType)
{
    Owned<ITypeInfo> newType = _newType;
    switch (newType->getTypeCode())
    {
    case type_row:
    case type_record:
        return createRow(no_typetransfer, LINK(queryOriginalRecord(newType)), expr);
    case type_table:
    case type_groupedtable:
        return createDataset(no_typetransfer, LINK(queryOriginalRecord(newType)), expr);
    case type_dictionary:
        return createDictionary(no_typetransfer, LINK(queryOriginalRecord(newType)), expr);
    default:
        return createValue(no_typetransfer, newType.getClear(), expr);
    }
}


//Make sure the expression doesn't get leaked if an exception occurs when closing it.
IHqlExpression * closeAndLink(IHqlExpression * expr)
{
    expr->Link();
    try
    {
        return expr->closeExpr();
    }
    catch (...)
    {
        expr->Release();
        throw;
    }
}

//MORE: This should probably be handled via the Lookup context instead (which shoudl be renamed parse
static bool legacyImportMode = false;
static bool legacyWhenMode = false;
extern HQL_API void setLegacyEclSemantics(bool _legacyImport, bool _legacyWhen)
{
    legacyImportMode = _legacyImport;
    legacyWhenMode = _legacyWhen;
}
extern HQL_API bool queryLegacyImportSemantics()
{
    return legacyImportMode;
}
extern HQL_API bool queryLegacyWhenSemantics()
{
    return legacyWhenMode;
}


static bool readNumber(unsigned & value, const char * & cur)
{
    char * end;
    value = (unsigned)strtol(cur, &end, 10);
    if (cur == end)
        return false;
    cur = end;
    return true;
}

extern bool HQL_API extractVersion(unsigned & major, unsigned & minor, unsigned & sub, const char * version)
{
    major = minor = sub = 0;
    if (!version)
        return false;
    if (!readNumber(major, version))
        return false;
    if (*version++ != '.')
        return false;
    if (!readNumber(minor, version))
        return false;
    if (*version++ != '.')
        return false;
    if (!readNumber(sub, version))
        return false;
    return true;
}

/*
List of changes:

* Rename queryTransformExtra() in transformer class for clarity?

1) Make transformStack non linking - it shouldn't be possible for expressions to disappear while a child transform is happening

4) Add a HEF2workflow instead of HEF2sandbox, and use for define, globalscope, and workflow optimizations.

5) How can I remove the need for temporary classes if no transformations occur?  
   E.g., if createTransform() returns same, set extra to expr instead of the appropriate new class.
   More complicated, but would save lots of allocations, linking etc..  How would this be handled without too much pain?

   - flag in the transformer????

   E.g.,Would adding another extra field help remove a temporary class?
    - splitter verifier usecount
    - sharing count in other situations.

7) How can I avoid linking the expression on return from createTransformed()?

- Dangerous.......but potentially good.

- existing createTransformed() renamed createTransformedEx(), which can return NULL if same.
- transformEx() instead of transform()
  calls createTransformEx(), returns NULL if already exists, and matches expr.
- createTransform() and transform() implemented as inline functions that wrap the functions above, and force them to be non-null
- transformChildren() etc. can call transformEx(), and if there is a difference then need to unwindChildren(expr, start, cur-1)

8) When can analysis be simplified to not create the helper classes?

N) Produce a list of all the transformations that are done - as a useful start to a brown bag talk, and see if any can be short circuited.

*) What flags can be used to terminate some of the transforms early
- contains no_setmeta


*/

