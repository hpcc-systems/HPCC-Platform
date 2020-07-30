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
#ifndef HQLEXPR_IPP_INCL
#define HQLEXPR_IPP_INCL

//The following needs to be defined to allow multi-threaded access to the expressions
//Required inside esp and other areas for parsing record definitions etc.
//On x64 increases processing time by 3-4%, so left enabled.  Other platforms might benefit from disabling.
#define HQLEXPR_MULTI_THREADED

#define NUM_PARALLEL_TRANSFORMS 1
//I'm not sure if the following is needed or not - I'm slight concerned that remote scopes (e.g.,, plugins)
//may be accessed in parallel from multiple threads, causing potential conflicts
#ifdef HQLEXPR_MULTI_THREADED
#define THREAD_SAFE_SYMBOLS
#endif

//Define the following to provide information about how many unique expressions of each type are created
//use in conjunction with --leakcheck flag, otherwise the destructor does not get called
//#define GATHER_LINK_STATS
//#define GATHER_COMMON_STATS

//The following flag is to allow tracing when expressions are created, linked, released, destroyed
//It allocates a unique id to each expression that is created, then add the unique ids into the
//checkSeqId() function, and add a breakpoint there
#ifdef _DEBUG
#define DEBUG_TRACK_INSTANCEID
#endif

#include "jexcept.hpp"
#include "javahash.hpp"
#include "defvalue.hpp"
#include "hqlexpr.hpp"

//There is currently 1 spare byte in the base CHqlExpression - so there is no problem with this being 2 bytes
//but if space becomes very tight it could be reduced back down to 1 byte
typedef unsigned short transformdepth_t;
#define TRANSFORM_DEPTH_MASK    0x7fff
#define TRANSFORM_DEPTH_NOLINK  0x8000
#define TRANSFORM_DEPTH(depth) (depth & TRANSFORM_DEPTH_MASK)

//following are only stored in the save array
#define TRANSFORM_DEPTH_SAVE_MATCH_EXPR     0x10000

#define RELEASE_TRANSFORM_EXTRA(depth, extra)   { if (!(depth & TRANSFORM_DEPTH_NOLINK)) ::Release(extra); }

class NullCriticalBlock
{
public:
    inline NullCriticalBlock(CriticalSection &) {}
};

#ifdef HQLEXPR_MULTI_THREADED
typedef CriticalBlock HqlCriticalBlock;
#else
typedef NullCriticalBlock HqlCriticalBlock;
#endif

class CHqlExprMeta;

class HQL_API CHqlDynamicProperty
{
    friend class CHqlRealExpression;
public:
    inline CHqlDynamicProperty(ExprPropKind _kind, IInterface *_value)
        : kind(_kind), value(_value)
    {
        next = NULL;
    }
    ~CHqlDynamicProperty() { delete next; }

protected:
    CHqlDynamicProperty * next;
    ExprPropKind kind;
    Linked<IInterface> value;
};


class CUsedTablesBuilder;

//Optimized representation of the number of used tables
//Special case a single row and don't create a child array
//Create once all the processing is done so the array is exactly the right size.
class HQL_API CUsedTables
{
public:
    CUsedTables();
    ~CUsedTables();

    inline bool isIndependentOfScope() const { return (numActiveTables == 0); }
    bool usesSelector(IHqlExpression * selector) const;
    void gatherTablesUsed(CUsedTablesBuilder & used) const;
    void gatherTablesUsed(HqlExprCopyArray & inScope) const;
    void set(HqlExprCopyArray & _activeTables);
    void setActiveTable(IHqlExpression * expr);

private:
    union
    {
        IHqlExpression * single;
        IHqlExpression * * multi;
    } tables;
    unsigned numActiveTables;
};

class HQL_API UsedExpressionHashTable : public SuperHashTableOf<IHqlExpression, IHqlExpression>
{
public:
    UsedExpressionHashTable() : SuperHashTableOf<IHqlExpression,IHqlExpression>(false) {}
    ~UsedExpressionHashTable() { _releaseAll(); }

    inline void zap(IHqlExpression & expr) { remove(&expr); }

protected:
    virtual void onAdd(void *next) override {}
    virtual void onRemove(void *) override {}
    virtual bool matchesFindParam(const void * _element, const void * _key, unsigned fphash) const override
    {
        const IHqlExpression * element = static_cast<const IHqlExpression *>(_element);
        const IHqlExpression * key = static_cast<const IHqlExpression *>(_key);
        return element==key;
    }
    virtual unsigned getHashFromElement(const void * et) const override
    {
        return static_cast<const IHqlExpression *>(et)->getHash();
    }
    virtual unsigned getHashFromFindParam(const void * et) const override
    {
        return static_cast<const IHqlExpression *>(et)->getHash();
    }
    inline const void * getFindParam(const void * et) const override { return et; }
};


class HQL_API CUsedTablesBuilder
{
public:
    void addActiveTable(IHqlExpression * expr);
    void cleanupProduction();
    inline void removeActive(IHqlExpression * expr)
    {
        HqlExprCopyArray toRemove;
        for (IHqlExpression & cur : inScopeTables)
        {
            IHqlExpression * selector = &cur;
            for(;;)
            {
                if (selector == expr)
                {
                    toRemove.append(cur);
                    break;
                }
                if (selector->getOperator() != no_select)
                    break;
                selector = selector->queryChild(0);
            }
        }
        ForEachItemIn(i, toRemove)
            removeActiveSelector(&toRemove.item(i));
    }
    void removeActiveSelector(IHqlExpression * expr) { inScopeTables.remove(expr); }
    void removeParent(IHqlExpression * expr);
    void removeActiveRecords();
    void removeRows(IHqlExpression * expr, IHqlExpression * left, IHqlExpression * right);
    void set(CUsedTables & tables);

    inline bool isIndependentOfScope() const { return inScopeTables.ordinality() == 0; }

protected:
    UsedExpressionHashTable inScopeTables;     // may need to rename, since use has changed.
};

#ifdef HQLEXPR_MULTI_THREADED
typedef CInterfaceOf<IHqlExpression> LinkedBaseIHqlExpression;
#else
typedef CSingleThreadInterfaceOf<IHqlExpression> LinkedBaseIHqlExpression;
#endif

class HQL_API CHqlExpression : public LinkedBaseIHqlExpression
{
public:
    friend class CHqlExprMeta;
    typedef LinkedBaseIHqlExpression Parent;

protected:
    unsigned hashcode;          // CInterface is 4 byte aligned in 64bits, so use this to pad
                                // Worth storing because it significantly speeds up equality checking
#ifdef DEBUG_TRACK_INSTANCEID
public:
    unsigned __int64 seqid;
protected:
#endif
    IInterface * transformExtra[NUM_PARALLEL_TRANSFORMS];
    HqlExprArray operands;
    node_operator op;                           // 2 bytes
    unsigned short infoFlags2 = 0;              // 2 bytes, pack with previous
    transformdepth_t transformDepth[NUM_PARALLEL_TRANSFORMS];           // 1 byte
    bool observed = false;                      // 1 byte.  this could be packed into infoFlags2 if the space was required

protected:
    CHqlExpression(node_operator op);

    //protected virtual members not in public interface
    virtual void sethash();
    virtual void addProperty(ExprPropKind kind, IInterface * value) = 0;
    virtual IInterface * queryExistingProperty(ExprPropKind kind) const = 0;

protected:
    //For a no_select, is this the root no_select (rather than a.b.c), and is it also an active selector.
    //That requires the left hand side to be a dataset, and not marked as new.
    //Used for determining how a no_select should be interpreted e.g., in table gathering. 
    inline bool isSelectRootAndActive() const
    {
        dbgassertex(op == no_select);
        if (hasAttribute(newAtom))
            return false;
        //If lhs is a row then this is not the root selection from a dataset
        IHqlExpression * ds = queryChild(0);
        if (ds->isDatarow())
        {
            switch (ds->getOperator())
            {
            //This is a complete hack - but prevents scrub2 from generating invalid code.  HPCC-21084 created for a correct fix.
            case no_typetransfer:
                return true;
            }
            return false;
        }
        return true;
    }

    IHqlExpression * commonUpExpression();
    IHqlExpression * calcNormalizedSelector() const;
    void setInitialHash(unsigned typeHash);

public:
#if (defined(GATHER_LINK_STATS) || defined(DEBUG_TRACK_INSTANCEID))
    virtual void Link(void) const;
    virtual bool Release(void) const;
#endif

    virtual ~CHqlExpression();

    virtual bool isAggregate() override;
    virtual bool isExprClosed() const override { return hashcode!=0; }
    virtual IAtom * queryName() const override { return NULL; }
    virtual IIdAtom * queryId() const override;
    virtual node_operator getOperator() const override { return op; }
    virtual IHqlDataset *queryDataset() override { return NULL; };
    virtual IHqlScope *queryScope() override;
    virtual IHqlSimpleScope *querySimpleScope() override;
    virtual IHqlExpression *queryFunctionDefinition() const override { return NULL; };
    virtual IHqlExpression *queryExternalDefinition() const override { return NULL; };
    virtual bool isBoolean() override;
    virtual bool isDataset() override;
    virtual bool isDictionary() override;
    virtual bool isDatarow() override;
    virtual bool isScope() override;
    virtual bool isMacro() override;
    virtual bool isType() override;
    virtual bool isList() override;
    virtual bool isField() override;
    virtual bool isRecord() override;
    virtual bool isAction() override;
    virtual bool isTransform() override;
    virtual bool isFunction() override;
    virtual annotate_kind getAnnotationKind() const override { return annotate_none; }
    virtual IHqlAnnotation * queryAnnotation() override { return NULL; }
    virtual bool isAttribute() const override { return false; }
    virtual IHqlExpression *queryNormalizedSelector() override { return this; }

    virtual int  getStartLine() const override { throwUnexpected(); }
    virtual int  getStartColumn() const override { throwUnexpected(); }
    virtual IPropertyTree * getDocumentation() const override { return NULL; }

    virtual IHqlExpression *queryBody(bool singleLevel = false) override { return this; }
    virtual IValue *queryValue() const override { return NULL; }
    virtual IInterface *queryUnknownExtra() override { return NULL; }
    virtual unsigned __int64 querySequenceExtra() override { return 0; }

    virtual StringBuffer &toString(StringBuffer &ret) override;
    virtual IHqlExpression *queryChild(unsigned idx) const override;
    virtual unsigned numChildren() const override;

    virtual ITypeInfo *queryRecordType() override;
    virtual IHqlExpression *queryRecord() override;

    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) override { return LINK(body); }
    virtual IHqlExpression * cloneAllAnnotations(IHqlExpression * body) override { return LINK(body); }
    virtual void unwindList(HqlExprArray &dst, node_operator) override;

    virtual IIdAtom * queryFullContainerId() const override { return NULL; }
    virtual ISourcePath * querySourcePath() const override { return NULL; }

    virtual IInterface * queryTransformExtra() override;
    virtual void setTransformExtra(IInterface * x) override;
    virtual void setTransformExtraOwned(IInterface * x) override;
    virtual void setTransformExtraUnlinked(IInterface * x) override;

    virtual IHqlExpression *closeExpr() override; // MORE - should be in expressionBuilder interface!

    virtual StringBuffer& getTextBuf(StringBuffer& buf) override { assertex(false); return buf; }
    virtual IFileContents * queryDefinitionText() const override { return NULL; }
    virtual bool isExported() const override { return false; }

    virtual IHqlExpression * queryAnnotationParameter(unsigned i) const override { return NULL; }

    virtual void addObserver(IObserver & observer) override;
    virtual void removeObserver(IObserver & observer) override;
    virtual unsigned getHash() const override;
    
    virtual void beforeDispose() override;               // called before item is freed so whole object still valid
    virtual unsigned getSymbolFlags() const override;

public:
    inline void doSetTransformExtra(IInterface * x, unsigned depthMask);
    inline void resetTransformExtra(IInterface * _extra, unsigned depth);
    inline unsigned queryHash() const { return hashcode; }
};

class HQL_API CHqlRealExpression : public CHqlExpression
{
public:
    CHqlRealExpression(node_operator op);
    ~CHqlRealExpression();

    //virtual because some specialist properties are stored differently in derived classes
    virtual IHqlExpression *queryProperty(ExprPropKind kind) override;
    virtual void addProperty(ExprPropKind kind, IInterface * value) override;
    virtual IInterface * queryExistingProperty(ExprPropKind kind) const override;
    virtual IHqlExpression *queryAttribute(IAtom * propName) const override;
    virtual IHqlExpression *addOperand(IHqlExpression *) override; // MORE - should be in expressionBuilder interface!

    virtual bool isFullyBound() const override { return fullyBound(); };
    virtual unsigned getInfoFlags() const override { return infoFlags; }
    virtual unsigned getInfoFlags2() const override { return infoFlags2; }
    virtual bool isGroupAggregateFunction() override { return functionOfGroupAggregate(); }
    virtual bool isPure() override { return pure(); }
    virtual bool isConstant() override;
    virtual IHqlExpression *closeExpr() override; // MORE - should be in expressionBuilder interface!
    virtual bool equals(const IHqlExpression & other) const override;

protected:
    inline bool constant() const { return (infoFlags2 & HEF2constant) != 0; }
    inline bool functionOfGroupAggregate() const { return (infoFlags & HEFfunctionOfGroupAggregate) != 0; }
    inline bool fullyBound() const { return (infoFlags & HEFunbound) == 0; }
    inline bool pure() const { return (infoFlags & HEFimpure) == 0; }
    virtual unsigned getCachedEclCRC() override;

    void appendSingleOperand(IHqlExpression * arg0);
    void setOperands(HqlExprArray & ownedOperands);
    void onAppendOperand(IHqlExpression & child, unsigned whichOperand);
    inline void doAppendOperand(IHqlExpression & child)
    {
        unsigned which = operands.ordinality();
        operands.append(child);
        onAppendOperand(child, which);
    }

    void initFlagsBeforeOperands();
    void updateFlagsAfterOperands();

protected:
    unsigned cachedCRC;
    unsigned infoFlags;
    std::atomic<CHqlDynamicProperty *> attributes = { nullptr };
};

//The following couple of classes are here primarily to save memory.  
//It is preferrable not to artificially introduce extra classes, but one representative large example has
//12M+ instances, and not including the tables/type save 16 and 8 bytes each.  That quickly becomes a significant
//amount of memory.
//
//The nodes with significant number of instances are (no_assign, no_select and annotations).  
//One further possibilitiy is to add a CHqlAssignExpression which could also remove the tables and type. 
//If any more class splitting is contemplated it would be worth revisiting in terms of policies.

//This class calculates which tables the expression references to ensure it is evaluated in the correct conext.
class HQL_API CHqlExpressionWithTables : public CHqlRealExpression
{
public:
    inline CHqlExpressionWithTables(node_operator op) : CHqlRealExpression(op) {}

    virtual bool isIndependentOfScope() override;
    virtual bool isIndependentOfScopeIgnoringInputs() override;
    virtual bool usesSelector(IHqlExpression * selector) override;
    virtual void gatherTablesUsed(CUsedTablesBuilder & used) override;
    virtual void gatherTablesUsed(HqlExprCopyArray & inScope) override;

protected:
    void cacheChildrenTablesUsed(CUsedTablesBuilder & used, unsigned from, unsigned to);
    void cacheInheritChildTablesUsed(IHqlExpression * ds, CUsedTablesBuilder & used, const HqlExprCopyArray & childInScopeTables);
    void cachePotentialTablesUsed(CUsedTablesBuilder & used);
    void cacheTablesProcessChildScope(CUsedTablesBuilder & used, bool ignoreInputs);
    void cacheTablesUsed();
    void cacheTableUseage(CUsedTablesBuilder & used, IHqlExpression * expr);

    void calcTablesUsed(CUsedTablesBuilder & used, bool ignoreInputs);

protected:
    CUsedTables usedTables;
    //unsigned spare = 0; // in 64bit this space is currently wasted
};

class HQL_API CHqlExpressionWithType : public CHqlExpressionWithTables
{
    friend HQL_API IHqlExpression *createOpenValue(node_operator op, ITypeInfo *type);
public:
    static CHqlExpression *makeExpression(node_operator op, ITypeInfo *type, HqlExprArray &operands);
    static CHqlExpression *makeExpression(node_operator op, ITypeInfo *type, const std::initializer_list<IHqlExpression *> &operands, bool expandCommas = false);

    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;

protected:
    inline CHqlExpressionWithType(node_operator op, ITypeInfo * _type) : CHqlExpressionWithTables(op), type(_type) {}
    
    CHqlExpressionWithType(node_operator op, ITypeInfo *type, HqlExprArray & ownedOperands);
    ~CHqlExpressionWithType();

protected:
    ITypeInfo *type;
};

class CHqlNamedExpression : public CHqlExpressionWithType
{
    friend HQL_API IHqlExpression *createOpenNamedValue(node_operator op, ITypeInfo *type, IIdAtom * id);
    friend HQL_API IHqlExpression *createNamedValue(node_operator op, ITypeInfo *type, IIdAtom * id, HqlExprArray & args);

protected:
    IIdAtom * id;

protected:
    CHqlNamedExpression(node_operator _op, ITypeInfo *_type, IIdAtom * _id);
    CHqlNamedExpression(node_operator _op, ITypeInfo *_type, IIdAtom * _id, HqlExprArray & _ownedOperands);

    virtual void sethash() override;
    virtual bool equals(const IHqlExpression & other) const override;

public:
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IAtom * queryName() const override { return lower(id); }
    virtual IIdAtom * queryId() const override { return id; }
};


class CHqlSelectBaseExpression : public CHqlRealExpression
{
public:
    static IHqlExpression * makeSelectExpression(IHqlExpression * left, IHqlExpression * right, IHqlExpression * attr);
    static IHqlExpression * makeSelectExpression(HqlExprArray & ownedOperands);

    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;

    virtual bool isIndependentOfScope() override;
    virtual bool isIndependentOfScopeIgnoringInputs() override;
    virtual bool usesSelector(IHqlExpression * selector) override;
    virtual void gatherTablesUsed(CUsedTablesBuilder & used) override;
    virtual void gatherTablesUsed(HqlExprCopyArray & inScope) override;

    virtual void calcNormalized() = 0;

protected:
    CHqlSelectBaseExpression();

    void setOperands(IHqlExpression * left, IHqlExpression * right, IHqlExpression * attr);
    void setOperands(HqlExprArray & _ownedOperands);
};

class CHqlNormalizedSelectExpression : public CHqlSelectBaseExpression
{
    friend class CHqlSelectBaseExpression;
public:
    virtual IHqlExpression *queryNormalizedSelector() override;
    virtual void calcNormalized() override;

protected:
    CHqlNormalizedSelectExpression() {}
};

class CHqlSelectExpression : public CHqlSelectBaseExpression
{
    friend class CHqlSelectBaseExpression;
public:
    virtual IHqlExpression *queryNormalizedSelector() override;
    virtual void calcNormalized() override;

protected:
    CHqlSelectExpression() {}

protected:
    HqlExprAttr normalized;
};

class CFileContents : public CInterfaceOf<IFileContents>
{
private:
    bool delayedRead = false;
    bool implicitlySigned = false;
    enum : byte { unchecked, unknown, dirty, clean } dirtyState = unchecked;
    timestamp_type ts = 0;
    Linked<IFile> file;
    MemoryAttr fileContents;
    Linked<ISourcePath> sourcePath;
    LinkedHqlExpr gpgSignature;
public:
    CFileContents(IFile * _file, ISourcePath * _sourcePath, bool _isSigned, IHqlExpression * _gpgSignature);
    CFileContents(const char *query, ISourcePath * _sourcePath, bool _isSigned, IHqlExpression * _gpgSignature, timestamp_type _ts);
    CFileContents(unsigned len, const char *query, ISourcePath * _sourcePath, bool _isSigned, IHqlExpression * _gpgSignature, timestamp_type _ts);

    virtual IFile * queryFile() override { return file; }
    virtual ISourcePath * querySourcePath() override { return sourcePath; }
    virtual const char * getText() override
    {
        ensureLoaded();
        return (const char *)fileContents.get();
    }
    //NB: This is the string length, so subtract one to remove the null terminator
    virtual size32_t length() override
    { 
        ensureLoaded();
        return (size32_t)(fileContents.length()-1);
    }
    virtual bool isImplicitlySigned() override
    {
        return implicitlySigned;
    }
    virtual IHqlExpression * queryGpgSignature() override
    {
        return gpgSignature.get();
    }
    virtual bool isDirty() override
    {
        if (dirtyState==unchecked)
        {
            dirtyState = unknown;
            if (sourcePath)
            {
                Owned<IPipeProcess> pipe = createPipeProcess();
                VStringBuffer statusCmd("git status --porcelain -z -- %s", sourcePath->queryStr());
                if (!pipe->run("git", statusCmd, ".", false, true, false, 0, false))
                {
                    UWARNLOG("Failed to run git status for %s", sourcePath->queryStr());
                }
                else
                {
                    try
                    {
                        unsigned retcode = pipe->wait();
                        StringBuffer buf;
                        Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
                        readSimpleStream(buf, *pipeReader, 128);
                        if (retcode)
                            UWARNLOG("Failed to run git status for %s: returned %d (%s)", sourcePath->queryStr(), retcode, buf.str());
                        else if (buf.length())
                            dirtyState = dirty;
                        else
                            dirtyState = clean;
                    }
                    catch (IException *e)
                    {
                        EXCLOG(e, "Exception running git status");
                        e->Release();
                    }
                }
            }
        }
        return dirtyState==dirty;
    }
    virtual timestamp_type getTimeStamp();
private:
    bool preloadFromFile();
    void ensureLoaded();
    void ensureUtf8(MemoryBuffer & contents);
    void setContents(size32_t len, const char * query);
    void setContentsOwn(MemoryBuffer & contents);
};

class HQL_API CHqlAnnotation: public CHqlExpression
{
protected:
    IHqlExpression *body;

protected:
    virtual void sethash() override;

public:
    CHqlAnnotation(IHqlExpression * _body);
    ~CHqlAnnotation();

//Following methods are implemented in CHqlExpression
//  virtual node_operator getOperator() const;
//  virtual IInterface*         queryTransformExtra();
//  virtual void                setTransformExtra(IInterface *);
//  virtual void                setTransformExtraOwned(IInterface *);
//  virtual void                setTransformExtraUnlinked(IInterface *);
//  virtual IHqlExpression *closeExpr();
//  virtual bool isExprClosed() const;
//  virtual void                addObserver(IObserver & observer);
//  virtual void                removeObserver(IObserver & observer);
//  virtual unsigned            getHash() const;
//  virtual ITypeInfo *queryType() const;
//  virtual ITypeInfo *getType();
//  virtual void unwindList(HqlExprArray &dst, node_operator);
//  virtual bool isAggregate();
//  virtual IHqlDataset *queryDeepestTable();

//Using methods in CHqlExpression should realy be inline in IHqlExpression, all based on type
//  virtual bool isBoolean();
//  virtual bool isDataset();
//  virtual bool isDatarow();
//  virtual bool isRecord();
//  virtual bool isAction();
//  virtual bool isTransform();
//  virtual bool isList();
//  virtual bool isFunction();
//  virtual ITypeInfo *queryRecordType();
//  virtual IHqlExpression *queryRecord();

//  virtual bool isField();

    virtual annotate_kind getAnnotationKind() const = 0;

//Following are redirected to body
    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;
    virtual IAtom * queryName() const override;
    virtual IIdAtom * queryId() const override;
    virtual bool isScope() override;
    virtual bool isType() override;
    virtual bool isConstant() override;
    virtual bool isMacro() override;
    virtual bool isGroupAggregateFunction() override;
    virtual bool isPure() override;
    virtual bool isAttribute() const override;
    virtual unsigned getInfoFlags() const override;
    virtual unsigned getInfoFlags2() const override;
    virtual int  getStartLine() const override;
    virtual int  getStartColumn() const override;
    virtual IPropertyTree * getDocumentation() const override;
    virtual StringBuffer &toString(StringBuffer &ret) override;
    virtual IHqlExpression *queryChild(unsigned idx) const override;
    virtual unsigned numChildren() const override;
    virtual bool isIndependentOfScope() override;
    virtual bool isIndependentOfScopeIgnoringInputs() override;
    virtual bool usesSelector(IHqlExpression * selector) override;
    virtual void gatherTablesUsed(CUsedTablesBuilder & used) override;
    virtual void gatherTablesUsed(HqlExprCopyArray & inScope) override;
    virtual IValue *queryValue() const override;
    virtual IInterface *queryUnknownExtra() override;
    virtual unsigned __int64 querySequenceExtra() override;
    virtual IHqlDataset *queryDataset() override;
    virtual IHqlScope *queryScope() override;
    virtual IHqlSimpleScope *querySimpleScope() override;
    virtual IHqlExpression *queryFunctionDefinition() const override;
    virtual IHqlExpression *queryExternalDefinition() const override;
    virtual IHqlExpression *queryNormalizedSelector() override;
    virtual IHqlExpression *queryAttribute(IAtom * propName) const override;
    virtual IHqlExpression *queryProperty(ExprPropKind kind) override;
    virtual IHqlExpression * clone(HqlExprArray &) override;
    virtual IHqlExpression * cloneAllAnnotations(IHqlExpression * body) override;
    virtual IIdAtom * queryFullContainerId() const override;
    virtual bool isFullyBound() const override;
    virtual IHqlExpression *addOperand(IHqlExpression *) override;
    virtual StringBuffer& getTextBuf(StringBuffer& buf) override;
    virtual IFileContents * queryDefinitionText() const override;
    virtual bool isExported() const override;
    virtual unsigned getSymbolFlags() const override;
    virtual unsigned            getCachedEclCRC() override;

//Actually implemented by this class
    virtual bool equals(const IHqlExpression & other) const override;
    virtual IHqlExpression *queryBody(bool singleLevel = false) override;
    virtual IHqlExpression * queryAnnotationParameter(unsigned i) const override;

    virtual void addProperty(ExprPropKind kind, IInterface * value) override;
    virtual IInterface * queryExistingProperty(ExprPropKind kind) const override;
};


class HQL_API CHqlSymbolAnnotation : public CHqlAnnotation, public IHqlNamedAnnotation
{
public:
    IMPLEMENT_IINTERFACE_USING(CHqlAnnotation)

    virtual IAtom * queryName() const override { return lower(id); }
    virtual IIdAtom * queryId() const override { return id; }
    virtual IIdAtom * queryFullContainerId() const override { return moduleId; }
    virtual IHqlExpression *queryFunctionDefinition() const override;
    virtual unsigned getSymbolFlags() const override;

    virtual annotate_kind getAnnotationKind() const override { return annotate_symbol; }
    virtual IHqlAnnotation * queryAnnotation() override { return this; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) override;

    virtual bool equals(const IHqlExpression & other) const override;

//interface IHqlNamedAnnotation
    virtual IHqlExpression * queryExpression() override;
    virtual bool isExported() const override { return (symbolFlags&ob_exported)!=0; };
    virtual bool isShared() const override { return (symbolFlags&ob_shared)!=0; };
    virtual bool isPublic() const override { return (symbolFlags&(ob_shared|ob_exported))!=0; };
    virtual bool isVirtual() const override { return (symbolFlags & ob_virtual)!=0; };
    virtual void setRepositoryFlags(unsigned _flags) override { symbolFlags |= (_flags & ob_registryflags); }

protected:
    CHqlSymbolAnnotation(IIdAtom * _id, IIdAtom * _moduleId, IHqlExpression *_expr, IHqlExpression *_funcdef, unsigned _obFlags);
    ~CHqlSymbolAnnotation();

    virtual void sethash() override;

protected:
    IIdAtom * id;
    IIdAtom * moduleId;
    IHqlExpression *funcdef;
    unsigned symbolFlags;
};

class HQL_API CHqlSimpleSymbol : public CHqlSymbolAnnotation
{
public:
    static IHqlExpression * makeSymbol(IIdAtom * _id, IIdAtom * _moduleId, IHqlExpression *_expr, IHqlExpression *_funcdef, unsigned _obFlags);

//interface IHqlNamedAnnotation
    virtual IFileContents * getBodyContents() override { return NULL; }
    virtual IHqlExpression * cloneSymbol(IIdAtom * optname, IHqlExpression * optnewbody, IHqlExpression * optnewfuncdef, HqlExprArray * optargs) override;
    virtual int getStartLine() const override { return 0; }
    virtual int getStartColumn() const override { return 0; }
    virtual int getStartPos() const override { return 0; }
    virtual int getBodyPos() const override { return 0; }
    virtual int getEndPos() const override { return 0; }

protected:
    CHqlSimpleSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, IHqlExpression *_funcdef, unsigned _obFlags);
};

class HQL_API CHqlNamedSymbol: public CHqlSymbolAnnotation
{
public:
    static CHqlNamedSymbol *makeSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, bool _exported, bool _shared, unsigned _flags);
    static CHqlNamedSymbol *makeSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, IHqlExpression *_funcdef, bool _exported, bool _shared, unsigned _flags, IFileContents *_text, int lineno, int column, int _startpos, int _bodypos, int _endpos);

    virtual ISourcePath * querySourcePath() const override;
    
    virtual StringBuffer& getTextBuf(StringBuffer& buf) override { return buf.append(text->length(),text->getText()); }
    virtual IFileContents * queryDefinitionText() const override;
    virtual int  getStartLine() const override { return startLine; }
    virtual int  getStartColumn() const override { return startColumn; }
    virtual int getStartPos() const override { return startpos; }
    virtual int getBodyPos() const override { return bodypos; }
    virtual int getEndPos() const override
    {
        if ((endpos == 0) && text)
            return text->length();
        return endpos;
    }

//interface IHqlNamedAnnotation
    virtual IFileContents * getBodyContents() override;
    virtual IHqlExpression * cloneSymbol(IIdAtom * optname, IHqlExpression * optnewbody, IHqlExpression * optnewfuncdef, HqlExprArray * optargs) override;

protected:
    CHqlNamedSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, bool _exported, bool _shared, unsigned _obFlags);
    CHqlNamedSymbol(IIdAtom * _id, IIdAtom * _module, IHqlExpression *_expr, IHqlExpression *_funcdef, bool _exported, bool _shared, unsigned _flags, IFileContents *_text, int _startLine, int _startColumn, int _startpos, int _bodypos, int _endpos);

protected:
    Linked<IFileContents> text;
    int startpos;
    int bodypos;
    int endpos;
    int startLine;
    unsigned short startColumn;
};

class HQL_API CHqlAnnotationWithOperands: public CHqlAnnotation
{
public:
    virtual bool equals(const IHqlExpression & other) const override;

protected:
    CHqlAnnotationWithOperands(IHqlExpression * _expr, HqlExprArray & _args);

    virtual void sethash() override;
};

class HQL_API CHqlMetaAnnotation: public CHqlAnnotationWithOperands
{
public:
    static IHqlExpression * createAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args);

    virtual annotate_kind getAnnotationKind() const override { return annotate_meta; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) override;

protected:
    CHqlMetaAnnotation(IHqlExpression * _expr, HqlExprArray & _args);
};

class HQL_API CHqlParseMetaAnnotation: public CHqlAnnotationWithOperands
{
public:
    static IHqlExpression * createAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args);

    virtual annotate_kind getAnnotationKind() const override { return annotate_parsemeta; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) override;

protected:
    CHqlParseMetaAnnotation(IHqlExpression * _expr, HqlExprArray & _args);
};

class HQL_API CHqlLocationAnnotation: public CHqlAnnotation
{
public:
    static IHqlExpression * createLocationAnnotation(IHqlExpression * _ownedBody, ISourcePath * _sourcePath, int _lineno, int _column);

    virtual annotate_kind getAnnotationKind() const override { return annotate_location; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) override;
    virtual bool equals(const IHqlExpression & other) const override;

    virtual ISourcePath * querySourcePath() const override { return sourcePath; }
    virtual int  getStartLine() const override { return lineno; }
    virtual int  getStartColumn() const override { return column; }

protected:
    CHqlLocationAnnotation(IHqlExpression * _expr, ISourcePath * _sourcePath, int _lineno, int _column);

    virtual void sethash() override;

protected:
    Linked<ISourcePath> sourcePath;
    unsigned lineno;
    unsigned column;
};

class HQL_API CHqlAnnotationExtraBase: public CHqlAnnotation
{
public:
    virtual bool equals(const IHqlExpression & other) const override;

protected:
    CHqlAnnotationExtraBase(IHqlExpression * _expr, IInterface * _ownedExtra);
    virtual void sethash() override;

protected:
    Owned<IInterface> extra;
};

class HQL_API CHqlWarningAnnotation: public CHqlAnnotationExtraBase
{
public:
    static IHqlExpression * createWarningAnnotation(IHqlExpression * _ownedBody, IError * _ownedWarning);
    virtual annotate_kind getAnnotationKind() const override { return annotate_warning; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) override;

    inline IError * queryWarning() const { return static_cast<IError *>(extra.get()); }

protected:
    CHqlWarningAnnotation(IHqlExpression * _expr, IError * _ownedWarning);
};

class HQL_API CHqlJavadocAnnotation: public CHqlAnnotationExtraBase
{
public:
    static IHqlExpression * createJavadocAnnotation(IHqlExpression * _ownedBody, IPropertyTree * _ownedWarning);
    virtual annotate_kind getAnnotationKind() const override { return annotate_javadoc; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) override;

    virtual IPropertyTree * getDocumentation() const override { return LINK(queryDocumentation()); }

    inline IPropertyTree * queryDocumentation() const { return static_cast<IPropertyTree *>(extra.get()); }

protected:
    CHqlJavadocAnnotation(IHqlExpression * _expr, IPropertyTree * _ownedJavadoc);
};

class CHqlField: public CHqlExpressionWithType
{
private:
    IIdAtom * id;

    virtual void sethash();
    virtual bool equals(const IHqlExpression & other) const;
    void onCreateField();
public:
    CHqlField(IIdAtom *, ITypeInfo *type, IHqlExpression *defaultValue);
    CHqlField(IIdAtom * _id, ITypeInfo *_type, HqlExprArray &_ownedOperands);

    virtual StringBuffer &toString(StringBuffer &ret) override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IAtom * queryName() const override { return lower(id); }
    virtual IIdAtom * queryId() const override { return id; }
};

class CHqlRow: public CHqlExpressionWithType
{
    CHqlRow(node_operator op, ITypeInfo * type, HqlExprArray & _operands);

    HqlExprAttr     normalized;
public:
    static CHqlRow *makeRow(node_operator op, ITypeInfo * type, HqlExprArray & _operands);
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IHqlSimpleScope *querySimpleScope() override;
    virtual IHqlDataset *queryDataset() override;
    virtual IAtom * queryName() const override;
    virtual IHqlExpression *queryNormalizedSelector() override;
};


class CHqlExternal: public CHqlExpressionWithType
{
    IIdAtom * id;

    CHqlExternal(IIdAtom * id, ITypeInfo *, HqlExprArray &_ownedOperands);
    virtual bool equals(const IHqlExpression & other) const override;
public:
    static CHqlExternal *makeExternalReference(IIdAtom * id, ITypeInfo *, HqlExprArray &_ownedOperands);
    virtual IAtom * queryName() const override { return lower(id);  }
    virtual IIdAtom * queryId() const override { return id; }
};

class CHqlExternalCall: public CHqlExpressionWithType
{
protected:
    OwnedHqlExpr funcdef;

    CHqlExternalCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &parms);
    virtual IAtom * queryName() const override { return funcdef->queryName(); }
    virtual IIdAtom * queryId() const override { return funcdef->queryId(); }
    virtual void sethash() override;
    virtual bool equals(const IHqlExpression & other) const override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IHqlExpression *queryExternalDefinition() const override;
public:
    static IHqlExpression *makeExternalCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &operands);
};

class CHqlExternalDatasetCall: public CHqlExternalCall, implements IHqlDataset
{
    friend class CHqlExternalCall;

    CHqlExternalDatasetCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &_ownedOperands) : CHqlExternalCall(_funcdef, type, _ownedOperands) {}
    IMPLEMENT_IINTERFACE_O_USING(CHqlExternalCall)

    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IHqlDataset *queryDataset() override { return this; }
    virtual IHqlExpression * queryExpression() override { return this; }

//interface IHqlDataset
    virtual IHqlDataset* queryTable() override { return this; }
    virtual IHqlSimpleScope *querySimpleScope() override { return queryRecord()->querySimpleScope(); }
    virtual IHqlDataset * queryRootTable() override { return this; }
    virtual IHqlExpression * queryContainer() override { return NULL; }

    virtual bool isAggregate() override { return false; }
};

#ifdef THREAD_SAFE_SYMBOLS
class SymbolTable
{
    typedef MapXToMyClass<IAtom *, IAtom *, IHqlExpression> MAP;
public:
    inline void setValue(IAtom * name, IHqlExpression * value)
    {
        CriticalBlock block(cs);
        map.setValue(name, value);
    }
    inline bool contain(IAtom * name) const
    {
        CriticalBlock block(cs);
        IHqlExpression * ret = map.getValue(name);
        return (ret != NULL);
    }
    inline IHqlExpression * getLinkedValue(IAtom * name) const
    {
        CriticalBlock block(cs);
        IHqlExpression * ret = map.getValue(name);
        if (ret)
            ret->Link();
        return ret;
    }
    inline IHqlExpression * mapToValue(IMapping * mapping) const   {
        return map.mapToValue(mapping);
    }
    inline void kill()
    {
        CriticalBlock block(cs);
        map.kill();
    }
    inline void remove(IAtom * name)
    {
        CriticalBlock block(cs);
        map.remove(name);
    }
    inline HashTable & lock()
    {
        cs.enter();
        return map;
    }
    inline void unlock()
    {
        cs.leave();
    }
    inline unsigned count() const
    {
        CriticalBlock block(cs);
        return map.count();
    }

    MAP::ConstHashItem begin() const { return map.begin(); }
    MAP::ConstHashItem end() const { return map.end(); }

protected:
    MAP map;
    mutable CriticalSection cs;
};

class SymbolTableLock
{
public:
    SymbolTableLock(SymbolTable & _table) : table(_table)
    {
        table.lock();
    }
    ~SymbolTableLock()
    {
        table.unlock();
    }
protected:
    SymbolTable & table;
};

class SymbolTableIterator : public HashIterator
{
public:
    SymbolTableIterator(SymbolTable & _table) : HashIterator(_table.lock()), table(_table)
    {
    }
    ~SymbolTableIterator()
    {
        table.unlock();
    }
protected:
    SymbolTable & table;
};
#else
class SymbolTable : public MapXToMyClass<IAtom *, IAtom *, IHqlExpression>
{
public:
    inline IHqlExpression * getLinkedValue(IAtom * name) const
    {
        IHqlExpression * ret = getValue(name);
        if (ret)
            ret->Link();
        return ret;
    }
};
typedef HashIterator SymbolTableIterator;
class SymbolTableLock
{
public:
    SymbolTableLock(SymbolTable &) {}
    ~SymbolTableLock() {}
};


#endif

inline IHqlExpression * lookupSymbol(SymbolTable & symbols, IIdAtom * searchName, bool sharedOK)
{
    OwnedHqlExpr ret = symbols.getLinkedValue(lower(searchName));

    if (!ret)
        return NULL; 
    
    if (!(ret->isExported() || sharedOK))
        return NULL;

    return ret.getClear();
}

typedef class MapXToMyClassViaBase<IAtom *, IAtom *, CHqlField, IHqlExpression> FieldTable;

typedef class MapXToMyClassViaBase<IAtom *, IAtom *, IFileContents, IFileContents> FileContentsTable;

class CHqlDelayedCall: public CHqlExpressionWithType
{
    OwnedHqlExpr funcdef;
protected:
    CHqlDelayedCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &parms);
    virtual IAtom * queryName() const override { return funcdef->queryName(); }
    virtual IIdAtom * queryId() const override { return funcdef->queryId(); }
    virtual void sethash() override;
    virtual bool equals(const IHqlExpression & other) const override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IHqlExpression *queryFunctionDefinition() const override { return funcdef; };
public:
    static IHqlExpression *makeDelayedCall(IHqlExpression * _funcdef, HqlExprArray &operands);
};


class CHqlDelayedDatasetCall: public CHqlDelayedCall, implements IHqlDataset
{
    friend class CHqlDelayedCall;

    CHqlDelayedDatasetCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &parms) : CHqlDelayedCall(_funcdef, type, parms) {}
    IMPLEMENT_IINTERFACE_O_USING(CHqlDelayedCall)

    virtual IHqlDataset *queryDataset() override { return this; }
    virtual IHqlExpression * queryExpression() override { return this; }

//interface IHqlDataset
    virtual IHqlDataset* queryTable() override               { return this; }
    virtual IHqlSimpleScope *querySimpleScope() override     { return queryRecord()->querySimpleScope(); }
    virtual IHqlDataset * queryRootTable() override          { return this; }
    virtual IHqlExpression * queryContainer() override       { return NULL; }

    virtual bool isAggregate() override                      { return false; }
};

class CHqlDelayedScopeCall: public CHqlDelayedCall, implements IHqlScope
{
public:
    friend class CHqlDelayedCall;

    CHqlDelayedScopeCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &parms);
    IMPLEMENT_IINTERFACE_O_USING(CHqlDelayedCall)

    virtual void defineSymbol(IIdAtom * id, IIdAtom * moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags, IFileContents *fc, int lineno, int column, int _startpos, int _bodypos, int _endpos) override { throwUnexpected(); }
    virtual void defineSymbol(IIdAtom * id, IIdAtom * moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags) override { throwUnexpected(); }
    virtual void defineSymbol(IHqlExpression * expr) override { throwUnexpected(); }
    virtual void removeSymbol(IIdAtom * id) override { throwUnexpected(); }

    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) override;
    virtual IFileContents * lookupContents(IIdAtom * searchName, HqlLookupContext & ctx) override;
    virtual void    getSymbols(HqlExprArray& exprs) const override;
    virtual IAtom *   queryName() const override;
    virtual IIdAtom * queryId() const override;
    virtual const char * queryFullName() const override{ throwUnexpected(); }
    virtual ISourcePath * querySourcePath() const override { throwUnexpected(); }
    virtual bool hasBaseClass(IHqlExpression * searchBase) override;
    virtual bool allBasesFullyBound() const override { return false; } // Assume the worst
    virtual bool isEquivalentScope(const IHqlScope & other) const override { return this == &other; }

    virtual void ensureSymbolsDefined(HqlLookupContext & ctx) override { }

    virtual bool isImplicit() const override { return false; }
    virtual bool isPlugin() const override { return false; }
    virtual int getPropInt(IAtom * a, int def) const override { return def; }
    virtual bool getProp(IAtom * a, StringBuffer &ret) const override { return false; }

    using CHqlDelayedCall::clone;
    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) override { throwUnexpected(); }

    virtual IHqlScope * queryConcreteScope() override;
    virtual IHqlScope * queryResolvedScope(HqlLookupContext * context) override { return this; }

    virtual IHqlExpression * queryExpression() override { return this; }
    virtual IHqlScope * queryScope() override { return this; }

protected:
    IHqlScope * typeScope;
};

//This is abstract.  Either CHqlRemoteScope or CHqlLocalScope should be used......
class HQL_API CHqlScope : public CHqlExpressionWithType, implements IHqlScope, implements ITypeInfo
{
protected:
    Owned<IFileContents> text;
    IIdAtom * id;
    IIdAtom * containerId;
    StringAttr fullName;                //Fully qualified name of this nested module   E.g.: PARENT.CHILD.GRANDCHILD
    SymbolTable symbols;

    virtual bool equals(const IHqlExpression & other) const;

public:
    CHqlScope(node_operator _op, IIdAtom * _id, const char * _fullName);
    CHqlScope(node_operator _op);
    CHqlScope(IHqlScope* scope);
    ~CHqlScope();
    IMPLEMENT_IINTERFACE_O_USING(CHqlExpression)

    IHqlScope * cloneAndClose(HqlExprArray & children, HqlExprArray & symbols);

//interface IHqlExpression
    virtual IHqlScope *queryScope() override { return this; }
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual void sethash() override;

//interface IHqlScope
    virtual IHqlExpression * queryExpression() override { return this; }
    virtual void defineSymbol(IIdAtom * id, IIdAtom * moduleName, IHqlExpression *value, bool isPublic, bool isShared, unsigned symbolFlags, IFileContents *, int lineno, int column, int _startpos, int _bodypos, int _endpos) override;
    virtual void defineSymbol(IIdAtom * id, IIdAtom * moduleName, IHqlExpression *value, bool exported, bool shared, unsigned symbolFlags) override;
    virtual void defineSymbol(IHqlExpression * expr) override;
    virtual void removeSymbol(IIdAtom * id) override;

    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) override;
    virtual IFileContents * lookupContents(IIdAtom * searchName, HqlLookupContext & ctx) override;

    virtual IAtom * queryName() const override {return lower(id);}
    virtual IIdAtom * queryId() const override { return id; }
    virtual const char * queryFullName() const override  { return fullName; }
    virtual IIdAtom * queryFullContainerId() const override { return containerId; }
    virtual ISourcePath * querySourcePath() const override { return text ? text->querySourcePath() : NULL; }
    virtual bool isEquivalentScope(const IHqlScope & other) const override { return this == &other; }

    virtual void ensureSymbolsDefined(HqlLookupContext & ctx) override { }
    virtual bool isImplicit() const override { return false; }
    virtual bool isPlugin() const override { return false; }
    virtual int getPropInt(IAtom *, int) const override;
    virtual bool getProp(IAtom *, StringBuffer &) const override;

    virtual void    getSymbols(HqlExprArray& exprs) const override;
    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) override { throwUnexpected(); }
    virtual bool hasBaseClass(IHqlExpression * searchBase) override;
    virtual IHqlScope * queryResolvedScope(HqlLookupContext * context) override { return this; }
    virtual IFileContents * queryDefinitionText() const override { return text; }

//ITypeInfo
    virtual type_t getTypeCode() const override          { return type_scope; }
    virtual size32_t getSize() override                  { return 0; }
    virtual unsigned getAlignment() override             { return 1; }
    virtual unsigned getPrecision() override             { return 0; }
    virtual unsigned getBitSize() override               { return 0; }
    virtual unsigned getStringLen() override             { return 0; }
    virtual unsigned getDigits() override                { return 0; }
    virtual IValue * castFrom(bool isSignedValue, __int64 value) override    { return NULL; }
    virtual IValue * castFrom(double value) override  { return NULL; }
    virtual IValue * castFrom(size32_t len, const char * text) override  { return NULL; }
    virtual IValue * castFrom(size32_t len, const UChar * text) override  { return NULL; }
    virtual StringBuffer &getECLType(StringBuffer & out) override  { return out.append("MODULE"); }
    virtual StringBuffer &getDescriptiveType(StringBuffer & out) override { return out.append("MODULE"); }
    virtual const char *queryTypeName() override         { return str(id); }
    virtual unsigned getCardinality() override           { return 0; }
    virtual bool isInteger() override                    { return false; }
    virtual bool isReference() override                  { return false; }
    virtual bool isScalar() override                     { return false; }
    virtual bool isSigned() override                     { return false; }
    virtual bool isSwappedEndian() override              { return false; }
    virtual ICharsetInfo * queryCharset() override       { return NULL; }
    virtual ICollationInfo * queryCollation() override   { return NULL; }
    virtual IAtom * queryLocale() override                 { return NULL; }
    virtual ITypeInfo * queryChildType() override        { return NULL; }
    virtual ITypeInfo * queryPromotedType() override     { return this; }
    virtual ITypeInfo * queryTypeBase() override         { return this; }
    virtual typemod_t queryModifier() override           { return typemod_none; }
    virtual IInterface * queryModifierExtra() override   { return NULL; }
    virtual unsigned getCrc() override;
    virtual bool assignableFrom(ITypeInfo * source) override;
    virtual IHqlExpression * castToExpression() override { return this; }
    virtual IHqlScope * castToScope() override { return this; }

    virtual void serialize(MemoryBuffer &) override { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &) override { UNIMPLEMENTED; }

protected:
    void initContainer();
    void throwRecursiveError(IIdAtom * id);
};

class HQL_API CHqlRemoteScope : public CHqlScope, implements IHqlRemoteScope
{
protected:
    IEclRepositoryCallback * ownerRepository;
    IProperties* props;
    Owned<IHqlScope> resolved;
    Linked<IEclSource> eclSource;
    CriticalSection generalCS;
    bool loadedAllSymbols;

protected:
    void preloadSymbols(HqlLookupContext & ctx, bool forceAll);
    void doParseScopeText(HqlLookupContext & ctx);
    IHqlExpression * repositoryLoadSymbol(IIdAtom * attrName);
    void repositoryLoadModule(HqlLookupContext & ctx, bool forceAll);
    void noteExternalLookup(HqlLookupContext & ctx, IHqlExpression * expr);

    virtual bool equals(const IHqlExpression & other) const override;

public:
    CHqlRemoteScope(IIdAtom * _id, const char * _fullName, IEclRepositoryCallback *_repository, IProperties* _props, IFileContents * _text, bool _lazy, IEclSource * _eclSource);
    ~CHqlRemoteScope();
    IMPLEMENT_IINTERFACE_O_USING(CHqlScope)

    virtual IHqlScope * queryScope() override { return this; }    // remove ambiguous call

//interface IHqlExpression
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual void sethash() override;

//interface IHqlScope
    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) override;
    virtual IFileContents * lookupContents(IIdAtom * searchName, HqlLookupContext & ctx) override;
    virtual void ensureSymbolsDefined(HqlLookupContext & ctx) override;
    virtual void defineSymbol(IHqlExpression * expr) override;
    using CHqlScope::defineSymbol;

    virtual bool allBasesFullyBound() const override { return true; }
    virtual IHqlScope * queryConcreteScope() override { return this; }
    virtual IFileContents * queryDefinitionText() const override;

    virtual void getSymbols(HqlExprArray& exprs) const override;

    virtual bool isImplicit() const override;
    virtual bool isPlugin() const override;
    virtual int getPropInt(IAtom *, int) const override;
    virtual bool getProp(IAtom *, StringBuffer &) const override;
    virtual void setProp(IAtom *, const char *) override;
    virtual void setProp(IAtom *, int) override;
    virtual IEclSource * queryEclSource() const override { return eclSource; }
};

class HQL_API CHqlLocalScope : public CHqlScope
{
protected:
    virtual bool equals(const IHqlExpression & other) const;

public:
    CHqlLocalScope(node_operator _op, IIdAtom * _id, const char * _fullName);
    CHqlLocalScope(IHqlScope* scope);

//interface IHqlExpression
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual void sethash() override;

//interface IHqlScope
    virtual bool allBasesFullyBound() const override { return true; }
    virtual IHqlScope * queryConcreteScope() override { return this; }
    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) override;
};

class HQL_API CHqlVirtualScope : public CHqlScope
{
protected:
    Owned<IHqlScope> concrete;
    bool isAbstract;
    bool complete;
    bool containsVirtual;
    bool allVirtual;
    bool fullyBoundBase;

protected:
    virtual bool equals(const IHqlExpression & other) const;
    IHqlScope * deriveConcreteScope();
    IHqlExpression * lookupBaseSymbol(IHqlExpression * & definitionModule, IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx);
    void resolveUnboundSymbols();
    void ensureVirtualSeq();

public:
    CHqlVirtualScope(IIdAtom * _id, const char * _fullName);

//interface IHqlExpression
    virtual IHqlExpression *addOperand(IHqlExpression * arg) override;
    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IHqlExpression *closeExpr() override;
    virtual void defineSymbol(IHqlExpression * expr) override;
    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) override;
    virtual void sethash() override;

//interface IHqlScope
    virtual bool allBasesFullyBound() const override { return fullyBoundBase; }
    virtual IHqlScope * queryConcreteScope() override { return containsVirtual ? concrete.get() : this; }
};


class HQL_API CHqlMergedScope : public CHqlScope
{
public:
    CHqlMergedScope(IIdAtom * _id, const char * _fullName) : CHqlScope(no_mergedscope, _id, _fullName) { mergedAll = false; }

    void addScope(IHqlScope * scope);

    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) override;
    virtual void ensureSymbolsDefined(HqlLookupContext & ctx) override;
    virtual bool allBasesFullyBound() const override;
    virtual bool isImplicit() const override;
    virtual bool isPlugin() const override;
    virtual bool isEquivalentScope(const IHqlScope & other) const override;
    virtual IHqlScope * queryConcreteScope() override { return this; }

protected:
    CriticalSection cs;
    HqlScopeArray mergedScopes;
    bool mergedAll;
};

//MORE: I'm not 100% sure why this is different from a CLocalScope... it should be merged
class CHqlContextScope : public CHqlScope
{
protected:
    SymbolTable defined;

public:
    CHqlContextScope() : CHqlScope(no_scope) { }
    // copy constructor
    CHqlContextScope(IHqlScope* scope);

    virtual void defineSymbol(IIdAtom * id, IIdAtom * moduleName, IHqlExpression *value,bool exported, bool shared, unsigned symbolFlags) override
    {  defined.setValue(lower(id),value);  }

    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) override
{  return defined.getLinkedValue(lower(searchName)); }

    virtual IHqlScope * queryConcreteScope() override { return this; }
    virtual bool allBasesFullyBound() const override { return false; }

};

class CHqlTemplateFunctionContext : public CHqlExpressionWithType
{
public:
    CHqlTemplateFunctionContext(IHqlExpression* expr,  IHqlScope* scope)
        : CHqlExpressionWithType(no_template_context,expr->getType()), context(scope)
    {
        addOperand(expr);
    }

    virtual IHqlScope* queryScope() override { return context; }
    virtual IHqlDataset* queryDataset() override { return queryChild(0)->queryDataset(); }
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;

protected: 
    Owned<IHqlScope> context;             // allows symbols to be resolved when reparsing.
    virtual void sethash();
};

class CHqlConstant : public CHqlRealExpression
{
protected:
    IValue *val;

    CHqlConstant(IValue *);
    virtual ~CHqlConstant();
    virtual bool equals(const IHqlExpression & other) const override;
    virtual void sethash() override;
public:
    static CHqlConstant HQL_API *makeConstant(IValue *);
    virtual StringBuffer &toString(StringBuffer &ret) override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IValue *queryValue() const override { return val; }
    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;

    virtual bool isIndependentOfScope() override { return true; }
    virtual bool isIndependentOfScopeIgnoringInputs() override { return true; }
    virtual bool usesSelector(IHqlExpression * selector) override { return false; }
    virtual void gatherTablesUsed(CUsedTablesBuilder & used) override {}
    virtual void gatherTablesUsed(HqlExprCopyArray & inScope) override {}
};

class CHqlParameter : public CHqlExpressionWithType
{
protected:
    unique_id_t uid;
    IIdAtom * id;
    unsigned idx;

    CHqlParameter(IIdAtom * id, unsigned idx, ITypeInfo *type);
    ~CHqlParameter();

    virtual bool equals(const IHqlExpression & other) const override;
    virtual void sethash() override;
public:
    static IHqlExpression *makeParameter(IIdAtom * id, unsigned idx, ITypeInfo *type, HqlExprArray & attrs);

//interface IHqlExpression

    virtual StringBuffer &toString(StringBuffer &ret) override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IHqlSimpleScope *querySimpleScope() override;
    virtual IAtom * queryName() const override { return lower(id);  }
    virtual IIdAtom * queryId() const override { return id; }
    virtual unsigned __int64 querySequenceExtra() override { return idx; }
};

class CHqlDatasetParameter : public CHqlParameter, implements IHqlDataset
{
public:
    IMPLEMENT_IINTERFACE_O_USING(CHqlParameter)

    CHqlDatasetParameter(IIdAtom * id, unsigned idx, ITypeInfo *type)
     : CHqlParameter(id, idx, type) { }

//IHqlExpression
    virtual IHqlDataset *queryDataset() override { return this; }
    virtual IHqlExpression * queryExpression() override { return this; }

//IHqlDataset
    virtual IHqlDataset* queryTable() override { return this; }
    virtual IHqlDataset * queryRootTable() override { return this; }
    virtual IHqlExpression * queryContainer() override { return NULL; }
    virtual bool isAggregate() override { return false; }

//Overlapped methods
    virtual IHqlSimpleScope* querySimpleScope() override { return CHqlParameter::querySimpleScope(); }
};

class CHqlDictionaryParameter : public CHqlParameter, implements IHqlDataset
{
public:
    IMPLEMENT_IINTERFACE_O_USING(CHqlParameter)

    CHqlDictionaryParameter(IIdAtom * id, unsigned idx, ITypeInfo *type)
     : CHqlParameter(id, idx, type) { }

//IHqlExpression
    virtual IHqlDataset *queryDataset() override { return this; }
    virtual IHqlExpression * queryExpression() override { return this; }

//IHqlDataset
    virtual IHqlDataset* queryTable() override { return this; }
    virtual IHqlDataset * queryRootTable() override { return this; }
    virtual IHqlExpression * queryContainer() override { return NULL; }

    virtual bool isAggregate() override { return false; }

//Overlapped methods
    virtual IHqlSimpleScope* querySimpleScope() override { return CHqlParameter::querySimpleScope(); }
};

class CHqlScopeParameter : public CHqlScope
{
protected:
    unique_id_t uid;
    IHqlScope * typeScope;
    unsigned idx;

    virtual void sethash() override;
public:
    CHqlScopeParameter(IIdAtom * id, unsigned idx, ITypeInfo *type);

//IHqlExpression
    virtual bool assignableFrom(ITypeInfo * source) override;
    virtual bool equals(const IHqlExpression & other) const override;
    virtual StringBuffer &toString(StringBuffer &ret) override;

//IHqlScope
    virtual void defineSymbol(IHqlExpression * expr) override { throwUnexpected(); }
    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) override;

    virtual void getSymbols(HqlExprArray& exprs) const override { typeScope->getSymbols(exprs); }
    virtual IHqlScope * queryConcreteScope() override { return NULL; }
    virtual bool allBasesFullyBound() const override { return false; }

    virtual IHqlExpression * clone(HqlExprArray & children) override;
    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) override;
};


//I'm not convinced that this should be derived from CHqlScope.
class CHqlLibraryInstance : public CHqlScope
{
protected:
    OwnedHqlExpr scopeFunction;             // a no_funcdef with queryChild(0) as the actual scope
    IHqlScope * libraryScope;

    virtual void sethash() override;
public:
    CHqlLibraryInstance(IHqlExpression * _scopeFunction, HqlExprArray &parms);

//IHqlExpression
    virtual IHqlExpression *queryFunctionDefinition() const override { return scopeFunction; }
    virtual bool equals(const IHqlExpression & other) const override;
    virtual IHqlExpression * clone(HqlExprArray & children) override;

//IHqlScope
    virtual void defineSymbol(IHqlExpression * expr) override { throwUnexpected(); }
    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) override;
    virtual bool hasBaseClass(IHqlExpression * searchBase) override { return libraryScope->hasBaseClass(searchBase); }
    virtual bool allBasesFullyBound() const override { return false; }
    virtual IHqlScope * queryConcreteScope() override { return this; }

    virtual void getSymbols(HqlExprArray& exprs) const override { libraryScope->getSymbols(exprs); }

    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) override;
};

class HQL_API CHqlDelayedScope : public CHqlExpressionWithTables, implements IHqlScope
{
public:
    CHqlDelayedScope(HqlExprArray & _ownedOperands);
    IMPLEMENT_IINTERFACE_O_USING(CHqlExpression)

//IHqlExpression
    virtual bool equals(const IHqlExpression & other) const override;
    virtual IHqlExpression * clone(HqlExprArray & children) override;
    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;
    virtual IHqlScope *queryScope() override { return this; }

//IHqlScope
    virtual IHqlExpression * queryExpression() override { return this; }
    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) override;
    virtual IFileContents * lookupContents(IIdAtom * searchName, HqlLookupContext & ctx) override;

    virtual void    getSymbols(HqlExprArray& exprs) const override;
    virtual IAtom *   queryName() const override { return NULL; }
    virtual IIdAtom * queryId() const override { return NULL; }
    virtual const char * queryFullName() const override { return NULL; }
    virtual ISourcePath * querySourcePath() const override { return NULL; }
    virtual bool hasBaseClass(IHqlExpression * searchBase) override;
    virtual bool allBasesFullyBound() const override { return false; }
    virtual bool isEquivalentScope(const IHqlScope & other) const override { return this == &other; }

    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) override;
    virtual IHqlScope * queryConcreteScope() override;
    virtual IHqlScope * queryResolvedScope(HqlLookupContext * context) override;
    virtual void ensureSymbolsDefined(HqlLookupContext & ctx) override;

    virtual bool isImplicit() const override { return false; }
    virtual bool isPlugin() const override { return false; }
    virtual int getPropInt(IAtom *, int dft) const override { return dft; }
    virtual bool getProp(IAtom *, StringBuffer &) const override { return false; }

//IHqlCreateScope
    virtual void defineSymbol(IIdAtom * id, IIdAtom * moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags, IFileContents *fc, int lineno, int column, int _startpos, int _bodypos, int _endpos) override { throwUnexpected(); }
    virtual void defineSymbol(IIdAtom * id, IIdAtom * moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags) override { throwUnexpected(); }
    virtual void defineSymbol(IHqlExpression * expr) override { throwUnexpected(); }
    virtual void removeSymbol(IIdAtom * id) override { throwUnexpected(); }

protected:
    ITypeInfo * type;
    IHqlScope * typeScope;
};

class CHqlVariable : public CHqlRealExpression
{
protected:
    StringAttr name;
    ITypeInfo * type;
    CHqlVariable(node_operator _op, const char * _name, ITypeInfo * _type);
    ~CHqlVariable();
    virtual bool equals(const IHqlExpression & other) const override;
    virtual void sethash() override;
public:
    static CHqlVariable *makeVariable(node_operator op, const char * name, ITypeInfo * type);
    virtual StringBuffer &toString(StringBuffer &ret) override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;
    virtual bool isIndependentOfScope() override { return true; }
    virtual bool isIndependentOfScopeIgnoringInputs() override { return true; }
    virtual bool usesSelector(IHqlExpression * selector) override { return false; }
    virtual void gatherTablesUsed(CUsedTablesBuilder & used) override {}
    virtual void gatherTablesUsed(HqlExprCopyArray & inScope) override {}
};

class CHqlAttribute : public CHqlExpressionWithTables
{
protected:
    IAtom * name;
    CHqlAttribute(node_operator _op, IAtom * _name);
    virtual bool equals(const IHqlExpression & other) const override;
    virtual void sethash() override;
    virtual bool isAttribute() const override { return true; }
public:
    static CHqlAttribute *makeAttribute(node_operator op, IAtom * name);
    virtual IAtom * queryName() const override { return name; }
    virtual StringBuffer &toString(StringBuffer &ret) override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;
};

class CHqlUnknown : public CHqlExpressionWithType
{
protected:
    IAtom * name;
    LinkedIInterface extra;
    CHqlUnknown(node_operator _op, ITypeInfo * _type, IAtom * _name, IInterface * _extra);
    virtual bool equals(const IHqlExpression & other) const override;
    virtual void sethash() override;
public:
    static CHqlUnknown *makeUnknown(node_operator _op, ITypeInfo * _type, IAtom * _name, IInterface * _extra);
    virtual IAtom * queryName() const override { return name; }
    virtual IInterface *queryUnknownExtra() override;
    virtual StringBuffer &toString(StringBuffer &ret) override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
};

class CHqlSequence : public CHqlExpressionWithType
{
protected:
    unsigned __int64 seq;
    IAtom * name;

    CHqlSequence(node_operator _op, ITypeInfo * _type, IAtom * _name, unsigned __int64 _seq);
    virtual bool equals(const IHqlExpression & other) const override;
    virtual bool isAttribute() const override { return op==no_attr; }
    virtual void sethash() override;
public:
    static CHqlSequence *makeSequence(node_operator _op, ITypeInfo * _type, IAtom * _name, unsigned __int64 _seq);
    virtual IAtom * queryName() const override { return name; }
    virtual unsigned __int64 querySequenceExtra() override { return seq; }
    virtual StringBuffer &toString(StringBuffer &ret) override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
};

class CHqlCachedBoundFunction : public CHqlExpressionWithTables
{
public:
    CHqlCachedBoundFunction(IHqlExpression * func, bool _forceOutOfLineExpansion);

    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;
    virtual IHqlExpression *clone(HqlExprArray &) override;

public:
    LinkedHqlExpr bound;
};

class CHqlRecord: public CHqlExpressionWithTables, implements ITypeInfo, implements IHqlSimpleScope
{
private:
    FieldTable fields;
    size32_t       thisAlignment;

public:
    CHqlRecord();
    CHqlRecord(HqlExprArray &operands);
    ~CHqlRecord();

    IMPLEMENT_IINTERFACE_O_USING(CHqlExpression)

//IHqlExpression
    virtual IHqlSimpleScope *querySimpleScope() override { return this; };
    virtual IHqlExpression *addOperand(IHqlExpression *field) override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual bool equals(const IHqlExpression & other) const override;
    virtual IAtom * queryName() const override { return lower(unnamedId); }
    virtual void sethash() override;
    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;

//ITypeInfo
    virtual type_t getTypeCode() const override { return type_record; }
    virtual size32_t getSize() override;
    virtual unsigned getAlignment() override;
    virtual unsigned getPrecision() override { return 0; }
    virtual unsigned getBitSize() override { return 0; }
    virtual unsigned getStringLen() override { return 0; }
    virtual unsigned getDigits() override { return 0; }
    virtual bool assignableFrom(ITypeInfo * source) override;
    virtual IValue * castFrom(bool isSignedValue, __int64 value) override { return NULL; }
    virtual IValue * castFrom(double value) override { return NULL; }
    virtual IValue * castFrom(size32_t len, const char * text) override { return NULL; }
    virtual IValue * castFrom(size32_t len, const UChar * text) override { return NULL; }
    virtual StringBuffer &getECLType(StringBuffer & out) override;
    virtual StringBuffer &getDescriptiveType(StringBuffer & out) override { return getECLType(out); }
    virtual unsigned getCrc() override;

    virtual const char *queryTypeName() override { return str(queryName()); }
    virtual IHqlExpression * castToExpression() override { return this; }
    virtual IHqlScope * castToScope() override { return NULL; }
    
    virtual unsigned getCardinality() override           { return 0; }
    virtual bool isInteger() override                    { return false; }
    virtual bool isReference() override                  { return false; }
    virtual bool isScalar() override                     { return false; }
    virtual bool isSigned()  override                    { return false; }
    virtual bool isSwappedEndian() override              { return false; }
    virtual ICharsetInfo * queryCharset() override       { return NULL; }
    virtual ICollationInfo * queryCollation() override   { return NULL; }
    virtual IAtom * queryLocale() override                 { return NULL; }
    virtual ITypeInfo * queryChildType() override        { return NULL; }
    virtual ITypeInfo * queryPromotedType() override     { return this; }
    virtual ITypeInfo * queryTypeBase() override         { return this; }
    virtual typemod_t queryModifier() override           { return typemod_none; }
    virtual IInterface * queryModifierExtra() override   { return NULL; }

    virtual void serialize(MemoryBuffer &) override { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &) override { UNIMPLEMENTED; }

// IHqlSimpleScope
    virtual IHqlExpression *lookupSymbol(IIdAtom * fieldName) override;

    void insertSymbols(IHqlExpression * expr);
};

class CHqlDictionary : public CHqlExpressionWithType
{
public:
    static CHqlDictionary *makeDictionary(node_operator op, ITypeInfo *type, HqlExprArray &operands);

public:
    CHqlDictionary(node_operator op, ITypeInfo *_type, HqlExprArray &_ownedOperands);
    ~CHqlDictionary();

    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual IHqlExpression *queryNormalizedSelector() override;
    
protected:
    OwnedHqlExpr normalized;
};


class CHqlDataset : public CHqlExpressionWithType, implements IHqlDataset
{
public:
    IMPLEMENT_IINTERFACE_O_USING(CHqlExpression)
    static CHqlDataset *makeDataset(node_operator op, ITypeInfo *type, HqlExprArray &operands);

    virtual IHqlDataset *queryDataset() override { return this; };
    virtual IHqlExpression * queryExpression() override { return this; }
    virtual IHqlSimpleScope *querySimpleScope() override;
    virtual IHqlDataset* queryTable() override;
    virtual IHqlExpression *queryNormalizedSelector() override;

    virtual bool equals(const IHqlExpression & r) const override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;

    virtual void addProperty(ExprPropKind kind, IInterface * value) override;
    virtual IInterface * queryExistingProperty(ExprPropKind kind) const override;

protected:
    IHqlDataset *rootTable;
    IHqlExpression * container;
    OwnedHqlExpr normalized;
    std::atomic<IInterface *> metaProperty = { nullptr };

    void cacheParent();

public:
    CHqlDataset(node_operator op, ITypeInfo *_type, HqlExprArray &_ownedOperands);
    ~CHqlDataset();

//interface IHqlDataset 
    virtual IHqlDataset *queryRootTable() override { return rootTable; };
    virtual IHqlExpression * queryContainer() override { return container; }
    virtual bool isAggregate() override;
};


class CHqlAlienType : public CHqlExpressionWithTables, implements ITypeInfo, implements IHqlSimpleScope, implements IHqlAlienTypeInfo
{
private:
    IHqlScope *scope;
    ITypeInfo *logical;
    ITypeInfo *physical;
    IHqlExpression *funcdef;
    IIdAtom * id;

public:
    CHqlAlienType(IIdAtom *, IHqlScope *, IHqlExpression * _funcdef);
    ~CHqlAlienType();

    IMPLEMENT_IINTERFACE_O_USING(CHqlExpression)

//IHqlExpression
    virtual IHqlExpression *queryFunctionDefinition() const override;
    virtual IHqlScope *queryScope() override { return scope; }
    virtual IHqlSimpleScope *querySimpleScope() override { return this; }
    virtual bool equals(const IHqlExpression & other) const override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual void sethash() override;
    virtual ITypeInfo *queryType() const override;
    virtual ITypeInfo *getType() override;

//ITypeInfo
    virtual type_t getTypeCode() const override { return type_alien; }
    virtual size32_t getSize() override;
    virtual unsigned getAlignment() override { return 1; }
    virtual unsigned getPrecision() override             { return logical->getPrecision(); }
    virtual unsigned getBitSize() override               { return logical->getBitSize(); }
    virtual unsigned getStringLen() override             { return logical->getStringLen(); }
    virtual unsigned getDigits() override                { return logical->getDigits(); }
    virtual bool assignableFrom(ITypeInfo * source) override { return logical->assignableFrom(source->queryPromotedType());}
    virtual IValue * castFrom(bool isSignedValue, __int64 value) override    { return NULL; }
    virtual IValue * castFrom(double value) override  { return NULL; }
    virtual IValue * castFrom(size32_t len, const char * text) override { return NULL; }
    virtual IValue * castFrom(size32_t len, const UChar * text) override { return NULL; }
    virtual StringBuffer &getECLType(StringBuffer & out) override { return out.append(lower(id)); }
    virtual StringBuffer &getDescriptiveType(StringBuffer & out) override { return getECLType(out); }
    virtual const char *queryTypeName() override { return id ? str(id->queryLower()) : NULL ; }
    virtual unsigned getCardinality() override;
    virtual bool isInteger() override                    { return logical->isInteger(); }
    virtual bool isReference() override                  { return false; }
    virtual bool isScalar() override                     { return true; }
    virtual bool isSigned() override                     { return logical->isSigned(); }
    virtual bool isSwappedEndian() override              { return logical->isSwappedEndian(); }
    virtual ICharsetInfo * queryCharset() override       { return logical->queryCharset(); }
    virtual ICollationInfo * queryCollation() override   { return logical->queryCollation(); }
    virtual IAtom * queryLocale() override                 { return logical->queryLocale(); }
    virtual IAtom * queryName() const override { return lower(id);  }
    virtual IIdAtom * queryId() const override { return id; }
    virtual ITypeInfo * queryChildType() override { return logical; }
    virtual ITypeInfo * queryPromotedType() override     { return logical->queryPromotedType(); }
    virtual ITypeInfo * queryTypeBase() override         { return this; }
    virtual typemod_t queryModifier() override           { return typemod_none; }
    virtual IInterface * queryModifierExtra() override   { return NULL; }
    virtual unsigned getCrc() override;
    virtual IHqlExpression * castToExpression() override { return this; }
    virtual IHqlScope * castToScope() override { return NULL; }

    virtual void serialize(MemoryBuffer &) override { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &) override { UNIMPLEMENTED; }

// IHqlSimpleScope
    virtual IHqlExpression *lookupSymbol(IIdAtom * fieldName) override;

// interface IHqlAlienTypeInfo
    virtual ITypeInfo *getLogicalType() override { return LINK(logical); }
    virtual ITypeInfo *getPhysicalType() override { return LINK(physical); }
    virtual ITypeInfo * queryLogicalType() override { return logical; }
    virtual ITypeInfo * queryPhysicalType() override { return physical; }
    virtual int getLogicalTypeSize() override  { return logical->getSize(); }
    virtual int getPhysicalTypeSize() override { return physical->getSize(); }
    virtual unsigned getMaxSize() override;
    virtual IHqlExpression * queryLoadFunction() override;
    virtual IHqlExpression * queryLengthFunction() override;
    virtual IHqlExpression * queryStoreFunction() override;
    virtual IHqlExpression * queryFunction(IIdAtom * id) override;

private:
    IHqlExpression * queryMemberFunc(IIdAtom * id);
};

class CHqlEnumType : public CHqlExpressionWithType
{
private:
    IHqlScope *scope;

public:
    CHqlEnumType(ITypeInfo * baseType, IHqlScope * _scope);
    ~CHqlEnumType();

//IHqlExpression
    virtual IHqlScope *queryScope() override { return scope; }
    virtual bool equals(const IHqlExpression & other) const override;
    virtual IHqlExpression *clone(HqlExprArray &newkids) override;
    virtual void sethash() override;
};


class HqlGramCtx;
extern void defineSymbol(IIdAtom * id, IHqlExpression *value);
extern void parseAttribute(IHqlScope *scope, IFileContents * contents, HqlLookupContext & ctx, IIdAtom * id, const char * fullName);
extern IHqlExpression * parseDefinition(const char * ecl, IIdAtom * name, MultiErrorReceiver &errors);
extern bool parseForwardModuleMember(HqlGramCtx & _parent, IHqlScope *scope, IHqlExpression * forwardSymbol, HqlLookupContext & ctx);

IHqlExpression *createAttribute(node_operator op, IAtom * name, IHqlExpression * value, IHqlExpression *value2, IHqlExpression * value3);
IHqlExpression *createAttribute(node_operator op, IAtom * name, HqlExprArray & args);

#endif
