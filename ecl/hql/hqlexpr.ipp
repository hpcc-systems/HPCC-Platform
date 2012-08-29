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
#ifndef HQLEXPR_IPP_INCL
#define HQLEXPR_IPP_INCL

#define NUM_PARALLEL_TRANSFORMS 1
//I'm not sure if the following is needed or not - I'm slight concerned that remote scopes (e.g.,, plugins)
//may be accessed in parallel from multiple threads, causing potential conflicts
#define THREAD_SAFE_SYMBOLS

#include "jexcept.hpp"
#include "javahash.hpp"
#include "defvalue.hpp"
#include "hqlexpr.hpp"

#ifdef USE_TBB
#include "tbb/scalable_allocator.h"
#endif

typedef byte transformdepth_t;
#define TRANSFORM_DEPTH_MASK    0x7f
#define TRANSFORM_DEPTH_NOLINK  0x80
#define TRANSFORM_DEPTH(depth) (depth & TRANSFORM_DEPTH_MASK)

//following are only stored in the save array
#define TRANSFORM_DEPTH_SAVE_MATCH_EXPR     0x100

#define RELEASE_TRANSFORM_EXTRA(depth, extra)   { if (!(depth & TRANSFORM_DEPTH_NOLINK)) ::Release(extra); }

class CHqlExprMeta;

//class HQL_API CHqlExpression : implements IHqlExpression, public CInterface
class HQL_API CHqlDynamicAttribute
{
    friend class CHqlExpression;
public:
    inline CHqlDynamicAttribute(_ATOM _name, IHqlExpression *_value)
        : name(_name), value(_value)
    {
        next = NULL;
    }
    ~CHqlDynamicAttribute() { delete next; }

protected:
    _ATOM name;
    CHqlDynamicAttribute * next;
    LinkedHqlExpr value;
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
    void gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope) const;
    void set(HqlExprCopyArray & _activeTables, HqlExprCopyArray & _newTables);
    void setActiveTable(IHqlExpression * expr);

private:
    union
    {
        IHqlExpression * single;
        IHqlExpression * * multi;
    } tables;
    unsigned numTables;
    unsigned numActiveTables;
};

class HQL_API CUsedTablesBuilder
{
public:
    void addNewTable(IHqlExpression * expr);
    void addHiddenTable(IHqlExpression * expr, IHqlExpression * selSeq);
    void addActiveTable(IHqlExpression * expr);
    void cleanupProduction();
    inline void removeActive(IHqlExpression * expr) { inScopeTables.zap(*expr); }
    void removeRows(IHqlExpression * expr, IHqlExpression * left, IHqlExpression * right);
    void set(CUsedTables & tables) { tables.set(inScopeTables, newScopeTables); }

protected:
    HqlExprCopyArray inScopeTables;     // may need to rename, since use has changed.
    HqlExprCopyArray newScopeTables;
};

class HQL_API CHqlExpression : public CInterfaceOf<IHqlExpression>
{
public:
    friend class CHqlExprMeta;
    typedef CInterfaceOf<IHqlExpression> Parent;

#ifdef USE_TBB
    void *operator new(size32_t size) { return scalable_malloc(size); }
    void operator delete(void *ptr) { return scalable_free(ptr); }
#endif

protected:
    unsigned hashcode;          // CInterface is 4 byte aligned in 64bits, so use this to pad
                                // Worth storing becuase it significantly speeds up equality checking
    IInterface * transformExtra[NUM_PARALLEL_TRANSFORMS];
    unsigned cachedCRC;
    unsigned infoFlags;
    node_operator op;                           // 2 bytes
    unsigned short infoFlags2;
    transformdepth_t transformDepth[NUM_PARALLEL_TRANSFORMS];           // 1 byte

    CHqlDynamicAttribute * attributes;
    HqlExprArray operands;

protected:
    CHqlExpression(node_operator op);
    void appendOperands(IHqlExpression * arg0, ...);
    void setOperands(HqlExprArray & ownedOperands);

    //protected virtual members not in public interface
    virtual void sethash();

protected:
    inline bool constant() const { return (infoFlags2 & HEF2constant) != 0; }
    inline bool functionOfGroupAggregate() const { return (infoFlags & HEFfunctionOfGroupAggregate) != 0; }
    inline bool fullyBound() const { return (infoFlags & HEFunbound) == 0; }
    inline bool pure() const { return (infoFlags & HEFimpure) == 0; }

    //For a no_select, is this the root no_select (rather than a.b.c), and is it also an active selector.
    //Used for determining how a no_select should be interpreted e.g., in table gathering. 
    inline bool isSelectRootAndActive() const
    {
        if (hasProperty(newAtom))
            return false;
        IHqlExpression * ds = queryChild(0);
        if ((ds->getOperator() == no_select) && ds->isDatarow())
            return false;
        return true;
    }

    bool isAggregate();
    IHqlExpression * commonUpExpression();

    void onAppendOperand(IHqlExpression & child, unsigned whichOperand);
    inline void doAppendOperand(IHqlExpression & child)
    {
        unsigned which = operands.ordinality();
        operands.append(child);
        onAppendOperand(child, which);
    }
    IHqlExpression * queryExistingAttribute(_ATOM propName) const;

    void initFlagsBeforeOperands();
    void updateFlagsAfterOperands();

    IHqlExpression * calcNormalizedSelector() const;
    IHqlExpression *fixScope(IHqlDataset *table);
    virtual unsigned getCachedEclCRC();
    void setInitialHash(unsigned typeHash);

    void addAttribute(_ATOM name, IHqlExpression * value);

public:
    virtual void Link(void) const;
    virtual bool Release(void) const;

    virtual ~CHqlExpression();

    virtual bool isExprClosed() const { return hashcode!=0; }
    virtual bool isFullyBound() const { return fullyBound(); };
    virtual _ATOM queryName() const { return NULL; }
    virtual node_operator getOperator() const { return op; }
    virtual IHqlDataset *queryDataset() { return NULL; };
    virtual IHqlScope *queryScope();
    virtual IHqlSimpleScope *querySimpleScope();
    virtual IHqlExpression *queryProperty(_ATOM propName) const;
    virtual IHqlExpression *queryAttribute(_ATOM propName);
    virtual IHqlExpression *queryFunctionDefinition() const { return NULL; };
    virtual IHqlExpression *queryExternalDefinition() const { return NULL; };
    virtual unsigned getInfoFlags() const { return infoFlags; }
    virtual unsigned getInfoFlags2() const { return infoFlags2; }
    virtual bool isBoolean();
    virtual bool isConstant();
    virtual bool isDataset();
    virtual bool isDictionary();
    virtual bool isDatarow();
    virtual bool isScope();
    virtual bool isMacro();
    virtual bool isType();
    virtual bool isList();
    virtual bool isField();
    virtual bool isGroupAggregateFunction() { return functionOfGroupAggregate(); }
    virtual bool isRecord();
    virtual bool isAction();
    virtual bool isTransform();
    virtual bool isFunction();
    virtual annotate_kind getAnnotationKind() const { return annotate_none; }
    virtual IHqlAnnotation * queryAnnotation() { return NULL; }
    virtual bool isPure()       { return pure(); }
    virtual bool isAttribute() const { return false; }
    virtual IHqlExpression *queryNormalizedSelector(bool skipIndex) { return this; }

    virtual int  getStartLine() const { throwUnexpected(); }
    virtual int  getStartColumn() const { throwUnexpected(); }
    virtual IPropertyTree * getDocumentation() const { return NULL; }

    virtual IHqlExpression *queryBody(bool singleLevel = false) { return this; }
    virtual IValue *queryValue() const { return NULL; }
    virtual IInterface *queryUnknownExtra() { return NULL; }
    virtual unsigned __int64 querySequenceExtra() { return 0; }

    virtual StringBuffer &toString(StringBuffer &ret);
    virtual IHqlExpression *queryChild(unsigned idx) const;
    virtual unsigned numChildren() const ;

    virtual ITypeInfo *queryRecordType();
    virtual IHqlExpression *queryRecord();

    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) { return LINK(body); }
    virtual IHqlExpression * cloneAllAnnotations(IHqlExpression * body) { return LINK(body); }
    virtual void unwindList(HqlExprArray &dst, node_operator);

    virtual _ATOM           queryFullModuleName() const { return NULL; }
    virtual ISourcePath *   querySourcePath() const { return NULL; }

    virtual IInterface *    queryTransformExtra();
    virtual void                setTransformExtra(IInterface * x);
    virtual void                setTransformExtraOwned(IInterface * x);
    virtual void                setTransformExtraUnlinked(IInterface * x);

    virtual IHqlExpression *closeExpr(); // MORE - should be in expressionBuilder interface!
    virtual IHqlExpression *addOperand(IHqlExpression *); // MORE - should be in expressionBuilder interface!

    virtual StringBuffer& getTextBuf(StringBuffer& buf) { assertex(false); return buf; }
    virtual IFileContents * queryDefinitionText() const { return NULL; }
    virtual bool isExported() const { return false; }

    virtual IHqlExpression * queryAnnotationParameter(unsigned i) const { return NULL; }

    virtual void                addObserver(IObserver & observer);
    virtual void                removeObserver(IObserver & observer);
    virtual unsigned getHash() const;
    virtual bool                equals(const IHqlExpression & other) const;
    
    virtual void beforeDispose();               // called before item is freed so whole object still valid
    virtual unsigned getSymbolFlags() const;

public:
    inline void doSetTransformExtra(IInterface * x, unsigned depthMask);
    inline void resetTransformExtra(IInterface * _extra, unsigned depth);
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
class HQL_API CHqlExpressionWithTables : public CHqlExpression
{
public:
    inline CHqlExpressionWithTables(node_operator op) : CHqlExpression(op) {}

    virtual bool isIndependentOfScope();
    virtual bool usesSelector(IHqlExpression * selector);
    virtual void gatherTablesUsed(CUsedTablesBuilder & used);
    virtual void gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope);

protected:
    void cacheChildrenTablesUsed(CUsedTablesBuilder & used, unsigned from, unsigned to);
    void cacheInheritChildTablesUsed(IHqlExpression * ds, CUsedTablesBuilder & used, const HqlExprCopyArray & childInScopeTables);
    void cachePotentialTablesUsed(CUsedTablesBuilder & used);
    void cacheTablesProcessChildScope(CUsedTablesBuilder & used);
    void cacheTablesUsed();
    void cacheTableUseage(CUsedTablesBuilder & used, IHqlExpression * expr);

protected:
    CUsedTables usedTables;
};

class HQL_API CHqlExpressionWithType : public CHqlExpressionWithTables
{
    friend HQL_API IHqlExpression *createOpenValue(node_operator op, ITypeInfo *type);
public:
    static CHqlExpression *makeExpression(node_operator op, ITypeInfo *type, HqlExprArray &operands);
    static CHqlExpression *makeExpression(node_operator op, ITypeInfo *type, ...);

    virtual ITypeInfo *queryType() const;
    virtual ITypeInfo *getType();
    virtual IHqlExpression *clone(HqlExprArray &newkids);

protected:
    inline CHqlExpressionWithType(node_operator op, ITypeInfo * _type) : CHqlExpressionWithTables(op), type(_type) {}
    
    CHqlExpressionWithType(node_operator op, ITypeInfo *type, HqlExprArray & ownedOperands);
    ~CHqlExpressionWithType();

protected:
    ITypeInfo *type;
};

class CHqlNamedExpression : public CHqlExpressionWithType
{
    friend HQL_API IHqlExpression *createOpenNamedValue(node_operator op, ITypeInfo *type, _ATOM name);
    friend HQL_API IHqlExpression *createNamedValue(node_operator op, ITypeInfo *type, _ATOM name, HqlExprArray & args);

protected:
    _ATOM name;

protected:
    CHqlNamedExpression(node_operator _op, ITypeInfo *_type, _ATOM _name, ...);
    CHqlNamedExpression(node_operator _op, ITypeInfo *_type, _ATOM _name, HqlExprArray & _ownedOperands);

    virtual void sethash();
    virtual bool                equals(const IHqlExpression & other) const;

public:
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual _ATOM queryName() const { return name; }

};


class CHqlSelectBaseExpression : public CHqlExpression
{
public:
    static IHqlExpression * makeSelectExpression(IHqlExpression * left, IHqlExpression * right, IHqlExpression * attr);
    static IHqlExpression * makeSelectExpression(HqlExprArray & ownedOperands);

    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual ITypeInfo *queryType() const;
    virtual ITypeInfo *getType();

    virtual bool isIndependentOfScope();
    virtual bool usesSelector(IHqlExpression * selector);
    virtual void gatherTablesUsed(CUsedTablesBuilder & used);
    virtual void gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope);

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
    virtual IHqlExpression *queryNormalizedSelector(bool skipIndex);
    virtual void calcNormalized();

protected:
    CHqlNormalizedSelectExpression() {}
};

class CHqlSelectExpression : public CHqlSelectBaseExpression
{
    friend class CHqlSelectBaseExpression;
public:
    virtual IHqlExpression *queryNormalizedSelector(bool skipIndex);
    virtual void calcNormalized();

protected:
    CHqlSelectExpression() {}

protected:
    HqlExprAttr normalized;
};

class CFileContents : public CInterfaceOf<IFileContents>
{
private:
    Linked<IFile> file;
    StringAttr fileContents;
    Linked<ISourcePath> sourcePath;
    bool delayedRead;

public:
    CFileContents(IFile * _file, ISourcePath * _sourcePath);
    CFileContents(const char *query, ISourcePath * _sourcePath);
    CFileContents(unsigned len, const char *query, ISourcePath * _sourcePath);

    virtual IFile * queryFile() { return file; }
    virtual ISourcePath * querySourcePath() { return sourcePath; }
    virtual const char *getText()
    {
        ensureLoaded();
        return fileContents.sget();
    }
    virtual size32_t length() 
    { 
        ensureLoaded();
        return fileContents.length();
    }

private:
    bool preloadFromFile();
    void ensureLoaded();
    void ensureUtf8(MemoryBuffer & contents);
};

class HQL_API CHqlAnnotation: public CHqlExpression
{
protected:
    IHqlExpression *body;

protected:
    virtual void sethash();

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

//Following are redirected to body
    virtual ITypeInfo *queryType() const;
    virtual ITypeInfo *getType();
    virtual _ATOM queryName() const;
    virtual bool isScope();
    virtual bool isType();
    virtual bool isConstant();
    virtual bool isMacro();
    virtual bool isGroupAggregateFunction();
    virtual annotate_kind getAnnotationKind() const = 0;
    virtual bool isPure();
    virtual bool isAttribute() const;
    virtual unsigned getInfoFlags() const;
    virtual unsigned getInfoFlags2() const;
    virtual int  getStartLine() const;
    virtual int  getStartColumn() const;
    virtual IPropertyTree * getDocumentation() const;
    virtual StringBuffer &toString(StringBuffer &ret);
    virtual IHqlExpression *queryChild(unsigned idx) const;
    virtual unsigned numChildren() const;
    virtual bool isIndependentOfScope();
    virtual bool usesSelector(IHqlExpression * selector);
    virtual void gatherTablesUsed(CUsedTablesBuilder & used);
    virtual void gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope);
    virtual IValue *queryValue() const;
    virtual IInterface *queryUnknownExtra();
    virtual unsigned __int64 querySequenceExtra();
    virtual IHqlDataset *queryDataset();
    virtual IHqlScope *queryScope();
    virtual IHqlSimpleScope *querySimpleScope();
    virtual IHqlExpression *queryFunctionDefinition() const;
    virtual IHqlExpression *queryExternalDefinition() const;
    virtual IHqlExpression *queryNormalizedSelector(bool skipIndex=false);
    virtual IHqlExpression *queryProperty(_ATOM propName) const;
    virtual IHqlExpression *queryAttribute(_ATOM propName);
    virtual IHqlExpression * clone(HqlExprArray &);
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) = 0;
    virtual IHqlExpression * cloneAllAnnotations(IHqlExpression * body);
    virtual _ATOM               queryFullModuleName() const;
    virtual bool isFullyBound() const;
    virtual IHqlExpression *addOperand(IHqlExpression *);
    virtual StringBuffer& getTextBuf(StringBuffer& buf);
    virtual IFileContents * queryDefinitionText() const;
    virtual bool isExported() const;
    virtual unsigned getSymbolFlags() const;
    virtual unsigned            getCachedEclCRC();

//Actually implemented by this class
    virtual bool                equals(const IHqlExpression & other) const;
    virtual IHqlExpression *queryBody(bool singleLevel = false);
    virtual IHqlExpression * queryAnnotationParameter(unsigned i) const;
};


class HQL_API CHqlSymbolAnnotation : public CHqlAnnotation, public IHqlNamedAnnotation
{
public:
    IMPLEMENT_IINTERFACE_USING(CHqlAnnotation)

    virtual _ATOM queryName() const { return name; }
    virtual _ATOM queryFullModuleName() const { return module; }
    virtual IHqlExpression *queryFunctionDefinition() const;
    virtual unsigned getSymbolFlags() const;

    virtual annotate_kind getAnnotationKind() const { return annotate_symbol; }
    virtual IHqlAnnotation * queryAnnotation() { return this; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body);

    virtual bool                equals(const IHqlExpression & other) const;

//interface IHqlNamedAnnotation
    virtual IHqlExpression * queryExpression();
    virtual bool isExported() const { return (symbolFlags&ob_exported)!=0; };
    virtual bool isShared() const { return (symbolFlags&ob_shared)!=0; };
    virtual bool isPublic() const { return (symbolFlags&(ob_shared|ob_exported))!=0; };
    virtual void setRepositoryFlags(unsigned _flags) { symbolFlags |= (_flags & ob_registryflags); }

protected:
    CHqlSymbolAnnotation(_ATOM _name, _ATOM _module, IHqlExpression *_expr, IHqlExpression *_funcdef, unsigned _obFlags);
    ~CHqlSymbolAnnotation();

    virtual void sethash();

protected:
    _ATOM name;
    _ATOM module;
    IHqlExpression *funcdef;
    unsigned symbolFlags;
};

class HQL_API CHqlSimpleSymbol : public CHqlSymbolAnnotation
{
public:
    static IHqlExpression * makeSymbol(_ATOM _name, _ATOM _module, IHqlExpression *_expr, IHqlExpression *_funcdef, unsigned _obFlags);

//interface IHqlNamedAnnotation
    virtual IFileContents * getBodyContents() { return NULL; }
    virtual IHqlExpression * cloneSymbol(_ATOM optname, IHqlExpression * optnewbody, IHqlExpression * optnewfuncdef, HqlExprArray * optargs);
    virtual int getStartLine() const { return 0; }
    virtual int getStartColumn() const { return 0; }
    virtual int getStartPos() const { return 0; }
    virtual int getBodyPos() const { return 0; }
    virtual int getEndPos() const { return 0; }

protected:
    CHqlSimpleSymbol(_ATOM _name, _ATOM _module, IHqlExpression *_expr, IHqlExpression *_funcdef, unsigned _obFlags);
};

class HQL_API CHqlNamedSymbol: public CHqlSymbolAnnotation
{
public:
    static CHqlNamedSymbol *makeSymbol(_ATOM _name, _ATOM _module, IHqlExpression *_expr, bool _exported, bool _shared, unsigned _flags);
    static CHqlNamedSymbol *makeSymbol(_ATOM _name, _ATOM _module, IHqlExpression *_expr, IHqlExpression *_funcdef, bool _exported, bool _shared, unsigned _flags, IFileContents *_text, int lineno, int column, int _startpos, int _bodypos, int _endpos);

    virtual ISourcePath * querySourcePath() const;
    
    virtual StringBuffer& getTextBuf(StringBuffer& buf) { return buf.append(text->length(),text->getText()); }
    virtual IFileContents * queryDefinitionText() const;
    virtual int  getStartLine() const { return startLine; }
    virtual int  getStartColumn() const { return startColumn; }
    virtual int getStartPos() const { return startpos; }
    virtual int getBodyPos() const { return bodypos; }
    virtual int getEndPos() const
    {
        if ((endpos == 0) && text)
            return text->length();
        return endpos;
    }

//interface IHqlNamedAnnotation
    virtual IFileContents * getBodyContents();
    virtual IHqlExpression * cloneSymbol(_ATOM optname, IHqlExpression * optnewbody, IHqlExpression * optnewfuncdef, HqlExprArray * optargs);

protected:
    CHqlNamedSymbol(_ATOM _name, _ATOM _module, IHqlExpression *_expr, bool _exported, bool _shared, unsigned _obFlags);
    CHqlNamedSymbol(_ATOM _name, _ATOM _module, IHqlExpression *_expr, IHqlExpression *_funcdef, bool _exported, bool _shared, unsigned _flags, IFileContents *_text, int _startLine, int _startColumn, int _startpos, int _bodypos, int _endpos);

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
    virtual bool equals(const IHqlExpression & other) const;

protected:
    CHqlAnnotationWithOperands(IHqlExpression * _expr, HqlExprArray & _args);

    virtual void sethash();
};

class HQL_API CHqlMetaAnnotation: public CHqlAnnotationWithOperands
{
public:
    static IHqlExpression * createAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args);

    virtual annotate_kind getAnnotationKind() const { return annotate_meta; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body);

protected:
    CHqlMetaAnnotation(IHqlExpression * _expr, HqlExprArray & _args);
};

class HQL_API CHqlParseMetaAnnotation: public CHqlAnnotationWithOperands
{
public:
    static IHqlExpression * createAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args);

    virtual annotate_kind getAnnotationKind() const { return annotate_parsemeta; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body);

protected:
    CHqlParseMetaAnnotation(IHqlExpression * _expr, HqlExprArray & _args);
};

class HQL_API CHqlLocationAnnotation: public CHqlAnnotation
{
public:
    static IHqlExpression * createLocationAnnotation(IHqlExpression * _ownedBody, ISourcePath * _sourcePath, int _lineno, int _column);

    virtual annotate_kind getAnnotationKind() const { return annotate_location; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body);
    virtual bool equals(const IHqlExpression & other) const;

    virtual ISourcePath * querySourcePath() const { return sourcePath; }
    virtual int  getStartLine() const { return lineno; }
    virtual int  getStartColumn() const { return column; }

protected:
    CHqlLocationAnnotation(IHqlExpression * _expr, ISourcePath * _sourcePath, int _lineno, int _column);

    virtual void sethash();

protected:
    Linked<ISourcePath> sourcePath;
    int lineno;
    int column;
};

class HQL_API CHqlAnnotationExtraBase: public CHqlAnnotation
{
public:
    virtual bool equals(const IHqlExpression & other) const;

protected:
    CHqlAnnotationExtraBase(IHqlExpression * _expr, IInterface * _ownedExtra);
    virtual void sethash();

protected:
    Owned<IInterface> extra;
};

class HQL_API CHqlWarningAnnotation: public CHqlAnnotationExtraBase
{
public:
    static IHqlExpression * createWarningAnnotation(IHqlExpression * _ownedBody, IECLError * _ownedWarning);
    virtual annotate_kind getAnnotationKind() const { return annotate_warning; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body);

    inline IECLError * queryWarning() const { return static_cast<IECLError *>(extra.get()); }

protected:
    CHqlWarningAnnotation(IHqlExpression * _expr, IECLError * _ownedWarning);
};

class HQL_API CHqlJavadocAnnotation: public CHqlAnnotationExtraBase
{
public:
    static IHqlExpression * createJavadocAnnotation(IHqlExpression * _ownedBody, IPropertyTree * _ownedWarning);
    virtual annotate_kind getAnnotationKind() const { return annotate_javadoc; }
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body);

    virtual IPropertyTree * getDocumentation() const { return LINK(queryDocumentation()); }

    inline IPropertyTree * queryDocumentation() const { return static_cast<IPropertyTree *>(extra.get()); }

protected:
    CHqlJavadocAnnotation(IHqlExpression * _expr, IPropertyTree * _ownedJavadoc);
};

class CHqlField: public CHqlExpressionWithType
{
private:
    _ATOM               name;

    virtual void sethash();
    virtual bool equals(const IHqlExpression & other) const;
    void onCreateField();
public:
    CHqlField(_ATOM, ITypeInfo *type, IHqlExpression *defaultValue);
    CHqlField(_ATOM _name, ITypeInfo *_type, HqlExprArray &_ownedOperands);
//  CHqlField(_ATOM _name, ITypeInfo *_type, IHqlExpression *_defValue, IHqlExpression *_attrs);

    virtual StringBuffer &toString(StringBuffer &ret);
//  virtual StringBuffer &toSQL(StringBuffer &, bool paren, IHqlDataset *scope, bool useAliases, node_operator op = no_none);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual StringBuffer &printAliases(StringBuffer &s, unsigned, bool &) { return s; }
    virtual _ATOM queryName() const { return name; }
};

class CHqlRow: public CHqlExpressionWithType
{
    CHqlRow(node_operator op, ITypeInfo * type, HqlExprArray & _operands);

    HqlExprAttr     normalized;
public:
    static CHqlRow *makeRow(node_operator op, ITypeInfo * type, HqlExprArray & _operands);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual IHqlSimpleScope *querySimpleScope();
    virtual IHqlDataset *queryDataset();
    virtual _ATOM queryName() const;
    virtual IHqlExpression *queryNormalizedSelector(bool skipIndex);
};


class CHqlExternal: public CHqlExpressionWithType
{
    _ATOM name;

    CHqlExternal(_ATOM name, ITypeInfo *, HqlExprArray &_ownedOperands);
    virtual bool equals(const IHqlExpression & other) const;
public:
    static CHqlExternal *makeExternalReference(_ATOM name, ITypeInfo *, HqlExprArray &_ownedOperands);
    virtual _ATOM queryName() const { return name; }
};

class CHqlExternalCall: public CHqlExpressionWithType
{
protected:
    OwnedHqlExpr funcdef;

    CHqlExternalCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &parms);
    virtual _ATOM queryName() const { return funcdef->queryName(); }
    virtual void sethash();
    virtual bool equals(const IHqlExpression & other) const;
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual IHqlExpression *queryExternalDefinition() const;
public:
    static IHqlExpression *makeExternalCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &operands);
};

class CHqlExternalDatasetCall: public CHqlExternalCall, implements IHqlDataset
{
    friend class CHqlExternalCall;

    CHqlExternalDatasetCall(IHqlExpression * _funcdef, ITypeInfo * type, HqlExprArray &_ownedOperands) : CHqlExternalCall(_funcdef, type, _ownedOperands) {}
    IMPLEMENT_IINTERFACE_USING(CHqlExternalCall)

    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual IHqlDataset *queryDataset() { return this; }

//interface IHqlDataset
    virtual IHqlDataset* queryTable()               { return this; }
    virtual IHqlSimpleScope *querySimpleScope()     { return queryRecord()->querySimpleScope(); }
    virtual IHqlDataset * queryRootTable()          { return this; }
    virtual IHqlExpression * queryContainer()       { return NULL; }

    virtual bool isAggregate()                      { return false; }
};

#ifdef THREAD_SAFE_SYMBOLS
class SymbolTable
{
public:
    inline void setValue(_ATOM name, IHqlExpression * value)
    {
        CriticalBlock block(cs);
        map.setValue(name, value);
    }
    inline bool contain(_ATOM name) const
    {
        CriticalBlock block(cs);
        IHqlExpression * ret = map.getValue(name);
        return (ret != NULL);
    }
    inline IHqlExpression * getLinkedValue(_ATOM name) const
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
    inline void remove(_ATOM name)
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

protected:
    MapXToMyClass<_ATOM, _ATOM, IHqlExpression> map;
    mutable CriticalSection cs;
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
class SymbolTable : public MapXToMyClass<_ATOM, _ATOM, IHqlExpression>
{
public:
    inline IHqlExpression * getLinkedValue(_ATOM name) const
    {
        IHqlExpression * ret = getValue(name);
        if (ret)
            ret->Link();
        return ret;
    }
};
typedef HashIterator SymbolTableIterator;
#endif

inline IHqlExpression * lookupSymbol(SymbolTable & symbols, _ATOM searchName, bool sharedOK)
{
    OwnedHqlExpr ret = symbols.getLinkedValue(searchName);

    if (!ret)
        return NULL; 
    
    if (!(ret->isExported() || sharedOK))
        return NULL;

    return ret.getClear();
}

typedef class MapXToMyClassViaBase<_ATOM, _ATOM, CHqlField, IHqlExpression> FieldTable;

typedef class MapXToMyClassViaBase<_ATOM, _ATOM, IFileContents, IFileContents> FileContentsTable;

class CHqlDelayedCall: public CHqlExpressionWithType
{
    OwnedHqlExpr param;
protected:
    CHqlDelayedCall(IHqlExpression * _param, ITypeInfo * type, HqlExprArray &parms);
    virtual _ATOM queryName() const { return param->queryName(); }
    virtual void sethash();
    virtual bool equals(const IHqlExpression & other) const;
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual IHqlExpression *queryFunctionDefinition() const { return param; };
public:
    static IHqlExpression *makeDelayedCall(IHqlExpression * _param, HqlExprArray &operands);
};


class CHqlDelayedDatasetCall: public CHqlDelayedCall, implements IHqlDataset
{
    friend class CHqlDelayedCall;

    CHqlDelayedDatasetCall(IHqlExpression * _param, ITypeInfo * type, HqlExprArray &parms) : CHqlDelayedCall(_param, type, parms) {}
    IMPLEMENT_IINTERFACE_USING(CHqlDelayedCall)

    virtual IHqlDataset *queryDataset() { return this; }

//interface IHqlDataset
    virtual IHqlDataset* queryTable()               { return this; }
    virtual IHqlSimpleScope *querySimpleScope()     { return queryRecord()->querySimpleScope(); }
    virtual IHqlDataset * queryRootTable()          { return this; }
    virtual IHqlExpression * queryContainer()       { return NULL; }

    virtual bool isAggregate()                      { return false; }
};

class CHqlDelayedScopeCall: public CHqlDelayedCall, implements IHqlScope
{
public:
    friend class CHqlDelayedCall;

    CHqlDelayedScopeCall(IHqlExpression * _param, ITypeInfo * type, HqlExprArray &parms);
    IMPLEMENT_IINTERFACE_USING(CHqlDelayedCall)

    virtual void defineSymbol(_ATOM name, _ATOM moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags, IFileContents *fc, int lineno, int column, int _startpos, int _bodypos, int _endpos) { throwUnexpected(); }
    virtual void defineSymbol(_ATOM name, _ATOM moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags) { throwUnexpected(); }
    virtual void defineSymbol(IHqlExpression * expr) { throwUnexpected(); }
    virtual void removeSymbol(_ATOM name) { throwUnexpected(); }

    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual void    getSymbols(HqlExprArray& exprs) const;
    virtual _ATOM   queryName() const;
    virtual const char * queryFullName() const  { throwUnexpected(); }
    virtual ISourcePath * querySourcePath() const   { throwUnexpected(); }
    virtual bool hasBaseClass(IHqlExpression * searchBase);

    virtual void ensureSymbolsDefined(HqlLookupContext & ctx) { }

    virtual bool isImplicit() const { return false; }
    virtual bool isPlugin() const { return false; }
    virtual int getPropInt(_ATOM a, int def) const { return def; }
    virtual bool getProp(_ATOM a, StringBuffer &ret) const { return false; }

    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) { throwUnexpected(); }

    virtual IHqlScope * queryConcreteScope() { return this; }
    virtual IHqlScope * queryResolvedScope(HqlLookupContext * context) { return this; }

    virtual IHqlExpression * queryExpression() { return this; }
    virtual IHqlScope * queryScope() { return this; }

protected:
    IHqlScope * typeScope;
};

//This is abstract.  Either CHqlRemoteScope or CHqlLocalScope should be used......
class HQL_API CHqlScope : public CHqlExpressionWithType, implements IHqlScope, implements ITypeInfo
{
protected:
    Owned<IFileContents> text;
    _ATOM name;
    StringAttr fullName;                //Fully qualified name of this nested module   E.g.: PARENT.CHILD.GRANDCHILD
    SymbolTable symbols;

    IHqlDataset *lookupDataset(_ATOM name);

    virtual bool equals(const IHqlExpression & other) const;

public:
    CHqlScope(node_operator _op, _ATOM _name, const char * _fullName);
    CHqlScope(node_operator _op);
    CHqlScope(IHqlScope* scope);
    ~CHqlScope();
    IMPLEMENT_IINTERFACE_USING(CHqlExpression)

    IHqlScope * cloneAndClose(HqlExprArray & children, HqlExprArray & symbols);

//interface IHqlExpression
    virtual IHqlScope *queryScope() { return this; };
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual void sethash();

//interface IHqlScope
    virtual IHqlExpression * queryExpression() { return this; }
    virtual void defineSymbol(_ATOM name, _ATOM moduleName, IHqlExpression *value, bool isPublic, bool isShared, unsigned symbolFlags, IFileContents *, int lineno, int column, int _startpos, int _bodypos, int _endpos);
    virtual void defineSymbol(_ATOM name, _ATOM moduleName, IHqlExpression *value, bool exported, bool shared, unsigned symbolFlags);
    virtual void defineSymbol(IHqlExpression * expr);
    virtual void removeSymbol(_ATOM name);

    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx);

    virtual _ATOM   queryName() const {return name;}
    virtual const char * queryFullName() const  { return fullName; }
    virtual ISourcePath * querySourcePath() const { return text ? text->querySourcePath() : NULL; }

    virtual void ensureSymbolsDefined(HqlLookupContext & ctx) { }
    virtual bool isImplicit() const { return false; }
    virtual bool isPlugin() const { return false; }
    virtual int getPropInt(_ATOM, int) const;
    virtual bool getProp(_ATOM, StringBuffer &) const;

    virtual void    getSymbols(HqlExprArray& exprs) const;
    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) { throwUnexpected(); }
    virtual bool hasBaseClass(IHqlExpression * searchBase);
    virtual IHqlScope * queryConcreteScope()    { return this; }
    virtual IHqlScope * queryResolvedScope(HqlLookupContext * context)  { return this; }
    virtual IFileContents * queryDefinitionText() const { return text; }

//ITypeInfo
    virtual type_t getTypeCode() const          { return type_scope; }
    virtual size32_t getSize()                  { return 0; }
    virtual unsigned getAlignment()             { return 1; }
    virtual unsigned getPrecision()             { return 0; }
    virtual unsigned getBitSize()               { return 0; }
    virtual unsigned getStringLen()             { return 0; }
    virtual unsigned getDigits()                { return 0; }
    virtual IValue * castFrom(bool isSignedValue, __int64 value)    { return NULL; }
    virtual IValue * castFrom(double value)  { return NULL; }
    virtual IValue * castFrom(size32_t len, const char * text)  { return NULL; }
    virtual IValue * castFrom(size32_t len, const UChar * text)  { return NULL; }
    virtual StringBuffer &getECLType(StringBuffer & out)  { return out.append("MODULE"); }
    virtual StringBuffer &getDescriptiveType(StringBuffer & out)  { return out.append("MODULE"); }
    virtual const char *queryTypeName()         { return name->str(); }
    virtual unsigned getCardinality()           { return 0; }
    virtual bool isInteger()                    { return false; }
    virtual bool isReference()                  { return false; }
    virtual bool isScalar()                     { return false; }
    virtual bool isSigned()                     { return false; }
    virtual bool isSwappedEndian()              { return false; }
    virtual ICharsetInfo * queryCharset()       { return NULL; }
    virtual ICollationInfo * queryCollation()   { return NULL; }
    virtual _ATOM queryLocale()                 { return NULL; }
//  virtual _ATOM queryName() const             { return name; }
    virtual ITypeInfo * queryChildType()        { return NULL; }
    virtual IInterface * queryDistributeInfo()  { return NULL; }
    virtual IInterface * queryGroupInfo()       { return NULL; }
    virtual IInterface * queryGlobalSortInfo()  { return NULL; }
    virtual IInterface * queryLocalUngroupedSortInfo()   { return NULL; }
    virtual IInterface * queryGroupSortInfo()   { return NULL; }
    virtual ITypeInfo * queryPromotedType()     { return this; }
    virtual ITypeInfo * queryTypeBase()         { return this; }
    virtual typemod_t queryModifier()           { return typemod_none; }
    virtual IInterface * queryModifierExtra()   { return NULL; }
    virtual StringBuffer & appendStringFromMem(StringBuffer & out, const void * data) {assertex(!"tbd"); return out; }
    virtual unsigned getCrc();
    virtual bool assignableFrom(ITypeInfo * source);

    virtual void serialize(MemoryBuffer &) { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &) { UNIMPLEMENTED; }

protected:
    void throwRecursiveError(_ATOM name);
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
    virtual bool equals(const IHqlExpression & other) const;

    virtual void repositoryLoadModule(HqlLookupContext & ctx, bool forceAll);
    virtual IHqlExpression * repositoryLoadSymbol(_ATOM attrName);

public:
    CHqlRemoteScope(_ATOM _name, const char * _fullName, IEclRepositoryCallback *_repository, IProperties* _props, IFileContents * _text, bool _lazy, IEclSource * _eclSource);
    ~CHqlRemoteScope();
    IMPLEMENT_IINTERFACE_USING(CHqlScope)

    virtual IHqlScope * queryScope()        { return this; }    // remove ambiguous call

//interface IHqlExpression
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual void sethash();

//interface IHqlScope
    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual void ensureSymbolsDefined(HqlLookupContext & ctx);
    virtual void defineSymbol(IHqlExpression * expr);
    using CHqlScope::defineSymbol;

    virtual IFileContents * queryDefinitionText() const;

    virtual void getSymbols(HqlExprArray& exprs) const;

    virtual bool isImplicit() const;
    virtual bool isPlugin() const;
    virtual int getPropInt(_ATOM, int) const;
    virtual bool getProp(_ATOM, StringBuffer &) const;
    virtual void setProp(_ATOM, const char *);
    virtual void setProp(_ATOM, int);
    virtual IEclSource * queryEclSource() const { return eclSource; }
};

class HQL_API CHqlLocalScope : public CHqlScope
{
protected:
    virtual bool equals(const IHqlExpression & other) const;

public:
    CHqlLocalScope(node_operator _op, _ATOM _name, const char * _fullName);
    CHqlLocalScope(IHqlScope* scope);

//interface IHqlExpression
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual void sethash();

//interface IHqlScope

    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols);
};

class HQL_API CHqlVirtualScope : public CHqlScope
{
protected:
    Owned<IHqlScope> concrete;
    bool isAbstract;
    bool complete;
    bool isVirtual;
    bool fullyBoundBase;

protected:
    virtual bool equals(const IHqlExpression & other) const;
    IHqlScope * deriveConcreteScope();
    void ensureVirtual();

public:
    CHqlVirtualScope(_ATOM _name, const char * _fullName);

//interface IHqlExpression
    virtual IHqlExpression *addOperand(IHqlExpression * arg);
    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual IHqlExpression *closeExpr();
    virtual void defineSymbol(IHqlExpression * expr);
    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual void sethash();

//interface IHqlScope

    virtual _ATOM   queryName() const {return name;}
    virtual IHqlScope * queryConcreteScope() { return isVirtual ? concrete.get() : this; }
};


class HQL_API CHqlMergedScope : public CHqlScope
{
public:
    CHqlMergedScope(_ATOM _name, const char * _fullName) : CHqlScope(no_mergedscope, _name, _fullName) { mergedAll = false; }

    void addScope(IHqlScope * scope);

    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual void ensureSymbolsDefined(HqlLookupContext & ctx);
    virtual bool isImplicit() const;
    virtual bool isPlugin() const;

protected:
    CriticalSection cs;
    HqlScopeArray mergedScopes;
    bool mergedAll;
};

/*
Used for syntax checking an attribute.  It allows specific attributes within a module to be overridden,
so that syntax check can work as if it is checking a particular attribute
I suspect it should be done a different way...
*/

class CHqlSyntaxCheckScope : public CHqlScope
{
private:
    IHqlScope *parent;
    SymbolTable redefine;

public:
    CHqlSyntaxCheckScope(IHqlScope *parent, IEclRepository *_ds, const char *attribute, bool clearImportedModule);

    virtual void defineSymbol(_ATOM name, _ATOM _moduleName, IHqlExpression *value, bool exported, bool shared, unsigned symbolFlags, IFileContents *, int lineno, int column, int _startpos, int _bodypos, int _endpos);
    virtual void defineSymbol(_ATOM name, _ATOM _moduleName, IHqlExpression *value, bool exported, bool shared, unsigned symbolFlags);
    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx);
};

class CHqlMultiParentScope : public CHqlScope
{
protected:
    CopyArray parents;

public:
    CHqlMultiParentScope(_ATOM, IHqlScope *parent1, ...);

    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx);
};

class CHqlContextScope : public CHqlScope
{
protected:
    SymbolTable defined;

public:
    CHqlContextScope() : CHqlScope(no_scope) { }
    // copy constructor
    CHqlContextScope(IHqlScope* scope);

    virtual void defineSymbol(_ATOM name, _ATOM moduleName, IHqlExpression *value,bool exported, bool shared, unsigned symbolFlags)
    {  defined.setValue(name,value);  }

    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx)
    {  return defined.getLinkedValue(searchName); }

};

class CHqlTemplateFunctionContext : public CHqlExpressionWithType
{
public:
    CHqlTemplateFunctionContext(IHqlExpression* expr,  IHqlScope* scope)
        : CHqlExpressionWithType(no_template_context,expr->getType())
    {
        addOperand(expr);
        context = scope;
    }
    virtual ~CHqlTemplateFunctionContext() { ::Release(context); }

    virtual IHqlScope* queryScope() { return context; }
    virtual IHqlDataset* queryDataset() { return queryChild(0)->queryDataset(); }

protected: 
    IHqlScope* context;             // allows symbols to be resolved when reparsing.
    virtual void sethash();
};

class CHqlConstant : public CHqlExpression
{
protected:
    IValue *val;

    CHqlConstant(IValue *);
    virtual ~CHqlConstant();
    virtual bool equals(const IHqlExpression & other) const;
    virtual void sethash();
public:
    static CHqlConstant HQL_API *makeConstant(IValue *);
    virtual StringBuffer &toString(StringBuffer &ret);
//  virtual StringBuffer &toSQL(StringBuffer &, bool paren, IHqlDataset *scope, bool useAliases, node_operator op = no_none);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual StringBuffer &printAliases(StringBuffer &s, unsigned, bool &) { return s; }
    virtual IValue *queryValue() const { return val; }
    virtual ITypeInfo *queryType() const;
    virtual ITypeInfo *getType();

    virtual bool isIndependentOfScope() { return true; }
    virtual bool usesSelector(IHqlExpression * selector) { return false; }
    virtual void gatherTablesUsed(CUsedTablesBuilder & used) {}
    virtual void gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope) {}
};

class CHqlParameter : public CHqlExpressionWithType
{
protected:
    unique_id_t uid;
    _ATOM name;
    unsigned idx;

    virtual void sethash();
    CHqlParameter(_ATOM name, unsigned idx, ITypeInfo *type);
    ~CHqlParameter();

    virtual bool equals(const IHqlExpression & other) const;
public:
    static IHqlExpression *makeParameter(_ATOM name, unsigned idx, ITypeInfo *type, HqlExprArray & attrs);

//interface IHqlExpression

    StringBuffer &toString(StringBuffer &ret);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual IHqlSimpleScope *querySimpleScope();
    virtual _ATOM queryName() const { return name; }
    virtual unsigned __int64 querySequenceExtra() { return idx; }
};

class CHqlDatasetParameter : public CHqlParameter, implements IHqlDataset
{
public:
    IMPLEMENT_IINTERFACE_USING(CHqlParameter)

    CHqlDatasetParameter(_ATOM name, unsigned idx, ITypeInfo *type)
     : CHqlParameter(name, idx, type) { }

//IHqlExpression
    virtual bool assignableFrom(ITypeInfo * source) { type_t tc = source->getTypeCode(); return tc==type_table; }
    virtual IHqlDataset *queryDataset() { return this; }

//CHqlParameter

    //virtual IHqlSimpleScope *querySimpleScope();

//IHqlDataset
    virtual IHqlDataset* queryTable() { return this; }
    virtual IHqlDataset * queryRootTable() { return this; }
    virtual IHqlExpression * queryContainer() { return NULL; }
    
    virtual bool isAggregate() { return false; }

//Overlapped methods
    virtual IHqlSimpleScope* querySimpleScope() { return CHqlParameter::querySimpleScope(); }
};

class CHqlDictionaryParameter : public CHqlParameter, implements IHqlDataset
{
public:
    IMPLEMENT_IINTERFACE_USING(CHqlParameter)

    CHqlDictionaryParameter(_ATOM name, unsigned idx, ITypeInfo *type)
     : CHqlParameter(name, idx, type) { }

//IHqlExpression
    virtual bool assignableFrom(ITypeInfo * source) { type_t tc = source->getTypeCode(); return tc==type_dictionary; }
    virtual IHqlDataset *queryDataset() { return this; }

//CHqlParameter

    //virtual IHqlSimpleScope *querySimpleScope();

//IHqlDataset
    virtual IHqlDataset* queryTable() { return this; }
    virtual IHqlDataset * queryRootTable() { return this; }
    virtual IHqlExpression * queryContainer() { return NULL; }

    virtual bool isAggregate() { return false; }

//Overlapped methods
    virtual IHqlSimpleScope* querySimpleScope() { return CHqlParameter::querySimpleScope(); }
};

class CHqlScopeParameter : public CHqlScope
{
protected:
    unique_id_t uid;
    IHqlScope * typeScope;
    unsigned idx;

    virtual void sethash();
public:
    CHqlScopeParameter(_ATOM name, unsigned idx, ITypeInfo *type);

//IHqlExpression
    virtual bool assignableFrom(ITypeInfo * source);
    virtual bool equals(const IHqlExpression & other) const;
    virtual StringBuffer &toString(StringBuffer &ret);

//IHqlDataset
    virtual void defineSymbol(IHqlExpression * expr)            { throwUnexpected(); }
    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx);

    virtual void getSymbols(HqlExprArray& exprs) const          { typeScope->getSymbols(exprs); }

    virtual IHqlExpression * clone(HqlExprArray & children);
    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols);
};

class CHqlLibraryInstance : public CHqlScope
{
protected:
    OwnedHqlExpr scopeFunction;             // a no_funcdef with queryChild(0) as the actual scope
    IHqlScope * libraryScope;

    virtual void sethash();
public:
    CHqlLibraryInstance(IHqlExpression * _scopeFunction, HqlExprArray &parms);

//IHqlExpression
    virtual IHqlExpression *queryFunctionDefinition() const { return scopeFunction; }
    virtual bool equals(const IHqlExpression & other) const;
    virtual IHqlExpression * clone(HqlExprArray & children);

//IHqlDataset
    virtual void defineSymbol(IHqlExpression * expr)            { throwUnexpected(); }
    virtual IHqlExpression *lookupSymbol(_ATOM searchName, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual bool hasBaseClass(IHqlExpression * searchBase)      { return libraryScope->hasBaseClass(searchBase); }

    virtual void getSymbols(HqlExprArray& exprs) const          { libraryScope->getSymbols(exprs); }

    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols);
};

class CHqlVariable : public CHqlExpressionWithType
{
protected:
    StringAttr name;
    CHqlVariable(node_operator _op, const char * _name, ITypeInfo * _type);
    virtual bool equals(const IHqlExpression & other) const;
    virtual void sethash();
public:
    static CHqlVariable *makeVariable(node_operator op, const char * name, ITypeInfo * type);
    virtual StringBuffer &toString(StringBuffer &ret);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual StringBuffer &printAliases(StringBuffer &s, unsigned, bool &) { return s; }
};

class CHqlAttribute : public CHqlExpressionWithTables
{
protected:
    _ATOM name;
    CHqlAttribute(node_operator _op, _ATOM _name);
    virtual bool equals(const IHqlExpression & other) const;
    virtual void sethash();
    virtual bool isAttribute() const { return true; }
public:
    static CHqlAttribute *makeAttribute(node_operator op, _ATOM name);
    virtual _ATOM queryName() const { return name; }
    virtual StringBuffer &toString(StringBuffer &ret);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual StringBuffer &printAliases(StringBuffer &s, unsigned, bool &) { return s; }
    virtual ITypeInfo *queryType() const;
    virtual ITypeInfo *getType();
};

class CHqlUnknown : public CHqlExpressionWithType
{
protected:
    _ATOM name;
    LinkedIInterface extra;
    CHqlUnknown(node_operator _op, ITypeInfo * _type, _ATOM _name, IInterface * _extra);
    virtual bool equals(const IHqlExpression & other) const;
    virtual void sethash();
public:
    static CHqlUnknown *makeUnknown(node_operator _op, ITypeInfo * _type, _ATOM _name, IInterface * _extra);
    virtual _ATOM queryName() const { return name; }
    virtual IInterface *queryUnknownExtra();
    virtual StringBuffer &toString(StringBuffer &ret);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual StringBuffer &printAliases(StringBuffer &s, unsigned, bool &) { return s; }
};

class CHqlSequence : public CHqlExpressionWithType
{
protected:
    unsigned __int64 seq;
    _ATOM name;

    CHqlSequence(node_operator _op, ITypeInfo * _type, _ATOM _name, unsigned __int64 _seq);
    virtual bool equals(const IHqlExpression & other) const;
    virtual bool isAttribute() const { return op==no_attr; }
    virtual void sethash();
public:
    static CHqlSequence *makeSequence(node_operator _op, ITypeInfo * _type, _ATOM _name, unsigned __int64 _seq);
    virtual _ATOM queryName() const { return name; }
    virtual unsigned __int64 querySequenceExtra() { return seq; }
    virtual StringBuffer &toString(StringBuffer &ret);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual StringBuffer &printAliases(StringBuffer &s, unsigned, bool &) { return s; }
};

class CHqlCachedBoundFunction : public CHqlExpressionWithTables
{
public:
    CHqlCachedBoundFunction(IHqlExpression * func, bool _forceOutOfLineExpansion);

    virtual ITypeInfo *queryType() const;
    virtual ITypeInfo *getType();
    virtual IHqlExpression *clone(HqlExprArray &);

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

    IMPLEMENT_IINTERFACE_USING(CHqlExpression)

//IHqlExpression
    virtual IHqlSimpleScope *querySimpleScope() { return this; };
    virtual IHqlExpression *addOperand(IHqlExpression *field);
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual bool equals(const IHqlExpression & other) const;
    virtual _ATOM queryName() const { return unnamedAtom; }
    virtual void sethash();
    virtual ITypeInfo *queryType() const;
    virtual ITypeInfo *getType();

//ITypeInfo
    virtual type_t getTypeCode() const { return type_record; }
    virtual size32_t getSize() { return 0; }
    virtual unsigned getAlignment();
    virtual unsigned getPrecision() { assertex(!"tbd"); return 0; }
    virtual unsigned getBitSize()  { return 0; }
    virtual unsigned getStringLen() { assertex(!"tbd"); return 0; }
    virtual unsigned getDigits() { assertex(!"tbd"); return 0; }
    virtual bool assignableFrom(ITypeInfo * source);
    virtual IValue * castFrom(bool isSignedValue, __int64 value) { return NULL; }
    virtual IValue * castFrom(double value)  { return NULL; }
    virtual IValue * castFrom(size32_t len, const char * text)  { return NULL; }
    virtual IValue * castFrom(size32_t len, const UChar * text)  { return NULL; }
    virtual StringBuffer &getECLType(StringBuffer & out); 
    virtual StringBuffer &getDescriptiveType(StringBuffer & out) { return getECLType(out); }
    virtual unsigned getCrc();

    virtual const char *queryTypeName()  { return queryName()->str(); }
    
    virtual unsigned getCardinality()           { return 0; }
    virtual bool isInteger()                    { return false; }
    virtual bool isReference()                  { return false; }
    virtual bool isScalar()                     { return false; }
    virtual bool isSigned()                     { return false; }
    virtual bool isSwappedEndian()              { return false; }
    virtual ICharsetInfo * queryCharset()       { return NULL; }
    virtual ICollationInfo * queryCollation()   { return NULL; }
    virtual _ATOM queryLocale()                 { return NULL; }
    virtual ITypeInfo * queryChildType()        { return NULL; }
    virtual IInterface * queryDistributeInfo()  { return NULL; }
    virtual IInterface * queryGroupInfo()       { return NULL; }
    virtual IInterface * queryGlobalSortInfo()  { return NULL; }
    virtual IInterface * queryLocalUngroupedSortInfo()   { return NULL; }
    virtual IInterface * queryGroupSortInfo()   { return NULL; }
    virtual ITypeInfo * queryPromotedType()     { return this; }
    virtual ITypeInfo * queryTypeBase()         { return this; }
    virtual typemod_t queryModifier()           { return typemod_none; }
    virtual IInterface * queryModifierExtra()   { return NULL; }
    virtual StringBuffer & appendStringFromMem(StringBuffer & out, const void * data) {assertex(!"tbd"); return out; }

    virtual void serialize(MemoryBuffer &) { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &) { UNIMPLEMENTED; }

// IHqlSimpleScope
    IHqlExpression *lookupSymbol(_ATOM fieldName);
    void insertSymbols(IHqlExpression * expr);
};

class CHqlDictionary : public CHqlExpressionWithType // , implements IHqlDictionary
{
public:
    IMPLEMENT_IINTERFACE_USING(CHqlExpression)
    static CHqlDictionary *makeDictionary(node_operator op, ITypeInfo *type, HqlExprArray &operands);

public:
    CHqlDictionary(node_operator op, ITypeInfo *_type, HqlExprArray &_ownedOperands);
    ~CHqlDictionary();

    virtual IHqlExpression *clone(HqlExprArray &newkids);
};


class CHqlDataset : public CHqlExpressionWithType, implements IHqlDataset
{
public:
    IMPLEMENT_IINTERFACE_USING(CHqlExpression)
    static CHqlDataset *makeDataset(node_operator op, ITypeInfo *type, HqlExprArray &operands);

    virtual IHqlDataset *queryDataset() { return this; };
    virtual IHqlSimpleScope *querySimpleScope();
    virtual IHqlDataset* queryTable();
    virtual IHqlExpression *queryNormalizedSelector(bool skipIndex);

    bool equals(const IHqlExpression & r) const;
    virtual IHqlExpression *clone(HqlExprArray &newkids);

protected:
    IHqlDataset *rootTable;
    IHqlExpression * container;
    OwnedHqlExpr normalized;

    void cacheParent();

public:
    CHqlDataset(node_operator op, ITypeInfo *_type, HqlExprArray &_ownedOperands);
    ~CHqlDataset();

    virtual StringBuffer &printAliases(StringBuffer &s, unsigned, bool &) { return s; }
    virtual void sethash();     // needed while it has a name....

//interface IHqlDataset 
    virtual IHqlDataset *queryRootTable() { return rootTable; };
    virtual IHqlExpression * queryContainer() { return container; }
    virtual bool isAggregate();
};


class CHqlAlienType : public CHqlExpressionWithTables, implements ITypeInfo, implements IHqlSimpleScope, implements IHqlAlienTypeInfo
{
private:
    IHqlScope *scope;
    ITypeInfo *logical;
    ITypeInfo *physical;
    IHqlExpression *funcdef;
    _ATOM name;

public:
    CHqlAlienType(_ATOM, IHqlScope *, IHqlExpression * _funcdef);
    ~CHqlAlienType();

    IMPLEMENT_IINTERFACE_USING(CHqlExpression)

//IHqlExpression
    virtual IHqlExpression *queryFunctionDefinition() const;
    virtual IHqlScope *queryScope() { return scope; };
    virtual IHqlSimpleScope *querySimpleScope() { return this; };
    virtual bool equals(const IHqlExpression & other) const;
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual void sethash();
    virtual ITypeInfo *queryType() const;
    virtual ITypeInfo *getType();

//ITypeInfo
    virtual type_t getTypeCode() const { return type_alien; }
    virtual size32_t getSize();
    virtual unsigned getAlignment() { return 1; }
    virtual unsigned getPrecision()             { return logical->getPrecision(); }
    virtual unsigned getBitSize()               { return logical->getBitSize(); }
    virtual unsigned getStringLen()             { return logical->getStringLen(); }
    virtual unsigned getDigits()                { return logical->getDigits(); }
    virtual bool assignableFrom(ITypeInfo * source) { return logical->assignableFrom(source->queryPromotedType());}
    virtual IValue * castFrom(bool isSignedValue, __int64 value)    { return NULL; }
    virtual IValue * castFrom(double value)  { return NULL; }
    virtual IValue * castFrom(size32_t len, const char * text)  { return NULL; }
    virtual IValue * castFrom(size32_t len, const UChar * text)  { return NULL; }
    virtual StringBuffer &getECLType(StringBuffer & out)  { return out.append(name); }
    virtual StringBuffer &getDescriptiveType(StringBuffer & out)  { return getECLType(out); }
    virtual const char *queryTypeName()         { return name->str(); }
    virtual unsigned getCardinality();
    virtual bool isInteger()                    { return logical->isInteger(); }
    virtual bool isReference()                  { return false; }
    virtual bool isScalar()                     { return true; }
    virtual bool isSigned()                     { return logical->isSigned(); }
    virtual bool isSwappedEndian()              { return logical->isSwappedEndian(); }
    virtual ICharsetInfo * queryCharset()       { return logical->queryCharset(); }
    virtual ICollationInfo * queryCollation()   { return logical->queryCollation(); }
    virtual _ATOM queryLocale()                 { return logical->queryLocale(); }
    virtual _ATOM queryName() const             { return name; }
    virtual ITypeInfo * queryChildType() { return logical; }
    virtual IInterface * queryDistributeInfo()  { return NULL; }
    virtual IInterface * queryGroupInfo()       { return NULL; }
    virtual IInterface * queryGlobalSortInfo()  { return NULL; }
    virtual IInterface * queryLocalUngroupedSortInfo()   { return NULL; }
    virtual IInterface * queryGroupSortInfo()   { return NULL; }
    virtual ITypeInfo * queryPromotedType()     { return logical->queryPromotedType(); }
    virtual ITypeInfo * queryTypeBase()         { return this; }
    virtual typemod_t queryModifier()           { return typemod_none; }
    virtual IInterface * queryModifierExtra()   { return NULL; }
    virtual StringBuffer & appendStringFromMem(StringBuffer & out, const void * data) {assertex(!"tbd"); return out; }
    virtual unsigned getCrc();

    virtual void serialize(MemoryBuffer &) { UNIMPLEMENTED; }
    virtual void deserialize(MemoryBuffer &) { UNIMPLEMENTED; }

// IHqlSimpleScope
    IHqlExpression *lookupSymbol(_ATOM fieldName);

// interface IHqlAlienTypeInfo
    virtual ITypeInfo *getLogicalType() { return LINK(logical); }
    virtual ITypeInfo *getPhysicalType() { return LINK(physical); }
    virtual ITypeInfo * queryLogicalType() { return logical; }
    virtual ITypeInfo * queryPhysicalType() { return physical; }
    virtual int getLogicalTypeSize()  { return logical->getSize(); }
    virtual int getPhysicalTypeSize() { return physical->getSize(); }
    virtual unsigned getMaxSize();
    virtual IHqlExpression * queryLoadFunction();
    virtual IHqlExpression * queryLengthFunction();
    virtual IHqlExpression * queryStoreFunction();
    virtual IHqlExpression * queryFunction(_ATOM name);

private:
    IHqlExpression * queryMemberFunc(_ATOM name);
};

class CHqlEnumType : public CHqlExpressionWithType
{
private:
    IHqlScope *scope;

public:
    CHqlEnumType(ITypeInfo * baseType, IHqlScope * _scope);
    ~CHqlEnumType();

//IHqlExpression
    virtual IHqlScope *queryScope() { return scope; };
    virtual bool equals(const IHqlExpression & other) const;
    virtual IHqlExpression *clone(HqlExprArray &newkids);
    virtual void sethash();
};

/*
class SilentErrorReceiver : implements IErrorReceiver
{
private:
    unsigned errcount;
public:
    SilentErrorReceiver() { errcount = 0; }
    virtual void reportError(int errNo, const char *msg, _ATOM filename, int _lineno, int _column, int _pos);
    unsigned getErrCount() { return errcount; }
};

*/

class HqlGramCtx;
extern void defineSymbol(_ATOM name, IHqlExpression *value);
extern void parseAttribute(IHqlScope *scope, IFileContents * contents, HqlLookupContext & ctx, _ATOM name);
extern bool parseForwardModuleMember(HqlGramCtx & _parent, IHqlScope *scope, IHqlExpression * forwardSymbol, HqlLookupContext & ctx);

IHqlExpression *createAttribute(node_operator op, _ATOM name, IHqlExpression * value, IHqlExpression *value2, IHqlExpression * value3);
IHqlExpression *createAttribute(node_operator op, _ATOM name, HqlExprArray & args);

#endif
