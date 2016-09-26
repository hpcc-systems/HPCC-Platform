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
#include "platform.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jdebug.hpp"

#include "defvalue.hpp"
#include "hql.hpp"
#include "hqlattr.hpp"
#include "hqlfold.ipp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "hqlerror.hpp"
#include "hqlerrors.hpp"
#include "hqlutil.hpp"
#include "hqlpmap.hpp"
#include "hqlmeta.hpp"

#include "hqlfold.hpp"
#include "hqlthql.hpp"
#include "eclhelper.hpp"

#ifdef __APPLE__
#include <dlfcn.h>
#endif

//#define LOG_ALL_FOLDING

//---------------------------------------------------------------------------
// The following functions do not attempt to reorder datasets, e.g., filter(project)->project(filter).
// Those changes can inadvertently cause common code to be lost.  Those optimizations are performed by
// hqlopt which ensures it keeps track of the number of times a dataset expression is used.

IHqlExpression * createNullValue(IHqlExpression * expr)
{
    return createConstant(createNullValue(expr->queryType()));
}

static bool isDuplicateMapCondition(const HqlExprArray & values, IHqlExpression * cond)
{
    ForEachItemIn(i, values)
    {
        if (values.item(i).queryChild(0) == cond)
            return true;
    }
    return false;
}

static bool areIndenticalMapResults(const HqlExprArray & values, IHqlExpression * defaultExpr)
{
    unsigned max = values.ordinality();
    for (unsigned i=0; i < max; i++)
    {
        if (values.item(i).queryChild(1)->queryBody() != defaultExpr->queryBody())
            return false;
    }
    return true;
}

static bool isOrderedType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_boolean:
        return false;
    }
    return true;
}

static IHqlExpression * createCompareResult(node_operator op, int compare)
{
    switch (op)
    {
    case no_eq:
        return createConstant(compare == 0);
    case no_ne:
        return createConstant(compare != 0);
    case no_lt:
        return createConstant(compare < 0);
    case no_le:
        return createConstant(compare <= 0);
    case no_gt:
        return createConstant(compare > 0);
    case no_ge:
        return createConstant(compare >= 0);
    case no_order:
        return createConstant(createIntValue(compare, 4, true));
    default:
        throwUnexpectedOp(op);
    }
}

/*In castExpr, constExpr: NOT linked. Out: linked */
static IHqlExpression * optimizeCast(node_operator compareOp, IHqlExpression * castExpr, IHqlExpression * constExpr)
{
    assertex(isCast(castExpr));
    bool createTrueConst = false;
    bool createFalseConst = false;
    node_operator newNode = no_none;

    //castXXX refers to types/values with the cast in place uncastXXX refer to types/values with it removed.
    ITypeInfo * castType = castExpr->queryType();
    IHqlExpression * uncastChild = castExpr->queryChild(0);
    ITypeInfo * uncastType = uncastChild->queryType();

    //If the cast loses information then we can't remove it....
    if (!preservesValue(castType, uncastType))
        return NULL;

    //If the comparison is non equality and the cast changes the collation sequence then you can't remove it.
    if ((compareOp != no_eq) && (compareOp != no_ne))
    {
        if (!preservesOrder(castType, uncastType))
            return NULL;

        //This seems an arbitrary exception, but if the comparison is ordered, and value being cast doesn't really
        //have a sensible ordering (i.e. boolean) then the cast shouldn't be removed.
        //i.e. make sure "(real)boolval < 0.5" does not become "boolval <= true".
        if (!isOrderedType(uncastType))
            return NULL;
    }

    IValue * castValue = constExpr->queryValue();
    OwnedIValue uncastValue(castValue->castTo(uncastType));

    if (uncastValue)
    {
        //Check whether casting the value to the new type can be represented.  If not then 
        int rc = castValue->rangeCompare(uncastType);
        if (rc != 0)
        {
            //This is effectively RHS compare min/max lhs, so invert the compare result
            return createCompareResult(compareOp, -rc);
        }
        else
        {
            OwnedIValue recast(uncastValue->castTo(castType));
            if (recast)
            {
                int test = recast->compare(castValue);
                //test = newValue <=> oldValue
                switch (compareOp)
                {
                case no_eq:
                    if (test == 0)
                        newNode = no_eq;
                    else
                        createFalseConst = true;
                    break;
                case no_ne:
                    if (test == 0)
                        newNode = no_ne;
                    else
                        createTrueConst = true;
                    break;
                case no_lt:
                    //If new value less than old value, so < now becomes <=
                    newNode = (test < 0) ? no_le : no_lt;
                    break;
                case no_ge:
                    //If new value less than old value, so >= now becomes >
                    newNode = (test < 0) ? no_gt : no_ge;
                    break;
                case no_le:
                    //If new value is greater than old value, <= becomes <
                    newNode = (test > 0) ? no_lt : no_le;
                    break;
                case no_gt:
                    //If new value is greater than old value, > becomes >=
                    newNode = (test > 0) ? no_ge : no_gt;
                    break;
                default:
                    throwUnexpected();
                }
            }
        }
    }
    else
    {
        createTrueConst = (compareOp == no_ne);
        createFalseConst = (compareOp == no_eq);
    }

    if (createTrueConst)
        return createConstant(true);
    if (createFalseConst)
        return createConstant(false);
    
    if (newNode != no_none)
        return createBoolExpr(newNode, LINK(uncastChild), createConstant(uncastValue.getClear()));

    return NULL;
}

//In castExpr: not linked. Out: linked 
static IHqlExpression * optimizeCastList(IHqlExpression * castExpr, HqlExprArray & inList, node_operator op)
{
    assertex(isCast(castExpr));
    IHqlExpression * castChild = castExpr->queryChild(0);
    ITypeInfo * targetType = castChild->queryType();
    ITypeInfo * currentType = castExpr->queryType();

    //If the cast loses information then we can't remove it....
    if (!preservesValue(currentType, targetType))
        return NULL;

    //(cast)search in <list>
    //Produce a new list of values which only includes values that could possibly match the search value
    HqlExprArray newInConstants;
    ForEachItemIn(i, inList)
    {
        bool skip = true;
        IValue * constValue = inList.item(i).queryValue();
        if (!constValue)
            return NULL;
        OwnedIValue cast(constValue->castTo(targetType));
        if (cast)
        {
            int rc = constValue->rangeCompare(targetType);
            if (rc == 0)
            {
                OwnedIValue recast(cast->castTo(constValue->queryType()));
                if (recast)
                {
                    int test = recast->compare(constValue);
                    if (test == 0)
                        skip = false;
                }
            }
        }
        if (!skip)
            newInConstants.append(*createConstant(cast.getClear()));
    }

    if (newInConstants.ordinality())
    {
        IHqlExpression * newList = createValue(no_list, makeSetType(LINK(targetType)), newInConstants);
        return createBoolExpr(op, LINK(castChild), newList);
    }
    return createConstant(op == no_notin);
}

static bool isInList(IValue * v, IHqlExpression * list)
{
    if (list->getOperator()==no_all)
        return true;

    unsigned num = list->numChildren();
    for (unsigned idx = 0; idx < num; idx++)
    {
        IHqlExpression * elem = list->queryChild(idx);
        IValue * constValue = elem->queryValue();
        if (constValue)
        {
            if (orderValues(v, constValue) == 0)
                return true;
        }
    }
    return false;
}

static IValue * compareValues(node_operator op, IValue * leftValue, IValue * rightValue)
{
    IValue * newConst;
    switch (op)
    {
    case no_eq:
        newConst = equalValues(leftValue,rightValue);
        break;
    case no_ne:
        newConst = notEqualValues(leftValue,rightValue);
        break;
    case no_lt:
        newConst = lessValues(leftValue,rightValue);
        break;
    case no_le:
        newConst = lessEqualValues(leftValue,rightValue);
        break;
    case no_gt:
        newConst = greaterValues(leftValue,rightValue);
        break;
    case no_ge:
        newConst = greaterEqualValues(leftValue,rightValue);
        break;
    case no_order:
        newConst = createIntValue(leftValue->compare(rightValue), 4, true);
        break;
    default:
        throwUnexpectedOp(op);
    }
    return newConst;
}

static IHqlExpression * compareLists(node_operator op, IHqlExpression * leftList, IHqlExpression * rightList)
{
    unsigned lnum = leftList->numChildren();
    unsigned rnum = rightList->numChildren();
    int order = 0;
    unsigned num = lnum > rnum ? rnum : lnum;
    for (unsigned i=0; i < num; i++)
    {
        IValue * leftValue = leftList->queryChild(i)->queryValue();
        IValue * rightValue = rightList->queryChild(i)->queryValue();
        if (!leftValue || !rightValue)
            return NULL;
        order = orderValues(leftValue, rightValue);
        if (order != 0)
            return createCompareResult(op, order);
    }
    if (lnum != rnum)
        order = lnum > rnum ? +1 : -1;
    return createCompareResult(op, order);
}


static IHqlExpression * optimizeListConstant(node_operator op, IHqlExpression * list, IValue * constVal)
{
    if ((list->getOperator() != no_list) || !list->isConstant())
        return NULL;

    //I don't really know what this function is trying to do.  I think it is trying to optimize the case where 
    //comparing against any of the values in the list will give the same result.
    OwnedIValue nullVal = createNullValue(list->queryType()->queryChildType());
    OwnedIValue result = compareValues(op, nullVal, constVal);
    ForEachChild(i, list)
    {
        IValue * curValue = list->queryChild(i)->queryValue();
        if (!curValue)
            return NULL;
        Owned<IValue> curResult = compareValues(op, curValue, constVal);
        if (curResult->compare(result) != 0)
            return NULL;
    }
    return createConstant(result.getClear());
}

static bool flattenConstantCase(IHqlExpression * caseExpr, HqlExprArray & constants, bool out)
{
    assertex(caseExpr->getOperator()==no_case);
    unsigned num = caseExpr->numChildren()-1;
    for (unsigned i=1; i<num; i++)
    {
        IHqlExpression * map = caseExpr->queryChild(i);
        IHqlExpression * val = map->queryChild(out);
        if (!val->queryValue())
            return false;
        constants.append(*LINK(val));
    }   
    return true;
}

static IHqlExpression * optimizeCaseConstant(node_operator op, IHqlExpression * caseExpr, IValue * constVal, bool swap)
{
    HqlExprArray caseResults;
    if (flattenConstantCase(caseExpr, caseResults, true))
    {
        IValue * defValue = caseExpr->queryChild(caseExpr->numChildren()-1)->queryValue();
        if (defValue)
        {
            switch (op)
            {
            case no_eq:
            case no_ne:
                {
                    //CASE(x,a1=>v1,a2=>v2,a3=>v3,v0) [not]= y
                    //If y ==a0 then transform to x [NOT] IN [a<n>] where v<n>!=y
                    bool matchesDefault = (defValue->compare(constVal) == 0);
                    HqlExprCopyArray matches;
                    HqlExprArray exceptions;
                    for (unsigned i=0; i<caseResults.ordinality(); i++)
                    {
                        IHqlExpression * key = caseExpr->queryChild(i+1)->queryChild(0);
                        IHqlExpression * val = &caseResults.item(i);
                        bool caseMatches = (val->queryValue()->compare(constVal) == 0);
                        if (caseMatches == matchesDefault)
                            matches.append(*key->queryBody());
                        else if (!matches.contains(*key->queryBody()))
                            exceptions.append(*LINK(key));
                    }
                    bool defaultsToTrue = (matchesDefault && (op == no_eq)) || (!matchesDefault && (op == no_ne));
                    if (exceptions.ordinality() == 0)
                        return createConstant(defaultsToTrue);
                    node_operator inOp = defaultsToTrue ? no_notin : no_in;
                    IHqlExpression * test = caseExpr->queryChild(0);
                    return createBoolExpr(inOp,
                            LINK(test),
                            createValue(no_list, makeSetType(test->getType()), exceptions));
                }
            }
        }
    }
    return NULL;
}

static IHqlExpression * optimizeCompare(IHqlExpression * expr)
{
    IHqlExpression * leftChild = expr->queryChild(0);
    IHqlExpression * rightChild = expr->queryChild(1);
    node_operator op = expr->getOperator();
    node_operator leftOp = leftChild->getOperator();
    node_operator rightOp = rightChild->getOperator();

    if ((leftChild->queryBody() == rightChild->queryBody()) ||
        (leftOp == no_all && rightOp == no_all))
    {
        return createCompareResult(op, 0);
    }

    if ((leftOp == no_all) && rightChild->isConstant())
        return createCompareResult(op, +1);
    
    if ((rightOp == no_all) && leftChild->isConstant())
        return createCompareResult(op, -1);
    
    if (((leftOp == no_sortlist) || (leftOp == no_list)) && ((rightOp == no_sortlist) || (rightOp == no_list)))
        return compareLists(op, leftChild, rightChild);

    IValue * leftValue = leftChild->queryValue();
    IValue * rightValue = rightChild->queryValue();
    if (leftValue && rightValue)
    {
        int order = orderValues(leftValue, rightValue);
        return createCompareResult(op, order);
    }

    if (op == no_order)
        return NULL;
    
    bool swap = false;
    IHqlExpression * castChild = NULL;
    IHqlExpression * constChild = NULL;
    if (leftValue)
    {
        ITypeInfo * rType = rightChild->queryType();
        if (rType->isUnsignedNumeric() && isZero(leftChild))
        {
            switch (op)
            {
            case no_le:
                return createConstant(true);
            case no_gt:
                return createConstant(false);
            }
        }
        if (rType->getTypeCode() == type_boolean)
        {
            bool val = leftValue->getBoolValue();
            switch (op)
            {
            case no_eq:
                if (val)
                    return LINK(rightChild);
                return getInverse(rightChild);
            case no_ne:
                if (!val)
                    return LINK(rightChild);
                return getInverse(rightChild);
            }
        }
        swap = true;

        switch(rightChild->getOperator())
        {
        case no_cast:
        case no_implicitcast:
            castChild = rightChild;
            constChild = leftChild; 
            break;
        case no_index:
            return optimizeListConstant(getReverseOp(op), rightChild->queryChild(0), leftValue);
        }
    }
    else if (rightValue)
    {
        ITypeInfo * lType = leftChild->queryType();
        if (lType->isUnsignedNumeric() && isZero(rightChild))
        {
            switch (op)
            {
            case no_ge:
                return createConstant(true);
            case no_lt:
                return createConstant(false);
            }
        }
        if (lType->getTypeCode() == type_boolean)
        {
            bool val = rightValue->getBoolValue();
            switch (op)
            {
            case no_eq:
                if (val)
                    return LINK(leftChild);
                return getInverse(leftChild);
            case no_ne:
                if (!val)
                    return LINK(leftChild);
                return getInverse(leftChild);
            }
        }
        switch(leftChild->getOperator())
        {
        case no_cast:
        case no_implicitcast:
            castChild = leftChild;
            constChild = rightChild; 
            break;
        case no_index:
            return optimizeListConstant(op, leftChild->queryChild(0), rightValue);
        case no_case:
            return optimizeCaseConstant(op, leftChild, rightValue, false);
        }
    }

    if (castChild)
    {
        OwnedHqlExpr value = optimizeCast(swap ? getReverseOp(op) : op, castChild, constChild);
        if (value)
            return value.getClear();

    }

    if (swap)
    {
        //Normalize simple comparisons so they are always (field op value)
        return createValue(getReverseOp(op), makeBoolType(), LINK(rightChild), LINK(leftChild));
    }

    return NULL;
}

static bool isSimpleComparisonArg(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_constant:
    case no_getresult:
        return true;
    }
    return false;
}

//---------------------------------------------------------------------------

/*********************************************************
 * Constant folding for an external function call
 * Supports the following external function parameter types:
 *       - INTEGER (Tested)
 *       - REAL
 *       - STRINGN (Tested)
 *       - STRING  (Tested)
 *       - VARSTRINGN
 *       - VARSTRING
 *       - BOOLEAN
 * Supports the following external function return types:
 *       - INTEGER (TESTED)
 *       - STRING  (tested)
 *       - STRINGN (Tested)
 *       - VARSTRING
 *       - VARSTRINGN
 *       - REAL
 *       - BOOLEAN
 * NOTE: Tested with the functions in default.StringLib. The 
 *       functions need to be declared with extern "C".
 *********************************************************/
 //MORE: This function never unloads the plugin dll - this may cause problems in the long run.

bool checkExternFoldable(IHqlExpression* expr, unsigned foldOptions, StringBuffer &library, StringBuffer &entry)
{
    IHqlExpression * funcdef = expr->queryExternalDefinition();
    if(!funcdef)
        return false;
    IHqlExpression *body = funcdef->queryChild(0);
    if(!body)
        return false;

    //Check all parameters are constant - saves dll load etc.
    unsigned numParam = expr->numChildren();
    for(unsigned iparam = 0; iparam < numParam; iparam++)
    {
        if (!expr->queryChild(iparam)->queryValue())            //NB: Already folded...
            return false;
    }

    IHqlExpression * formals = funcdef->queryChild(1);
    unsigned numArg = formals->numChildren();
    if(numParam > numArg)
    {
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_PARAM_TOOMANY,"Too many parameters passed to function '%s': expected %d, given %d",
                                      str(expr->queryName()), numParam, numArg);
        return false;
    }
    else if(numParam < numArg)
    {
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_PARAM_TOOFEW,"Not enough parameters passed to function '%s': expected %d, given %d",
                                      str(expr->queryName()), numParam, numArg);
        return false;
    }

    StringBuffer mangledEntry;
    getAttribute(body, entrypointAtom, entry);
    getAttribute(body, libraryAtom, library);
    if (!library.length())
        getAttribute(body, pluginAtom, library);
    if(entry.length() == 0)
    {
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_SVC_NOENTRYPOINT,"Missing entrypoint for function folding");
        return false;
    }
    if (library.length() == 0)
    {
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_SVC_NOLIBRARY,"Missing library for function folding");
        return false;
    }

    if (!pathExtension(library))
    {
        library.insert(0, SharedObjectPrefix);
        ensureFileExtension(library, SharedObjectExtension);
    }

    if (!body->hasAttribute(foldAtom) || body->hasAttribute(nofoldAtom))
    {
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_TMPLT_NOFOLDFUNC, "%s does not have FOLD specified, can't constant fold it", str(expr->queryName()));
        return false;
    }
    if (body->hasAttribute(_disallowed_Atom))
    {
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_TMPLT_NOFOLDFUNC, "You do not have permission to constant-fold %s", str(expr->queryName()));
        return false;
    }
    if(!body->hasAttribute(pureAtom) && !body->hasAttribute(templateAtom) && !(foldOptions & (HFOfoldimpure|HFOforcefold)))
    {
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_TMPLT_NONPUREFUNC, "%s/%s is not a pure function, can't constant fold it", library.str(), entry.str());
        return false;
    }

    if(body->hasAttribute(contextAtom) || body->hasAttribute(globalContextAtom) ||
       body->hasAttribute(gctxmethodAtom) || body->hasAttribute(ctxmethodAtom) || body->hasAttribute(omethodAtom))
    {
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_TMPLT_NONEXTERNCFUNC, "%s/%s requires a runtime context to be executed, can't constant fold it", library.str(), entry.str());
        return false;
    }

    if(!body->hasAttribute(cAtom))
    {
        if (!createMangledFunctionName(mangledEntry, funcdef))
        {
            if (foldOptions & HFOthrowerror)
                throw MakeStringException(ERR_TMPLT_NONEXTERNCFUNC, "%s/%s is not declared as extern C, can't constant fold it", library.str(), entry.str());
            return false;
        }
        entry.set(mangledEntry);
    }
    return true;
}

IValue * doFoldExternalCall(IHqlExpression* expr, unsigned foldOptions, ITemplateContext *templateContext, const char *library, const char *entrypoint)
{
    IHqlExpression * funcdef = expr->queryExternalDefinition();
    IHqlExpression *body = funcdef->queryChild(0);

    // Get the handle to the library and procedure.
#ifdef __APPLE__
    StringBuffer fullLibraryPath;
    // OSX is not good at finding eclrtl. This hack is a workaround
    if (streq(library, "libeclrtl.dylib"))
    {
        Dl_info info;
        if (dladdr((const void *) rtlStrToUInt4, &info))  // Any function in eclrtl would do...
        {
            fullLibraryPath.set(info.dli_fname);
            library = fullLibraryPath.str();
        }
    }
#ifdef _DEBUG
    if (streq(library, "libpyembed.dylib") || streq(library, "libv8embed.dylib") || streq(library, "libjavaembed.dylib"))
    {
        Dl_info info;
        if (dladdr((const void *) rtlStrToUInt4, &info))  // Any function in eclrtl would do...
        {
            fullLibraryPath.set(info.dli_fname);
            fullLibraryPath.replaceString("libeclrtl.dylib", library);
            library = fullLibraryPath.str();
        }
    }
#endif
#endif

    HINSTANCE hDLL=LoadSharedObject(library, false, false);
    if (!LoadSucceeded(hDLL))
    {
        if (body->hasAttribute(templateAtom))
            throw MakeStringException(ERR_SVC_LOADLIBFAILED, "Error happened when trying to load library %s for template helper function", library);
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_SVC_LOADLIBFAILED, "Error happened when trying to load library %s", library);
        return NULL;
    }
    void* fh = GetSharedProcedure(hDLL, entrypoint);
    if (!fh)
    {
        FreeSharedObject(hDLL);
        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_SVC_LOADFUNCFAILED, "Error happened when trying to load procedure %s from library %s", entrypoint, library);
        return NULL;
    }

    // create a FuncCallStack to generate a stack used to pass parameters to 
    // the called function
    FuncCallStack fstack;
    
    if(body->hasAttribute(templateAtom))
        fstack.pushPtr(templateContext);

    //if these were allowed to be optional - then the following code would be needed
    if(body->hasAttribute(contextAtom) || body->hasAttribute(globalContextAtom))
        fstack.pushPtr(NULL);

    bool retCharStar = false;
    bool retUCharStar = false;
    bool charStarInParam = false;
    unsigned resultsize = 4; // the number of bytes of the result.
    int isRealvalue = 0;

    unsigned tlen = 0;
    char* tgt = NULL;
    // Process return value
    ITypeInfo * retType = funcdef->queryType()->queryChildType();
    type_t typecode = retType->getTypeCode();
    switch (typecode) 
    {
    case type_varstring:
    case type_varunicode:
        if (retType->getSize() == UNKNOWN_LENGTH)
        {
            // variable length varstring, should return as char*
            retCharStar = true;
            if(typecode==type_varunicode) retUCharStar = true;
            resultsize = sizeof(char *);
            break;
        }
        //fallthrough
    case type_string:
    case type_data:
    case type_qstring:
    case type_unicode:
    case type_utf8:
        if (retType->getSize() == UNKNOWN_LENGTH)
        {
            // string, pass in the reference of length var and char* var. After function call,
            // values will be stored in them.
            fstack.pushRef(tlen);
            fstack.pushRef(tgt);
        }
        else
        {
            // stringn or varstringn, create a char array and pass it. Don't pass length var(as the
            // length is fixed).
            tlen = retType->getSize();
            tgt = (char*) malloc(tlen + 1); // To be safe, allocate one byte more.
            fstack.push(tgt);
        }
        charStarInParam = true;
        break;
    case type_real:
        // For real, get the result size
        resultsize = retType->getSize();
        isRealvalue = 1;
        break;
    case type_boolean:
    case type_int:
    case type_decimal:
    case type_date:
    case type_char:
    case type_enumerated:
    case type_swapint:
    case type_packedint:
        resultsize = retType->getSize();
        break;
    case type_void:
        if (!(foldOptions & (HFOfoldimpure|HFOforcefold)))
        {
            if (foldOptions & HFOthrowerror)
                throw MakeStringException(ERR_TMPLT_NONPUREFUNC, "%s/%s is not an action, can't constant fold it", library, entrypoint);
            return NULL;
        }
        break;
    default:
        //can't fold things that return sets/datasets etc.
        return NULL;
    }

    // process all the parameters passed in
    unsigned numParam = expr->numChildren();
    IHqlExpression * formals = funcdef->queryChild(1);
    for(unsigned i = 0; i < numParam; i++) 
    {
        IHqlExpression * curParam = expr->queryChild(i);            //NB: Already folded...
        IHqlExpression * curArg = formals->queryChild(i);
        if(!curArg) {
            free(tgt);
            return NULL;
        }
        ITypeInfo * argType = curArg->queryType();
        if(!argType) {
            free(tgt);
            return NULL;
        }
        IValue * paramValue = curParam->queryValue();
        if (fstack.push(argType, paramValue) == -1)
        {
            free(tgt);
            return NULL;
        }
    }

    // Get the length and address of the stack
    unsigned len = fstack.getSp();
#ifdef REGPARAMS
    while (len < REGPARAMS*REGSIZE)
        len = fstack.pushPtr(NULL);         // ensure enough to fill REGPARAMS registers
#endif
#ifdef ODD_STACK_ALIGNMENT
    // Some architectures (arm) require that the total amount pushed onto the stack for parameters is an odd number of words
    // (so that the stack alignment is always an even number of words once the return IP is pushed)
    if ((len & REGSIZE) == 0)
        len = fstack.pushPtr(NULL);
#endif
#ifdef EVEN_STACK_ALIGNMENT
    // Other architectures (x86) require that the total amount pushed onto the stack for parameters is an even number of words
    // (so that the stack alignment is always an even number of words after the callq, which pushes an even number of words)
    if ((len & REGSIZE) == REGSIZE)
        len = fstack.pushPtr(NULL);
#endif
    char* strbuf = fstack.getMem();

    int intresult = 0;
#ifdef __64BIT__
    __int64 int64result = 0;
#else
    int intresulthigh = 0;
#endif
    float floatresult = 0.0;
    double doubleresult = 0.0;

#ifdef _ARCH_X86_64_
//  __asm__ ("\tint $0x3\n"); // for debugging
#endif

    try{
    // Assembly code that does the dynamic function call. The calling convention is a combination of 
    // Pascal and C, that is the parameters are pushed from left to right, the stack goes downward(i.e.,
    // the stack pointer decreases as you push), and the caller is responsible for restoring the 
    // stack pointer.

// **** Windows ****
#ifdef _WIN32
 // Note - we assume X86/X86_64 Procedure Call Standard
 #if defined (_ARCH_X86_64_)
        UNIMPLEMENTED;
 #elif defined (_ARCH_X86_)
        _asm{
        ;save registers that will be used
        push   ecx
        push   esi
        push   eax
        push   edx
        push   ebx

        ;copy parameters to the stack
        mov    ecx, len
        sub    esp, len
        mov    esi, strbuf
        jcxz   loop1tail

    loop1:
        mov    al, [esi]
        mov    [esp], al
        inc    esp
        inc    esi
        dec    ecx
        jnz    loop1
    loop1tail:

        ;call the procedure
        sub    esp, len
        call   fh
        add    esp, len

        ;save result
        mov    ebx, isRealvalue
        cmp    ebx, 1
        je     isreal
        mov    intresult, eax
        mov    intresulthigh, edx
        jmp    finish
    isreal:
        mov    ebx, resultsize
        cmp    ebx, 4
        ja     isdouble
        fstp   DWORD PTR floatresult
        jmp    finish
    isdouble:
        fstp   QWORD PTR doubleresult
    finish: 
        ;restore registers that were saved
        pop    ebx
        pop    edx
        pop    eax
        pop    esi
        pop    ecx
    }
 #else
    UNIMPLEMENTED;
 #endif
#else // WIN32

// **** Linux/Mac ****
 #ifdef _ARCH_X86_64_
        assertex((len & 15) == 0);  // We need to make sure we add an EVEN number of words to stack, so that it is 16-byte aligned before the callq

        __int64 dummy1, dummy2,dummy3,dummy4;

        void * floatstack = fstack.getFloatMem(); 
        if (floatstack) { // sets xmm0-7
            unsigned * floatSizes = fstack.getFloatSizes();
            __asm__ (
            ".doparm0: \n\t"
                "cmpl  $4,(%%rdi) \n\t"
                "jl  .floatdone \n\t"
                "je  .dofloat0 \n\t"
                "movsd  (%%rsi),%%xmm0 \n\t"
                "jmp .doparm1 \n\t"
            ".dofloat0: \n\t"
                "movss  (%%rsi),%%xmm0 \n\t"

            ".doparm1: \n\t"
                "cmpl  $4,4(%%rdi) \n\t"
                "jl  .floatdone \n\t"
                "je  .dofloat1 \n\t"
                "movsd  8(%%rsi),%%xmm1 \n\t"
                "jmp .doparm2 \n\t"
            ".dofloat1: \n\t"
                "movss  8(%%rsi),%%xmm1 \n\t"

            ".doparm2: \n\t"
                "cmpl  $4,8(%%rdi) \n\t"
                "jl  .floatdone \n\t"
                "je  .dofloat2 \n\t"
                "movsd  16(%%rsi),%%xmm2 \n\t"
                "jmp .doparm3 \n\t"
            ".dofloat2: \n\t"
                "movss  16(%%rsi),%%xmm2 \n\t"

            ".doparm3: \n\t"
                "cmpl  $4,12(%%rdi) \n\t"
                "jl  .floatdone \n\t"
                "je  .dofloat3 \n\t"
                "movsd  24(%%rsi),%%xmm3 \n\t"
                "jmp .doparm4 \n\t"
            ".dofloat3: \n\t"
                "movss  24(%%rsi),%%xmm3 \n\t"

            ".doparm4: \n\t"
                "cmpl  $4,16(%%rdi) \n\t"
                "jl  .floatdone \n\t"
                "je  .dofloat4 \n\t"
                "movsd  32(%%rsi),%%xmm4 \n\t"
                "jmp .doparm5 \n\t"
            ".dofloat4: \n\t"
                "movss  32(%%rsi),%%xmm4 \n\t"

            ".doparm5: \n\t"
                "cmpl  $4,20(%%rdi) \n\t"
                "jl  .floatdone \n\t"
                "je  .dofloat5 \n\t"
                "movsd  40(%%rsi),%%xmm5 \n\t"
                "jmp .doparm6 \n\t"
            ".dofloat5: \n\t"
                "movss  40(%%rsi),%%xmm5 \n\t"

            ".doparm6: \n\t"
                "cmpl  $4,24(%%rdi) \n\t"
                "jl  .floatdone \n\t"
                "je  .dofloat6 \n\t"
                "movsd  48(%%rsi),%%xmm6 \n\t"
                "jmp .doparm7 \n\t"
            ".dofloat6: \n\t"
                "movss  48(%%rsi),%%xmm6 \n\t"

            ".doparm7: \n\t"
                "cmpl  $4,28(%%rdi) \n\t"
                "jl  .floatdone \n\t"
                "je  .dofloat7 \n\t"
                "movsd  56(%%rsi),%%xmm7 \n\t"
                "jmp .floatdone \n\t"
            ".dofloat7: \n\t"
                "movss  56(%%rsi),%%xmm7 \n\t"

            ".floatdone: \n\t"
            : 
            : "S"(floatstack),"D"(floatSizes)
            : "cc","xmm0","xmm1","xmm2","xmm3","xmm4","xmm5","xmm6","xmm7"
            );
        }           
        __asm__ (
            "sub   %%rcx, %%rsp \n\t"
            "mov   %%rsp, %%rdi \n\t"
            "cld \n\t"
            "rep \n\t"
            "movsb \n\t"
            "pop %%rdi \n\t"
            "pop %%rsi \n\t"
            "pop %%rdx \n\t"
            "pop %%rcx \n\t"
            "pop %%r8 \n\t"
            "pop %%r9 \n\t"
            "call   *%%rax \n\t"
            "add    %%rbx, %%rsp \n\t" // Restore stack pointer (note have popped 6 registers)
            : "=a"(int64result),"=d"(dummy1),"=c"(dummy1),"=S"(dummy3),"=D"(dummy4)
            : "c"(len),"b"(len-REGPARAMS*REGSIZE),"S"(strbuf),"a"(fh)
            : "cc","r8","r9","xmm0"
            );
        
        // Get real (float/double) return values;
        if(isRealvalue)
        {
            if(resultsize <= 4)
            {
                __asm__  __volatile__(
                    "movss  %%xmm0,(%%rdi) \n\t"
                    :
                    : "D"(&(floatresult))
                );
            }
            else
            {
                __asm__  __volatile__(
                    "movsd  %%xmm0, (%%rdi) \n\t"
                    :
                    : "D"(&(doubleresult))
                );
            }
        }
        else {
            intresult = (int)int64result;
        }
 #elif defined(_ARCH_X86_)
        int dummy1, dummy2,dummy3;
        __asm__ __volatile__(
            "push   %%ebx \n\t"
            "movl   %%ecx, %%ebx \n\t"
            "subl   %%ecx, %%esp \n\t"
            "movl   %%esp, %%edi \n\t"
            "cld \n\t"
            "rep \n\t"
            "movsb \n\t"
            "call   *%%edx \n\t"
            "addl    %%ebx, %%esp \n\t"
            "pop    %%ebx \n\t"
            : "=a"(intresult),"=d"(intresulthigh),"=c"(dummy1),"=S"(dummy2),"=D"(dummy3)
            : "c"(len),"S"(strbuf),"d"(fh)
            : "cc"
            );

        // Get real (float/double) return values;
        if(isRealvalue)
        {
            if(resultsize <= 4)
            {
                __asm__  __volatile__(
                    "fstps (%%edi) \n\t"
                    :
                    : "D"(&(floatresult))
                );
            }
            else
            {
                __asm__  __volatile__(
                    "fstpl (%%edi) \n\t"
                    :
                    : "D"(&(doubleresult))
                );
            }
        }
 #elif defined(_ARCH_ARM32_)
        // http://infocenter.arm.com/help/topic/com.arm.doc.ihi0042e/IHI0042E_aapcs.pdf
  #ifdef MAXFPREGS
        void * floatstack = fstack.getFloatMem();
        if (floatstack) {
            unsigned * floatSizes = fstack.getFloatSizes();
           __asm__ __volatile__ (
           ".doparm0: \n\t"
               "ldr  r0,[%[sizes],#0] \n\t"
               "cmp  r0, #4 \n\t"
               "blt  .floatdone \n\t"
               "beq  .dofloat0 \n\t"
               "fldd  d0,[%[vals], #0] \n\t"
               "b .doparm1 \n\t"
           ".dofloat0: \n\t"
               "flds  s0,[%[vals], #0] \n\t"

           ".doparm1: \n\t"
               "ldr  r0,[%[sizes],#4] \n\t"
               "cmp  r0, #4 \n\t"
               "blt  .floatdone \n\t"
               "beq  .dofloat1 \n\t"
               "fldd  d1,[%[vals], #8] \n\t"
               "b .doparm2 \n\t"
           ".dofloat1: \n\t"
               "flds  s2,[%[vals], #8] \n\t"

           ".doparm2: \n\t"
               "ldr  r0,[%[sizes],#8] \n\t"
               "cmp  r0, #4 \n\t"
               "blt  .floatdone \n\t"
               "beq  .dofloat2 \n\t"
               "fldd  d2,[%[vals], #16] \n\t"
               "b .doparm3 \n\t"
           ".dofloat2: \n\t"
               "flds  s4,[%[vals], #16] \n\t"

           ".doparm3: \n\t"
               "ldr  r0,[%[sizes],#12] \n\t"
               "cmp  r0, #4 \n\t"
               "blt  .floatdone \n\t"
               "beq  .dofloat3 \n\t"
               "fldd  d3,[%[vals], #24] \n\t"
               "b .doparm4 \n\t"
           ".dofloat3: \n\t"
               "flds  s6,[%[vals], #24] \n\t"

           ".doparm4: \n\t"
               "ldr  r0,[%[sizes],#16] \n\t"
               "cmp  r0, #4 \n\t"
               "blt  .floatdone \n\t"
               "beq  .dofloat4 \n\t"
               "fldd  d4,[%[vals], #32] \n\t"
               "b .doparm5 \n\t"
           ".dofloat4: \n\t"
               "flds  s8,[%[vals], #32] \n\t"

           ".doparm5: \n\t"
               "ldr  r0,[%[sizes],#20] \n\t"
               "cmp  r0, #4 \n\t"
               "blt  .floatdone \n\t"
               "beq  .dofloat4 \n\t"
               "fldd  d5,[%[vals], #40] \n\t"
               "b .doparm6 \n\t"
           ".dofloat5: \n\t"
               "flds  s10,[%[vals], #40] \n\t"

           ".doparm6: \n\t"
               "ldr  r0,[%[sizes],#24] \n\t"
               "cmp  r0, #4 \n\t"
               "blt  .floatdone \n\t"
               "beq  .dofloat6 \n\t"
               "fldd  d6,[%[vals], #48] \n\t"
               "b .doparm7 \n\t"
           ".dofloat6: \n\t"
               "flds  s12,[%[vals], #48] \n\t"

           ".doparm7: \n\t"
               "ldr  r0,[%[sizes],#28] \n\t"
               "cmp  r0, #4 \n\t"
               "blt  .floatdone \n\t"
               "beq  .dofloat7 \n\t"
               "fldd  d7,[%[vals], #56] \n\t"
               "b .floatdone \n\t"
           ".dofloat7: \n\t"
               "flds  s14,[%[vals], #56] \n\t"

            ".floatdone: \n\t"
            :
            : [vals] "r"(floatstack), [sizes] "r"(floatSizes)
            : "r0"
           );
        }
  #endif
        assertex((len & 7) == 4);  // We need to make sure we add an ODD number of words to stack, so that it gets 8-byte aligned once pc is pushed by the call
        register unsigned _intresult asm("r0");                       // Specific register for result
        register unsigned _intresulthigh asm("r1");                   // Specific register for result
        register unsigned _poplen asm("r4") = len-REGPARAMS*REGSIZE;  // Needs to survive the call
        register void *_fh asm("r5") = fh;                             // Needs to survive until the call
        __asm__ __volatile__ (
            "subs sp, sp, %[len] \n\t"        // Make space on stack
            "mov r2, sp \n\t"                 // r2 = destination for loop
            ".repLoop: \n\t"
            "ldrb r3, [%[strbuf]], #1 \n\t"   // copy a byte from src array to r3
            "strb r3, [r2], #1 \n\t"          // and then from r3 onto stack
            "subs %[len], %[len], #1 \n\t"    // decrement and repeat
            "bne .repLoop \n\t"
            "pop {r0,r1,r2,r3} \n\t"          // first 4 parameters go in registers
            "blx %[fh] \n\t"                  // make the call
            "adds sp, sp, %[poplen] \n\t"     // Restore stack pointer (note have popped 4 registers, so poplen is len - 16)
            : "=r"(_intresult), "=r"(_intresulthigh)
            : [len] "r"(len), [poplen] "r"(_poplen), [strbuf] "r"(strbuf), [fh] "r"(_fh)
            : "r2","r3","lr"                  // function we call may corrupt lr
            );
        intresult = _intresult;
        intresulthigh = _intresulthigh;
        if (isRealvalue)
        {
  #ifdef MAXFPREGS
            if(resultsize <= 4)
            {
                __asm__  __volatile__(
                    "fsts  s0,[%[fresult]] \n\t"
                    :
                    : [fresult] "r"(&(floatresult))
                );
            }
            else
            {
                __asm__  __volatile__(
                    "fstd  d0,[%[fresult]] \n\t"
                    :
                    : [fresult] "r"(&(doubleresult))
                );
            }
  #else
            if(resultsize <= 4)
            {
                floatresult = *(float*)&intresult;
            }
            else
            {
                union
                {
                    struct { int lo, int hi } i;
                    double d;
                } u;
                u.lo = intresult;
                u.hi = intresulthigh;
                doubleresult = u.d;
            }
  #endif
        }
 #elif defined(_ARCH_ARM64_)
        // http://infocenter.arm.com/help/topic/com.arm.doc.ihi0055c/IHI0055C_beta_aapcs64.pdf
        UNIMPLEMENTED;
 #else
        // Unknown architecture
        UNIMPLEMENTED;
 #endif
#endif //win32
    }
    catch (...) {
        FreeSharedObject(hDLL);
        if(retCharStar || charStarInParam) { // Char* return type, need to free up tgt.
            free(tgt);
        }

        if (foldOptions & HFOthrowerror)
            throw MakeStringException(ERR_SVC_EXCPTIONEXEFUNC,"Exception thrown when try to execute function %s/%s, please check the function\n", library, entrypoint);
        return NULL;
    }

    // NOTE - we do not call FreeSharedObject(hDLL); here - the embedded language folding requires that the dll stay loaded, and it's also more efficient for other cases

    IValue* result = NULL;

    if(retCharStar || charStarInParam) { // Char* return type
        if(retCharStar) {
#ifdef __64BIT__
            tgt = (char *)int64result;
#else
            tgt = (char *)intresult;
#endif
            tlen = retUCharStar ? rtlUnicodeStrlen((UChar *)tgt) : strlen(tgt);
        }
        
        Linked<ITypeInfo> resultType = retType;
        if (resultType->getSize() == UNKNOWN_LENGTH)
            resultType.setown(getStretchedType(tlen, resultType));

        switch (typecode)
        {
        case type_varstring:
            result = createVarStringValue(tlen+1, tgt, resultType.getLink());
            break;
        case type_data:
            result = createDataValue(tgt, tlen);
            break;
        case type_qstring:
            result = createQStringValue(tlen, tgt, resultType.getLink());
            break;
        case type_unicode:
            result = createUnicodeValue(tlen, tgt, LINK(resultType));
            break;
        case type_varunicode:
            result = createVarUnicodeValue(tlen, tgt, LINK(resultType));
            break;
        case type_utf8:
            result = createUtf8Value(tlen, tgt, LINK(resultType));
            break;
        default:
            result = createStringValue(tgt, resultType.getLink());
            break;
        }
        rtlFree(tgt);
    }
    else if(isRealvalue) { // REAL return type
        if(resultsize == 4) {
            result = createRealValue(floatresult, resultsize);
        }
        else {
            result = createRealValue(doubleresult, resultsize);
        }
    }
    else if(typecode == type_boolean) { // BOOLEAN return type
        intresult = intresult & 0xff;
        result = createBoolValue(intresult != 0);
    }
    else if (typecode == type_void)
    {
        result = NULL;
    }
    else { // By default, we take the return type as INTEGER
        LINK(retType);

#ifndef __64BIT__
        __int64 int64result = (__int64)(((unsigned __int64) intresulthigh) << 32) + (unsigned)intresult;
#endif
        unsigned shift = (sizeof(int64result)-resultsize) * 8;
        if (retType->isSigned())
            int64result = (int64result << shift) >> shift;
        else
            int64result = (((__uint64)int64result) << shift) >> shift;

        result = createIntValue(int64result, retType);
    }

    return result;
}

IValue * foldExternalCall(IHqlExpression* expr, unsigned foldOptions, ITemplateContext *templateContext)
{
    StringBuffer library;
    StringBuffer entry;
    if (!checkExternFoldable(expr, foldOptions, library, entry))
        return NULL;
    return doFoldExternalCall(expr, foldOptions, templateContext, library.str(), entry.str());
}

//------------------------------------------------------------------------------------------

bool checkEmbeddedFoldable(IHqlExpression* expr, unsigned foldOptions)
{
    IHqlExpression * funcdef = expr->queryBody()->queryFunctionDefinition();
    IHqlExpression * outofline = funcdef->queryChild(0);
    assertex(outofline->getOperator() == no_outofline);
    IHqlExpression * body = outofline->queryChild(0);
    assertex(body->getOperator()==no_embedbody);
    IHqlExpression * formals = funcdef->queryChild(1);

    ITypeInfo * returnType = funcdef->queryType()->queryChildType();

    if (body->hasAttribute(_disallowed_Atom) || !body->hasAttribute(foldAtom))
        return false;  // Not allowed
    ForEachChild(idx, body)
    {
        IHqlExpression *child = body->queryChild(idx);
        if (child->isAttribute() && !isInternalEmbedAttr(child->queryName()) && !child->isConstant())
            // Note that the language attr is a function call and thus NOT considered constant, hence we can't just
            // test body->isConstant()
            return false;
    }
    IHqlExpression *languageAttr = body->queryAttribute(languageAtom);
    if (!languageAttr)
        return false;  // Can't fold embedded C++

    switch (returnType->getTypeCode())
    {
    case type_row:
    case type_table:
    case type_groupedtable:
        // Can't do as yet without the type info
        return false;
    }

    // check all the parameters passed in
    unsigned numParam = formals->numChildren();
    for(unsigned i = 0; i < numParam; i++)
    {
        IHqlExpression * curArg = formals->queryChild(i);
        assertex(curArg != NULL);
        ITypeInfo * paramType = curArg->queryType();
        switch (paramType->getTypeCode())
        {
        case type_row:
        case type_table:
        case type_groupedtable:
            // Can't do as yet without the type info
            return false;
        }
    }
    return true;
}

class DummyContext: implements ICodeContext
{
    // Perhaps this should go into eclrtl - address cleaner uses it too

    virtual const char *loadResource(unsigned id) { throwUnexpected(); }

    // Fetching interim results from workunit/query context

    virtual bool getResultBool(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence) { throwUnexpected(); }
    virtual void getResultDictionary(size32_t & tcount, byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) { throwUnexpected(); }
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual __int64 getResultInt(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual double getResultReal(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultRowset(size32_t & tcount, byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual char *getResultVarString(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual UChar *getResultVarUnicode(const char * name, unsigned sequence) { throwUnexpected(); }

    // Writing results to workunit/query context/output

    virtual void setResultBool(const char *name, unsigned sequence, bool value) { throwUnexpected(); }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) { throwUnexpected(); }
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) { throwUnexpected(); }
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) { throwUnexpected(); }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) { throwUnexpected(); }
    virtual void setResultReal(const char * stepname, unsigned sequence, double value) { throwUnexpected(); }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) { throwUnexpected(); }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) { throwUnexpected(); }
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) { throwUnexpected(); }
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) { throwUnexpected(); }
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value) { throwUnexpected(); }
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) { throwUnexpected(); }

    // Checking persists etc are up to date

    virtual unsigned getResultHash(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 crc) { throwUnexpected(); }

    // Fetching various environment information, typically accessed via std.system

    virtual char *getClusterName() { throwUnexpected(); } // caller frees return string.
    virtual char *getEnv(const char *name, const char *defaultValue) const { throwUnexpected(); }
    virtual char *getGroupName() { throwUnexpected(); } // caller frees return string.
    virtual char *getJobName() { throwUnexpected(); } // caller frees return string.
    virtual char *getJobOwner() { throwUnexpected(); } // caller frees return string.
    virtual unsigned getNodeNum() { throwUnexpected(); }
    virtual unsigned getNodes() { throwUnexpected(); }
    virtual char *getOS() { throwUnexpected(); } // caller frees return string
    virtual char *getPlatform() { throwUnexpected(); } // caller frees return string.
    virtual unsigned getPriority() const { throwUnexpected(); }
    virtual char *getWuid() { throwUnexpected(); } // caller frees return string.

    // Exception handling

    virtual void addWuException(const char*, unsigned int, unsigned int, const char*) { throwUnexpected(); } //n.b. this might be better named: it should only be used for adding user-generated exceptions (via the logging plug-in) --- there's a call in IAgentContext which takes a source argument too
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort) { throwUnexpected(); }

    // File resolution etc

    virtual char * getExpandLogicalName(const char * logicalName) { throwUnexpected(); }
    virtual unsigned __int64 getFileOffset(const char *logicalPart) { throwUnexpected(); }
    virtual char *getFilePart(const char *logicalPart, bool create=false) { throwUnexpected(); } // caller frees return string.
    virtual IDistributedFileTransaction *querySuperFileTransaction() { throwUnexpected(); }
    virtual IUserDescriptor *queryUserDescriptor() { throwUnexpected(); }

    // Graphs, child queries etc

    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract) { throwUnexpected(); }
    virtual unsigned getGraphLoopCounter() const { return 0; }
    virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal) { throwUnexpected(); }
    virtual IEclGraphResults * resolveLocalQuery(__int64 activityId) { return NULL; }

    // Logging etc

    virtual unsigned logString(const char *text) const { throwUnexpected(); }
    virtual IDebuggableContext *queryDebugContext() const { return NULL; }

    // Memory management

    virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const { throwUnexpected(); }
    virtual const char * cloneVString(const char *str) const { throwUnexpected(); }
    virtual const char * cloneVString(size32_t len, const char *str) const { throwUnexpected(); }

    // Called from generated code for FROMXML/TOXML

    virtual const void * fromXml(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) { throwUnexpected(); }
    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) { throwUnexpected(); }

    // Miscellaneous

    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }    // shouldn't really be here, but it broke thor.
    virtual char * queryIndexMetaData(char const * lfn, char const * xpath) { throwUnexpected(); }

    // Called from generated code for FROMJSON

    virtual const void * fromJson(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) { throwUnexpected(); }
    virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) { throwUnexpected(); }

    virtual const IContextLogger &queryContextLogger() const
    {
        return queryDummyContextLogger();
    }
    virtual IEngineContext *queryEngineContext() { return NULL; }
    virtual char *getDaliServers() { throwUnexpected(); }
    virtual IWorkUnit* updateWorkUnit() const { throwUnexpected(); }
    virtual ISectionTimer * registerTimer(unsigned activityId, const char * name) { throwUnexpected(); }
    virtual IEngineRowAllocator * getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned flags) const { throwUnexpected(); }

};

IHqlExpression *deserializeConstantSet(ITypeInfo *type, bool isAll, size32_t len, const void *vdata)
{
    ITypeInfo *childType = type->queryChildType();
    if (isAll)
        return createValue(no_all, LINK(childType));
    else if (!len)
        return createValue(no_null, LINK(childType));
    else
    {
        const char *data = (const char *) vdata;
        const char *end = data + len;
        HqlExprArray values;
        while (data < end)
        {
            size32_t size = childType->getSize();
            size32_t numChars = childType->getStringLen();
            switch (childType->getTypeCode())
            {
            case type_int:
            case type_real:
            case type_boolean:
                values.append(*createConstant(createValueFromMem(LINK(childType), data)));
                break;
            case type_varstring:
                values.append(*createConstant(data));
                if (size==UNKNOWN_LENGTH)
                    size = strlen(data)+1;
                break;
            case type_string:
                if (size==UNKNOWN_LENGTH)
                {
                    size = *(size32_t *) data;
                    data += sizeof(size32_t);
                }
                values.append(*createConstant(createStringValue(data, size)));
                break;
            case type_data:
                if (size==UNKNOWN_LENGTH)
                {
                    size = *(size32_t *) data;
                    data += sizeof(size32_t);
                }
                values.append(*createConstant(createDataValue(data, size)));
                break;
            case type_unicode:
                if (size==UNKNOWN_LENGTH)
                {
                    numChars = *(size32_t *) data;  // in characters
                    data += sizeof(size32_t);
                    values.append(*createConstant(createUnicodeValue((const UChar *) data, numChars, LINK(childType))));
                    size = numChars * sizeof(UChar);
                }
                else
                {
                    values.append(*createConstant(createUnicodeValue((const UChar *) data, numChars, LINK(childType))));
                }
                break;
            case type_utf8:
                // size is always UNKNOWN_LENGTH for uft8
                assertex(size==UNKNOWN_LENGTH);
                numChars = *(size32_t *) data;  // in characters
                data += sizeof(size32_t);
                values.append(*createConstant(createUtf8Value(numChars, data, LINK(childType))));
                size = rtlUtf8Size(numChars, data);
                break;
            default:
                return NULL;
            }
            if (size != UNKNOWN_LENGTH)
                data += size;
        }
        return createValue(no_list, LINK(type), values);
    }
}

IHqlExpression * foldEmbeddedCall(IHqlExpression* expr, unsigned foldOptions, ITemplateContext *templateContext)
{
    if (!checkEmbeddedFoldable(expr, foldOptions))
        return NULL;
    IHqlExpression * funcdef = expr->queryBody()->queryFunctionDefinition();
    IHqlExpression * outofline = funcdef->queryChild(0);
    assertex(outofline->getOperator() == no_outofline);
    IHqlExpression * body = outofline->queryChild(0);
    assertex(body->getOperator()==no_embedbody);
    ITypeInfo * returnType = funcdef->queryType()->queryChildType();

    bool isImport = body->hasAttribute(importAtom);
    IHqlExpression * formals = funcdef->queryChild(1);
    assertex(formals->numChildren() == expr->numChildren());  // MORE - do default params change this?
    unsigned flags = isImport ? EFimport : EFembed;
    if (formals->numChildren()==0)
        flags |= EFnoparams;
    if (returnType->getTypeCode()==type_void)
        flags |= EFnoreturn;

    StringBuffer optionsStr;
    ForEachChild(idx, body)
    {
        IHqlExpression *child = body->queryChild(idx);
        if (child->isAttribute() && !isInternalEmbedAttr(child->queryName()))
        {
            if (optionsStr.length())
                optionsStr.append(",");
            optionsStr.append(child->queryName());
            IHqlExpression * value = child->queryChild(0);
            if (value)
            {
                optionsStr.append("=");
                value->queryValue()->getUTF8Value(optionsStr);
            }
        }
    }
    IHqlExpression *languageAttr = body->queryAttribute(languageAtom);
    HqlExprArray noParams;
    OwnedHqlExpr langLoadCall = createTranslatedExternalCall(NULL, languageAttr->queryChild(0), noParams);
    Owned<IValue> plugin = foldExternalCall(langLoadCall, foldOptions, templateContext);
    if (plugin == nullptr)
        return NULL;

    Owned<IEmbedContext> __plugin = (IEmbedContext *) plugin->getIntValue();  // We declared as int since ecl has no pointer type - not sure what the clean fix is here...
    DummyContext dummyContext;
    Owned<IEmbedFunctionContext> __ctx = __plugin->createFunctionContextEx(&dummyContext,flags,optionsStr.str());

    IValue *query = body->queryChild(0)->queryValue();
    assertex(query);
    StringBuffer queryText;
    query->getUTF8Value(queryText);
    if (isImport)
        __ctx->importFunction(queryText.lengthUtf8(), queryText.str());
    else
        __ctx->compileEmbeddedScript(queryText.lengthUtf8(), queryText.str());

    // process all the parameters passed in
    unsigned numParam = expr->numChildren();
    for(unsigned i = 0; i < numParam; i++)
    {
        IHqlExpression * curParam = expr->queryChild(i);            //NB: Already folded...
        IHqlExpression * curArg = formals->queryChild(i);
        assertex(curArg != NULL);
        ITypeInfo * paramType = curArg->queryType();
        assertex(paramType != NULL);
        IValue * paramValue = curParam->queryValue();
        assertex(curParam->isConstant());
        unsigned paramSize = paramType->getSize();
        IIdAtom * paramId = curArg->queryId();
        const char * name = str(paramId);
        switch (paramType->getTypeCode())
        {
        case type_int:
        {
            __int64 value = paramValue->getIntValue();
            if (paramSize<8)
            {
                if (paramType->isSigned())
                    __ctx->bindSignedSizeParam(name, paramSize, value);
                else
                    __ctx->bindUnsignedSizeParam(name, paramSize, value);
            }
            else
            {
                if (paramType->isSigned())
                    __ctx->bindSignedParam(name, value);
                else
                    __ctx->bindUnsignedParam(name, value);
            }
            break;
        }
        case type_varstring:
        {
            StringBuffer value;
            paramValue->getStringValue(value);
            __ctx->bindVStringParam(name, value.str());
            break;
        }
        case type_string:
        {
            StringBuffer value;
            paramValue->getStringValue(value);
            __ctx->bindStringParam(name, value.length(), value.str());
            break;
        }
        case type_real:
        {
            double value = paramValue->getRealValue();
            if (paramType->getSize()==4)
                __ctx->bindFloatParam(name, value);
            else
                __ctx->bindRealParam(name, value);
            break;
        }
        case type_boolean:
            __ctx->bindBooleanParam(name, paramValue->getBoolValue());
            break;
        case type_utf8:
        {
            StringBuffer value;
            paramValue->getUTF8Value(value);
            __ctx->bindUTF8Param(name, value.lengthUtf8(), value.str());
            break;
        }
        case type_unicode:
        {
            unsigned len = paramValue->queryType()->getStringLen();
            UChar * value = (UChar *)malloc(len*2);
            paramValue->getUCharStringValue(len, value);
            __ctx->bindUnicodeParam(name, len, value);
            free(value);
            break;
        }
        case type_data:
        {
            __ctx->bindDataParam(name, paramValue->getSize(), paramValue->queryValue());
            break;
        }
        case type_row:
        case type_table:
        case type_groupedtable:
            // Can't do as yet without the type info
            return NULL;
        case type_set:
        {
            MemoryBuffer setValue;
            if (!createConstantField(setValue, curArg, curParam))
                return NULL;
            bool isAll;
            size32_t totalSize;
            setValue.read(isAll);
            setValue.read(totalSize);
            ITypeInfo *childType = paramType->queryChildType();
            type_t typeCode = childType->getTypeCode();
            if (childType->isInteger() && !childType->isSigned())
                typeCode = type_unsigned;
            __ctx->bindSetParam(name, (unsigned) typeCode, paramType->queryChildType()->getSize(), isAll, totalSize, setValue.readDirect(totalSize));
            break;
        }
        default:
            return NULL;
        }
    }
    __ctx->callFunction();

    switch (returnType->getTypeCode())
    {
    case type_int:
        return createConstant(returnType->isSigned() ? __ctx->getSignedResult() : __ctx->getUnsignedResult(), LINK(returnType));
    case type_varstring:
    case type_string:
    {
        size32_t lenResult;
        rtlDataAttr result;
        __ctx->getStringResult(lenResult, result.refstr());
        return createConstant(createStringValue(result.getstr(), lenResult));
    }
    case type_real:
        return createConstant(createRealValue(__ctx->getRealResult(), LINK(returnType)));
    case type_boolean:
        return createConstant(__ctx->getBooleanResult());
    case type_unicode:
    {
        size32_t lenResult;
        rtlDataAttr result;
        __ctx->getUnicodeResult(lenResult, result.refustr());
        return createConstant(createUnicodeValue(result.getustr(), lenResult, LINK(returnType)));
    }
    case type_utf8:
    {
        size32_t lenResult;
        rtlDataAttr result;
        __ctx->getUTF8Result(lenResult, result.refstr());
        return createConstant(createUtf8Value(lenResult, result.getstr(), LINK(returnType)));
    }
    case type_data:
    {
        size32_t lenResult;
        rtlDataAttr result;
        __ctx->getDataResult(lenResult, result.refdata());
        return createConstant(createDataValue(result.getstr(), lenResult));
    }
    case type_set:
    {
        ITypeInfo *childType = returnType->queryChildType();
        type_t typeCode = childType->getTypeCode();
        if (childType->isInteger() && !childType->isSigned())
            typeCode = type_unsigned;
        bool isAllResult;
        size32_t resultBytes;
        rtlDataAttr result;
        __ctx->getSetResult(isAllResult, resultBytes, result.refdata(), (unsigned) typeCode, returnType->queryChildType()->getSize());
        return deserializeConstantSet(returnType, isAllResult,resultBytes, result.getdata());
    }
    case type_row:
    case type_table:
    case type_transform:
    case type_void:
        // Can't do yet, maybe never - should probably check this earlier!'
        return NULL;
    }
    return NULL;
}

//------------------------------------------------------------------------------------------


// optimize ((a BAND b) <> 0) OR ((a BAND c) <> 0) to ((a BAND (b BOR c)) <> 0)
bool isFieldMask(IHqlExpression * expr)
{
    if (expr->getOperator() != no_ne)
        return false;
    IHqlExpression * left = expr->queryChild(0);
    if (left->getOperator() != no_band)
        return false;
    IValue * rightValue = expr->queryChild(1)->queryValue();
    if (!rightValue || rightValue->getIntValue() != 0)
        return false;
    return true;
}


bool isFieldMask(IHqlExpression * field, IHqlExpression * expr)
{
    return isFieldMask(expr) && (expr->queryChild(0)->queryChild(0) == field);
}

IHqlExpression * combineMask(IHqlExpression * left, IHqlExpression * right)
{
    IHqlExpression * zero = left->queryChild(1);
    IHqlExpression * field = left->queryChild(0)->queryChild(0);
    IHqlExpression * mask1 = left->queryChild(0)->queryChild(1);
    IHqlExpression * mask2 = right->queryChild(0)->queryChild(1);
    ITypeInfo * borType = getBorType(mask1->queryType(), mask2->queryType());
    ITypeInfo * bandType = getBandType(field->queryType(), borType);

    OwnedHqlExpr newMask = createValue(no_bor, borType, ensureExprType(mask1, borType), ensureExprType(mask2, borType));
    IHqlExpression * newTest = createValue(no_band, bandType, ensureExprType(field, bandType), ensureExprType(newMask, bandType));
    return createBoolExpr(no_ne, newTest, ensureExprType(zero, bandType));
}

bool constantComparison(IHqlExpression * field, IHqlExpression * expr, HqlExprArray & values)
{
    IHqlExpression * left = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    switch (expr->getOperator())
    {
    case no_eq:
        if (field && field!= left)
            return false;
        if (!right->queryValue())
            return false;
        if (values.find(*right) == NotFound)
            values.append(*LINK(right));
        return true;
    case no_in:
        {
            if (field && field != left)
                return false;
            if (right->getOperator() != no_list)
                return false;
            ForEachChild(i, right)
            {
                IHqlExpression * cur = right->queryChild(i);
                if (values.find(*cur) == NotFound)
                    values.append(*LINK(cur));
            }
            return true;
        }
    }
    return false;
}

bool isFilteredWithin(IHqlExpression * expr, IHqlExpression * dataset, HqlExprArray & filters)
{
    bool invert = false;
    if (expr->getOperator() == no_not)
    {
        invert = true;
        expr = expr->queryChild(0);
    }
    if (expr->getOperator() != no_within)
        return false;
    IHqlExpression * child0 = expr->queryChild(0);
    if (child0->getOperator() != no_filter)
        return false;
    if (dataset && dataset != child0->queryChild(0))
        return false;
    unsigned max = child0->numChildren();
    for (unsigned idx=1; idx < max; idx++)
    {
        IHqlExpression * cur = LINK(child0->queryChild(idx));
        if (invert)
            cur = createValue(no_not, makeBoolType(), cur);
        filters.append(*cur);
    }
    return true;
}


void mergeWithins(node_operator op, HqlExprArray & transformedArgs)
{
    for (unsigned idxWithin = 0; idxWithin < transformedArgs.ordinality(); idxWithin++)
    {
        IHqlExpression & cur = transformedArgs.item(idxWithin);
        HqlExprArray filters;
        if (isFilteredWithin(&cur, NULL, filters))
        {
            IHqlExpression * dataset = cur.queryChild(0)->queryChild(0);
            bool changed = false;

            for (unsigned idxMatch = idxWithin+1; idxMatch < transformedArgs.ordinality();)
            {
                IHqlExpression & match = transformedArgs.item(idxMatch);
                if (isFilteredWithin(&match, dataset, filters))
                {
                    changed = true;
                    transformedArgs.remove(idxMatch);
                }
                else
                    idxMatch++;
            }

            if (changed)
            {
                HqlExprArray filterArgs;
                filterArgs.append(*LINK(dataset));
                filterArgs.append(*createBinaryList(op, filters));
                IHqlExpression * filteredDataset = createDataset(no_filter, filterArgs);
                IHqlExpression * within = createValue(no_within, makeBoolType(), filteredDataset);
                transformedArgs.replace(*within, idxWithin);
            }
        }
    }
}


IHqlExpression * foldOrExpr(IHqlExpression * expr, bool fold_x_op_not_x)
{
    HqlExprArray args, transformedArgs;
    expr->unwindList(args, expr->getOperator());

    //First transform all the arguments, removing and always false, and short circuit if always true
    //also remove duplicates a || a == a
    ForEachItemIn(idx, args)
    {
        IHqlExpression * transformed = &args.item(idx);
        IValue * value = transformed->queryValue();

        if (value)
        {
            if (value->getBoolValue())
                return LINK(transformed);
        }
        else
        {
            if (transformedArgs.find(*transformed) == NotFound)
            {
                if (fold_x_op_not_x)
                {
                    //Check for x OR NOT x  => always true...  
                    //Needs to be done this way because the no_not often gets folded...
                    OwnedHqlExpr inverse = createValue(no_not, makeBoolType(), LINK(transformed));
                    if (transformedArgs.contains(*inverse))
                        return createConstant(true);
                }
                transformedArgs.append(*LINK(transformed));
            }
        }
    }

    if (transformedArgs.ordinality() == 0)
        return createConstant(false);

    // optimize ((a BAND b) <> 0) OR ((a BAND c) <> 0) to ((a BAND (b BOR c)) <> 0)
    for (unsigned idx2 = 0; idx2 < transformedArgs.ordinality()-1; idx2++)
    {
        IHqlExpression & cur = transformedArgs.item(idx2);
        if (isFieldMask(&cur))
        {
            IHqlExpression * masked = cur.queryChild(0)->queryChild(0);
            LinkedHqlExpr combined = &cur;
            for (unsigned idx3 = transformedArgs.ordinality()-1; idx3 != idx2; idx3--)
            {
                IHqlExpression & cur2 = transformedArgs.item(idx3);
                if (isFieldMask(masked, &cur2))
                {
                    combined.setown(combineMask(combined, &cur2));
                    transformedArgs.remove(idx3);
                }
            }
            if (combined != &cur)
                transformedArgs.replace(*combined.getClear(), idx2);
        }
    }

    //optimize x=a|x=b|x=c to x in (a,b,c)
    HqlExprArray constantValues;
    for (unsigned idx4 = 0; idx4 < transformedArgs.ordinality()-1; idx4++)
    {
        IHqlExpression & cur = transformedArgs.item(idx4);
        constantValues.kill();

        if (constantComparison(NULL, &cur, constantValues))
        {
            bool merged = false;
            IHqlExpression * compare = cur.queryChild(0);
            for (unsigned idx5 = idx4+1; idx5 < transformedArgs.ordinality(); )
            {
                IHqlExpression & cur2 = transformedArgs.item(idx5);
                if (constantComparison(compare, &cur2, constantValues))
                {
                    merged = true;
                    transformedArgs.remove(idx5);
                }
                else
                    idx5++;
            }

            if (merged)
            {
                //MORE: Should promote all items in the list to the same type.
                IHqlExpression & first = constantValues.item(0);
                IHqlExpression * list = createValue(no_list, makeSetType(first.getType()), constantValues);
                OwnedHqlExpr combined = createBoolExpr(no_in, LINK(compare), list);
                transformedArgs.replace(*combined.getClear(), idx4);
            }
        }
    }

#if 0
    else 
    {
        // optimize (( BOOL)(a BAND b) ) OR ((bool)(a BAND c) ) to ((bool)(a BAND (b BOR c)) )
        // Lots of work for a very particular special case that happens a lot in DMS
        assertex (leftChild->queryType() == rightChild->queryType());
        assertex (leftChild->queryType()->getTypeCode() == type_boolean);
#if 0
        dbglogExpr(leftChild->queryBody());
        dbglogExpr(rightChild->queryBody());
#endif
        IHqlExpression *select = NULL;
        if (leftChild->getOperator()==no_select && rightChild->getOperator()==no_select && leftChild->queryChild(0)==rightChild->queryChild(0))
        {
            select = leftChild->queryChild(0);
            leftChild.set(leftChild->queryChild(1));
            rightChild.set(rightChild->queryChild(1));
        }
        while (leftChild && leftChild->getOperator()==no_field)
            leftChild.set(leftChild->queryChild(0));
        while (rightChild && rightChild->getOperator()==no_field)
            rightChild.set(rightChild->queryChild(0));
        if (leftChild && rightChild)
        {
#if 0
        dbglogExpr(leftChild->queryBody());
        dbglogExpr(rightChild->queryBody());
#endif
            if (isCast(leftChild) && isCast(rightChild))
            {
                IHqlExpression * lBand = leftChild->queryChild(0);
                IHqlExpression * rBand = rightChild->queryChild(0);
                if (lBand->getOperator() == no_band && rBand->getOperator() == no_band)
                {
                    IHqlExpression * aLeft = lBand->queryChild(0);
                    IHqlExpression * aRight = rBand->queryChild(0);
                    if (aLeft == aRight)
                    {
                        IHqlExpression * bLeft = lBand->queryChild(1);
                        IHqlExpression * cRight = rBand->queryChild(1);
                        IHqlExpression * newBor = createValue(no_bor, getPromotedType(bLeft->queryType(), cRight->queryType()), LINK(bLeft), LINK(cRight));
                        IHqlExpression * newBand = createValue(no_band, getPromotedType(aLeft->queryType(), newBor->queryType()), LINK(aLeft), newBor);
                        OwnedHqlExpr newNode = createBoolExpr(no_cast, newBand);
                        if (select)
                            newNode.setown(createBoolExpr(no_select, LINK(select), newNode.getClear());
                        return transform(newNode);
                    }
                }
            }
        }
    }
#endif

    //mergeWithins(no_or, transformedArgs);

    if (arraysSame(args, transformedArgs))
        return LINK(expr);

    return createBinaryList(no_or, transformedArgs);
}



IHqlExpression * foldAndExpr(IHqlExpression * expr, bool fold_x_op_not_x)
{
    HqlExprArray args, transformedArgs;
    expr->unwindList(args, expr->getOperator());

    //First transform all the arguments, removing if always true, and short circuit if always false
    //also remove duplicates a && a == a
    ForEachItemIn(idx, args)
    {
        IHqlExpression * cur = &args.item(idx);
        IValue * value = cur->queryValue();
        if (value)
        {
            if (!value->getBoolValue())
                return LINK(cur);
        }
        else
        {
            if (transformedArgs.find(*cur) == NotFound)
            {
                if (fold_x_op_not_x)
                {
                    //Check for x AND NOT x. => false
                    OwnedHqlExpr inverse = getInverse(cur);
                    if (transformedArgs.find(*inverse) != NotFound)
                        return createConstant(false);
                }
                transformedArgs.append(*LINK(cur));
            }
        }
    }

    //mergeWithins(no_and, transformedArgs);

    if (transformedArgs.ordinality() == 0)
        return createConstant(true);

    if (arraysSame(args, transformedArgs))
        return LINK(expr);

    return createBinaryList(no_and, transformedArgs);
}


IHqlExpression * applyBinaryFold(IHqlExpression * expr, binaryFoldFunc folder)
{
    IHqlExpression * leftChild = expr->queryChild(0);
    IHqlExpression * rightChild = expr->queryChild(1);

    IValue * leftValue = leftChild->queryValue(); 
    IValue * rightValue = rightChild->queryValue();
    if (leftValue && rightValue) 
    {
        IValue * res = folder(leftValue, rightValue);
        assertex(res);
        return createConstant(res);
    }

    return LINK(expr);
}


static bool isStringOrUnicode(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_data:
    case type_string:
    case type_varstring:
    case type_qstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        return true;
    }
    return false;
}

static bool isNonAscii(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_string:
    case type_varstring:
        return type->queryCharset()->queryName() != asciiAtom;
    }
    return false;
}

static bool castHidesConversion(ITypeInfo * t1, ITypeInfo * t2, ITypeInfo * t3)
{
    return (t1->getTypeCode() == type_data) && (isNonAscii(t2) || isNonAscii(t3));
}


static IHqlExpression * optimizeConstInList(IValue * search, IHqlExpression * list)
{
    if (!list) return NULL;
    switch (list->getOperator())
    {
    case no_null:
    case no_all:
        return LINK(list);
    case no_list:
        {
            HqlExprArray values;
            bool same = true;
            unsigned num = list->numChildren();
            for (unsigned idx = 0; idx < num; idx++)
            {
                IHqlExpression * elem = list->queryChild(idx);
                IValue * constValue = elem->queryValue();
                if (constValue)
                {
                    if (orderValues(search, constValue) == 0)
                        return createValue(no_all, list->getType());
                    same = false;
                }
                else
                    values.append(*LINK(elem));
            }
            if (same)
                return LINK(list);
            return list->clone(values);
        }
    case no_addsets:
        {
            IHqlExpression * lhs = list->queryChild(0);
            IHqlExpression * rhs = list->queryChild(1);
            OwnedHqlExpr newLhs = optimizeConstInList(search, lhs);
            OwnedHqlExpr newRhs = optimizeConstInList(search, rhs);
            if ((newLhs->getOperator() == no_all) || isNullList(newRhs))
                return LINK(newLhs);
            if ((newRhs->getOperator() == no_all) || isNullList(newLhs))
                return LINK(newRhs);
            if ((lhs == newLhs) && (rhs == newRhs))
                return LINK(list);
            if ((newLhs->getOperator() != no_list) || (newRhs->getOperator() != no_list))
                return createValue(no_addsets, list->getType(), newLhs.getClear(), newRhs.getClear());
            HqlExprArray args;
            unwindChildren(args, newLhs);
            unwindChildren(args, newRhs);
            return createValue(no_list, list->getType(), args);
        }
    case no_if:
        {
            IHqlExpression * lhs = list->queryChild(1);
            IHqlExpression * rhs = list->queryChild(2);
            OwnedHqlExpr newLhs = optimizeConstInList(search, lhs);
            OwnedHqlExpr newRhs = optimizeConstInList(search, rhs);
            //might both turn out to be all/empty
            if (newLhs->queryBody() == newRhs->queryBody())
                return LINK(newLhs);
            if ((lhs == newLhs) && (rhs == newRhs))
                return LINK(list);
            HqlExprArray args;
            unwindChildren(args, list);
            args.replace(*newLhs.getClear(), 1);
            args.replace(*newRhs.getClear(), 2);
            return list->clone(args);
        }
    }
    return LINK(list);
}

static bool hashElement(node_operator op, IHqlExpression * expr, unsigned __int64 & hashCode)
{
    IValue * value = expr->queryValue();
    if (!value)
        return false;

    ITypeInfo * type = value->queryType();
    switch (type->getTypeCode())
    {
        case type_qstring:
        case type_string:
            {
                Owned<ITypeInfo> unknownLengthString = makeStringType(UNKNOWN_LENGTH);
                Owned<IValue> castValue = value->castTo(unknownLengthString);
                const char * cdata = static_cast<const char *>(castValue->queryValue());
                size32_t len = rtlTrimStrLen(castValue->queryType()->getStringLen(), cdata);
                hashCode = (op == no_hash32) ? rtlHash32Data(len, cdata, (unsigned)hashCode) : rtlHash64Data(len, cdata, hashCode);
                return true;
            }
        case type_data:
            {
                size32_t len = type->getSize();
                const char * cdata = static_cast<const char *>(value->queryValue());
                hashCode = (op == no_hash32) ? rtlHash32Data(len, cdata, (unsigned)hashCode) : rtlHash64Data(len, cdata, hashCode);
                return true;
            }
        case type_varstring:
            {
                const char * cdata = static_cast<const char *>(value->queryValue());
                hashCode = (op == no_hash32) ? rtlHash32VStr(cdata, (unsigned)hashCode) : rtlHash64VStr(cdata, hashCode);
                return true;
            }
        case type_unicode:
            {
                const UChar * udata = static_cast<const UChar *>(value->queryValue());
                size32_t len = type->getStringLen();
                hashCode = (op == no_hash32) ? rtlHash32Unicode(len, udata, (unsigned)hashCode) : rtlHash64Unicode(len, udata, hashCode);
                return true;
            }
        case type_varunicode:
            {
                const UChar * udata = static_cast<const UChar *>(value->queryValue());
                hashCode = (op == no_hash32) ? rtlHash32VUnicode(udata, (unsigned)hashCode) : rtlHash64VUnicode(udata, hashCode);
                return true;
            }
        case type_utf8:
            {
                const char * udata = static_cast<const char *>(value->queryValue());
                size32_t len = rtlTrimUtf8StrLen(type->getStringLen(), udata);
                hashCode = (op == no_hash32) ? rtlHash32Utf8(len, udata, (unsigned)hashCode) : rtlHash64Utf8(len, udata, hashCode);
                return true;
            }
        case type_int:
        case type_swapint:
            {
                unsigned __int64 intValue = value->getIntValue();
                hashCode = (op == no_hash32) ? rtlHash32Data(sizeof(intValue), &intValue, (unsigned)hashCode) : rtlHash64Data(sizeof(intValue), &intValue, hashCode);
                return true;
            }
            break;
    }

    return false;
}

static IHqlExpression * foldHashXX(IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);
    node_operator op = expr->getOperator();

    unsigned __int64 hashCode = 0;
    switch (op)
    {
    case no_hash32:
        hashCode = HASH32_INIT;
        break;
    case no_hash64:
        hashCode = HASH64_INIT;
        break;
    }

    if (child->getOperator() == no_sortlist)
    {
        ForEachChild(i, child)
        {
            if (!hashElement(op, child->queryChild(i), hashCode))
                return NULL;
        }
    }
    else
    {
        if (!hashElement(op, child, hashCode))
            return NULL;
    }

    return createConstant(expr->queryType()->castFrom(true, (__int64)hashCode));
}


//---------------------------------------------------------------------------

IHqlExpression * foldConstantOperator(IHqlExpression * expr, unsigned foldOptions, ITemplateContext * templateContext)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_assertkeyed:
        {
            assertex(expr->hasAttribute(_selectors_Atom));
            IHqlExpression * child = expr->queryChild(0);
            IValue * value = child->queryValue();
            if (value)
            {
                if (!value->getBoolValue())
                    return LINK(child);
                IHqlExpression * opt = expr->queryAttribute(extendAtom);
                IHqlExpression * selectors = expr->queryAttribute(_selectors_Atom);
                return createValue(no_assertwild, makeBoolType(), createValue(no_all), LINK(selectors), LINK(opt));
            }
            break;
        }
    case no_or:
    case no_and:
        {
            IHqlExpression * left = expr->queryChild(0);
            IHqlExpression * right = expr->queryChild(1);
            IValue * leftValue = left->queryValue();
            if (leftValue)
            {
                bool leftBool = leftValue->getBoolValue();
                if ((op == no_and) ? leftBool : !leftBool)
                    return LINK(right);
                return LINK(left);
            }
            IValue * rightValue = right->queryValue();
            if (rightValue)
            {
                bool rightBool = rightValue->getBoolValue();
                if ((op == no_and) ? rightBool : !rightBool)
                    return LINK(left);
                return LINK(right);
            }
            break;
        }
    case no_assertconstant:
    case no_globalscope:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (child->queryValue())
                return LINK(child);
            break;
        }
    case no_not:
        {
            node_operator inverseOp = no_none;
            IHqlExpression * child = expr->queryChild(0);
            switch (child->getOperator())
            {
            case no_not:
                return ensureExprType(child->queryChild(0), expr->queryType());
            case no_constant:
                return createConstant(!child->queryValue()->getBoolValue());
            case no_notnot:
                inverseOp = no_not;
                break;
            case no_eq:
                inverseOp = no_ne;
                break;
            case no_ne:
                inverseOp = no_eq;
                break;
            case no_lt:
                inverseOp = no_ge;
                break;
            case no_le:
                inverseOp = no_gt;
                break;
            case no_gt:
                inverseOp = no_le;
                break;
            case no_ge:
                inverseOp = no_lt;
                break;
            case no_in:
                inverseOp = no_notin;
                break;
            case no_notin:
                inverseOp = no_in;
                break;
            case no_between:
                inverseOp = no_notbetween;
                break;
            case no_notbetween:
                inverseOp = no_between;
                break;
            case no_if:
                if (child->queryChild(1)->isConstant() || child->queryChild(2)->isConstant())
                    return getInverse(child);
                break;
            }
            if (inverseOp)
            {
                HqlExprArray children;
                unwindChildren(children, child);
                return createValue(inverseOp, child->getType(), children);
            }
            break;
        }
    case no_add:
        {
            IHqlExpression * left = expr->queryChild(0);
            IHqlExpression * right = expr->queryChild(1);
            if (isZero(left))
                return ensureExprType(right, expr->queryType());
            if (isZero(right))
                return ensureExprType(left, expr->queryType());
            return applyBinaryFold(expr, addValues);
        }
    case no_sub:
    {
        IHqlExpression * left = expr->queryChild(0);
        IHqlExpression * right = expr->queryChild(1);
        if (isZero(right))
            return ensureExprType(left, expr->queryType());
        return applyBinaryFold(expr, subtractValues);
    }
    case no_hash32:
    case no_hash64:
        {
            IHqlExpression * folded = foldHashXX(expr);
            if (folded)
                return folded;
            break;
        }
    case no_mul:
        {
            IHqlExpression * left = expr->queryChild(0);
            IHqlExpression * right = expr->queryChild(1);
            //Multiply by zero (from constant folding count(ds)) can reduce a non-constant dataset to constant
            if (isZero(left) || isZero(right))
            {
                OwnedHqlExpr zero = getSizetConstant(0);
                return ensureExprType(zero, expr->queryType());
            }
            if (matchesConstantValue(left, 1))
                return ensureExprType(right, expr->queryType());
            if (matchesConstantValue(right, 1))
                return ensureExprType(left, expr->queryType());
            return applyBinaryFold(expr, multiplyValues);
        }
    case no_div:
        if (matchesConstantValue(expr->queryChild(1), 1))
        {
            IHqlExpression * left = expr->queryChild(0);
            if (left->queryType()->isInteger())
                return ensureExprType(left, expr->queryType());
        }
        //fall through
    case no_modulus:
        {
            IValue * leftValue = expr->queryChild(0)->queryValue();
            IValue * rightValue = expr->queryChild(1)->queryValue();
            if (leftValue && rightValue)
            {
                DBZaction onZero = (foldOptions & HFOforcefold) ? DBZfail : DBZnone;
                IValue * res;
                if (op == no_div)
                    res = divideValues(leftValue, rightValue, onZero);
                else
                    res = modulusValues(leftValue, rightValue, onZero);
                if (res)
                    return createConstant(res);
            }
            return LINK(expr);
        }
    case no_concat:
        return applyBinaryFold(expr, concatValues);
    case no_band:
        {
            if (isZero(expr->queryChild(0)) || isZero(expr->queryChild(1)))
                return createConstant(expr->queryType()->castFrom(true, I64C(0)));

            OwnedHqlExpr ret = applyBinaryFold(expr, binaryAndValues);
            if (ret->getOperator() == no_band)
            {
                // ((x BAND y) BAND z) == (x BAND (y BAND z))   - especially if y + z are constants.
                IHqlExpression * leftChild = ret->queryChild(0);
                if (leftChild->getOperator()==no_band)
                {
                    IValue * rightValue = ret->queryChild(1)->queryValue();
                    if (rightValue)
                    {
                        IValue * grandValue = leftChild->queryChild(1)->queryValue();
                        if (grandValue)
                        {
                            IHqlExpression * mask = createConstant(binaryAndValues(grandValue, rightValue));
                            IHqlExpression * newBand = createValue(no_band, expr->getType(), LINK(leftChild->queryChild(0)), mask);
                            return newBand;
                        }
                    }
                }
            }
            return ret.getClear();
        }
    case no_bor:
        {
            IHqlExpression * lhs = expr->queryChild(0);
            IHqlExpression * rhs = expr->queryChild(1);
            if (isZero(lhs))
                return ensureExprType(rhs, expr->queryType());
            if (isZero(rhs))
                return ensureExprType(lhs, expr->queryType());
            return applyBinaryFold(expr, binaryOrValues);
        }
    case no_bxor:
        return applyBinaryFold(expr, binaryXorValues);
    case no_power:
        return applyBinaryFold(expr, powerValues);
    case no_atan2:
        return applyBinaryFold(expr, atan2Value);
    case no_lshift:
        return applyBinaryFold(expr, shiftLeftValues);
    case no_rshift:
        return applyBinaryFold(expr, shiftRightValues);
    case no_regex_find:
        {
            IValue * t0 = expr->queryChild(0)->queryValue();
            IValue * t1 = expr->queryChild(1)->queryValue();
            IHqlExpression * c2 = queryRealChild(expr, 2);
            IValue * t2 = c2 ? c2->queryValue() : NULL;
            if (t0 && t1 && (!c2 || t2))
            {
                IValue * result;
                if(isUnicodeType(t0->queryType()))
                {
                    unsigned plen = t0->queryType()->getStringLen();
                    unsigned slen = t1->queryType()->getStringLen();
                    UChar * pattern = (UChar *)malloc((plen+1)*2);
                    UChar * search = (UChar *)malloc((slen)*2);
                    t0->getUCharStringValue(plen+1, pattern); //plen+1 so get null-terminated
                    t1->getUCharStringValue(slen, search);
                    ICompiledUStrRegExpr * compiled = rtlCreateCompiledUStrRegExpr(pattern, !expr->hasAttribute(noCaseAtom));
                    IUStrRegExprFindInstance * match = compiled->find(search, 0, slen);
                    ITypeInfo * type = expr->queryType();
                    if(type->getTypeCode() == type_boolean)
                        result = createBoolValue(match->found());
                    else
                    {
                        size32_t len;
                        UChar * data;
                        match->getMatchX(len, data, (unsigned)t2->getIntValue());
                        result = createUnicodeValue(len, data, LINK(type));
                        rtlFree(data);
                    }
                    rtlDestroyUStrRegExprFindInstance(match);
                    rtlDestroyCompiledUStrRegExpr(compiled);
                    free(pattern);
                    free(search);
                }
                else
                {
                    StringBuffer pattern, search;
                    t0->getStringValue(pattern);
                    t1->getStringValue(search);
                    rtlCompiledStrRegex compiled;
                    compiled.setPattern(pattern.str(), !expr->hasAttribute(noCaseAtom));
                    IStrRegExprFindInstance * match = compiled->find(search.str(), 0, search.length(), false);
                    ITypeInfo * type = expr->queryType();
                    if(type->getTypeCode() == type_boolean)
                        result = createBoolValue(match->found());
                    else
                    {
                        size32_t len;
                        char * data;
                        match->getMatchX(len, data, (unsigned)t2->getIntValue());
                        result = type->castFrom(len, data);
                        rtlFree(data);
                    }
                    rtlDestroyStrRegExprFindInstance(match);
                }
                return createConstant(result);
            }
            break;
        }
    case no_regex_findset:
        {
            IValue * v0 = expr->queryChild(0)->queryValue();
            IValue * v1 = expr->queryChild(1)->queryValue();
            if (v0 && v1)
            {
                bool isAllResult;
                size32_t resultBytes;
                rtlDataAttr matchResults;

                if(isUnicodeType(v0->queryType()))
                {
                    size32_t plen = v0->queryType()->getStringLen();
                    OwnedMalloc<UChar> pattern (plen+1);
                    v0->getUCharStringValue(plen+1, pattern.get()); //plen+1 so get null-terminated
                    size32_t slen = v1->queryType()->getStringLen();
                    OwnedMalloc<UChar> search (slen);
                    v1->getUCharStringValue(slen, search);
                    ICompiledUStrRegExpr * compiled = rtlCreateCompiledUStrRegExpr(pattern, !expr->hasAttribute(noCaseAtom));
                    compiled->getMatchSet(isAllResult, resultBytes, matchResults.refdata(), slen, search.get());
                    rtlDestroyCompiledUStrRegExpr(compiled);
                }
                else
                {
                    StringBuffer pattern, search;
                    v0->getStringValue(pattern);
                    v1->getStringValue(search);
                    rtlCompiledStrRegex compiled;
                    compiled.setPattern(pattern.str(), !expr->hasAttribute(noCaseAtom));
                    compiled->getMatchSet(isAllResult, resultBytes, matchResults.refdata(), search.length(), search.str());
                }
                return convertSetToExpression(isAllResult, resultBytes, matchResults.getdata(), expr->queryType());
            }
            break;
        }
    case no_regex_replace:
        {
            IValue * t0 = expr->queryChild(0)->queryValue();
            IValue * t1 = expr->queryChild(1)->queryValue();
            IValue * t2 = expr->queryChild(2)->queryValue();
            if (t0 && t1 && t2)
            {
                IValue * result;
                if(isUnicodeType(t0->queryType()))
                {
                    unsigned plen = t0->queryType()->getStringLen();
                    unsigned slen = t1->queryType()->getStringLen();
                    unsigned rlen = t2->queryType()->getStringLen();
                    UChar * pattern = (UChar *)malloc((plen+1)*2);
                    UChar * search = (UChar *)malloc(slen*2);
                    UChar * replace = (UChar *)malloc(rlen*2);
                    t0->getUCharStringValue(plen+1, pattern); //plen+1 so null-terminated
                    t1->getUCharStringValue(slen, search);
                    t2->getUCharStringValue(rlen, replace);
                    size32_t outlen;
                    UChar * out;
                    ICompiledUStrRegExpr * compiled = rtlCreateCompiledUStrRegExpr(pattern, !expr->hasAttribute(noCaseAtom));
                    compiled->replace(outlen, out, slen, search, rlen, replace);
                    result = createUnicodeValue(outlen, out, expr->getType());
                    rtlFree(out);
                    rtlDestroyCompiledUStrRegExpr(compiled);
                    free(pattern);
                    free(search);
                    free(replace);
                }
                else
                {
                    StringBuffer pattern, search, replace;
                    t0->getStringValue(pattern);
                    t1->getStringValue(search);
                    t2->getStringValue(replace);
                    size32_t outlen;
                    char * out;
                    rtlCompiledStrRegex compiled;
                    compiled.setPattern(pattern.str(), !expr->hasAttribute(noCaseAtom));
                    compiled->replace(outlen, out, search.length(), search.str(), replace.length(), replace.str());
                    result = createStringValue(out, outlen);
                    rtlFree(out);
                }
                return createConstant(result);
            }
            break;
        }
    case no_intformat:
        {
            IValue * c0 = expr->queryChild(0)->queryValue();
            IValue * c1 = expr->queryChild(1)->queryValue();
            IValue * c2 = expr->queryChild(2)->queryValue();
            if (c0 && c1 && c2)
            {
                __int64 value = c0->getIntValue();
                unsigned width = (unsigned)c1->getIntValue();
                unsigned flags = (unsigned)c2->getIntValue();

                if ((int) width < 0)
                    width = 0;
                
                MemoryAttr tempBuffer(width);
                holeIntFormat(width, (char *)tempBuffer.bufferBase(), value, width, flags);
                return createConstant(createStringValue((char *)tempBuffer.bufferBase(), width));
            }
            break;
        }
    case no_realformat:
        {
            IValue * c0 = expr->queryChild(0)->queryValue();
            IValue * c1 = expr->queryChild(1)->queryValue();
            IValue * c2 = expr->queryChild(2)->queryValue();
            if (c0 && c1 && c2)
            {
                double value = c0->getRealValue();
                unsigned width = (unsigned)c1->getIntValue();
                unsigned places = (unsigned)c2->getIntValue();

                unsigned len;
                char * ptr;
                rtlRealFormat(len, ptr, value, width, places);
                IHqlExpression * ret = createConstant(createStringValue(ptr, len));
                rtlFree(ptr);
                return ret;
            }
            break;
        }
    case no_indict:
        if (isNull(expr->queryChild(1)))
            return createConstant(false);
        break;
    case no_in:
    case no_notin:
        {
            IHqlExpression * child = expr->queryChild(0);
            IHqlExpression * originalList = expr->queryChild(1);
            IValue * constValue = child->queryValue();

            OwnedHqlExpr inList = normalizeListCasts(originalList);
            if (constValue)
                inList.setown(optimizeConstInList(constValue, inList));

            switch (inList->getOperator())
            {
            case no_all:
                return createConstant((op == no_in));
            case no_null:
                return createConstant((op != no_in));
            case no_if:
                {
                    IHqlExpression * lhs = inList->queryChild(1);
                    IHqlExpression * rhs = inList->queryChild(2);
                    if ((foldOptions & (HFOcanbreakshared|HFOforcefold)) || (lhs->isConstant() && rhs->isConstant()))
                    {
                        IHqlExpression * ret = querySimplifyInExpr(expr);
                        if (ret)
                            return ret;
                    }
                    break;
                }
            case no_addsets:
                {
                    if (foldOptions & (HFOcanbreakshared|HFOforcefold))
                    {
                        IHqlExpression * ret = querySimplifyInExpr(expr);
                        if (ret)
                            return ret;
                    }
                    break;
                }
            }

            if (inList->getOperator() == no_list)
            {
                if (inList->numChildren() == 0)
                    return createConstant((op != no_in));

                bool allConst = inList->isConstant();
                if (inList->numChildren() == 1)
                {
                    op = (op==no_in) ? no_eq : no_ne;
                    IHqlExpression * item1 = inList->queryChild(0);
                    Owned<ITypeInfo> type = getPromotedCompareType(child->queryType(), item1->queryType());
                    return createBoolExpr(op, ensureExprType(child, type), ensureExprType(item1, type));
                }

                //MORE: Could still remove cases that were impossible to reduce the comparison time,
                //      even if the default value is included in the list.
                if (allConst)
                {
                    switch (child->getOperator())
                    {
                    case no_case:
                        {
                            // CASE(x,a1=>v1,a2=>v2,v3) IN [x1,x2,x3,x4]
                            // becomes CASE(x,a1=>v1 IN X,a2=>v2 IN X, v3 IN X)
                            // becomes x [NOT] IN [am] where vm is in x1..xn
                            HqlExprArray caseResults;
                            if (flattenConstantCase(child, caseResults, true))
                            {
                                IValue *defval = child->queryChild(child->numChildren()-1)->queryValue();
                                if (defval)
                                {
                                    bool defaultInList = isInList(defval, inList);
                                    HqlExprArray exceptions;
                                    ForEachItemIn(i, caseResults)
                                    {
                                        IHqlExpression * inConst = &caseResults.item(i);
                                        IValue * inValue = inConst->queryValue();
                                        bool thisInList = isInList(inValue, inList);
                                        if (thisInList != defaultInList)
                                            exceptions.append(*LINK(child->queryChild(i+1)->queryChild(0)));
                                    }
                                    bool defaultReturn = (defaultInList && op==no_in) || (!defaultInList && op==no_notin);
                                    if (exceptions.ordinality() == 0)
                                        return createConstant(defaultReturn);
                                    node_operator inOp = defaultReturn ? no_notin : no_in;
                                    IHqlExpression * test = child->queryChild(0);
                                    return createBoolExpr(inOp,
                                            LINK(test),
                                            createValue(no_list, makeSetType(test->getType()), exceptions));
                                }
                            }
                        }
                        break;
                    case no_cast:
                    case no_implicitcast:
                        {
                            HqlExprArray inConstants;
                            unwindChildren(inConstants, inList);
                            IHqlExpression * ret = optimizeCastList(child, inConstants, op);
                            if (ret)
                                return ret;
                            break;
                        }
                    }
                }
                if (inList != originalList)
                    return replaceChild(expr, 1, inList);
            }
            break;
        }
    case no_if:
        {
            IHqlExpression * child = expr->queryChild(0);
            IValue * constValue = child->queryValue();
            if (constValue)
            {
                unsigned idx = constValue->getBoolValue() ? 1 : 2;
                IHqlExpression * branch = expr->queryChild(idx);
                if (!branch)
                {
                    assertex(expr->isAction());
                    return createValue(no_null, makeVoidType());
                }
                return LINK(branch);
            }

            if (expr->queryChild(2))
            {
                IHqlExpression * trueValue = expr->queryChild(1);
                IHqlExpression * falseValue = expr->queryChild(2);
                if (trueValue == falseValue)        // occurs in generated code...
                    return LINK(trueValue);

                if (expr->queryType()->getTypeCode() == type_boolean)
                {
                    HqlExprAttr ret;
                    if (trueValue->queryValue())
                    {
                        //IF(cond1, true, cond2) == cond1 || cond2
                        //if(cond1, false, cond2) == !cond1 && cond2
                        if (trueValue->queryValue()->getBoolValue())
                            ret.setown(createBoolExpr(no_or, LINK(child), LINK(falseValue)));
                        else
                            ret.setown(createBoolExpr(no_and, getInverse(child), LINK(falseValue)));
                    }
                    else if (falseValue->queryValue())
                    {
                        //IF(cond1, cond2, true) == !cond1 || cond2
                        //if(cond1, cond2, false) == cond1 && cond2
                        if (falseValue->queryValue()->getBoolValue())
                            ret.setown(createBoolExpr(no_or, getInverse(child), LINK(trueValue)));
                        else
                            ret.setown(createBoolExpr(no_and, LINK(child), LINK(trueValue)));
                    }
                    if (ret)
                        return ret.getClear();
                }
            }
            break;
        }
    case no_choose:
    case no_chooseds:
        {
            IHqlExpression * child = expr->queryChild(0);
            IValue * constValue = child->queryValue();
            unsigned last = numNonAttributes(expr)-1;
            if (constValue)
            {
                unsigned idx = (unsigned)constValue->getIntValue();
                if (idx > last || idx == 0)
                    idx = last;
                return LINK(expr->queryChild(idx));
            }

            //Remove any trailing conditions which match the default condition
            IHqlExpression * defaultExpr = expr->queryChild(last);
            unsigned cur = last-1;
            while (cur != 0)
            {
                if (expr->queryChild(cur)->queryBody() != defaultExpr->queryBody())
                    break;
                cur--;
            }

            if (cur != last-1)
            {
                //All match default => just return the default
                if (cur == 0)
                    return LINK(defaultExpr);

                HqlExprArray args;
                for (unsigned i=0; i <= cur; i++)
                    args.append(*LINK(expr->queryChild(i)));
                args.append(*LINK(defaultExpr));
                return expr->clone(args);
            }
            break;
        }
    case no_charlen:
        {
            IHqlExpression * child = expr->queryChild(0);
            ITypeInfo * type = child->queryType();
            size32_t len = type->getStringLen();
            if (len != UNKNOWN_LENGTH)
                return getSizetConstant(len);

            if (child->getOperator() == no_substring)
            {
                IHqlExpression * range = child->queryChild(1);
                switch (range->getOperator())
                {
                case no_range:
                    {
                        IValue * lowValue = range->queryChild(0)->queryValue();
                        IValue * highValue = range->queryChild(1)->queryValue();
                        if (lowValue && highValue)
                        {
                            __int64 low = lowValue->getIntValue();
                            __int64 high = highValue->getIntValue()+1;
                            if (low < 1)
                                low = 1;
                            if (high < low)
                                high = low;
                            return getSizetConstant((unsigned)(high - low));
                        }
                        break;
                    }
                case no_rangeto:
                    {
                        IValue * highValue = range->queryChild(0)->queryValue();
                        if (highValue)
                        {
                            __int64 high = highValue->getIntValue();
                            if (high < 0)
                                high = 0;
                            return getSizetConstant((unsigned)high);
                        }
                        break;
                    }
                case no_constant:
                    return getSizetConstant(1);
                }
            }
        }
        break;

    case no_negate:
    case no_roundup:
    case no_truncate:
    case no_exp:
    case no_ln:
    case no_sin:
    case no_cos:
    case no_tan:
    case no_asin:
    case no_acos:
    case no_atan:
    case no_sinh:
    case no_cosh:
    case no_tanh:
    case no_log10:
    case no_sqrt:
    case no_abs:
        {
            //MORE: I'm sure this could be cleaned up.... e.g., have a function passed a pointer to function
            IHqlExpression * child = expr->queryChild(0);
            IValue * constValue = child->queryValue();
            if (constValue)
            {
                switch (op)
                {
                case no_negate:
                    if (isNumericType(child->queryType()))
                        return createConstant(negateValue(constValue));
                    break;
                case no_roundup:
                    return createConstant(roundUpValue(constValue));
                case no_truncate:
                    return createConstant(truncateValue(constValue));
                case no_exp:
                    return createConstant(expValue(constValue));
                case no_ln:
                    return createConstant(lnValue(constValue));
                case no_sin:
                    return createConstant(sinValue(constValue));
                case no_cos:
                    return createConstant(cosValue(constValue));
                case no_tan:
                    return createConstant(tanValue(constValue));
                case no_asin:
                    return createConstant(asinValue(constValue));
                case no_acos:
                    return createConstant(acosValue(constValue));
                case no_atan:
                    return createConstant(atanValue(constValue));
                case no_sinh:
                    return createConstant(sinhValue(constValue));
                case no_cosh:
                    return createConstant(coshValue(constValue));
                case no_tanh:
                    return createConstant(tanhValue(constValue));
                case no_log10:
                    return createConstant(log10Value(constValue));
                case no_sqrt:
                    return createConstant(sqrtValue(constValue));
                case no_abs:
                    return createConstant(absValue(constValue));
                }
            }
            break;
        }
    case no_round:
        {
            //MORE: I'm sure this could be cleaned up.... e.g., have a function passed a pointer to function
            IHqlExpression * arg = expr->queryChild(0);
            IHqlExpression * places = expr->queryChild(1);
            IValue * constValue = arg->queryValue();
            if (constValue)
            {
                if (places)
                {
                    if (places->queryValue())
                        return createConstant(roundToValue(constValue, (int)getIntValue(places)));
                }
                else
                    return createConstant(roundValue(constValue));
            }
            break;
        }
    case no_eq:
    case no_ne:
    case no_lt:
    case no_le: 
    case no_gt: 
    case no_ge:
    case no_order:
        {
            IHqlExpression * ret = optimizeCompare(expr);
            if (ret)
                return ret;

            //Note, don't optimize IF(a,b,c) op x to IF(a,b op x, c OP x) because it uncommons attributes increasing the size of the queries.
            break;
        }
    case no_unicodeorder:
        {
            IHqlExpression * left = expr->queryChild(0);
            IHqlExpression * right = expr->queryChild(1);
            IHqlExpression * locale = expr->queryChild(2);
            IHqlExpression * strength = expr->queryChild(3);
            IValue * leftv = left->queryValue();
            IValue * rightv = right->queryValue();
            IValue * locv = locale->queryValue();
            IValue * strv = strength->queryValue();
            if(leftv && rightv && locv && strv)
            {
                unsigned leftsz = leftv->queryType()->getStringLen()+1;
                unsigned rightsz = rightv->queryType()->getStringLen()+1;
                UChar * leftstr = new UChar[leftsz];
                UChar * rightstr = new UChar[rightsz];
                leftv->getUCharStringValue(leftsz, leftstr);
                rightv->getUCharStringValue(rightsz, rightstr);
                StringBuffer locstr;
                locv->getStringValue(locstr);
                int val = rtlCompareVUnicodeVUnicodeStrength(leftstr, rightstr, locstr.str(), (unsigned)strv->getIntValue());
                delete [] leftstr;
                delete [] rightstr;
                return createConstant(val);
            }
            break;
        }
    case no_notnot:
        {
            return ensureExprType(expr->queryChild(0), expr->queryType());
        }
    case no_cast:
    case no_implicitcast:
        {
            IHqlExpression * child = expr->queryChild(0);
            ITypeInfo * exprType = expr->queryType();
            if (exprType == child->queryType())
                return LINK(child);

            node_operator childOp = child->getOperator();
            switch (childOp)
            {
            case no_constant:
                return createConstant(child->queryValue()->castTo(exprType));
            case no_cast:
            case no_implicitcast:
                {
                    //MORE: Not sure if this is a good idea because it loses commonality between attributes.
                    // (T1)((T2)(X:T3))
                    // Can remove the cast to T2 if T3->T2 doesn't lose any information, 
                    // and if the convertion from T2->T1 produces same results as converting T3->T1
                    // (For the moment only assume this is true if target is numeric)
                    // could possibly remove if T3-T2 and T2->T1 lose information, but they might 
                    // lose different information
                    IHqlExpression * grand = child->queryChild(0);
                    ITypeInfo * g_type = grand->queryType();
                    ITypeInfo * c_type = child->queryType();
                    ITypeInfo * e_type = exprType;
                    bool preserveValueG2C = preservesValue(c_type, g_type);
                    if (preserveValueG2C)
                    {
                        bool sameResults = false;
                        if (isNumericType(e_type))
                            sameResults = true;
                        else if (isStringOrUnicode(e_type) && isStringOrUnicode(c_type) && isStringOrUnicode(g_type))
                            sameResults = true;         
                        
                        // Don't allow casts involving data and non-ascii datasets because it can cause ascii convertions to get lost
                        if (castHidesConversion(e_type, c_type, g_type) ||
                            castHidesConversion(c_type, e_type, g_type) ||
                            castHidesConversion(g_type, c_type, e_type))
                            sameResults = false;

                        if (sameResults)
                        {
                            if (e_type == g_type)
                                return LINK(grand);
                            return createValue(op, LINK(e_type), LINK(grand));
                        }
                    }
                    break;
                }
            case no_case:
                {
                    //Don't allow variable size string returns to be lost...
                    if (((child->queryType()->getTypeCode() == type_varstring) ||
                         (child->queryType()->getTypeCode() == type_varunicode)) &&
                        (exprType->getSize() == UNKNOWN_LENGTH))

                        break;

                    HqlExprArray caseResults;
                    if (flattenConstantCase(child, caseResults, true))
                    {
                        IValue * defVal = child->queryChild(child->numChildren()-1)->queryValue();
                        if (defVal)
                        {
                            HqlExprArray newCaseMaps;
                            ITypeInfo * newType = exprType;
                            for (unsigned i=0; i<caseResults.ordinality(); i++)
                            {
                                IHqlExpression * result = (IHqlExpression *)&caseResults.item(i);
                                IValue * castRes = result->queryValue()->castTo(newType);
                                IHqlExpression * newMapping = createValue(no_mapto, LINK(child->queryChild(i+1)->queryChild(0)), createConstant(castRes));
                                newCaseMaps.append(*newMapping);
                            }
                            newCaseMaps.append(*createConstant(defVal->castTo(newType)));
                            newCaseMaps.add(*LINK(child->queryChild(0)), 0);
                            IHqlExpression * newCase = createValue(no_case, LINK(newType), newCaseMaps);
                            return newCase;
                        }
                    }
                    break;
                }
#if 0
            case no_if:
                {
                    if (isStringType(exprType) && (exprType->getSize() != UNKNOWN_LENGTH) && (child->queryType()->getSize() == UNKNOWN_LENGTH))
                    {
                        HqlExprArray args;
                        unwindChildren(args, child);
                        args.replace(*ensureExprType(&args.item(1), exprType, op), 1);
                        if (queryRealChild(child, 2))
                            args.replace(*ensureExprType(&args.item(2), exprType, op), 2);
                        return child->clone(args);
                    }
                    break;
                }
#endif
            case no_all:
            case no_list:
                return ensureExprType(child, exprType);
            case no_substring:
                {
                    //(stringN)(X[1..m]) -> (stringN)X if m >= N
                    unsigned castLen = exprType->getStringLen();
                    type_t tc = exprType->getTypeCode();
                    if ((castLen != UNKNOWN_LENGTH) && ((tc == type_string) || (tc == type_data) || (tc == type_qstring) || (tc == type_unicode) || (tc == type_utf8)))
                    {
                        IHqlExpression * range = child->queryChild(1);
                        bool simplify = false;
                        if (range->getOperator() == no_range)
                            simplify = (getIntValue(range->queryChild(0), 0) == 1) && (getIntValue(range->queryChild(1), 0) >= castLen);
                        else if (range->getOperator() == no_rangeto)
                            simplify = (getIntValue(range->queryChild(0), 0) >= castLen);
                        else if (range->getOperator() == no_constant)
                            simplify = (castLen == 1) && (getIntValue(range, 0) == castLen);
                        if (simplify)
                        {
                            HqlExprArray children;
                            children.append(*LINK(child->queryChild(0)));
                            return expr->clone(children);
                        }
                    }
                    break;
                }
            }
            break;
        }
    case no_typetransfer:
        {
            IHqlExpression * child = expr->queryChild(0);
            IValue * childValue = child->queryValue();
            if (childValue)
            {
                Linked<ITypeInfo> exprType = expr->queryType();
                ITypeInfo * childType = child->queryType();
                size32_t childSize = childValue->getSize();
                const void * rawvalue = childValue->queryValue();
                unsigned newSize = exprType->getSize();
                if (newSize == UNKNOWN_LENGTH)
                {
                    unsigned newLen = UNKNOWN_LENGTH;
                    switch (exprType->getTypeCode())
                    {
                    case type_string:
                    case type_varstring:
                        newLen = childSize;
                        break;
                    case type_unicode:
                        newLen = childSize / sizeof(UChar);
                        break;
                    case type_utf8:
                        newLen = rtlUtf8Length(childSize, rawvalue);
                        break;
                    }
                    if (newLen != UNKNOWN_LENGTH)
                    {
                        newSize = childSize;
                        exprType.setown(getStretchedType(newLen, exprType));
                    }
                }
                if (newSize <= childSize)
                {
                    IValue * transferred = createValueFromMem(LINK(exprType), rawvalue);
                    if (transferred && transferred->isValid())
                        return createConstant(transferred);
                }
            }
            break;
        }

    case no_case:
        {
            IHqlExpression * leftExpr = expr->queryChild(0);
            unsigned max = expr->numChildren();
            unsigned numCases = max-2;
            IValue * leftValue = leftExpr->queryValue();
            if (leftValue)
            {
                HqlExprArray args;
                args.append(*LINK(leftExpr));
                for (unsigned idx = 1; idx <= numCases; idx++)
                {
                    IHqlExpression * child = expr->queryChild(idx);
                    IHqlExpression * grand = child->queryChild(0);
                    IValue * grandValue = grand->queryValue();
                    if (grandValue)
                    {
                        if (orderValues(leftValue, grandValue) == 0)
                        {
                            if (args.ordinality() == 1)
                                return LINK(child->queryChild(1));

                            args.append(*LINK(child->queryChild(1)));
                            return expr->clone(args);
                        }
                    }
                    else
                        args.append(*LINK(child));
                }

                IHqlExpression * defaultValue = expr->queryChild(numCases+1);
                if (args.ordinality()==1)
                    return LINK(defaultValue);

                if (args.ordinality() != numCases+1)
                {
                    args.append(*LINK(defaultValue));
                    return expr->clone(args);
                }
            }
            else if (leftExpr->getOperator() == no_case)
            {
                HqlExprArray caseResults1;
                HqlExprArray caseInput2;
                HqlExprArray newCaseMaps;
                if (flattenConstantCase(leftExpr, caseResults1, true) && 
                    flattenConstantCase(expr, caseInput2, false))
                {
                    IHqlExpression * defCase2 = leftExpr->queryChild(leftExpr->numChildren()-1);
                    IValue * defVal2 = defCase2->queryValue();
                    if (defVal2)
                    {
                        unsigned inRes = 0;
                        unsigned numInput = caseInput2.ordinality();
                        unsigned i;
                        for (i=0; i<numInput; i++)
                        {
                            IHqlExpression * val = (IHqlExpression*)&caseInput2.item(i);
                            if (val->queryValue()->compare(defVal2) == 0)
                            {
                                inRes = i+1;
                                break;
                            }
                        }
                        IHqlExpression * defCase1 = expr->queryChild(expr->numChildren()-1);
                        for (i=0; i<caseResults1.ordinality(); i++)
                        {
                            bool found = false;
                            IHqlExpression * val1 = (IHqlExpression*)&caseResults1.item(i);
                            for (unsigned k=0; k<numInput; k++)
                            {
                                IHqlExpression * val2 = (IHqlExpression*)&caseInput2.item(k);
                                if (val1->queryValue()->compare(val2->queryValue()) == 0)
                                {
                                    IHqlExpression * newMapping = createValue(no_mapto, LINK(leftExpr->queryChild(i+1)->queryChild(0)), LINK(expr->queryChild(k+1)->queryChild(1)));
                                    newCaseMaps.append(*newMapping);
                                    found = true;
                                    break;
                                }
                            }
                            if (inRes && !found)
                            {
                                IHqlExpression * newMapping = createValue(no_mapto, LINK(leftExpr->queryChild(i+1)->queryChild(0)), LINK(defCase1));
                                newCaseMaps.append(*newMapping);
                            }
                        }
                        if (inRes)
                            newCaseMaps.append(*LINK(expr->queryChild(inRes)->queryChild(1)));
                        else
                            newCaseMaps.append(*LINK(defCase1));
                        newCaseMaps.add(*LINK(leftExpr->queryChild(0)), 0);
                        return createValue(no_case, expr->getType(), newCaseMaps);
                    }
                }
            }

            IHqlExpression * defaultValue = expr->queryChild(max-1);
            bool allMatchDefault = true;
            for (unsigned i=1; i < max-1; i++)
            {
                if (expr->queryChild(i)->queryChild(1) != defaultValue)
                {
                    allMatchDefault = false;
                    break;
                }
            }
            if (allMatchDefault)
                return LINK(defaultValue);

            if (numCases == 1)
            {
                IHqlExpression * mapto = expr->queryChild(1);
                IHqlExpression * key = mapto->queryChild(0);
                OwnedITypeInfo type = getPromotedCompareType(leftExpr->queryType(), key->queryType());
                IHqlExpression * newEqual = createBoolExpr(no_eq, ensureExprType(leftExpr, type), ensureExprType(key, type));
                return createIf(newEqual, LINK(mapto->queryChild(1)), LINK(expr->queryChild(2)));
            }
            break;
        }
    case no_map:
        {
            assertex(expr->numChildren()>=1);
            unsigned num = expr->numChildren()-1; 
            
            LinkedHqlExpr defaultResult = expr->queryChild(num);
            HqlExprArray args;
            bool allAreIn = true;
            bool changed = false;
            IHqlExpression * allTestField = NULL;
            for (unsigned idx = 0; idx < num; idx++)
            {
                IHqlExpression * child = expr->queryChild(idx);
                IHqlExpression * cond = child->queryChild(0);
                IValue * value = cond->queryValue();
                if (value)
                {
                    changed = true;
                    if (value->getBoolValue())
                    {
                        //New default condition - don't check any more arguments.
                        defaultResult.set(child->queryChild(1));
                        break;
                    }
                    //otherwise ignore that argument...
                }
                else if (isDuplicateMapCondition(args, cond))
                {
                    //Can occur when other earlier conditions have been simplified by constant foldeding
                    changed = true;
                }
                else
                {
                    if (allAreIn && (cond->getOperator() == no_in))
                    {
                        IHqlExpression * condSearch = cond->queryChild(0);
                        IHqlExpression * condSet = cond->queryChild(1);
                        if ((allTestField && (allTestField != condSearch)) || !condSet->isConstant() || (condSet->getOperator() != no_list))
                            allAreIn = false;
                        else
                            allTestField = condSearch;
                    }
                    else if (allAreIn && (cond->getOperator() == no_eq))
                    {
                        IHqlExpression * condSearch = cond->queryChild(0);
                        IHqlExpression * condSet = cond->queryChild(1);
                        if ((allTestField && (allTestField != condSearch)) || (condSet->getOperator() != no_constant))
                            allAreIn = false;
                        else
                            allTestField = condSearch;
                    }
                    else
                        allAreIn = false;

                    args.append(*LINK(child));
                }
            }

            //If no conditions yet, then the true value is the result, otherwise it is the default...
            if (args.ordinality() == 0 || areIndenticalMapResults(args, defaultResult))
                return defaultResult.getLink();

            if (allAreIn)
            {
                //Transform this map to a case - it will be much more efficient.
                HqlExprArray args2;
                ICopyArray alreadyDone;
                args2.append(*LINK(allTestField));
                ForEachItemIn(i, args)
                {
                    IHqlExpression & cur = args.item(i);
                    IHqlExpression * cond = cur.queryChild(0);
                    IHqlExpression * condValue = cond->queryChild(1);
                    IHqlExpression * mapValue = cur.queryChild(1);
                    if (cond->getOperator() == no_in)
                    {
                        ForEachChild(j, condValue)
                        {
                            IHqlExpression * value = condValue->queryChild(j);
                            if (alreadyDone.find(*value) == NotFound)
                            {
                                alreadyDone.append(*value);
                                args2.append(*createValue(no_mapto, LINK(value), LINK(mapValue)));
                            }
                        }
                    }
                    else
                    {
                        if (alreadyDone.find(*condValue) == NotFound)
                        {
                            alreadyDone.append(*condValue);
                            args2.append(*createValue(no_mapto, mapValue->getType(), LINK(condValue), LINK(mapValue)));
                        }
                    }
                }
                args2.append(*defaultResult.getLink());
                return createWrapper(no_case, expr->queryType(), args2);
            }
            if (changed)
            {
                args.append(*defaultResult.getLink());
                return expr->clone(args);
            }

#if 0
            //This is a sensible change - but it causes a bit too much code to be included in expressions at the moment
            if (num == 1)
            {
                IHqlExpression * child = expr->queryChild(0);
                return createIf(LINK(child->queryChild(0)), LINK(child->queryChild(1)), LINK(expr->queryChild(1)));
            }
#endif
            return LINK(expr);
        }
    case no_between:
    case no_notbetween:
        {
            IHqlExpression * child = expr->queryChild(0);
            IHqlExpression * lowExpr = expr->queryChild(1);
            IHqlExpression * highExpr = expr->queryChild(2);
            IValue * constValue = child->queryValue();
            IValue * low = lowExpr->queryValue();
            IValue * high = highExpr->queryValue();
            if (constValue && low && high)
            {

                bool ret = false;
                if (orderValues(constValue, low) >= 0)
                {
                    if (orderValues(constValue, high) <= 0)
                        ret = true;
                }
                return createConstant(op == no_between ? ret : !ret);
            }
            if (lowExpr == highExpr)
                return createValue(op == no_between ? no_eq : no_ne, makeBoolType(), LINK(child), LINK(lowExpr));
            break;
        }
    case no_substring:
        {
            IHqlExpression * child = expr->queryChild(0);
            IValue * constValue = child->queryValue();
            if (constValue)
            {
                IHqlExpression * limit = expr->queryChild(1);
                IValue * subString = NULL;
                if (limit->isConstant())
                {
                    switch (limit->getOperator())
                    {
                    case no_range:
                        {
                            IValue * lower = limit->queryChild(0)->queryValue();
                            IValue * upper = limit->queryChild(1)->queryValue();
                            if (lower && upper)
                                subString = substringValue(constValue, lower, upper);
                            break;
                        }
                    case no_rangeto:
                        {
                            IValue * upper = limit->queryChild(0)->queryValue();
                            if (upper)
                                subString = substringValue(constValue, NULL, upper);
                            break;
                        }
                    case no_rangefrom:
                        {
                            IValue * lower = limit->queryChild(0)->queryValue();
                            if (lower)
                                subString = substringValue(constValue, lower, NULL);
                            break;
                        }
                    case no_constant:
                        {
                            IValue * v = limit->queryValue();
                            subString = substringValue(constValue, v, v);
                            break;
                        }
                    }
                }
                if (subString)
                    return createConstant(subString);
            }

            //((stringN)X)[1..m] -> (stringN)X if m == N
            if (isCast(child))
            {
                ITypeInfo * type = child->queryType();
                unsigned castLen = type->getStringLen();
                type_t tc = type->getTypeCode();
                if ((castLen != UNKNOWN_LENGTH) && ((tc == type_string) || (tc == type_data) || (tc == type_qstring) || (tc == type_unicode) || (tc == type_utf8)))
                {
                    IHqlExpression * range = expr->queryChild(1);
                    bool simplify = false;
                    if (range->getOperator() == no_range)
                        simplify = (getIntValue(range->queryChild(0), 0) == 1) && (getIntValue(range->queryChild(1), 0) == castLen);
                    else  if (range->getOperator() == no_rangeto)
                        simplify = (getIntValue(range->queryChild(0), 0) == castLen);
                    else if (range->getOperator() == no_constant)
                        simplify = (castLen == 1) && (getIntValue(range, 0) == castLen);
                    if (simplify)
                        return LINK(child);
                }
            }

            //x[n..0], x[m..n] n<m == ''
            IHqlExpression * range = expr->queryChild(1);
            if (range->getOperator() == no_range)
            {
                IHqlExpression * rangeLow = range->queryChild(0);
                IHqlExpression * rangeHigh = range->queryChild(1);
                if (isZero(rangeHigh))
                    return createNullExpr(expr);
                if (getIntValue(rangeLow, 1) > getIntValue(rangeHigh, I64C(0x7fffffffffffffff)))
                    return createNullExpr(expr);
            }
            break;
        }
    case no_externalcall:
        {   //external function folding. 
            IValue * result = foldExternalCall(expr, foldOptions, templateContext);
            if (result) 
                return createConstant(result);
            break;
        }
    case no_call:
        {
            ForEachChild(i, expr)
            {
                if (!expr->queryChild(i)->isConstant())
                    return LINK(expr);
            }
            IHqlExpression * def = expr->queryBody()->queryFunctionDefinition();
            IHqlExpression * body = def->queryChild(0);
            if (body->getOperator() == no_outofline && body->queryChild(0)->getOperator()==no_embedbody)
            {
                IHqlExpression * result = foldEmbeddedCall(expr, foldOptions, templateContext);
                if (result)
                    return result;
                break;
            }
            OwnedHqlExpr folded = expandOutOfLineFunctionCall(expr);
            if ((folded != expr) && folded->isConstant())
                return folded.getClear();
            break;
        }
    case no_trim:
        {
            IHqlExpression * child = expr->queryChild(0);

            // 'R' - trim right, 'L' - Left, 'B' - Left and Right, 'A' - All
            char typecode = 'R';
            if(expr->hasAttribute(allAtom))
                typecode = 'A';
            else if(expr->hasAttribute(leftAtom) && expr->hasAttribute(rightAtom))
                typecode = 'B';
            else if(expr->hasAttribute(leftAtom))
                typecode = 'L';

            IValue * constValue = child->queryValue();
            IValue* resultstr = NULL;
            if (constValue) 
                resultstr = trimStringValue(constValue, typecode);

            if (resultstr) 
                return createConstant(resultstr);

            //extendin a string won't change the alue of trim(x), unless not trimming the rhs
            //i.e., trim((string60)string12expression)  => trim(string12expression);
            if ((typecode != 'L') && isCast(child))
            {
                IHqlExpression * uncast = child->queryChild(0);
                ITypeInfo * castType = child->queryType();
                ITypeInfo * uncastType = uncast->queryType();
                if ((castType->getSize() >= uncastType->getSize()) && (castType->getTypeCode() == uncastType->getTypeCode()))
                {
                    OwnedITypeInfo stretched = getStretchedType(castType->getStringLen(), uncastType);
                    if (stretched == castType)
                    {
                        HqlExprArray args;
                        args.append(*LINK(uncast));
                        unwindChildren(args, expr, 1);
                        return expr->clone(args);
                    }
                }
            }
            break;
        }
    case no_which:
    case no_rejected:
        {
            bool isWhich = (op == no_which);
            unsigned num = expr->numChildren();
            ITypeInfo * exprType = expr->queryType();
            switch (num)
            {
            case 1:
                {
                    int trueValue = isWhich ? 1 : 0;
                    return createValue(no_if, LINK(exprType), LINK(expr->queryChild(0)), createConstant(trueValue, LINK(exprType)), createConstant(1 - trueValue, LINK(exprType)));
                }
            }

            bool allConst = true;
            IHqlExpression * newWhich = createOpenValue(op, expr->getType());
            for (unsigned idx = 0; idx < num; idx++)
            {
                IHqlExpression * child = expr->queryChild(idx);
                IValue * constValue = child->queryValue();
                if (constValue)
                {
                    bool bVal = constValue->getBoolValue();
                    if (isWhich ? bVal : !bVal)
                    {
                        if (allConst)
                        {
                            newWhich->closeExpr()->Release();
                            return createConstant((__int64)idx+1, LINK(exprType));
                        }
                        else
                        {
                            //Add a value which will always match
                            newWhich->addOperand(createConstant(isWhich));
                            return newWhich->closeExpr();
                        }
                    }
                    else
                        newWhich->addOperand(LINK(child));
                }
                else
                {
                    allConst = false;
                    newWhich->addOperand(LINK(child));
                }
            }
            newWhich->closeExpr()->Release();
            if (allConst)
                return createConstant(0, LINK(exprType));
            break;
        }
    case no_index:
    case no_rowsetindex:
        {
            IHqlExpression * leftChild = expr->queryChild(0);
            IHqlExpression * rightChild = expr->queryChild(1);
            node_operator leftOp = leftChild->getOperator();
            if (leftOp == no_null)
                return createNullValue(expr);
            if ((leftOp != no_list) && (leftOp != no_datasetlist))
                break;
            IValue * rightValue = rightChild->queryValue();
            if(rightValue)
            {
                unsigned idx = (unsigned)rightValue->getIntValue();
                if ((idx != 0) && (leftChild->numChildren()>=idx))
                    return LINK(leftChild->queryChild(idx-1));
                else
                    return createNullValue(expr);
            }
            else if (!leftChild->numChildren())
                return createNullValue(expr);
        }
        break;
    case no_addsets:
        {
            IHqlExpression * left = expr->queryChild(0);
            IHqlExpression * right = expr->queryChild(1);
            if (left->getOperator() == no_all)
                return LINK(left);
            if (right->getOperator() == no_all)
                return LINK(right);
            if ((left->getOperator() == no_list) && (right->getOperator() == no_list))
            {
                HqlExprArray args;
                unwindChildren(args, left);
                unwindChildren(args, right);
                return left->clone(args);
            }
            break;
        }
    case no_max:
    case no_min:
    case no_ave:
    case no_evaluate:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            if (dataset->getOperator() == no_null)
                return createNullValue(expr);

            //MORE: Not so sure about this - what if the dataset turns out to have 0 elements???
            IHqlExpression * child = expr->queryChild(1);
            IValue * value = child->queryValue();
            if (value)
                return createConstant(value->castTo(expr->queryType()));

            if (dataset->getOperator() == no_datasetfromrow)
                return replaceSelector(child, dataset, dataset->queryChild(0));
        }
        break;
    case no_countdict:
        {
            IHqlExpression * child = expr->queryChild(0);
            node_operator childOp = child->getOperator();
            // Can't optimize count of a dictionary in general, since the input dataset may contain duplicates which will be removed.
            switch (child->getOperator())
            {
            case no_null:
                return createConstant(0);
            }
            break;
        }
    case no_existsdict:
        {
            IHqlExpression * child = expr->queryChild(0);
            node_operator childOp = child->getOperator();
            switch (child->getOperator())
            {
            case no_null:
                return createConstant(false);
            }
            break;
        }
    case no_countlist:
        {
            IHqlExpression * child = expr->queryChild(0);
            switch (child->getOperator())
            {
            case no_null:
            case no_list:
                return createConstant(createIntValue(child->numChildren(), LINK(expr->queryType())));
            }
            break;
        }
    case no_existslist:
        {
            IHqlExpression * child = expr->queryChild(0);
            switch (child->getOperator())
            {
            case no_null:
                return createConstant(false);
            case no_list:
                return createConstant(child->numChildren() != 0);
            }
            break;
        }
    case no_minlist:
    case no_maxlist:
        {
            IHqlExpression * child = expr->queryChild(0);
            bool isUnsigned = expr->queryType()->isUnsignedNumeric();
            switch (child->getOperator())
            {
            case no_null:
                return createNullExpr(expr);
            case no_list:
                {
                    IValue * best = NULL;
                    bool allConstant = true;
                    HqlExprArray values;
                    bool same = true;
                    ForEachChild(i, child)
                    {
                        IHqlExpression * cur = child->queryChild(i);
                        IValue * value = cur->queryValue();
                        if (value)
                        {
                            if (isUnsigned && isZero(cur))
                            {
                                if (op==no_minlist)
                                    return LINK(cur);  // Nothing can be lower than zero in an unsigned minlist...
                                else
                                {
                                    same = false;
                                    continue;
                                }
                            }
                            if (best)
                            {
                                int c = value->compare(best);
                                if (op == no_minlist ? c < 0 : c > 0)
                                    best = value;
                            }
                            else
                                best = value;
                            values.append(*LINK(cur));
                        }
                        else
                        {
                            if (!values.containsBody(*cur))
                                values.append(*LINK(cur));
                            else
                                same = false;
                            allConstant = false;
                        }
                    }
                    if (allConstant)
                    {
                        if (!best)
                            return createNullExpr(expr);
                        return createConstant(LINK(best));
                    }
                    
                    if (values.ordinality() == 1)
                        return expr->cloneAllAnnotations(&values.item(0));

                    if (!same)
                    {
                        OwnedHqlExpr newList = child->clone(values);
                        return replaceChild(expr, 0, newList);
                    }
                }
                break;
            }
            break;
        }
    case no_sumlist:
        {
            IHqlExpression * child = expr->queryChild(0);
            OwnedHqlExpr folded;
            switch (child->getOperator())
            {
            case no_null:
                return createNullExpr(expr);
            case no_list:
                if (child->isConstant())
                {
                    ITypeInfo * exprType = expr->queryType();
                    Owned<IValue> sum = createNullValue(exprType);
                    bool ok = true;
                    ForEachChild(i, child)
                    {
                        IHqlExpression * cur = child->queryChild(i);
                        IValue * value = cur->queryValue();
                        if (value)
                        {
                            Owned<IValue> castValue = value->castTo(exprType);
                            sum.setown(addValues(sum, castValue));
                        }
                        else
                        {
                            ok = false;
                            break;
                        }
                    }
                    if (ok)
                        folded.setown(createConstant(sum.getClear()));
                }

                if (child->numChildren() == 1)
                    folded.set(child->queryChild(0));

                if (folded)
                {
                    OwnedHqlExpr cast = ensureExprType(folded, expr->queryType());
                    return expr->cloneAllAnnotations(cast);
                }
                break;
            }
            break;
        }
        break;
    case no_notwithin:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (child->getOperator() == no_null)
                return createConstant(true);
        }
        break;
    case no_createset:
        {
            //If constant folding has caused the argument to be a constant then can convert this to a simple list 
            IHqlExpression * ds = expr->queryChild(0);
            IHqlExpression * value = expr->queryChild(1);
            if (value->isConstant() && hasSingleRow(ds))
                return createValue(no_list, expr->getType(), LINK(value));
            break;
        }
    case no_list:
        break;
    case no_tounicode:
        {
            IHqlExpression * dataChild = expr->queryChild(0);
            IHqlExpression * codepageChild = expr->queryChild(1);
            IValue * dataValue = dataChild->queryValue(); 
            IValue * codepageValue = codepageChild->queryValue();
            if(dataValue && codepageValue) 
            {
                unsigned unicodeLength;
                UChar * unicode;
                StringBuffer buff;
                rtlCodepageToUnicodeX(unicodeLength, unicode, dataValue->getSize(), (char const *)dataValue->queryValue(), codepageValue->getStringValue(buff));
                ITypeInfo * unicodeType = makeUnicodeType(unicodeLength, 0);
                IValue * unicodeValue = createUnicodeValue(unicodeLength, unicode, unicodeType);
                rtlFree(unicode);
                return createConstant(unicodeValue);
            }
            break;
        }
    case no_fromunicode:
        {
            IHqlExpression * unicodeChild = expr->queryChild(0);
            IHqlExpression * codepageChild = expr->queryChild(1);
            IValue * unicodeValue = unicodeChild->queryValue(); 
            IValue * codepageValue = codepageChild->queryValue();
            if(unicodeValue && codepageValue) 
            {
                unsigned dataLength;
                char * data;
                StringBuffer buff;
                rtlUnicodeToCodepageX(dataLength, data, unicodeValue->queryType()->getStringLen(), (UChar const *)unicodeValue->queryValue(), codepageValue->getStringValue(buff));
                IValue * dataValue = createDataValue(data, dataLength);
                rtlFree(data);
                return createConstant(dataValue);
            }
            break;
        }
    case no_keyunicode:
        {
            IHqlExpression * val = expr->queryChild(0);
            IHqlExpression * locale = expr->queryChild(1);
            IHqlExpression * strength = expr->queryChild(2);
            IValue * valv = val->queryValue();
            IValue * locv = locale->queryValue();
            IValue * strv = strength->queryValue();
            if(valv && locv && strv)
            {
                unsigned outlen;
                void * out;
                unsigned vallen = valv->queryType()->getStringLen();
                UChar * valstr = new UChar[vallen];
                valv->getUCharStringValue(vallen, valstr);
                StringBuffer locstr;
                locv->getStringValue(locstr);
                rtlKeyUnicodeStrengthX(outlen, out, vallen, valstr, locstr.str(), (unsigned)strv->getIntValue());
                delete [] valstr;
                IValue * dataValue = createDataValue((char *)out, outlen);
                rtlFree(out);
                return createConstant(dataValue);
            }
            break;
        }
    case no_random:
        if (foldOptions & (HFOfoldimpure|HFOforcefold))
            return createConstant(expr->queryType()->castFrom(true, (__int64)rtlRandom()));
        break;
    case no_catch:
        if (expr->isConstant())
        {
            try
            {
                return LINK(expr->queryChild(0));
            }
            catch (IException * e)
            {
                e->Release();
                return LINK(expr->queryChild(1));
            }
        }   
        //maybe we should stop folding of the children.
        break;
    case no_section:
        if (expr->queryChild(0)->isConstant())
            return LINK(expr->queryChild(0));
        break;
    case no_sizeof:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (child->isRecord())
            {
                //Need to be careful to use the serialized record - otherwise record size can be inconsistent
                OwnedHqlExpr record = getSerializedForm(child, diskAtom);
                if (expr->hasAttribute(maxAtom))
                {
                    if (maxRecordSizeCanBeDerived(record))
                        return getSizetConstant(getMaxRecordSize(record, 0));
                }
                else if (expr->hasAttribute(minAtom))
                {
                    return getSizetConstant(getMinRecordSize(record));
                }
                else
                {
                    if (!isVariableSizeRecord(record))
                        return getSizetConstant(getMaxRecordSize(record, 0));
                }
            }
            //MORE: Handle types - but be very careful about maxlength attributes...  (no_typeof doesn't exist yet either)
            //else if (child->getOperator() == no_typeof)
            break;
        }
    case no_actionlist:
    case no_orderedactionlist:
        {
            bool same = true;
            HqlExprArray args;
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if (isNull(cur))
                {
                    if (same)
                        unwindChildren(args, expr, 0, i);
                    same = false;
                }
                else
                {
                    if (!same)
                        args.append(*LINK(cur));
                }
            }
            if (!same)
                return createActionList(op, args);
            break;
        }
    case no_exists:
        if (isNull(expr->queryChild(0)))
            return createConstant(false);
        break;
    case no_alias:
        {
            IHqlExpression * arg = expr->queryChild(0);
            if (arg->getOperator() == no_constant)
                return LINK(arg);
            break;
        }
    }

    return LINK(expr);
}


//---------------------------------------------------------------------------

bool isNullRowDs(IHqlExpression * expr)     
{ 
    return ((expr->getOperator() == no_datasetfromrow) && isNull(expr->queryChild(0)));
}


IHqlExpression * preserveGrouping(IHqlExpression * child, IHqlExpression * expr)
{
    if (!isGrouped(expr))
    {
        if (isGrouped(child))
            return createDataset(no_group, LINK(child));
    }
    else
    {
        //weird, but just about possible if grouped keyed join was replaced with rhs, check just in case
        assertex(isGrouped(child));
    }
    return LINK(child);
}

static bool matchesAtmost1(IHqlExpression * expr)
{
    IHqlExpression * atmost = expr->queryAttribute(atmostAtom);
    if (!atmost)
        return false;
    if (!matchesConstantValue(atmost->queryChild(0), 1))
        return false;
    return true;
}

static bool hasRowLimit(IHqlExpression * expr)
{
    IHqlExpression * limit = expr->queryAttribute(rowLimitAtom);
    return limit && !matchesConstantValue(limit->queryChild(0), 0);
}

IHqlExpression * NullFolderMixin::foldNullDataset(IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);

    //These items remove the current node - so don't need to check if the children are shared.
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_distribute:
    case no_distributed:
        {
            if (isNull(child))
                return replaceWithNull(expr);
            if (isFail(child))
                return removeParentNode(expr);
            if (expr->hasAttribute(skewAtom))
            	break;
            //Careful - distribute also destroys grouping, so don't remove if input is grouped.
            if ((queryDistribution(expr) == queryDistribution(child)) && !isGrouped(child))
                return removeParentNode(expr);
            break;
        }
    case no_sort:
    case no_subsort:
    case no_sorted:
        {
            //Subsort is unusual because the order applied to an unsorted dataset will also be unsorted
            if ((op != no_subsort) || hasKnownSortGroupDistribution(child, expr->hasAttribute(localAtom)))
            {
                //If action does not change the type information, then it can't have done anything...
                if (hasSameSortGroupDistribution(expr, child))
                    return removeParentNode(expr);
            }
            if (isNull(child) || hasNoMoreRowsThan(child, 1))
                return removeParentNode(expr);
            //If all arguments to sort are constant then remove it, otherwise the activities will not like it.
            //NOTE: MERGE has its sort order preserved, so it won't cause issues there.
            bool allConst = true;
            ForEachChildFrom(i, expr, 1)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if (!cur->isAttribute() && !cur->isConstant())
                {
                    allConst = false;
                    break;
                }
            }
            if (allConst && (op == no_sort))
                return removeParentNode(expr);
            break;
        }
    case no_if:
        {
            //Processed hereThis won't split shared nodes, but one of the children may be shared - so proce
            if (isNull(expr->queryChild(1)))
            {
                //A no_null action is treated the same as a non existant action.
                IHqlExpression * falseBranch = expr->queryChild(2);
                if (!falseBranch || isNull(falseBranch))
                    return replaceWithNull(expr);
            }
            break;
        }
    case no_group:
    case no_grouped:
//  case no_preservemeta:
        {
            //If action does not change the type information, then it can't have done anything...
            if (hasSameSortGroupDistribution(expr, child))
                return removeParentNode(expr);
            if (isNull(child))
                return replaceWithNull(expr);
            break;
        }
    case no_denormalize:
    case no_denormalizegroup:
    case no_join:
        {
            IHqlExpression * rhs = expr->queryChild(1);
            bool leftIsNull = isNull(child);
            bool rightIsNull = isNull(rhs);
            bool cvtLeftProject = false;
            bool cvtRightProject = false;
            const char * reason = NULL;
            if (leftIsNull || rightIsNull)
            {
                bool createNull = false;
                if (isFullJoin(expr))
                    createNull = leftIsNull && rightIsNull;
                else if (isLeftJoin(expr))
                    createNull = leftIsNull;
                else if (isRightJoin(expr))
                    createNull = rightIsNull;
                else
                    createNull = leftIsNull || rightIsNull;

                if (createNull)
                    return replaceWithNull(expr);

                if (leftIsNull)
                    cvtRightProject = true;
                else if (rightIsNull)
                {
                    //JOIN(ds,<null>) becomes a project
                    //DENORMALIZE(ds, <null>) becomes a nop (since the transform will not be called)
                    //DENORMALIZE(ds, <null>, GROUP) becomes a project
                    if (op == no_denormalize)
                        return removeParentNode(expr);  // ok because this returns queryChild(0)

                    cvtLeftProject = true;
                    reason = "(ds,<empty>)";
                }
            }

            //JOIN with false condition - can occur once constants are folded.
            IValue * condValue = expr->queryChild(2)->queryValue();
            if (condValue && !condValue->getBoolValue())
            {
                //Never matches, so either LHS is modified by the transform - like a project, or it never returns anything.
                if (isLeftJoin(expr))
                {
                    if (op == no_denormalize)
                        return removeParentNode(expr);  // ok because this returns queryChild(0)

                    cvtLeftProject = true;
                    reason = "(false)";
                }
                else if (isInnerJoin(expr))
                    return replaceWithNull(expr);
            }

            //JOIN, left outer, keep(1) with no reference to RIGHT in the transform => convert to a project!
            //again can occur once the implicit project has started getting to work.
            if (!cvtLeftProject)
            {
                const char * potentialLeftProjectReason = NULL;
                if (isSpecificJoin(expr, leftouterAtom))
                {
                    if (matchesConstantValue(queryAttributeChild(expr, keepAtom, 0), 1) && !hasRowLimit(expr))
                        potentialLeftProjectReason = "(,LEFT OUTER,KEEP(1))";
                    else if (matchesAtmost1(expr) && !hasRowLimit(expr))
                        potentialLeftProjectReason = "(,LEFT OUTER,ATMOST(1))";
                    else if (expr->hasAttribute(lookupAtom) && !expr->hasAttribute(manyAtom))
                        potentialLeftProjectReason = "(,LEFT OUTER,SINGLE LOOKUP)";
                    else if (hasNoMoreRowsThan(expr, 1))
                        potentialLeftProjectReason = "(<single-row>,LEFT OUTER)";
                }

                if (potentialLeftProjectReason)
                {
                    //This cannot match if the transform contains a skip - since that would
                    IHqlExpression * selSeq = querySelSeq(expr);
                    OwnedHqlExpr right = createSelector(no_right, rhs, selSeq);
                    IHqlExpression * transform = expr->queryChild(3);
                    if (!exprReferencesDataset(transform, right))
                    {
                        cvtLeftProject = true;
                        reason = potentialLeftProjectReason;
                    }

                    if (cvtLeftProject && (expr->getOperator() == no_denormalize))
                    {
                        OwnedHqlExpr left = createSelector(no_left, child, selSeq);
                        //Denormalize with no match will not call the transform, so we can't convert that to a project
                        //unless the transform is a nop
                        if (!transformReturnsSide(expr, no_left, 0))
                            cvtLeftProject = false;
                    }
                }
            }

            if (cvtLeftProject)
            {
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr right = createSelector(no_right, rhs, selSeq);
                OwnedHqlExpr null = createRow(no_newrow, createNullExpr(right));
                OwnedHqlExpr newTransform = replaceSelector(expr->queryChild(3), right, null);
                if (op == no_denormalizegroup)
                {
                    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);
                    OwnedHqlExpr rowsExpr = createDataset(no_rows, LINK(right), LINK(rowsid));
                    OwnedHqlExpr nullExpr = createDataset(no_null, LINK(rhs->queryRecord()));
                    newTransform.setown(replaceExpression(newTransform, rowsExpr, nullExpr));
                }
                if (op == no_denormalize)
                {
                    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
                    if (counter)
                    {
                        OwnedHqlExpr one = createConstant(counter->queryType()->castFrom(false, I64C(1)));
                        //Remove the annotations from the transform, otherwise it may say t(LEFT,COUNTER) which is confusing.
                        newTransform.setown(replaceExpression(newTransform->queryBody(), counter, one));
                    }
                }
                HqlExprArray args;
                args.append(*preserveGrouping(child, expr));
                args.append(*newTransform.getClear());
                args.append(*LINK(selSeq));
                OwnedHqlExpr ret = createDataset(no_hqlproject, args);
                DBGLOG("Folder: Replace %s%s with PROJECT", getOpString(op), reason);
                return ret.getClear();
            }

#if 0
            //This is pretty unlikely, and may introduce an ambiguity in LEFT (if selector sequences aren't unique)
            if (cvtRightProject && !isGrouped(expr))
            {
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr left = createSelector(no_left, child, selSeq);
                OwnedHqlExpr null = createRow(no_newrow, createNullExpr(left));
                OwnedHqlExpr transformNoLeft  = replaceSelector(expr->queryChild(3), left, null);
                OwnedHqlExpr right = createSelector(no_right, rhs, selSeq);
                OwnedHqlExpr newLeft = createSelector(no_left, child, selSeq);
                OwnedHqlExpr newTransform  = replaceSelector(transformNoLeft, right, newLeft);

                HqlExprArray args;
                args.append(*preserveGrouping(rhs, expr));
                args.append(*newTransform.getClear());
                args.append(*LINK(selSeq));
                OwnedHqlExpr ret = createDataset(no_hqlproject, args);
                DBGLOG("Folder: Replace JOIN(<empty>, ds) with PROJECT");
                return ret.getClear();
            }
#endif
            break;
        }
    case no_merge:
    case no_addfiles:
    case no_regroup:
    case no_nonempty:
    case no_cogroup:
        {
            HqlExprArray args;
            bool changed = false;
            IHqlExpression * lastInput = NULL;
            unsigned inputCount = 0;
            //Careful - the node may have attributes, which we want to preserve if the node is preserved.
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if ((cur->getOperator() != no_null) && ((op != no_nonempty) || !args.contains(*cur)))
                {
                    if (!cur->isAttribute())
                    {
                        lastInput = cur;
                        inputCount++;
                    }
                    args.append(*LINK(cur));
                }
                else
                    changed = true;
            }
            if (changed || (inputCount == 1))
            {
                //NOTE: The only branches removed are no_null, so don't need to worry about decrementing their link counts.
                switch (inputCount)
                {
                case 0:
                    return replaceWithNull(expr);
                case 1:
                    if (op == no_cogroup)
                    {
                        DBGLOG("Folder: Replace %s with group", getOpString(op));
                        IHqlExpression * grouping = queryAttributeChild(expr, groupAtom, 0);
                        IHqlExpression * mappedGrouping = replaceSelector(grouping, queryActiveTableSelector(), lastInput);
                        OwnedHqlExpr group = createDataset(no_group, LINK(lastInput), mappedGrouping);
                        return expr->cloneAllAnnotations(group);
                    }
                    else
                    {
                        DBGLOG("Folder: Replace %s with child", getOpString(op));
                        return LINK(lastInput);
                    }
                default:
                    DBGLOG("Folder: Remove %d inputs from %s", expr->numChildren()-args.ordinality(), getOpString(op));
                    return expr->clone(args);
                }
            }
            break;
        }
    case no_fetch:
        if (isNull(expr->queryChild(1)))
            return replaceWithNull(expr);
        break;
    case no_aggregate:
    case no_newaggregate:
        if (isNull(child))
        {
            if (isGrouped(child) || queryRealChild(expr, 3))
                return replaceWithNull(expr);
            return replaceWithNullRowDs(expr);
        }
        break;
    case no_inlinetable:
        if (expr->queryChild(0)->numChildren() == 0)
            return replaceWithNull(expr);
        break;
    case no_dataset_from_transform:
        if (isZero(expr->queryChild(0)))
            return replaceWithNull(expr);
        break;
    case no_temptable:
        {
            IHqlExpression * values = expr->queryChild(0);
            if (isNull(values) || ((values->getOperator() == no_list) && (values->numChildren() == 0)))
                return replaceWithNull(expr);
            break;
        }
    case no_newusertable:
        if (isNullProject(expr, false, false))
            return removeParentNode(expr);
        if (isNull(child))
        {
            //a grouped aggregate is ok - will generate no rows, as will a non-aggregate
            if (datasetHasGroupBy(expr) || !isAggregateDataset(expr))
                return replaceWithNull(expr);
        }
        break;
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskgroupaggregate:
    case no_compound_diskcount:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexgroupaggregate:
    case no_compound_indexcount:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childgroupaggregate:
    case no_compound_childcount:
    case no_compound_inline:
    case no_compound_selectnew:
        {
            node_operator childOp = child->getOperator();
            if ((op == childOp) || (childOp == no_null))
                return removeParentNode(expr);
            break;
        }
    case no_compound_diskaggregate:
    case no_compound_indexaggregate:
    case no_compound_childaggregate:
        {
            if (isNullRowDs(child))
                return removeParentNode(expr);
            node_operator childOp = child->getOperator();
            if ((op == childOp) || (childOp == no_null))
                return removeParentNode(expr);
            break;
        }
    case no_assert_ds:
        {
            if (isNull(child))
                return removeParentNode(expr);

            bool hasAssert = false;
            ForEachChildFrom(i, expr, 1)
            {
                IHqlExpression * cur = queryRealChild(expr, i);
                if (cur && (cur->getOperator() != no_null))
                {
                    hasAssert = true;
                    break;
                }
            }
            //All asserts have constant folded away...
            if (!hasAssert)
                return removeParentNode(expr);
            break;
        }

    case no_choosen:
        {
            if (isNull(child) || isFail(child))
                return removeParentNode(expr);
            IHqlExpression * choosenLimit = expr->queryChild(1);
            IValue * choosenValue = choosenLimit->queryValue();
            if (choosenValue)
            {
                __int64 v = choosenValue->getIntValue();
                if (v == 0)
                    return replaceWithNull(expr);

                ITypeInfo * type = choosenLimit->queryType();
                if (type->isSigned() && (v < 0))
                    return replaceWithNull(expr);

                if (!queryRealChild(expr, 2))
                {
                    //choosen(x, n) n>0 on a single row is same as the single row
                    if (hasNoMoreRowsThan(child, 1))        // could use v
                        return removeParentNode(expr);

                    if (!isGrouped(expr))
                    {
                        if (v == CHOOSEN_ALL_LIMIT)
                            return removeParentNode(expr);

                        if (!isLocalActivity(expr) && hasNoMoreRowsThan(child, v))
                            return removeParentNode(expr);
                    }
                }
            }
            break;
        }
    case no_dedup:
    case no_rollup: // rollup on a single row does not call the transform => can be removed.
        if (isNull(child) || hasNoMoreRowsThan(child, 1) || isFail(child))
            return removeParentNode(expr);
        break;
    case no_limit:
        {
            if (isNull(child) || isFail(child))
                return removeParentNode(expr);

            __int64 limit = getIntValue(expr->queryChild(1), 0);
            if (limit >= 1)
            {
                if (hasNoMoreRowsThan(child, 1))
                    return removeParentNode(expr);

                if (!isGrouped(expr) && !isLocalActivity(expr) && hasNoMoreRowsThan(child, limit))
                    return removeParentNode(expr);
            }
            break;
        }
    case no_catchds:
        {
            if (isNull(child))
                return removeParentNode(expr);
            break;
        }
    case no_filter:
    case no_keyeddistribute:
    case no_choosesets:
    case no_enth:
    case no_sample:
    case no_keyedlimit:
    case no_cosort:
    case no_topn:
    case no_iterate:
    case no_preload:
    case no_alias:
    case no_forcelocal:
    case no_nothor:
    case no_cluster:
    case no_forcenolocal:
    case no_assertsorted:
    case no_assertgrouped:
    case no_assertdistributed:
    case no_stepped:
    case no_filtergroup:
    case no_section:
    case no_related:
    case no_unordered:
    case no_preservemeta:
        if (isNull(child) || isFail(child))
            return removeParentNode(expr);
        break;
    case no_transformebcdic:
    case no_transformascii:
    case no_rollupgroup:
    case no_normalizegroup:
    case no_parse:
    case no_newparse:
    case no_xmlparse:
    case no_newxmlparse:
    case no_selfjoin:
    case no_process:
        if (isNull(child))
            return replaceWithNull(expr);
        break;
    case no_normalize:
        if (isNull(child) || matchesConstantValue(expr->queryChild(1), 0))
            return replaceWithNull(expr);
        break;
    case no_allnodes:
    case no_thisnode:
        if (isNull(child) && expr->isDataset())
            return replaceWithNull(expr);
        break;
    case no_combine:
    case no_combinegroup:
        if (isNull(child) && isNull(expr->queryChild(1)))
            return replaceWithNull(expr);
        break;
    case no_createdictionary:
        if (isNull(child))
            return replaceWithNull(expr);
        break;
    case no_selectmap:
        if (isNull(child))
            return replaceWithNullRow(child);
        break;
    case no_selectnth:
//      if (isNull(child) || isZero(expr->queryChild(1)))
        if (isNull(child))
            return replaceWithNullRow(child);
        break;  
    case no_select:
        if (isNull(child) && expr->hasAttribute(newAtom))
            return replaceWithNull(expr);
        break;
    case no_createset:
        if (isNull(child))
            return replaceWithNull(expr);
        break;
    case no_hqlproject:
    case no_projectrow:
        {
            if (isNullProject(expr, false, false))
                return removeParentNode(expr);
            if (isNull(child))
                return replaceWithNull(expr);
            break;
        }
    case no_output:
        {
            //Appending a null dataset to an output does nothing (sometimes occurs as a kind of nop)
            if (!queryRealChild(expr, 1) && expr->hasAttribute(extendAtom))
            {
                if (isNull(child) && child->isDataset())
                    return replaceWithNull(expr);
            }
            break;
        }
    case no_compound:
        if (isNull(child) && child->isAction())
            return LINK(expr->queryChild(1));       // Could cause overlinking of child when called from HqlOpt
        break;
    case no_executewhen:
        {
            IHqlExpression * action = expr->queryChild(1);
            if (isNull(action) && action->isAction())
                return removeParentNode(expr);
            break;
        }
    case no_globalscope:
        if (isRedundantGlobalScope(expr))
            return removeParentNode(expr);
        break;
    case no_evaluate_stmt:
        {
            IHqlExpression * arg = expr->queryChild(0);
            if (arg->isConstant() || arg->getOperator() == no_table)
                return createNullExpr(expr);
            break;
        }
    }
    return NULL;
}


IHqlExpression * NullFolderMixin::queryOptimizeAggregateInline(IHqlExpression * expr, __int64 numRows)
{
    node_operator specialOp = querySimpleAggregate(expr, false, true);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * assign = transform->queryChild(0);     // guaranteed to be in simple form
    LinkedHqlExpr value;
    switch (specialOp)
    {
    case no_existsgroup:
        value.setown(createConstant(numRows != 0));
        break;
    case no_countgroup:
        {
            ITypeInfo * type = assign->queryChild(0)->queryType();
            value.setown(createConstant(type->castFrom(true, numRows)));
            break;
        }
        //could do max/min/sum if really wanted to
    }
    if (!value)
        return NULL;

    HqlExprArray args;
    args.append(*createAssign(LINK(assign->queryChild(0)), LINK(value)));
    OwnedHqlExpr newTransform = createValue(no_transform, transform->getType(), args);
    OwnedHqlExpr values = createValue(no_transformlist, newTransform.getClear());
    return createDataset(no_inlinetable, values.getClear(), LINK(expr->queryRecord()));
}





//---------------------------------------------------------------------------

IHqlExpression * getLowerCaseConstant(IHqlExpression * expr)
{
    ITypeInfo * type = expr->queryType();
    switch (type->getTypeCode())
    {
    case type_unicode:
    case type_varunicode:
    case type_string:
    case type_data:
    case type_varstring:
    case type_utf8:
        break;
    default:
        return LINK(expr);
    }

    IValue * value = expr->queryValue();
    assertex(value);
    const void * data = value->queryValue();
    unsigned size = type->getSize();
    unsigned stringLen = type->getStringLen();

    MemoryAttr lower(size);
    memcpy(lower.bufferBase(), data, size);
    if (type->getTypeCode() == type_utf8)
        rtlUtf8ToLower(stringLen, (char *)lower.get(), str(type->queryLocale()));
    else if (isUnicodeType(type))
        rtlUnicodeToLower(stringLen, (UChar *)lower.get(), str(type->queryLocale()));
    else
    {
        if (type->queryCharset()->queryName() == ebcdicAtom)
            rtlEStrToStr(stringLen, (char*)lower.get(), stringLen, (char*)lower.get()); // Yes it does work in place.
        rtlStringToLower(stringLen, (char *)lower.get());
        if (type->queryCharset()->queryName() == ebcdicAtom)
            rtlStrToEStr(stringLen, (char*)lower.get(), stringLen, (char*)lower.get()); // Yes it does work in place.
    }
    if (memcmp(lower.get(), data, size) == 0)
        return LINK(expr);

    return createConstant(createValueFromMem(LINK(type), lower.get()));
}

//---------------------------------------------------------------------------

static HqlTransformerInfo constantReplacingTransformerInfo("ConstantReplacingTransformer");
class HQL_API ConstantReplacingTransformer : public NewHqlTransformer
{
public:
    ConstantReplacingTransformer(IHqlExpression * _selector) : NewHqlTransformer(constantReplacingTransformerInfo) { selector = _selector; }

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        node_operator op = expr->getOperator();
        //Special case for things that really shouldn't have substitutions
        switch (op)
        {
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            {
                IAtom * name = expr->queryName();
                if (name == _selectors_Atom)
                {
                    HqlExprArray args;
                    ForEachChild(i, expr)
                        args.append(*transformSelector(expr->queryChild(i)));
                    return expr->clone(args);
                }
                break;
            }
#if 0
            //MORE: These should be constant folded , otherwise it can mess up the graph commoning. (see outmod.xhql)
        case no_crc:
        case no_hash:
        case no_hash32:
        case no_hash64:
            return LINK(expr);
#endif
        }
        if (expr->isConstant())
            return LINK(expr);

        unsigned numNonHidden = activityHidesSelectorGetNumNonHidden(expr, selector);
        if (numNonHidden == 0)
            return NewHqlTransformer::createTransformed(expr);

        bool same = true;
        HqlExprArray children;
        for (unsigned i=0; i < numNonHidden; i++)
        {
            IHqlExpression * cur = expr->queryChild(i);
            IHqlExpression * mapped = transform(cur);
            children.append(*mapped);
            if (cur != mapped)
                same = false;
        }
        if (same)
            return LINK(expr);

        unwindChildren(children, expr, numNonHidden);
        return expr->clone(children);
    }

    void setMapping(IHqlExpression * oldValue, IHqlExpression * newValue)
    {
        NewHqlTransformer::setMapping(oldValue, newValue);
        //Nasty... I'm not sure if this should really happen, but...
        //if we are replacing a row, then the old active selector needs to become an inline row (e.g., prwo.xhql)
        if (oldValue->isDatarow())
        {
            OwnedHqlExpr newRow = createRow(no_newrow, LINK(newValue));
            setSelectorMapping(oldValue, newRow);
        }
    }


protected:
    IHqlExpression * selector;
};


static bool isWorthPercolating(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_attr:
    case no_attr_link:
    case no_constant:
    case no_getresult:
    case no_record:
    case no_all:
        return false;
    }
    return !expr->isConstant();
}

static bool isWorthPercolating(const HqlExprArray & exprs)
{
    ForEachItemIn(i, exprs)
        if (isWorthPercolating(&exprs.item(i)))
            return true;
    return false;
}


bool expressionsEquivalent(IHqlExpression * left, IHqlExpression * right)
{
    if (left->queryBody() == right->queryBody())
        return true;
    if ((left->getOperator() == no_null) && (right->getOperator() == no_null))
        return recordTypesMatch(left, right);
    return false;
}

bool exprsReferencesDataset(const HqlExprArray & source, IHqlExpression * exprSelector)
{
    ForEachItemIn(i, source)
    {
        if (exprReferencesDataset(&source.item(i), exprSelector))
            return true;
    }
    return false;
}

class HqlConstantPercolator : public CInterface
{
public:
    HqlConstantPercolator(IHqlExpression *ds = NULL) 
    { 
        if (ds)
            self.setown(getSelf(ds));
    }

    void addEquality(IHqlExpression * target, IHqlExpression * source)
    {
        addTransformMapping(target, source);
    }

    bool empty() { return targets.empty(); }

    IHqlExpression * expandFields(IHqlExpression * expr, IHqlExpression * exprSelector)
    {
        if (!isWorthPercolating(expr))
            return LINK(expr);
        if (!exprReferencesDataset(expr, exprSelector))
            return LINK(expr);
        ConstantReplacingTransformer transformer(exprSelector);
        initTransformer(exprSelector, transformer);
        return transformer.transformRoot(expr);
    }
    IHqlExpression * expandField(IHqlExpression * field)
    {
        ForEachItemIn(i, targets)
        {
            if (targets.item(i).queryChild(1) == field)
                return LINK(&sources.item(i));
        }
        return NULL;
    }
    bool expandFields(HqlExprArray & target, const HqlExprArray & source, IHqlExpression * exprSelector)
    {
        if (!isWorthPercolating(source))
            return false;
        if (!exprsReferencesDataset(source, exprSelector))
            return false;
        ConstantReplacingTransformer transformer(exprSelector);
        initTransformer(exprSelector, transformer);
        ForEachItemIn(i, source)
        {
            IHqlExpression & cur = source.item(i);
            if (exprReferencesDataset(&cur, exprSelector))
                target.append(*transformer.transformRoot(&cur));
            else
                target.append(*LINK(&cur));
        }
        return true;
    }
    void inheritMapping(const HqlConstantPercolator * other)
    {
        assertex(other);
        if (other->self)
        {
            assertex(!self || self == other->self);
            self.set(other->self);
        }
        ForEachItemIn(i, other->targets)
        {
            targets.append(OLINK(other->targets.item(i)));
            sources.append(OLINK(other->sources.item(i)));
        }
    }
    void intersectMapping(const HqlConstantPercolator * other)
    {
        assertex(other);
        ForEachItemInRev(i, targets)
        {
            unsigned match = other->targets.find(targets.item(i));
            if ((match == NotFound) || !expressionsEquivalent(&sources.item(i), &other->sources.item(match)))
            {
                sources.remove(i);
                targets.remove(i);
            }
        }
    }

    IHqlExpression * querySelf() { return self; }

    IHqlExpression * resolveField(IHqlExpression * search)
    {
        ForEachItemIn(i, targets)
        {
            IHqlExpression & cur = targets.item(i);
            if ((cur.queryChild(1) == search) && (cur.queryChild(0) == self))
                return &sources.item(i);
        }
        return NULL;
    }

    static HqlConstantPercolator * extractConstantMapping(IHqlExpression * transform)
    {
        if (!isKnownTransform(transform))
            return NULL;

        Owned<HqlConstantPercolator> mapping = new HqlConstantPercolator;
        mapping->extractConstantTransform(transform);
        if (mapping->empty())
            return NULL;
        return mapping.getClear();
    }

    static HqlConstantPercolator * extractNullMapping(IHqlExpression * record)
    {
        Owned<HqlConstantPercolator> mapping = new HqlConstantPercolator;
        mapping->extractNullTransform(record);
        if (mapping->empty())
            return NULL;
        return mapping.getClear();
    }

protected:
    void addMapping(IHqlExpression * select, IHqlExpression * expr)
    {
        assertex(select->getOperator() == no_select);
        targets.append(*LINK(select));
        sources.append(*LINK(expr));
//      if (select->isDatarow() && !expr->isDatarow())
    }
    void addTransformMapping(IHqlExpression * tgt, IHqlExpression * src)
    {
    #ifdef _DEBUG
        assertex(tgt->getOperator() == no_select);
        IHqlExpression * sel = queryDatasetCursor(tgt->queryChild(0));
        assertex(sel == self);
    #endif
        OwnedHqlExpr castRhs = ensureExprType(src, tgt->queryType());
        addMapping(tgt, castRhs);
    }

    void doExtractConstantTransform(IHqlExpression * transform, IHqlExpression * selector);
    void expandNullRowMapping(IHqlExpression * selector, IHqlExpression * record);
    void extractConstantTransform(IHqlExpression * transform);
    void initTransformer(IHqlExpression * selector, ConstantReplacingTransformer & transformer) const;
    void extractNullTransform(IHqlExpression * record)
    {
        self.setown(getSelf(record));
        expandNullRowMapping(self, record);
    }

protected:
    OwnedHqlExpr self;
    HqlExprArray targets;
    HqlExprArray sources;
};

void HqlConstantPercolator::extractConstantTransform(IHqlExpression * transform)
{
    self.setown(getSelf(transform));
    doExtractConstantTransform(transform, NULL);
}

void HqlConstantPercolator::expandNullRowMapping(IHqlExpression * selector, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            expandNullRowMapping(selector, cur);
            break;
        case no_ifblock:
            //valid - since if protecting fields are false, the fields will also be null
            expandNullRowMapping(selector, cur->queryChild(1));
            break;
        case no_field:
            {
                OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(cur));
                try
                {
                    OwnedHqlExpr null = createNullExpr(selected);
                    addMapping(selected, null);
                }
                catch (IException * e)
                {
                    e->Release();
                }
                if (selected->isDatarow())
                    expandNullRowMapping(selected, selected->queryRecord());
                break;
            }
        }
    }
}


void HqlConstantPercolator::doExtractConstantTransform(IHqlExpression * transform, IHqlExpression * selector)
{
    unsigned max = transform->numChildren();
    for (unsigned idx = 0; idx < max; idx++)
    {
        IHqlExpression * cur = transform->queryChild(idx);
        switch (cur->getOperator())
        {
        case no_assign:
            {
                IHqlExpression * lhs = cur->queryChild(0);
                IHqlExpression * rhs = cur->queryChild(1);
                IHqlExpression * lf = lhs->queryChild(1);
                IHqlExpression * self = lhs->queryChild(0);
                assertex(self->getOperator() == no_self);

                OwnedHqlExpr selected = selector ? createSelectExpr(LINK(selector), LINK(lf)) : LINK(lhs);
                if (rhs->isConstant())
                    addTransformMapping(selected, rhs);
                if (lhs->isDatarow())
                {
                    if (rhs->getOperator() == no_null)
                        expandNullRowMapping(selected, selected->queryRecord());
                    else if (rhs->getOperator() == no_createrow)
                        doExtractConstantTransform(rhs->queryChild(0), selected);
                }
            }
            break;
        case no_assignall:
            doExtractConstantTransform(cur, selector);
            break;
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
        case no_alias_scope:
        case no_skip:
        case no_assert:
            break;
        default:
            assertex(!"Transforms should only contain assignments");
            break;
        }
    }
}

void HqlConstantPercolator::initTransformer(IHqlExpression * selector, ConstantReplacingTransformer & transformer) const
{
    ForEachItemIn(i, sources)
    {
        OwnedHqlExpr value = replaceSelector(&targets.item(i), self, selector);
        transformer.setMapping(value, &sources.item(i));
    }
}

IHqlExpression * CExprFolderTransformer::doFoldTransformed(IHqlExpression * unfolded, IHqlExpression * original)
{   
    IHqlExpression * nullFolded = foldNullDataset(unfolded);
    if (nullFolded)
        return nullFolded;

#if 0
    IHqlExpression * body = unfolded->queryBody();
    OwnedHqlExpr expr = foldConstantOperator(body, foldOptions, templateContext);
    if ((unfolded != body) && !expr->isAnnotation() && !expr->queryValue())
        expr.setown(unfolded->cloneAllAnnotations(expr));
#else
    OwnedHqlExpr expr = foldConstantOperator(unfolded, foldOptions, templateContext);
#endif

    node_operator op = expr->getOperator();
    switch (op)
    {
    //Scalar operators that are not handled in foldConstantOperator()
    case no_or:
        return foldOrExpr(expr, (foldOptions & HFOx_op_not_x) != 0);
    case no_and:
        return foldAndExpr(expr, (foldOptions & HFOx_op_not_x) != 0);
    //Operations that involve constant folding on datasets.
    case no_normalize:
        {
            // Identify expressions
            IHqlExpression * ds = expr->queryChild(0);
            IHqlExpression * count = expr->queryChild(1);
            IHqlExpression * transform = expr->queryChild(2);
            OwnedHqlExpr left = createSelector(no_left, ds, querySelSeq(expr));
            if (!hasSingleRow(ds) || exprReferencesDataset(count, left)) // Complicate things more
                break;

            // Replace LEFT from normalize transform (if used) by ROW's contents
            OwnedHqlExpr newTransform;
            if (exprReferencesDataset(transform, left)) {
                OwnedHqlExpr newRow;
                // Make sure it's one of the recognised formats
                switch (ds->getOperator())
                {
                case no_datasetfromrow: // DATASET(ROW(transform))
                    {
                        IHqlExpression * row = ds->queryChild(0);
                        if (row->getOperator() == no_createrow)
                            newRow.set(row);
                        break;
                    }
                case no_inlinetable:    // DATASET([transform()]) or DATASET([value],{ myfield })
                    {
                        IHqlExpression * transformList = ds->queryChild(0);
                        assertex(transformList->getOperator() == no_transformlist);
                        newRow.setown(createRow(no_createrow, LINK(transformList->queryChild(0))));
                        break;
                    }
                }
                if (!newRow)
                    break;

                //Instead of evaluating once newRow will be evaluated multiple times.  Is that ok (e.g., volatile)
                if (!canDuplicateActivity(newRow))
                    break;

                OwnedHqlExpr replacementRow = createRow(no_newrow, LINK(newRow));
                newTransform.setown(replaceSelector(transform, left, replacementRow));
            }
            HqlExprArray args;
            unwindChildren(args, expr, 1); // (count, trans)
            if (newTransform)
                args.replace(*newTransform.getClear(), 1);
            removeAttribute(args, _selectorSequence_Atom);

            return createDataset(no_dataset_from_transform, args);
        }
    case no_filter:
        {
            IHqlExpression * child = expr->queryChild(0);
            HqlExprArray args;
            args.append(*LINK(child));
            unsigned num = expr->numChildren();
            for (unsigned idx = 1; idx < num; idx++)
            {
                IHqlExpression * cur = expr->queryChild(idx);
                IValue * value = cur->queryValue();
                if (value)
                {
                    if (!value->getBoolValue())
                        return createNullDataset(child);
                }
                else
                    args.append(*LINK(cur));
            }

            if (args.ordinality() == 1)
                return removeParentNode(expr);

            //Fold filter conditions with previous projects to see if they are always true, or always false.
            //Similar code also appears in the optimizer...
            switch (child->getOperator())
            {
            case no_newusertable:
            case no_hqlproject:
                if ((foldOptions & HFOfoldfilterproject) && !(foldOptions & HFOpercolateconstants))
                {
                // Following are possibilities, but aren't worth the extra cycles....
                //case no_join:
                //case no_iterate:
                //case no_denormalize:
                //case no_normalize:
                //case no_selfjoin:
                    if (!isAggregateDataset(child) && (args.ordinality() > 1))
                    {
                        NewProjectMapper2 mapper;
                        mapper.setMapping(queryNewColumnProvider(child));

                        //Iterate all but last 
                        for (unsigned i = args.ordinality(); --i != 0; )
                        {
                            IHqlExpression * cur = &args.item(i);
                            OwnedHqlExpr expandedFilter = mapper.expandFields(cur, child, NULL, NULL);

                            if (expandedFilter->isConstant())
                            {
                                //Following would be sensible, but can't call transform at this point, so replace arg, and wait for it to re-iterate
                                IIdAtom * nameF = expr->queryId();
                                IIdAtom * nameP = child->queryId();
                                DBGLOG("Folder: Combining FILTER %s with %s %s produces constant filter", nameF ? str(nameF) : "", getOpString(child->getOperator()), nameP ? str(nameP) : "");
                                expandedFilter.setown(transformExpanded(expandedFilter));
                                IValue * value = expandedFilter->queryValue();
                                if (value)
                                {
                                    if (!value->getBoolValue())
                                        return replaceWithNull(expr);
                                    args.remove(i);
                                }
                                else
                                    args.replace(*LINK(expandedFilter), i);
                            }
                        }
                    }
                }
                break;
            case no_inlinetable:
                if (foldOptions & HFOconstantdatasets)
                {
                    OwnedITypeInfo boolType = makeBoolType();
                    OwnedHqlExpr filterCondition = createBalanced(no_and, boolType, args, 1, args.ordinality());

                    HqlExprArray filtered;
                    bool allFilteredOk = true;
                    IHqlExpression * values = child->queryChild(0);
                    ForEachChild(i, values)
                    {
                        IHqlExpression * curTransform = values->queryChild(i);
                        NewProjectMapper2 mapper;
                        mapper.setMapping(curTransform);
                        OwnedHqlExpr expandedFilter = mapper.expandFields(filterCondition, child, NULL, NULL);
                        OwnedHqlExpr folded = transformExpanded(expandedFilter);
                        IValue * value = folded->queryValue();

                        if (value)
                        {
                            if (value->getBoolValue())
                                filtered.append(*LINK(curTransform));
                        }
                        else
                        {
                            allFilteredOk = false;
                            break;
                        }
                    }
                    if (allFilteredOk)
                    {
                        if (filtered.ordinality() == 0)
                            return replaceWithNull(expr);
                        if (filtered.ordinality() == values->numChildren())
                            return removeParentNode(expr);
                        StringBuffer s1, s2;
                        DBGLOG("Folder: Node %s reduce values in child: %s from %d to %d", queryChildNodeTraceText(s1, expr), queryChildNodeTraceText(s2, child), values->numChildren(), filtered.ordinality());
                        HqlExprArray args;
                        args.append(*values->clone(filtered));
                        unwindChildren(args, child, 1);
                        return child->clone(args);
                    }
                }
                break;
            }

            if (args.ordinality() == 1)
                return removeParentNode(expr);
            return cloneOrLink(expr, args);
        }
    case no_newaggregate:
        {
            //Duplicated in constant folder and optimizer
            IHqlExpression * child = expr->queryChild(0);
            node_operator childOp = child->getOperator();
            IHqlExpression * ret = NULL;
            switch (childOp)
            {
            case no_inlinetable:
                if ((foldOptions & HFOconstantdatasets) && isNoSkipInlineDataset(child))
                    ret = queryOptimizeAggregateInline(expr, child->queryChild(0)->numChildren());
                break;
            default:
                if ((foldOptions & HFOconstantdatasets) && hasSingleRow(child))
                    ret = queryOptimizeAggregateInline(expr, 1);
                break;
            }
            if (ret)
                return ret;
            break;
        }
    case no_count:
        {
            IHqlExpression * child = expr->queryChild(0);
            node_operator childOp = child->getOperator();
            switch (childOp)
            {
            case no_inlinetable:
                if (isNoSkipInlineDataset(child))
                    return createConstant(expr->queryType()->castFrom(false, (__int64)child->queryChild(0)->numChildren()));
                break;
            case no_null:
                return createNullValue(expr);
#if 0
            // Enabling this generally makes code worse because of count(file), count(x) > n, and extra hoisting.
            case no_addfiles:
                {
                    OwnedHqlExpr lhs = replaceChild(expr, 0, child->queryChild(0));
                    OwnedHqlExpr rhs = replaceChild(expr, 0, child->queryChild(1));
                    return createValue(no_add, expr->getType(), LINK(lhs), LINK(rhs));
                }
            case no_if:
                {
                    OwnedHqlExpr lhs = replaceChild(expr, 0, child->queryChild(1));
                    OwnedHqlExpr rhs = replaceChild(expr, 0, child->queryChild(2));
                    return createValue(no_if, expr->getType(), LINK(child->queryChild(0)), LINK(lhs), LINK(rhs));
                }
#endif
            default:
                if (hasSingleRow(child))
                    return createConstant(expr->queryType()->castFrom(false, I64C(1)));
                break;
            }
            break;
        }
    case no_exists:
        {
            IHqlExpression * child = expr->queryChild(0);
            node_operator childOp = child->getOperator();
            switch (childOp)
            {
            case no_inlinetable:
                if (isNoSkipInlineDataset(child))
                {
                    bool hasChildren = (child->queryChild(0)->numChildren() != 0);
                    return createConstant(hasChildren);
                }
                break;
#if 0
            case no_addfiles:
                {
                    OwnedHqlExpr lhs = replaceChild(expr, 0, child->queryChild(0));
                    OwnedHqlExpr rhs = replaceChild(expr, 0, child->queryChild(1));
                    return createValue(no_or, expr->getType(), LINK(lhs), LINK(rhs));
                }
            case no_if:
                {
                    OwnedHqlExpr lhs = replaceChild(expr, 0, child->queryChild(1));
                    OwnedHqlExpr rhs = replaceChild(expr, 0, child->queryChild(2));
                    return createValue(no_if, expr->getType(), LINK(child->queryChild(0)), LINK(lhs), LINK(rhs));
                }
#endif
            case no_null:
                return createConstant(op != no_exists);
            default:
                if (hasSingleRow(child))
                    return createConstant(op == no_exists);
                break;
            }
            break;
        }
    case no_within:
    case no_sum:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            if (dataset->getOperator() == no_null)
                return createNullValue(expr);
            if (op == no_sum && dataset->getOperator() == no_addfiles)
            {
                IHqlExpression * arg = expr->queryChild(1);
                IHqlExpression * addLeft = dataset->queryChild(0);
                IHqlExpression * addRight = dataset->queryChild(1);
                OwnedHqlExpr sumLeft = createValue(op, expr->getType(), LINK(addLeft), replaceSelector(arg, dataset, addLeft));
                OwnedHqlExpr sumRight = createValue(op, expr->getType(), LINK(addRight), replaceSelector(arg, dataset, addRight));
                return createValue(no_add, expr->getType(), LINK(sumLeft), LINK(sumRight));
            }
        }
        break;
#if 0
    //Following are not enabled because they have a strange side-effect of stopping the dataset being hoisted
    //since an inline dataset (which is hoisted) is converted to a no_list, which isn't.
    //If the key segment monitors for IN were improved then it may be worth 
    case no_createset:
        {
            IHqlExpression * child = expr->queryChild(0);
            switch (child->getOperator())
            {
            case no_inlinetable:
                {
                    IHqlExpression * select = expr->queryChild(1);
                    //check a simple select from the dataset
                    if ((select->getOperator() == no_select) && (select->queryChild(0) == child->queryNormalizedSelector()))
                    {
                        HqlExprArray args;
                        bool ok = true;
                        IHqlExpression * transforms = child->queryChild(0);
                        IHqlExpression * field = select->queryChild(1);
                        ForEachChild(i, transforms)
                        {
                            IHqlExpression * cur = transforms->queryChild(i);
                            if (!cur->isPure() || containsSkip(cur))
                            {
                                ok = false;
                                break;
                            }
                            IHqlExpression * match = getExtractSelect(cur, field, false);
                            if (!match)
                            {
                                ok = false;
                                break;
                            }
                            args.append(*match);
                        }
                        if (ok)
                            return createValue(no_list, expr->getType(), args);
                    }
                    break;
                }
            case no_temptable:
                {
                    IHqlExpression * list = child->queryChild(0);
                    if (list->getOperator() == no_list)
                        return ensureExprType(list, expr->queryType());
                    break;
                }
            }
            break;
        }
#endif
    case no_select:
        {
            //Don't fold dataset references that are in scope, 
            //otherwise the dataset will fail to match...
            if (expr->hasAttribute(newAtom))
            {
                IHqlExpression * left = expr->queryChild(0);
                switch (left->getOperator())
                {
                case no_null:
                    return createNullExpr(expr);
                case no_datasetfromrow:
                    if (left->queryChild(0)->getOperator() == no_null)
                        return createNullExpr(expr);
                    break;
                case no_createrow:
#if 1
                    if (!expr->isDataset() && !expr->isDatarow())
                    {
                        OwnedHqlExpr  match = getExtractSelect(left->queryChild(0), expr->queryChild(1), false);
                        if (match && match->isConstant())
                            return match.getClear();
                    }
#else
                    //This generates better code most of the time, but causes worse code for a few examples
                    //e.g., bug74112 cmorton47 ncferr
                    //Should enable once I've had time to investigate
                    if (!expr->isDataset())// && !expr->isDatarow())
                    {
                        OwnedHqlExpr  match = getExtractSelect(left->queryChild(0), expr->queryChild(1), false);
                        if (match)// && match->isConstant())
                            return match.getClear();
                    }
#endif
                    break;
                case no_selectnth:
                    if (foldOptions & HFOpercolateconstants)
                    {
                        IHqlExpression * ds = left->queryChild(0);
                        IHqlExpression * elem = left->queryChild(1);
                        if ((ds->getOperator() == no_inlinetable) && elem->queryValue())
                        {
                            __int64 idx = elem->queryValue()->getIntValue() - 1;
                            IHqlExpression * transforms = ds->queryChild(0);
                            if (idx >= 0 && idx < transforms->numChildren())
                            {
                                IHqlExpression * transform = transforms->queryChild((unsigned)idx);
                                HqlConstantPercolator * mapping = gatherConstants(transform);
                                if (mapping)
                                {
                                    IHqlExpression * resolved = mapping->resolveField(expr->queryChild(1));
                                    if (resolved)
                                        return LINK(resolved);
                                }
                            }
                            else
                                return createNullExpr(expr);
                        }
                    }
                }
            }
            // default is to call transformSelector() if not new
            break;
        }
    case no_sample:
        {
            IHqlExpression * limit = expr->queryChild(1);
            IValue * value = limit->queryValue();
            if (value && (value->getIntValue() == 1))
                return removeParentNode(expr);
            break;
        }
/*
    case no_alias_scope:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (child->queryValue())
                return LINK(child);
            break;
        }
*/
/*
        no_table,
        no_temptable,

        no_pipe:
        no_fetch,
        no_join,
        no_joined,
*/
    case no_usertable:
        //These should have been removed by the time we get called, but can very occasionally occur 
        //if it is called in the parser (e.g., when folding a filename)
        break;
    case no_selectfields:
        {
            //These should have been removed by the time we get called.
            //This is sometimes added by the SQL generator, but it shouldn't perform a project
            assertex(expr->queryChild(1)->getOperator() == no_null);
            IHqlExpression * dataset = expr->queryChild(0);
            if (isNull(dataset))
                return LINK(dataset);
            break;
        }
    case no_compound:
        if (foldOptions & HFOforcefold)
            return LINK(expr->queryChild(1));
        break;
    case no_hqlproject:
        if (expr != original)
        {
            //Could have removed whether or not somethin needs to be a count project
            IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
            if (counter && !transformContainsCounter(expr->queryChild(1), counter))
                return removeAttribute(expr, _countProject_Atom);
        }
        break;
    case no_temptable:
        {
            if (expr->queryChild(0)->getOperator() == no_list)
            {
                ECLlocation dummyLocation(0, 0, 0, NULL);
                OwnedHqlExpr inlineTable = convertTempTableToInlineTable(errorProcessor, dummyLocation, expr);
                if (expr != inlineTable)
                    return inlineTable.getClear();
            }
            break;
        }
    case no_assert:
        if (getBoolValue(expr->queryChild(0), false))
            return createValue(no_null, makeVoidType());
        break;
    case no_sequential:
    case no_parallel:
    case no_orderedactionlist:
        if (expr->numChildren() == 1)
        {
            if (expr->queryChild(0)->isAttribute())
                return createValue(no_null, makeVoidType());
            return removeParentNode(expr);
        }
        break;
    case no_likely:
    case no_unlikely:
        {
            IHqlExpression * child = expr->queryChild(0);
            switch(child->getOperator())
            {
            case no_likely:
            case no_unlikely:
                return replaceChild(expr, 0, child->queryChild(0));
            case no_constant:
                return LINK(child);
            }
            break;
        }
    }

    return LINK(expr);
}

//---------------------------------------------------------------------------

FolderTransformInfo::~FolderTransformInfo()
{
    ::Release(mapping);
}

static HqlTransformerInfo cExprFolderTransformerInfo("CExprFolderTransformer");
CExprFolderTransformer::CExprFolderTransformer(IErrorReceiver & _errorProcessor, ITemplateContext * _templateContext, unsigned _options)
: NewHqlTransformer(cExprFolderTransformerInfo), templateContext(_templateContext), errorProcessor(_errorProcessor)
{
    foldOptions = _options;
}


IHqlExpression * CExprFolderTransformer::createTransformedAnnotation(IHqlExpression * expr)
{   
    return CExprFolderTransformer::createTransformed(expr);
}


ANewTransformInfo * CExprFolderTransformer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(FolderTransformInfo, expr);
}


IHqlExpression * createListMatchStructure(node_operator op, IHqlExpression * expr, const HqlExprArray & args, unsigned & idx)
{
    if (expr->getOperator() != op)
        return &OLINK(args.item(idx++));
    IHqlExpression * lhs = expr->queryChild(0);
    IHqlExpression * rhs = expr->queryChild(1);
    OwnedHqlExpr newLhs = createListMatchStructure(op, lhs, args, idx);
    OwnedHqlExpr newRhs = createListMatchStructure(op, rhs, args, idx);
    if ((lhs == newLhs) && (rhs == newRhs))
        return LINK(expr);

    if (op == no_and)
    {
        if (matchesBoolean(newLhs, true))
            return newRhs.getClear();
        if (matchesBoolean(newRhs, true))
            return newLhs.getClear();
    }
    else
    {
        if (matchesBoolean(newLhs, false))
            return newRhs.getClear();
        if (matchesBoolean(newRhs, false))
            return newLhs.getClear();
    }

    OwnedHqlExpr value = createValue(op, expr->getType(), newLhs.getClear(), newRhs.getClear());
    return expr->cloneAllAnnotations(value);
}

IHqlExpression * createListMatchStructure(node_operator op, IHqlExpression * expr, const HqlExprArray & args)
{
    unsigned idx = 0;
    OwnedHqlExpr ret = createListMatchStructure(op, expr, args, idx);
    assertex(idx == args.ordinality());
    return ret.getClear();
}


//dedup and rollup need to be very careful substituting constants for the rollup conditions.
//If the original condition is something like right.combine, which is then replaced with false
//it would then be processed as an equality false=false condition, causing extra dedups.
//If it is substitued with true it will behave the same, so don't do anything.
static IHqlExpression * preserveRollupConditions(IHqlExpression * update, IHqlExpression * original, unsigned from, unsigned to)
{
    HqlExprArray args;
    unwindChildren(args, update);

    bool same = true;
    for (unsigned i=from; i < to; i++)
    {
        IHqlExpression * cur = &args.item(i);
        IHqlExpression * curOriginal = original->queryChild(i);
        LinkedHqlExpr mapped;
        if (cur->queryValue() && cur->queryType()->getTypeCode() == type_boolean && (cur->queryValue()->getBoolValue() == false))
        {
            if (!curOriginal->queryValue())
                mapped.set(curOriginal);
        }
        if (mapped && (cur != mapped))
        {
            args.replace(*mapped.getClear(), i);
            same = false;
        }
    }
    if (same)
        return LINK(update);
    return update->clone(args);
}


static IHqlExpression * stripDedupConditions(IHqlExpression * update, IHqlExpression * original)
{
    HqlExprArray args;
    unwindChildren(args, update);

    unsigned max = args.ordinality();
    bool same = true;
    bool hadCriteria = false;
    for (unsigned i=max-1; i != 0; i--)
    {
        IHqlExpression * cur = &args.item(i);
        LinkedHqlExpr mapped;
        switch (cur->getOperator())
        {
        case no_left:
        case no_right:
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
        case no_record:
            break;
        default:
            if (cur->isConstant())
            {
                args.remove(i);
                same = false;
            }
            else
                hadCriteria = true;
            break;
        }
    }
    if (same)
        return LINK(update);
    if (!hadCriteria)
        args.add(*createConstant(true), 1);
    return update->clone(args);
}



IHqlExpression * CExprFolderTransformer::percolateConstants(IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);
    LinkedHqlExpr updated = expr;
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_iterate:
        //only sustitute for right, left contains rolled up record.
        updated.setown(percolateConstants(updated, child, no_right));
        //could call gatherconstants() on updated, and then substitude for no_left
        //But unlikely to matching anything that was constant, and if it was, the subsitution may well be wrong.
        break;
    case no_rollup:
        {
            //only sustitute for right, left contains rolled up record.
            updated.setown(percolateConstants(updated, child, no_right));

            //If any assignments of the form
            //self.x := left.x then the constant can be percolated from the input, but 
            //self.x := left.x + 1 would cause invalid results.
            //updated.setown(percolateRollupInvariantConstants(updated, child, no_left));

            //changing the grouping conditions may confuse the code generator exactly what kind of dedup is going on,
            //so preserve any that would cause complications..
            OwnedHqlExpr fixed = preserveRollupConditions(updated, expr, 1, updated->numChildren());
            if (fixed != updated)
            {
                //If these are false then we should be able to turn the rollup into a project
                updated.set(fixed);
            }
        }
        break;
    case no_dedup:
        {
            //Need to be very careful about dedup criteria
            updated.setown(percolateConstants(updated, child, no_left));
            updated.setown(percolateConstants(updated, child, no_right));

            //Check if any conditions are now always false - that weren't before
            //if so it means the condition is always false, => will never dedup, => can just use the input
            OwnedHqlExpr fixed = preserveRollupConditions(updated, expr, 1, updated->numChildren());
            if (fixed != updated)
                return removeParentNode(expr);

            updated.setown(percolateConstants(updated, child, no_none));
            //any conditions just made constant, can be removed, if there are no conditions left then we'll add,true
            updated.setown(stripDedupConditions(updated, fixed));
            //updated.setown(preserveRollupConditions(updated, expr, 1, updated->numChildren()));
        }
        break;
    case no_selfjoin:
    case no_join:
    case no_denormalize:
    case no_denormalizegroup:
        {
            //The constants can only be percolated into the transform if certain conditions are met,
            //However they can always be percolated into the join condition... (test separately)
            IAtom * joinKind = queryJoinKind(expr);
            IHqlExpression * rhs = expr->queryChild(1);
            IHqlExpression * oldCond = updated->queryChild(2);
            IHqlExpression * atmost = updated->queryAttribute(atmostAtom);
            switch (op)
            {
            case no_denormalize:
                {
                    //Nasty: left is repeated, and likely to be left outer => only replace join condition
                    updated.setown(percolateConstants(updated, child, no_left, 2));
                    updated.setown(percolateConstants(updated, rhs, no_right, 2));
                    break;
                }
            case no_denormalizegroup:
                {
                    if ((joinKind == innerAtom) || (joinKind == leftonlyAtom) || (joinKind == leftouterAtom))
                        updated.setown(percolateConstants(updated, child, no_left));
                    else
                        updated.setown(percolateConstants(updated, child, no_left, 2));
                    updated.setown(percolateConstants(updated, rhs, no_right, 2));
                    break;
                }
            case no_selfjoin:
                rhs = child;    
                // fallthrough
            case no_join:
                {
                    if (joinKind == innerAtom)
                    {
                        updated.setown(percolateConstants(updated, child, no_left));
                        updated.setown(percolateConstants(updated, rhs, no_right));
                    }
                    else if ((joinKind == leftonlyAtom) || (joinKind == leftouterAtom))
                    {
                        updated.setown(percolateConstants(updated, child, no_left));
                        updated.setown(percolateConstants(updated, rhs, no_right, 2));
                        //MORE: Could also replace intersection of rhs constants with a NULL row
                    }
                    else if ((joinKind == rightonlyAtom) || (joinKind == rightouterAtom))
                    {
                        updated.setown(percolateConstants(updated, child, no_left, 2));
                        updated.setown(percolateConstants(updated, rhs, no_right));
                    }
                    else
                    {
                        updated.setown(percolateConstants(updated, child, no_left, 2));
                        updated.setown(percolateConstants(updated, rhs, no_right, 2));
                    }
                    break;
                }
            }

            //If we've turned a fake all join into a join(true), then add an all attribute
            IHqlExpression * updatedCond = updated->queryChild(2);
            if (updatedCond != oldCond)
            {
                //At most is too complicated - either remove the atmost, or restore the join condition, and old atmost
                if (atmost)
                {
                    if (matchesBoolean(updatedCond, false))
                        updated.setown(removeAttribute(updated, atmostAtom));
                    else
                    {
                        //KEYED joins support ATMOST and RIGHT.xxx = value
                        if (!isKeyedJoin(updated) && joinHasRightOnlyHardMatch(updated, false))
                        {
                            HqlExprArray args;
                            unwindChildren(args, updated);
                            args.replace(*LINK(oldCond), 2);
                            removeAttribute(args, atmostAtom);
                            args.append(*LINK(atmost));
                            updated.setown(updated->clone(args));
                        }
                        else
                            updated.setown(appendOwnedOperand(updated, createAttribute(_conditionFolded_Atom)));
                    }
                }
                //otherwise this might convert to an all join, accept variants that are supported by all joins
                //We don't currently have a self-join all.  Would possibly be a good idea...
                else if ((joinKind == innerAtom || joinKind == leftouterAtom || joinKind == leftonlyAtom) && (op != no_selfjoin))
                    updated.setown(appendOwnedOperand(updated, createAttribute(_conditionFolded_Atom)));
                else
                {
                    //check there is still some kind of join condition left
                    IHqlExpression * selSeq = querySelSeq(updated);
                    IHqlExpression * updatedLhs = updated->queryChild(0);
                    IHqlExpression * updatedRhs = (op == no_selfjoin) ? updatedLhs : updated->queryChild(1);
                    JoinSortInfo joinInfo(updatedCond, updatedLhs, updatedRhs, selSeq, atmost);
                    joinInfo.findJoinSortOrders(false);

                    //if will convert to an all join, then restore the old condition,
                    if (!joinInfo.hasRequiredEqualities())
                        updated.setown(replaceChild(updated, 2, oldCond));
                    else
                        updated.setown(appendOwnedOperand(updated, createAttribute(_conditionFolded_Atom)));
                }
            }
            break;
        }
    case no_process:
        {
            //only substitute left; right contains iterated values
            updated.setown(percolateConstants(updated, child, no_left));
            break;
        }
    case no_merge:
        {
            HqlConstantPercolator * mapping = gatherConstants(expr);
            if (mapping)
            {
                IHqlExpression * sorted = expr->queryAttribute(sortedAtom);
                assertex(sorted);
                OwnedHqlExpr newSorted = percolateConstants(mapping, sorted, child, no_activetable);
                updated.setown(replaceOwnedAttribute(updated, newSorted.getClear()));
            }
            break;
        }
    case no_loop:
    case no_graphloop:
    case no_keyeddistribute:
        //Safer to do nothing...
        break;
    case no_select:
        {
            //E.g., Substitute ds[1].x if x is fixed.  Useful in addition to the inline[n].x below
            //Useful in its own right, but latestcompreport.xhql required it because it was using it as a highly
            //unusual guard condition.
            //MORE: This needs more work to allow ds[1].x.y to be substituted, but that is very unusual
            //simplest would be to recurse, build up for a list of fields, and then pass to resolveFields()
            if (expr->hasAttribute(newAtom))
            {
                IHqlExpression * field = expr->queryChild(1);
                OwnedHqlExpr transformedDs = transform(expr->queryChild(0));
                HqlConstantPercolator * mapping = gatherConstants(transformedDs);
                if (mapping)
                {
                    IHqlExpression * resolved = mapping->resolveField(field);
                    if (resolved)
                        updated.set(resolved);
                }
            }
            break;
        }
        break;
    default:
        {
            childDatasetType type = getChildDatasetType(expr);
            switch (type)
            {
            case childdataset_none: 
            case childdataset_nway_left_right:
            case childdataset_many_noscope:
            case childdataset_many:
            case childdataset_if:
            case childdataset_case:
            case childdataset_map:
            case childdataset_evaluate:
                break;
            case childdataset_dataset_noscope:
            case childdataset_dataset:
                updated.setown(percolateConstants(updated, child, no_none));
                break;
            case childdataset_datasetleft: 
                updated.setown(percolateConstants(updated, child, no_none));
                updated.setown(percolateConstants(updated, child, no_left));
                break;
            case childdataset_left: 
                updated.setown(percolateConstants(updated, child, no_left));
                break;
            case childdataset_same_left_right:
                updated.setown(percolateConstants(updated, child, no_left));
                updated.setown(percolateConstants(updated, child, no_right));
                break;
            case childdataset_top_left_right:
                updated.setown(percolateConstants(updated, child, no_none));
                updated.setown(percolateConstants(updated, child, no_left));
                updated.setown(percolateConstants(updated, child, no_right));
                break;
            case childdataset_leftright: 
                updated.setown(percolateConstants(updated, child, no_left));
                updated.setown(percolateConstants(updated, expr->queryChild(1), no_right));
                break;
            default:
                UNIMPLEMENTED;
            }
            break;
        }
    }
    return updated.getClear();
}

IHqlExpression * CExprFolderTransformer::createTransformed(IHqlExpression * expr)
{   
    if (foldOptions & HFOloseannotations)
        expr = expr->queryBody();

    //Special cases which don't want children to be transformed first...
    OwnedHqlExpr dft;
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_alias:
        {
            OwnedHqlExpr folded = transform(expr->queryChild(0));
            if (folded->getOperator() == no_alias || folded->queryValue())
                return folded.getClear();
            break;
        }
    case no_or:
    case no_and:
        {
            //Transform all children that do not match the operator - otherwise we get an n^2 (or n^3) algorithm
            HqlExprArray args, transformedArgs;
            expr->unwindList(args, op);
            bool same = true;
            ForEachItemIn(i, args)
            {
                IHqlExpression * cur = &args.item(i);
                IHqlExpression * t = transform(cur);
                transformedArgs.append(*t);
                if (t != cur)
                    same = false;

                //Sort circuit always-true or always-false early to avoid subsequent transforms..
                IValue * value = t->queryValue();
                if (value)
                {
                    if (value->getBoolValue())
                    {
                        if (op == no_or)
                            return LINK(t);
                    }
                    else
                    {
                        if (op == no_and)
                            return LINK(t);
                    }
                }

            }
            if (same)
                dft.set(expr);
            else
            {
                //Need to preserve the no_and/no_or structure so that cse/alias opportunities aren't lost.
                //e.g.  x := a and b;    if (d and x and c, f(x), ...)
                dft.setown(createListMatchStructure(op, expr, transformedArgs));
            }
            break;
        }
    case no_if:
        {
            //transform this early - to short circuit lots of other work...
            OwnedHqlExpr child = transform(expr->queryChild(0));
            IValue * constValue = child->queryValue();
            if (constValue)
            {
                unsigned idx = constValue->getBoolValue() ? 1 : 2;
                IHqlExpression * branch = expr->queryChild(idx);
                OwnedHqlExpr ret;
                if (!branch)
                {
                    assertex(expr->isAction());
                    ret.setown(createValue(no_null, makeVoidType()));
                }
                else
                    ret.setown(transform(branch));

                if (hasNamedSymbol(ret))
                    return ret.getClear();
                return expr->cloneAllAnnotations(ret);
            }
            break;
        }
    case no_case:
        {
            OwnedHqlExpr leftExpr = transform(expr->queryChild(0));
            IValue * leftValue = leftExpr->queryValue();
            if (leftValue)
            {
                unsigned numCases = expr->numChildren()-2;
                IHqlExpression * result = expr->queryChild(numCases+1);
                for (unsigned idx = 1; idx <= numCases; idx++)
                {
                    IHqlExpression * map = expr->queryChild(idx);
                    OwnedHqlExpr grand = transform(map->queryChild(0));
                    IValue * grandValue = grand->queryValue();
                    if (grandValue)
                    {
                        if (orderValues(leftValue, grandValue) == 0)
                        {
                            result = map->queryChild(1);
                            break;
                        }
                    }
                    else
                    {
                        result = NULL;
                        break;
                    }
                }

                if (result)
                    return cloneAnnotationAndTransform(expr, result);
            }
            break;
        }
    case no_map:
        {
            unsigned num = expr->numChildren()-1;
            IHqlExpression * result = expr->queryChild(num);
            for (unsigned idx = 0; idx < num; idx++)
            {
                IHqlExpression * map = expr->queryChild(idx);
                OwnedHqlExpr cond = transform(map->queryChild(0));
                IValue * value = cond->queryValue();
                if (value)
                {
                    if (value->getBoolValue())
                    {
                        result = map->queryChild(1);
                        break;
                    }
                }
                else
                {
                    result = NULL;
                    break;
                }
            }

            if (result)
                return cloneAnnotationAndTransform(expr, result);
            break;
        }
    case no_call:
        {
            //Ensure the bodies of out of line function calls are also folded.
            IHqlExpression * funcDef = expr->queryDefinition();
            Owned<IHqlExpression> newFuncDef = transform(funcDef);
            if (funcDef != newFuncDef)
            {
                HqlExprArray children;
                transformChildren(expr, children);
                dft.setown(createReboundFunction(newFuncDef, children));
            }
            break;
        }
    }

    if (!dft)
        dft.setown(PARENT::createTransformed(expr));

    //If the parent has changed to no_null, then the active selector may have changed out of step with the parent dataset
    //so need to explcitly remap the dataset
    updateOrphanedSelectors(dft, expr);

    OwnedHqlExpr updated = (foldOptions & HFOpercolateconstants) ? percolateConstants(dft) : LINK(dft);
    OwnedHqlExpr transformed = doFoldTransformed(updated, expr);

#ifdef LOG_ALL_FOLDING
    if ((op != transformed->getOperator()) || (expr->numChildren() != transformed->numChildren()))
        DBGLOG("Folding %s to %s", getOpString(updated->getOperator()), getOpString(transformed->getOperator()));
#endif

#ifdef _DEBUG
    if (expr->isConstant() && !transformed->queryValue())
    {
        ITypeInfo * type = expr->queryType();
        if (type && type->isScalar())
        {
            switch (op)
            {
            case no_none: 
            case no_mapto: 
            case no_negate:
            case no_comma:
            case no_assign:
            case no_assignall:
            case no_transform:
            case no_newtransform:
                break;
                /*
            case no_count:
            case no_max:
            case no_min:
            case no_sum:
            case no_exists:
            case no_ave:
                //Could implement this on a temp table, or at least count...
                //not sufficient to just fix these, because functions of these also fail.
                break;
                */
#if 0
            default:
                {
                    DBGLOG("Error - expression is marked as constant but did not fold");
                    OwnedHqlExpr again = doFoldTransformed(dft, expr);
                    StringBuffer s;
                    expr->toString(s);
                    DBGLOG("%s", s.str());
                    throw MakeStringException(0, "Internal error - expression is marked as constant but did not fold");
                }
#endif
            }
        }
    }
#endif

    //No folding operation=>return
    if (transformed == dft)
        return transformed.getClear();

    //Just lost named symbol for some reason
    if (transformed->queryBody() == dft->queryBody())
        return LINK(dft);

    //Folded to a constant, or stripped parent node=>return
    if ((transformed->getOperator() == no_constant) || (transformed == dft->queryChild(0)))
        return transformed.getClear();

    return cloneAnnotationAndTransform(expr, transformed);
}


//Could handle NOT and OR, but generally makes it worse (it just messes up code generated for a few transforms)
//Could also handle skip attributes on transforms.
static void gatherConstantFilterMappings(HqlConstantPercolator & mappings, IHqlExpression * selector, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_and:
        gatherConstantFilterMappings(mappings, selector, expr->queryChild(0));
        gatherConstantFilterMappings(mappings, selector, expr->queryChild(1));
        break;
    case no_eq:
        {
            IHqlExpression * lhs = expr->queryChild(0);
            //MORE: Should also handle subselects now that the constant percolator does
            if ((lhs->getOperator() != no_select) || isNewSelector(lhs) || lhs->queryChild(0) != selector)
                break;

            IHqlExpression * rhs = expr->queryChild(1);
            if (!rhs->isConstant())
                break;

            OwnedHqlExpr newSelect = createSelectExpr(LINK(mappings.querySelf()), LINK(lhs->queryChild(1)));
            mappings.addEquality(newSelect, rhs);
            break;
        }
    case no_assertkeyed:
        gatherConstantFilterMappings(mappings, selector, expr->queryChild(0));
        break;
    }
}

static void gatherConstantFilterMappings(HqlConstantPercolator & mappings, IHqlExpression * expr)
{
    IHqlExpression * selector = expr->queryChild(0)->queryNormalizedSelector();
    ForEachChildFrom(i, expr, 1)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (!cur->isAttribute())
            gatherConstantFilterMappings(mappings, selector, cur);
    }
}


HqlConstantPercolator * CExprFolderTransformer::gatherConstants(IHqlExpression * expr)
{
    FolderTransformInfo * extra = queryBodyExtra(expr);
    if (extra->queryGatheredConstants())
        return extra->mapping;

    //gather constants for this particular activity.....
    Owned<HqlConstantPercolator> exprMapping;
    switch (expr->getOperator())
    {
    //The following can tell nothing about the values they contain
    case no_table:
    case no_anon:
    case no_pseudods:
    case no_fail:
    case no_skip:
    case no_all:
    case no_activetable:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_getresult:
    case no_rows:
    case no_internalselect:
    case no_delayedselect:
    case no_unboundselect:
    case no_libraryselect:
    case no_purevirtual:
    case no_libraryinput:
    case no_translated:
    case no_id2blob:
    case no_embedbody:
    case no_pipe:
    case no_keyindex:
    case no_newkeyindex:
    case no_colon:
    case no_keyed:
    case no_nofold:         // stop folding...
    case no_nohoist:
    case no_activerow:
    case no_newrow:
    case no_loop:
    case no_graphloop:
    case no_rowsetindex:
    case no_rowsetrange:
    case no_mergejoin:
    case no_nwaymerge:
    case no_temptable:
    case no_left:
    case no_right:
    case no_top:
    case no_externalcall:
    case no_call:
    case no_matchattr:
    case no_param:
    case no_deserialize:
    case no_serialize:
    case no_typetransfer:
    case no_fromxml:
    case no_fromjson:
    case no_httpcall:
        break;

    case no_null:
        //if (expr->isDatarow())
        //  exprMapping.setown(HqlConstantPercolator::extractNullMapping(expr->queryRecord()));
        break;
    case no_rollup:     
        {
            // transform may or may not be called, so can't just extract constants from the transform.
            // can only mark as constant if the inputDataset and the transform both assign them the same constant value
            IHqlExpression * dataset = expr->queryChild(0);
            IHqlExpression * transformExpr = queryNewColumnProvider(expr);
            OwnedHqlExpr invarientTransformExpr = percolateRollupInvariantConstants(transformExpr, dataset, no_left, querySelSeq(expr));
            exprMapping.setown(HqlConstantPercolator::extractConstantMapping(invarientTransformExpr));
            if (exprMapping)
            {
                HqlConstantPercolator * inputMapping = gatherConstants(dataset);
                if (inputMapping)
                    exprMapping->intersectMapping(inputMapping);
                else
                    exprMapping.clear();
            }
            break;
        }

    //The following get the values purely from the assocated transform - if it contains constant entires
    case no_xmlproject:
    case no_combine:
    case no_combinegroup:
    case no_process:
    case no_denormalize:
    case no_denormalizegroup:
    case no_fetch:
    case no_join:
    case no_selfjoin:
    case no_joincount:
    case no_iterate:
    case no_transformebcdic:
    case no_transformascii:
    case no_hqlproject:
    case no_normalize:
    case no_newparse:
    case no_newxmlparse:
    case no_rollupgroup:
    case no_soapcall:
    case no_newsoapcall:
    case no_soapcall_ds:
    case no_newsoapcall_ds:
    case no_parse:
    case no_xmlparse:
    case no_selectfields:
    case no_newaggregate:
    case no_newusertable:
    case no_usertable:
    case no_nwayjoin:
    case no_projectrow:
    case no_createrow:
    case no_dataset_from_transform:
    case no_quantile:
        {
            IHqlExpression * transform = queryNewColumnProvider(expr);
            exprMapping.setown(HqlConstantPercolator::extractConstantMapping(transform));
            break;
        }
    case no_aggregate:
        {
            if (expr->hasAttribute(mergeTransformAtom))
                break;
            IHqlExpression * transform = queryNewColumnProvider(expr);
            exprMapping.setown(HqlConstantPercolator::extractConstantMapping(transform));
            break;
        }

    case no_newtransform:
    case no_transform:
        exprMapping.setown(HqlConstantPercolator::extractConstantMapping(expr));
        break;

    //The following inherit the constant of the parent without modification
    case no_dedup:
    case no_group:
    case no_grouped:
    case no_assertgrouped:
    case no_distribute:
    case no_distributed:
    case no_unordered:
    case no_preservemeta:
    case no_assertdistributed:
    case no_keyeddistribute:
    case no_cosort:
    case no_sort:
    case no_subsort:
    case no_sorted:
    case no_assertsorted:
    case no_topn:
    case no_choosen:
    case no_choosesets:
    case no_enth:
    case no_sample:
    case no_alias_project:
    case no_alias_scope:
    case no_cachealias:
    case no_cloned:
    case no_globalscope:
    case no_sub:
    case no_thor:
    case no_nothor:
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
    case no_split:
    case no_spill:
    case no_readspill:
    case no_writespill:
    case no_commonspill:
    case no_throughaggregate:
    case no_limit:
    case no_keyedlimit:
    case no_compound_fetch:
    case no_preload:
    case no_alias:
    case no_assert_ds:
    case no_spillgraphresult:
    case no_forcenolocal:
    case no_allnodes:
    case no_thisnode:
    case no_forcelocal:
    case no_stepped:
    case no_cluster:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
    case no_filtergroup:
    case no_section:
    case no_sectioninput:
    case no_forcegraph:
    case no_related:
    case no_executewhen:
    case no_callsideeffect:
    case no_outofline:
    case no_owned_ds:
    case no_dataset_alias:
    case no_createdictionary:
    case no_nocombine:
    case no_likely:
    case no_unlikely:
        exprMapping.set(gatherConstants(expr->queryChild(0)));
        break;
    case no_normalizegroup:
        exprMapping.set(gatherConstants(expr->queryChild(1)));
        break;
    case no_catchds:
    case no_catch:
        //all bets are off.
        break;


    case no_selectnth:
        {
            //Careful - this can create a null row if it is out of range.
            bool inherit = false;
            if (expr->hasAttribute(noBoundCheckAtom))
                inherit = true;
            else if (matchesConstantValue(expr->queryChild(1), 1) && hasSingleRow(expr->queryChild(0)))
                inherit = true;

            if (inherit)
                exprMapping.set(gatherConstants(expr->queryChild(0)));
            break;
        }

    //The following inherit the constant of second argument
    case no_comma:
    case no_compound:
    case no_mapto:
        exprMapping.set(gatherConstants(expr->queryChild(1)));
        break;

    //Intersections of the inputs...
    case no_if:
        {
            IHqlExpression * rhs = expr->queryChild(2);
            if (!rhs)
                break;

            //fall through
        }
    case no_addfiles:
    case no_regroup:
    case no_nonempty:
    case no_case:
    case no_map:
    case no_merge:
    case no_cogroup:
    case no_chooseds:
        {
            unsigned from = getFirstActivityArgument(expr);
            unsigned max = from + getNumActivityArguments(expr);
            bool allMapped = true;
            for (unsigned i=0; i < max; i++)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if (cur->isDataset() && !gatherConstants(cur))
                {
                    allMapped = false;
                    break;
                }
            }
            if (allMapped)
            {
                for (unsigned i=0; i < max; i++)
                {
                    IHqlExpression * cur = expr->queryChild(i);
                    if (cur->isDataset())
                    {
                        HqlConstantPercolator * curMapping = gatherConstants(cur);
                        if (!exprMapping)
                        {
                            exprMapping.setown(new HqlConstantPercolator);
                            exprMapping->inheritMapping(curMapping);
                        }
                        else
                        {
                            exprMapping->intersectMapping(curMapping);
                            if (exprMapping->empty())
                                break;
                        }
                    }
                }
            }
            break;
        }

    //Now follow the special cases/
    case no_inlinetable:
        {
            IHqlExpression * transforms = expr->queryChild(0);
            unsigned numRows = transforms->numChildren();
            //MORE: Could theoretically create an intersection of the values, but not likely to be worth the processing time.
            if (numRows == 1)
                exprMapping.setown(HqlConstantPercolator::extractConstantMapping(transforms->queryChild(0)));
            break;
        }
    case no_filter:
        if (foldOptions & HFOpercolatefilters)
        {
            HqlConstantPercolator filterMappings(expr);
            gatherConstantFilterMappings(filterMappings, expr);
            
            HqlConstantPercolator * inheritedMappings = gatherConstants(expr->queryChild(0));
            if (!filterMappings.empty())
            {
                exprMapping.setown(new HqlConstantPercolator);
                if (inheritedMappings)
                    exprMapping->inheritMapping(inheritedMappings);
                exprMapping->inheritMapping(&filterMappings);
            }
            else
                exprMapping.set(inheritedMappings);
            break;
        }
        else
            exprMapping.set(gatherConstants(expr->queryChild(0)));
        break;

    case no_selectmap:
    case no_select:
    case no_record:
        break;

    default:
        if (expr->isAction())
            break;
        DBGLOG("Missing entry: %s", getOpString(expr->getOperator()));
        if (expr->isDatarow())
        {
            DBGLOG("Missing entry: %s", getOpString(expr->getOperator()));
            break;
        }
        throwUnexpectedOp(expr->getOperator());
    }
    if (exprMapping)
    {
        IHqlExpression * onFail = expr->queryAttribute(onFailAtom);
        if (onFail)
        {
            HqlConstantPercolator * onFailMapping = gatherConstants(onFail->queryChild(0));
            if (onFailMapping)
                exprMapping->intersectMapping(onFailMapping);
            else
                exprMapping.clear();
        }

        if (exprMapping && !exprMapping->empty())
            extra->mapping = exprMapping.getClear();
    }

    extra->setGatheredConstants(true);
    return extra->mapping;
}

IHqlExpression * CExprFolderTransformer::percolateConstants(HqlConstantPercolator * mapping, IHqlExpression * expr, IHqlExpression * dataset, node_operator side)
{
    OwnedHqlExpr selector = (side == no_none) ? LINK(dataset->queryNormalizedSelector()) : createSelector(side, dataset, querySelSeq(expr));
    return mapping->expandFields(expr, selector);
}

IHqlExpression * CExprFolderTransformer::percolateConstants(IHqlExpression * expr, IHqlExpression * dataset, node_operator side)
{
    OwnedHqlExpr transformedDs = transform(dataset);
    HqlConstantPercolator * mapping = gatherConstants(transformedDs);
    if (!mapping)
        return LINK(expr);

    unsigned from = getNumChildTables(expr);
    unsigned max = expr->numChildren();
    if (from >= max)
        return LINK(expr);

    HqlExprArray temp, args;
    unwindChildren(temp, expr, from);
    for (unsigned i=0; i < from;  i++)
        args.append(*LINK(expr->queryChild(i)));

    OwnedHqlExpr selector = (side == no_none) ? LINK(dataset->queryNormalizedSelector()) : createSelector(side, dataset, querySelSeq(expr));
    if (mapping->expandFields(args, temp, selector))
        return expr->clone(args);
    return LINK(expr);
}

IHqlExpression * CExprFolderTransformer::percolateRollupInvariantConstants(IHqlExpression * expr, HqlConstantPercolator * mapping, IHqlExpression * selector)
{
    switch (expr->getOperator())
    {
    case no_newtransform:
    case no_transform:
    case no_assignall:
        {
            HqlExprArray children;
            ForEachChild(i, expr)
                children.append(*percolateRollupInvariantConstants(expr->queryChild(i), mapping, selector));
            return cloneOrLink(expr, children);
        }
        break;
    case no_assign:
        {
            //If the assignment is self.x := left.x, then it is ok to percolate the constant in from input dataset.
            IHqlExpression * rhs = expr->queryChild(1);
            if (rhs->getOperator() == no_select)
            {
                if (rhs->queryChild(0) == selector)
                {
                    IHqlExpression * lhs = expr->queryChild(0);
                    IHqlExpression * field = lhs->queryChild(1);
                    if (field == rhs->queryChild(1))
                    {
                        OwnedHqlExpr mapped = mapping->expandField(field);
                        if (mapped)
                            return createAssign(LINK(lhs), mapped.getClear());
                    }
                }
            }
            break;
        }
    }
    return LINK(expr);
}

IHqlExpression * CExprFolderTransformer::percolateRollupInvariantConstants(IHqlExpression * expr, IHqlExpression * dataset, node_operator side, IHqlExpression * selSeq)
{
    HqlConstantPercolator * mapping = gatherConstants(dataset);
    if (!mapping)
        return LINK(expr);

    OwnedHqlExpr selector = (side == no_none) ? LINK(dataset->queryNormalizedSelector()) : createSelector(side, dataset, selSeq);
    return percolateRollupInvariantConstants(expr, mapping, selector);
}


//Expand a single child (used for joins)
IHqlExpression * CExprFolderTransformer::percolateConstants(IHqlExpression * expr, IHqlExpression * dataset, node_operator side, unsigned whichChild)
{
    OwnedHqlExpr transformedDs = transform(dataset);
    HqlConstantPercolator * mapping = gatherConstants(transformedDs);
    if (!mapping)
        return LINK(expr);

    OwnedHqlExpr selector = (side == no_none) ? LINK(dataset->queryNormalizedSelector()) : createSelector(side, dataset, querySelSeq(expr));
    HqlExprArray args;
    unwindChildren(args, expr);
    args.replace(*mapping->expandFields(&args.item(whichChild), selector), whichChild);
    return expr->clone(args);
}



IHqlExpression * CExprFolderTransformer::removeParentNode(IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);
    DBGLOG("Folder: Node %s remove self (now %s)", queryNode0Text(expr), queryNode1Text(child));
    return LINK(child);
}

IHqlExpression * CExprFolderTransformer::replaceWithNull(IHqlExpression * expr)
{
    IHqlExpression * ret = createNullExpr(expr);
    DBGLOG("Folder: Replace %s with %s", queryNode0Text(expr), queryNode1Text(ret));
    return ret;
}

IHqlExpression * CExprFolderTransformer::replaceWithNullRow(IHqlExpression * expr)
{
    IHqlExpression * ret = createRow(no_null, LINK(expr->queryRecord()));
    DBGLOG("Folder: Replace %s with %s", queryNode0Text(expr), queryNode1Text(ret));
    return ret;
}

IHqlExpression * CExprFolderTransformer::replaceWithNullRowDs(IHqlExpression * expr)
{
    assertex(!isGrouped(expr));
    return createDatasetFromRow(createRow(no_null, LINK(expr->queryRecord())));
}

IHqlExpression * CExprFolderTransformer::transformExpanded(IHqlExpression * expr)
{
    return transform(expr);
}


//---------------------------------------------------------------------------

IHqlExpression * foldHqlExpression(IHqlExpression * expr)
{
    NullErrorReceiver errorProcessor;
    return foldHqlExpression(errorProcessor, expr);
}

IHqlExpression * foldHqlExpression(IErrorReceiver & errorProcessor, IHqlExpression * expr, ITemplateContext *templateContext, unsigned foldOptions)
{
    if (!expr)
        return NULL;

    if (foldOptions & HFOloseannotations)
        expr = expr->queryBody();

    switch (expr->getOperator())
    {
    case no_constant:
    case no_param:
    case no_variable:
    case no_attr:
        return LINK(expr);
    case no_select:
        if (!isNewSelector(expr))
            return LINK(expr);
        break;
    }

    CExprFolderTransformer folder(errorProcessor, templateContext, foldOptions);

#if 0
    dbglogExpr(expr);
#endif

    IHqlExpression * ret = folder.transformRoot(expr);

#if 0
    dbglogExpr(ret);
#endif

    return ret;
}

IHqlExpression * foldScopedHqlExpression(IErrorReceiver & errorProcessor, IHqlExpression * dataset, IHqlExpression * expr, unsigned foldOptions)
{
    if (!expr)
        return NULL;

    CExprFolderTransformer folder(errorProcessor, NULL, foldOptions);

    if (dataset)
        folder.setScope(dataset);

    IHqlExpression * ret = folder.transformRoot(expr);

    return ret;
}


void foldHqlExpression(IErrorReceiver & errorProcessor, HqlExprArray & tgt, HqlExprArray & src, unsigned foldOptions)
{
    CExprFolderTransformer folder(errorProcessor, NULL, foldOptions);
    folder.transformRoot(src, tgt);
}


//Fold it to a constant if it is easy to otherwise return anything.
IHqlExpression * foldExprIfConstant(IHqlExpression * expr)
{
    if (expr->isConstant())
        return quickFoldExpression(expr);

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_and:
//  case no_or:
        {
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = expr->queryChild(i);
                OwnedHqlExpr folded = foldExprIfConstant(cur);
                IValue * foldedValue = folded->queryValue();

                if (foldedValue)
                {
                    bool ok = foldedValue->getBoolValue();
                    if (op == no_and ? !ok : ok)
                        return folded.getClear();
                }
                else
                    return LINK(expr);
            }
            return createConstant(op == no_and);
        }
    case no_not:
        {
            OwnedHqlExpr folded = foldExprIfConstant(expr->queryChild(0));
            if (folded->queryValue())
                return getInverse(folded);
            break;
        }
    }

    return LINK(expr);
}


//---------------------------------------------------------------------------

static HqlTransformerInfo lowerCaseTransformerInfo("LowerCaseTransformer");
class LowerCaseTransformer : public NewHqlTransformer
{
public:
    LowerCaseTransformer() : NewHqlTransformer(lowerCaseTransformerInfo) {}

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        case no_constant:
            return getLowerCaseConstant(expr);
        }
        return NewHqlTransformer::createTransformed(expr);
    }
};

IHqlExpression * lowerCaseHqlExpr(IHqlExpression * expr)
{
    if (expr->getOperator() == no_constant)
        return getLowerCaseConstant(expr);
    LowerCaseTransformer transformer;
    return transformer.transformRoot(expr);
}


static HqlTransformerInfo quickConstantTransformerInfo("QuickConstantTransformer");
class QuickConstantTransformer : public QuickHqlTransformer
{
public:
    QuickConstantTransformer(ITemplateContext * _templateContext, unsigned _foldOptions) : 
      QuickHqlTransformer(quickConstantTransformerInfo, NULL), templateContext(_templateContext), foldOptions(_foldOptions) {}

    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr)
    {
        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_if:
            {
                OwnedHqlExpr cond = transform(expr->queryChild(0));
                IValue * condValue = cond->queryValue();
                if (condValue)
                {
                    unsigned idx = condValue->getBoolValue() ? 1 : 2;
                    IHqlExpression * branch = expr->queryChild(idx);
                    if (branch)
                        return transform(branch);
                    assertex(expr->isAction());
                    return createValue(no_null, makeVoidType());
                }
                break;
            }
        case no_and:
            {
                OwnedHqlExpr left = transform(expr->queryChild(0));
                IValue * leftValue = left->queryValue();
                if (leftValue)
                {
                    if (!leftValue->getBoolValue())
                        return LINK(left);
                    return transform(expr->queryChild(1));
                }
                break;
            }
        case no_or:
            {
                OwnedHqlExpr left = transform(expr->queryChild(0));
                IValue * leftValue = left->queryValue();
                if (leftValue)
                {
                    if (leftValue->getBoolValue())
                        return LINK(left);
                    return transform(expr->queryChild(1));
                }
                break;
            }
        case  no_attr:
            if (expr->queryName() == _original_Atom)
                return LINK(expr);
            break;
        }

        OwnedHqlExpr transformed = QuickHqlTransformer::createTransformedBody(expr);
        return foldConstantOperator(transformed, foldOptions, templateContext);
    }

protected:
    ITemplateContext *templateContext;
    unsigned foldOptions;
};

extern HQLFOLD_API IHqlExpression * quickFoldExpression(IHqlExpression * expr, ITemplateContext *context, unsigned options)
{
    QuickConstantTransformer transformer(context, options);
    return transformer.transform(expr);
}

extern HQLFOLD_API void quickFoldExpressions(HqlExprArray & target, const HqlExprArray & source, ITemplateContext *context, unsigned options)
{
    QuickConstantTransformer transformer(context, options);
    ForEachItemIn(i, source)
        target.append(*transformer.transform(&source.item(i)));
}

//--------------------------------------------------------------------------------------------------------------------

static bool valueInList(IHqlExpression * search, IHqlExpression * list)
{
    if (list->getOperator() != no_list)
        return false;

    ForEachChild(i, list)
    {
        if (search == list->queryChild(i))
            return true;
    }
    return false;
}

static bool valueNotInList(IHqlExpression * search, IHqlExpression * list)
{
    if (list->getOperator() != no_list)
        return false;

    IValue * value = search->queryValue();
    if (!value)
        return false; // can't tell

    ForEachChild(i, list)
    {
        IHqlExpression * cur = list->queryChild(i);
        IValue * curValue = cur->queryValue();
        if (!curValue || value == curValue)
            return false;
    }
    return true;
}


//Is it guaranteed that both conditions cannot be true at the same time.  Avoid false positivies.
//Don't try and catch all examples, just the most common possibilities.
//This could be improved over time....
extern HQLFOLD_API bool areExclusiveConditions(IHqlExpression * left, IHqlExpression * right)
{
    node_operator leftOp = left->getOperator();
    node_operator rightOp = right->getOperator();

    // Check for x = constant1, x = constant2, constant1 != constant2
    if ((leftOp == no_eq) && (rightOp == no_eq))
    {
        if (left->queryChild(0) == right->queryChild(0))
        {
            //Can only really be sure if comparing against constants.
            IValue * leftValue = left->queryChild(1)->queryValue();
            IValue * rightValue = right->queryChild(1)->queryValue();
            return (leftValue && rightValue && leftValue != rightValue);
        }
        return false;
    }

    // Check for NOT x, x
    if ((leftOp == no_not) && (left->queryChild(0) == right))
        return true;

    // Check for x, NOT x
    if ((rightOp == no_not) && (right->queryChild(0) == left))
        return true;

    // two tests against the same condition (could also pass in here if both NULL..)
    if (left->queryChild(0) == right->queryChild(0))
    {
        // Check for x <op> y, x !<op> y  - no need for y to be a constant.
        if (leftOp == getInverseOp(rightOp))
        {
            return left->queryChild(1) == right->queryChild(1);
        }

        // Unusual, but occured in the main example I was trying to improve
        // x = c1, x not in [c1, ....]
        // x = c1, x in [c2, c3, c4, c5]
        if ((leftOp == no_eq) && (rightOp == no_notin))
            return valueInList(left->queryChild(1),  right->queryChild(1));

        if ((leftOp == no_eq) && (rightOp == no_in))
            return valueNotInList(left->queryChild(1),  right->queryChild(1));

        if ((rightOp == no_eq) && (leftOp == no_notin))
            return valueInList(right->queryChild(1),  left->queryChild(1));

        if ((rightOp == no_eq) && (leftOp == no_in))
            return valueNotInList(right->queryChild(1),  left->queryChild(1));
    }

    return false;
}


bool queryCompareConstantValues(int & result, IHqlExpression * left, IHqlExpression * right)
{
    IValue * leftValue = left->queryValue();
    IValue * rightValue = right->queryValue();
    if (!leftValue || !rightValue)
        return false;

    ITypeInfo * leftType = left->queryType();
    ITypeInfo * rightType = right->queryType();
    if (leftType != rightType)
    {
        Owned<ITypeInfo> type = ::getPromotedECLCompareType(leftType, rightType);
        OwnedHqlExpr castLeft = ensureExprType(left, type);
        OwnedHqlExpr castRight = ensureExprType(right, type);
        IValue * castLeftValue = castLeft->queryValue();
        IValue * castRightValue = castRight->queryValue();
        if (!castLeftValue || !castRightValue)
            return false;

        result = castLeftValue->compare(castRightValue);
        return true;
    }
    else
    {
        result = leftValue->compare(rightValue);
        return true;
    }
}

IHqlExpression * foldConstantCaseExpr(IHqlExpression * expr)
{
    IHqlExpression * search = expr->queryChild(0);
    if (!search->isConstant())
        return LINK(expr);

    OwnedHqlExpr foldedSearch = foldHqlExpression(search);
    ForEachChildFrom(i, expr, 1)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (cur->getOperator() == no_mapto)
        {
            IHqlExpression * mapValue = cur->queryChild(0);
            if (!mapValue->isConstant())
                return LINK(expr);

            OwnedHqlExpr foldedValue = foldHqlExpression(mapValue);
            int result;
            if (!queryCompareConstantValues(result, foldedSearch, foldedValue))
                return LINK(expr);

            if (result == 0)
                return LINK(cur->queryChild(1));
        }
        else if (!cur->isAttribute())
            return LINK(cur);
    }
    return LINK(expr);
}

IHqlExpression * foldConstantMapExpr(IHqlExpression * expr)
{
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (cur->getOperator() == no_mapto)
        {
            IHqlExpression * mapValue = cur->queryChild(0);
            if (!mapValue->isConstant())
                return LINK(expr);

            OwnedHqlExpr foldedValue = foldHqlExpression(mapValue);
            IValue * value = foldedValue->queryValue();
            if (!value)
                return LINK(expr);

            if (value->getBoolValue())
                return LINK(cur->queryChild(1));
        }
        else if (!cur->isAttribute())
            return LINK(cur);
    }
    return LINK(expr);
}
