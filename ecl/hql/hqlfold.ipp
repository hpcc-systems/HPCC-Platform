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
#ifndef __HQLFOLD_IPP_
#define __HQLFOLD_IPP_

#include "hqlexpr.hpp"
#include "hqltrans.ipp"
#include "hqlutil.hpp"

typedef IValue * (*binaryFoldFunc)(IValue * left, IValue * right);

#define FOLD_PARENT         NewHqlTransformer
#define FOLD_INFO_PARENT    NewTransformInfo

class HQL_API NullFolderMixin
{
public:
    virtual IHqlExpression * foldNullDataset(IHqlExpression * expr);
    virtual IHqlExpression * removeParentNode(IHqlExpression * expr) = 0;
    virtual IHqlExpression * replaceWithNull(IHqlExpression * expr) = 0;
    virtual IHqlExpression * replaceWithNullRow(IHqlExpression * expr) = 0;
    virtual IHqlExpression * replaceWithNullRowDs(IHqlExpression * expr) = 0;
    // only for use on something that has been expanded as a child, a transform() isn't used to help stop calling transform on result of folding.
    virtual IHqlExpression * transformExpanded(IHqlExpression * expr) = 0;              

protected:
    IHqlExpression * queryOptimizeAggregateInline(IHqlExpression * expr, __int64 numRows);
};


class HqlConstantPercolator;
//MORE: Could conceivably split this structure in two, and only create one with a mapping if it was possible for one to be created
//It would save 4/8 bytes per expression that couldn't support it, but probably not critical compareed with other potential changes.
class FolderTransformInfo : public FOLD_INFO_PARENT
{
public:
    FolderTransformInfo(IHqlExpression * _expr) : FOLD_INFO_PARENT(_expr) { setGatheredConstants(false); mapping = NULL; }
    ~FolderTransformInfo();

    inline void setGatheredConstants(bool value) { spareByte1 = value; }
    inline bool queryGatheredConstants() const { return spareByte1 != 0; }

public:
    HqlConstantPercolator * mapping;

private:
    using FOLD_INFO_PARENT::spareByte1;             //prevent derived classes from also using this spare byte
};


class CExprFolderTransformer : public FOLD_PARENT, public NullFolderMixin
{
    typedef FOLD_PARENT PARENT;
public:
    CExprFolderTransformer(IErrorReceiver & _errorProcessor, unsigned _options);

    void setScope(IHqlExpression * expr)                { stopDatasetTransform(expr); }

protected:
    virtual IHqlExpression * createTransformedAnnotation(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    inline const char * queryNode0Text(IHqlExpression * expr) { return queryChildNodeTraceText(nodeText[0], expr); }
    inline const char * queryNode1Text(IHqlExpression * expr) { return queryChildNodeTraceText(nodeText[1], expr); }
    inline FolderTransformInfo * queryBodyExtra(IHqlExpression * expr)  { return (FolderTransformInfo *)queryTransformExtra(expr->queryBody()); }

    IHqlExpression* doFoldTransformed(IHqlExpression * expr, IHqlExpression * original);
    HqlConstantPercolator * gatherConstants(IHqlExpression * expr);
    IHqlExpression * percolateConstants(IHqlExpression * expr);
    IHqlExpression * percolateConstants(IHqlExpression * expr, IHqlExpression * dataset, node_operator side);
    IHqlExpression * percolateConstants(IHqlExpression * expr, IHqlExpression * dataset, node_operator side, unsigned whichChild);
    IHqlExpression * percolateConstants(HqlConstantPercolator * mapping, IHqlExpression * expr, IHqlExpression * dataset, node_operator side);

    IHqlExpression * percolateRollupInvariantConstants(IHqlExpression * expr, HqlConstantPercolator * mapping, IHqlExpression * selector);
    IHqlExpression * percolateRollupInvariantConstants(IHqlExpression * expr, IHqlExpression * dataset, node_operator side, IHqlExpression * selSeq);

    inline IHqlExpression * cloneAnnotationAndTransform(IHqlExpression * expr, IHqlExpression * transformed)
    {
        //Ensure there is a symbol if appropriate, and then re-transform.
        if ((expr != expr->queryBody()) && (transformed == transformed->queryBody()))
        {
            OwnedHqlExpr cloned = expr->cloneAllAnnotations(transformed);
            return transform(cloned);
        }
        //MORE: Possibly other hidden attributes to clone.
        return transform(transformed);
    }

private:
//Null folder helpers.
    virtual IHqlExpression * removeParentNode(IHqlExpression * expr);
    virtual IHqlExpression * replaceWithNull(IHqlExpression * expr);
    virtual IHqlExpression * replaceWithNullRow(IHqlExpression * expr);
    virtual IHqlExpression * replaceWithNullRowDs(IHqlExpression * expr);
    virtual IHqlExpression * transformExpanded(IHqlExpression * expr);              

protected:
    IErrorReceiver & errorProcessor;
    unsigned foldOptions;
    StringBuffer nodeText[2];
};

#endif
