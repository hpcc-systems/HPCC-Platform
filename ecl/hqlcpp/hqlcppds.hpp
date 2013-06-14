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
#ifndef __HQLCPPDS_HPP_
#define __HQLCPPDS_HPP_

//---------------------------------------------------------------------------------------------------------------------

class ChildGraphExprBuilder : public CInterface
{
public:
    ChildGraphExprBuilder(unsigned _numInputs);

    IHqlExpression * addDataset(IHqlExpression * expr);
    void addAction(IHqlExpression * expr);
    unsigned addInput();
    IHqlExpression * getGraph(IAtom * extraAttrName = NULL);

    inline IHqlExpression * queryRepresents() const { return represents; }
    inline unsigned numResults() const { return numInputs + numOutputs; }

public:
    HqlExprArray results;
    OwnedHqlExpr represents;
    OwnedHqlExpr resultsExpr;
    unsigned numInputs;
    unsigned numOutputs;
};


//===========================================================================

class ChildGraphBuilder : public CInterface
{
public:
    ChildGraphBuilder(HqlCppTranslator & _translator, IHqlExpression * subgraph);

    unique_id_t buildGraphLoopBody(BuildCtx & ctx, bool multiInstance);
    unique_id_t buildLoopBody(BuildCtx & ctx, bool multiInstance);
    unique_id_t buildRemoteGraph(BuildCtx & ctx);
    void generateGraph(BuildCtx & ctx);
    void generatePrefetchGraph(BuildCtx & _ctx, OwnedHqlExpr * retGraphExpr);

protected:
    void createBuilderAlias(BuildCtx & ctx, ParentExtract * extractBuilder);

protected:
    HqlCppTranslator & translator;
    unsigned id;
    StringBuffer instanceName;
    LinkedHqlExpr childQuery;
    OwnedHqlExpr instanceExpr;
    OwnedHqlExpr resultInstanceExpr;
    OwnedHqlExpr represents;
    OwnedHqlExpr resultsExpr;
    unsigned numResults;
};

IHqlExpression * createCounterAsGraphResult(IHqlExpression * counter, IHqlExpression * represents, unsigned seq);

//===========================================================================

void addGraphIdAttribute(ActivityInstance * instance, BuildCtx & ctx, IHqlExpression * graphId);

#endif
