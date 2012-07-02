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
    IHqlExpression * getGraph();

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
    OwnedHqlExpr instanceExpr;
    OwnedHqlExpr resultInstanceExpr;
    OwnedHqlExpr represents;
    OwnedHqlExpr resultsExpr;
    HqlExprArray results;
    unsigned numResults;
};

IHqlExpression * createCounterAsGraphResult(IHqlExpression * counter, IHqlExpression * represents, unsigned seq);

//===========================================================================

void addGraphIdAttribute(ActivityInstance * instance, BuildCtx & ctx, IHqlExpression * graphId);

#endif
