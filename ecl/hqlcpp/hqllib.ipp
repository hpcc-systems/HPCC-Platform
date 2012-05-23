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
#ifndef __HQLLIB_IPP_
#define __HQLLIB_IPP_

#include "hqlhtcpp.ipp"
#include "hqlecl.hpp"

class HqlCppLibrary : public CInterface
{
public:
    HqlCppLibrary(HqlCppTranslator & _translator, IHqlExpression * libraryInterface, ClusterType _clusterType);

    unsigned numStreamedInputs() const  { return streamedCount; }
    unsigned totalInputs() const        { return inputMapper.numParameters(); }
    unsigned getInterfaceHash() const;
    unsigned queryOutputIndex(_ATOM name) const;
    IHqlExpression * queryParameter(unsigned i) { return &inputMapper.queryRealParameters().item(i); }

protected:
    void extractOutputs();
    unsigned getHash(const HqlExprArray & values, unsigned crc) const;

public:
    HqlCppTranslator & translator;
    LinkedHqlExpr libraryInterface;
    IHqlExpression * scopeExpr;
    ClusterType clusterType;
    unsigned streamedCount;
    LibraryInputMapper inputMapper;
    HqlExprArray outputs;                       // Only the names and types are significant, not the values
    bool allowStreamingInputs;
};


class HqlCppLibraryImplementation : public HqlCppLibrary
{
public:
    HqlCppLibraryImplementation(HqlCppTranslator & _translator, IHqlExpression * libraryInterface, IHqlExpression * libraryId, ClusterType _clusterType);

    void mapLogicalToImplementation(HqlExprArray & exprs, IHqlExpression * libraryExpr);
    unsigned numResultsUsed()           { return numStreamedInputs() + outputs.ordinality(); }
    IHqlExpression * queryInputExpr(unsigned i) { return &inputExprs.item(i); }

public:
    LinkedHqlExpr libraryId;
    HqlExprArray inputExprs;
    HqlExprArray logicalParams;
};



class HqlCppLibraryInstance : public CInterface
{
public:
    HqlCppLibraryInstance(HqlCppTranslator & _translator, IHqlExpression * libraryInstance, ClusterType _clusterType);

    unsigned numStreamedInputs()                        { return library->numStreamedInputs(); }
    unsigned numParameters()                            { return libraryFuncdef->queryChild(1)->numChildren(); }
    IHqlExpression * queryActual(unsigned i)            { return instanceExpr->queryChild(i); }
    IHqlExpression * queryParameter(unsigned i)         { return libraryFuncdef->queryChild(1)->queryChild(i); }

public:
    Owned<HqlCppLibrary> library;
    LinkedHqlExpr instanceExpr;
    IHqlExpression * libraryFuncdef;
};


class ThorBoundLibraryActivity : public ThorBoundActivity
{
public:
    ThorBoundLibraryActivity(ABoundActivity * activity, IPropertyTree * _graphNode, HqlCppLibraryInstance * _libraryInstance)
    : ThorBoundActivity(activity->queryDataset(), activity->queryBound(), activity->queryActivityId(), activity->queryContainerId(), activity->queryGraphId(), activity->queryActivityKind())
    {
        graphNode.set(_graphNode);
        libraryInstance.set(_libraryInstance);
    }

    void noteOutputUsed(_ATOM name);

public:
    Linked<IPropertyTree> graphNode;
    Linked<HqlCppLibraryInstance> libraryInstance;
};

#endif
