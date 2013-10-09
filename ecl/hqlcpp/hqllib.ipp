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
    unsigned queryOutputIndex(IAtom * name) const;
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

    void noteOutputUsed(IAtom * name);

public:
    Linked<IPropertyTree> graphNode;
    Linked<HqlCppLibraryInstance> libraryInstance;
};

#endif
