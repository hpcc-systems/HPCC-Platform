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
#ifndef __HQLSOURCE_IPP_
#define __HQLSOURCE_IPP_

#include "hqlcfilter.hpp"

IHqlExpression * convertToPhysicalTable(IHqlExpression * tableExpr, bool ensureSerialized);

//---------------------------------------------------------------------------

struct VirtualFieldsInfo
{
public:
    VirtualFieldsInfo()
    { 
        simpleVirtualsAtEnd = true;
        requiresDeserialize = false;
    }

    IHqlExpression * createPhysicalRecord();
    void gatherVirtualFields(IHqlExpression * record, bool ignoreVirtuals, bool ensureSerialized);
    bool hasVirtuals()      { return virtuals.ordinality() != 0; }
    bool hasVirtualsOrDeserialize() { return requiresDeserialize || virtuals.ordinality() != 0; }
    bool canAppendVirtuals() { return simpleVirtualsAtEnd; }

public:
    HqlExprArray    physicalFields;
    HqlExprArray    selects;
    HqlExprArray    virtuals;
    bool            simpleVirtualsAtEnd;
    bool            requiresDeserialize;
};

//---------------------------------------------------------------------------

unsigned getProjectCount(IHqlExpression * expr);

#endif
