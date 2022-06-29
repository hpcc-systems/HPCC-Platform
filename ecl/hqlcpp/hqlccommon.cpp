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
#include "jliball.hpp"
#include "hqlexpr.hpp"
#include "hqlcatom.hpp"

//===========================================================================

IHqlExpression * activeActivityMarkerExpr;
IHqlExpression * activeFailureMarkerExpr;
IHqlExpression * activeMatchTextExpr;
IHqlExpression * activeMatchUnicodeExpr;
IHqlExpression * activeMatchUtf8Expr;
IHqlExpression * activeNlpMarkerExpr;
IHqlExpression * activeProductionMarkerExpr;
IHqlExpression * activeValidateMarkerExpr;
IHqlExpression * activityContextMarkerExpr;
IHqlExpression * classMarkerExpr;
IHqlExpression * codeContextMarkerExpr;
IHqlExpression * colocalSameClassPreserveExpr;
IHqlExpression * constantMemberMarkerExpr;
IHqlExpression * globalContextMarkerExpr;
IHqlExpression * insideOnCreateMarker;
IHqlExpression * insideOnStartMarker;
IHqlExpression * parentExtractMarkerExpr;
IHqlExpression * xmlColumnProviderMarkerExpr;

//===========================================================================

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    activeActivityMarkerExpr = createAttribute(activeActivityMarkerAtom);
    activeFailureMarkerExpr = createAttribute(activeFailureAtom);
    activeMatchTextExpr = createAttribute(activeMatchTextAtom);
    activeMatchUnicodeExpr = createAttribute(activeMatchUnicodeAtom);
    activeMatchUtf8Expr = createAttribute(activeMatchUtf8Atom);
    activeNlpMarkerExpr = createAttribute(activeNlpAtom);
    activeProductionMarkerExpr = createAttribute(activeProductionMarkerAtom);
    activeValidateMarkerExpr = createAttribute(activeValidateMarkerAtom);
    activityContextMarkerExpr = createQuoted("activityContextMarker", makeVoidType());
    classMarkerExpr = createAttribute(classAtom);
    codeContextMarkerExpr = createQuoted("codeContextMarker", makeVoidType());
    colocalSameClassPreserveExpr = createVariable("internalColocalShouldNeverBeUsed", makeVoidType());
    constantMemberMarkerExpr = createQuoted("constantMemberFunction", makeVoidType());
    globalContextMarkerExpr = createQuoted("globalContextMarker", makeVoidType());
    insideOnCreateMarker = createAttribute(insideOnCreateAtom);
    insideOnStartMarker = createAttribute(insideOnStartAtom);
    parentExtractMarkerExpr = createVariable("parentExtractMarker", makeVoidType());
    xmlColumnProviderMarkerExpr = createAttribute(xmlColumnProviderAtom);
    return true;
}
MODULE_EXIT()
{
    xmlColumnProviderMarkerExpr->Release();
    parentExtractMarkerExpr->Release();
    insideOnCreateMarker->Release();
    insideOnStartMarker->Release();
    globalContextMarkerExpr->Release();
    constantMemberMarkerExpr->Release();
    colocalSameClassPreserveExpr->Release();
    codeContextMarkerExpr->Release();
    classMarkerExpr->Release();
    activityContextMarkerExpr->Release();
    activeValidateMarkerExpr->Release();
    activeProductionMarkerExpr->Release();
    activeNlpMarkerExpr->Release();
    activeMatchUtf8Expr->Release();
    activeMatchUnicodeExpr->Release();
    activeMatchTextExpr->Release();
    activeFailureMarkerExpr->Release();
    activeActivityMarkerExpr->Release();
}


