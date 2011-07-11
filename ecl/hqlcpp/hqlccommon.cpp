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
#include "jliball.hpp"
#include "hqlexpr.hpp"
#include "hqlcatom.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/hqlcpp/hqlccommon.cpp $ $Id: hqlccommon.cpp 62376 2011-02-04 21:59:58Z sort $");

//===========================================================================

IHqlExpression * activeActivityMarkerExpr;
IHqlExpression * activeFailureMarkerExpr;
IHqlExpression * activeMatchTextExpr;
IHqlExpression * activeMatchUnicodeExpr;
IHqlExpression * activeMatchUtf8Expr;
IHqlExpression * activeNlpMarkerExpr;
IHqlExpression * activeProductionMarkerExpr;
IHqlExpression * activeValidateMarkerExpr;
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
    activeValidateMarkerExpr->Release();
    activeProductionMarkerExpr->Release();
    activeNlpMarkerExpr->Release();
    activeMatchUtf8Expr->Release();
    activeMatchUnicodeExpr->Release();
    activeMatchTextExpr->Release();
    activeFailureMarkerExpr->Release();
    activeActivityMarkerExpr->Release();
}


