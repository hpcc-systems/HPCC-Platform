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
#ifndef __HQLCCOMMON_HPP_
#define __HQLCCOMMON_HPP_

#ifdef _WIN32
#ifdef HQLCPP_EXPORTS
#define HQLCPP_API __declspec(dllexport)
#else
#define HQLCPP_API __declspec(dllimport)
#endif
#endif

extern IHqlExpression * activeActivityMarkerExpr;
extern IHqlExpression * activeFailureMarkerExpr;
extern IHqlExpression * activeMatchTextExpr;
extern IHqlExpression * activeMatchUnicodeExpr;
extern IHqlExpression * activeMatchUtf8Expr;
extern IHqlExpression * activeNlpMarkerExpr;
extern IHqlExpression * activeProductionMarkerExpr;
extern IHqlExpression * activeValidateMarkerExpr;
extern IHqlExpression * classMarkerExpr;
extern IHqlExpression * codeContextMarkerExpr;
extern IHqlExpression * colocalSameClassPreserveExpr;
extern IHqlExpression * constantMemberMarkerExpr;
extern IHqlExpression * globalContextMarkerExpr;
extern IHqlExpression * insideOnCreateMarker;
extern IHqlExpression * insideOnStartMarker;
extern IHqlExpression * parentExtractMarkerExpr;
extern IHqlExpression * xmlColumnProviderMarkerExpr;

#endif
