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
