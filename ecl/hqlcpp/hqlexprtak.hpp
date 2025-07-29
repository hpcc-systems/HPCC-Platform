/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#ifndef _HQLEXPRTAK_HPP_
#define _HQLEXPRTAK_HPP_

#include "hqlexpr.hpp"
#include "eclhelper.hpp"

/**
 * Maps IHqlExpression instances to ThorActivityKind enum values.
 * 
 * This function analyzes an ECL expression and determines the most appropriate 
 * ThorActivityKind that would be used to implement it at runtime. The analysis
 * considers the expression's operator, type, attributes, and format specifications
 * to provide accurate activity mappings.
 * 
 * The mapping logic is based on analysis of how expressions are processed in the
 * HPCC codebase, particularly in buildActivity() and the various activity builders.
 * It handles format-specific optimizations (CSV, XML, JSON) and context-dependent
 * activity selection.
 * 
 * Key features:
 * - Handles format-specific activity variants (e.g., TAKcsvread vs TAKdiskread)
 * - Considers expression type (dataset, action, scalar, transform)
 * - Analyzes attributes for optimization hints (grouped, sorted, keyed, etc.)
 * - Maps compound operations to their primary activity types
 * - Returns most appropriate activity for logical execution analysis
 * 
 * @param expr  The IHqlExpression to analyze
 * @return      The corresponding ThorActivityKind, or TAKnone if no mapping exists
 * 
 * @note        This function performs static analysis and may not reflect all runtime
 *              optimizations that could change the actual activity type used.
 */
extern ThorActivityKind mapExpressionToActivityKind(IHqlExpression * expr);

#endif // _HQLEXPRTAK_HPP_
