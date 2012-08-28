/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

#ifndef HQLIR_INCL
#define HQLIR_INCL

#include "hqlexpr.hpp"

/*
 * This header declares the Intermediate Representation (IR)
 * for ECL graphs (Directed Acyclic Graphs - DAG), for debug
 * dumps, test checks and optimisation analysis.
 *
 * It needs to be included from a top-level header, to make sure
 * it'll be available throughout the compiler code, but it's best
 * to keep it inside _DEBUG areas, since it's where it'll be used
 * most.
 *
 * The IR is meant to be a simple, non-redundant and complete
 * description of ECL graphs. The aim is to produce a textual
 * description of the DAG in a way that is easy to read it again
 * and produce correct graphs from it.
 *
 * When things get more complicated, and the IR gets used for
 * internal processes, it might be good to expose some simple
 * interfaces for importing/exporting expressions to IR.
 *
 * See the C++ file for better description of the IR.
 */

extern HQL_API void dump_ir(IHqlExpression * expr);
extern HQL_API void dump_ir(HqlExprArray list);

#endif /* HQLIR_INCL */
