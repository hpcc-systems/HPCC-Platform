/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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
