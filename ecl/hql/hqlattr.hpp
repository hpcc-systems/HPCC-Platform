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
#ifndef HQLATTR_HPP
#define HQLATTR_HPP

#include "hqlexpr.hpp"

#define MAX_MAXLENGTH (INFINITE_LENGTH-1)

extern HQL_API IHqlExpression * queryProperty(ITypeInfo * type, _ATOM search);
extern HQL_API IHqlExpression * queryPropertyChild(ITypeInfo * type, _ATOM search, unsigned idx);
extern HQL_API void cloneFieldModifier(Owned<ITypeInfo> & type, ITypeInfo * donorType, _ATOM attr);
extern HQL_API ITypeInfo * cloneEssentialFieldModifiers(ITypeInfo * donor, ITypeInfo * rawtype);
extern HQL_API ITypeInfo * removeProperty(ITypeInfo * type, _ATOM search);

extern HQL_API size32_t getMinRecordSize(IHqlExpression * record);
extern HQL_API size32_t getExpectedRecordSize(IHqlExpression * record);
extern HQL_API unsigned getMaxRecordSize(IHqlExpression * record, unsigned defaultMaxRecordSize, bool & hasKnownSize, bool & usedDefault);
extern HQL_API unsigned getMaxRecordSize(IHqlExpression * record, unsigned defaultMaxRecordSize);
extern HQL_API bool maxRecordSizeUsesDefault(IHqlExpression * record);          // wrapper around above.
extern HQL_API bool maxRecordSizeIsAmbiguous(IHqlExpression * record, size32_t & specifiedSize, size32_t & derivedSize);
extern HQL_API bool maxRecordSizeCanBeDerived(IHqlExpression * record);
extern HQL_API bool reducesRowSize(IHqlExpression * expr);
extern HQL_API bool increasesRowSize(IHqlExpression * expr);
extern HQL_API bool isVariableSizeRecord(IHqlExpression * record);

extern HQL_API bool recordRequiresDestructor(IHqlExpression * expr);
extern HQL_API bool recordRequiresSerialization(IHqlExpression * expr);
extern HQL_API IHqlExpression * getSerializedForm(IHqlExpression * expr);
extern HQL_API ITypeInfo * getSerializedForm(ITypeInfo * type);
extern HQL_API IHqlExpression * getPackedRecord(IHqlExpression * expr);
extern HQL_API IHqlExpression * getUnadornedExpr(IHqlExpression * expr);

extern HQL_API IHqlExpression * queryUID(IHqlExpression * expr);
extern HQL_API IHqlExpression * querySelf(IHqlExpression * record);
extern HQL_API IHqlExpression * queryNewSelector(node_operator op, IHqlExpression * datasetOrRow);
extern HQL_API IHqlExpression * queryLocationIndependent(IHqlExpression * expr);
extern HQL_API ITypeInfo * preserveTypeQualifiers(ITypeInfo * ownedType, IHqlExpression * donor);
extern HQL_API bool preserveTypeQualifiers(HqlExprArray & args, ITypeInfo * donor);
extern HQL_API IHqlExpression * preserveTypeQualifiers(IHqlExpression * ownedField, ITypeInfo * donor);


extern unsigned getOperatorMetaFlags(node_operator op);
extern HQL_API bool isLinkedRowset(ITypeInfo * t);
extern HQL_API bool isArrayRowset(ITypeInfo * t);
extern HQL_API bool hasLinkedRow(ITypeInfo * t);

inline bool hasLinkCountedModifier(ITypeInfo * t)    { return queryProperty(t, _linkCounted_Atom) != NULL; }
inline bool hasOutOfLineRows(ITypeInfo * type) { return (hasOutOfLineModifier(type) || hasLinkCountedModifier(type)); }
inline bool hasLinkCountedModifier(IHqlExpression * expr)    { return hasLinkCountedModifier(expr->queryType()); }
inline bool hasStreamedModifier(ITypeInfo * t)   { return queryProperty(t, streamedAtom) != NULL; }

extern HQL_API ITypeInfo * setLinkCountedAttr(ITypeInfo * _type, bool setValue);
extern HQL_API ITypeInfo * setStreamedAttr(ITypeInfo * _type, bool setValue);
extern HQL_API bool isSmallGrouping(IHqlExpression * sortlist);
extern HQL_API void getRecordCountText(StringBuffer & result, IHqlExpression * expr);

extern HQL_API IHqlExpression * queryRecordCountInfo(IHqlExpression * expr);
extern HQL_API IHqlExpression * getRecordCountInfo(IHqlExpression * expr);
extern HQL_API bool hasNoMoreRowsThan(IHqlExpression * expr, __int64 limit);
extern HQL_API bool spillToWorkunitNotFile(IHqlExpression * expr);

#endif
