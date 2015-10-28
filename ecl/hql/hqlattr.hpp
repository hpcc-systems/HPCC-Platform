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
#ifndef HQLATTR_HPP
#define HQLATTR_HPP

#include "hqlexpr.hpp"
#include "workunit.hpp"

#define MAX_MAXLENGTH (INFINITE_LENGTH-1)

extern HQL_API IHqlExpression * queryAttribute(ITypeInfo * type, IAtom * search);
extern HQL_API IHqlExpression * queryAttributeChild(ITypeInfo * type, IAtom * search, unsigned idx);
extern HQL_API void cloneFieldModifier(Owned<ITypeInfo> & type, ITypeInfo * donorType, IAtom * attr);
extern HQL_API ITypeInfo * cloneEssentialFieldModifiers(ITypeInfo * donor, ITypeInfo * rawtype);
extern HQL_API ITypeInfo * removeAttribute(ITypeInfo * type, IAtom * search);

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
inline bool isFixedSizeRecord(IHqlExpression * record) { return !isVariableSizeRecord(record); }

extern HQL_API bool recordRequiresLinkCount(IHqlExpression * expr);
extern HQL_API bool recordRequiresDestructor(IHqlExpression * expr);
extern HQL_API bool recordRequiresSerialization(IHqlExpression * expr, IAtom * serializeForm);
extern HQL_API bool typeRequiresDeserialization(ITypeInfo * type, IAtom * serializeForm); // or can we use the serialized form directly
extern HQL_API bool recordSerializationDiffers(IHqlExpression * expr, IAtom * serializeForm1, IAtom * serializeForm2);
extern HQL_API IHqlExpression * getSerializedForm(IHqlExpression * expr, IAtom * variation);
extern HQL_API ITypeInfo * getSerializedForm(ITypeInfo * type, IAtom * variation);
extern HQL_API IHqlExpression * getPackedRecord(IHqlExpression * expr);

//This returns a record that compares equal with another result if the normalized records will compare equal
extern HQL_API IHqlExpression * getUnadornedRecordOrField(IHqlExpression * expr);

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

inline bool hasLinkCountedModifier(ITypeInfo * t)    { return queryAttribute(t, _linkCounted_Atom) != NULL; }
inline bool hasOutOfLineRows(ITypeInfo * type) { return (hasOutOfLineModifier(type) || hasLinkCountedModifier(type)); }
inline bool hasLinkCountedModifier(IHqlExpression * expr)    { return hasLinkCountedModifier(expr->queryType()); }
inline bool hasStreamedModifier(ITypeInfo * t)   { return queryAttribute(t, streamedAtom) != NULL; }
inline bool isStreamed(IHqlExpression * expr) { return hasStreamedModifier(expr->queryType()); }

extern HQL_API ITypeInfo * setLinkCountedAttr(ITypeInfo * _type, bool setValue);
extern HQL_API ITypeInfo * setStreamedAttr(ITypeInfo * _type, bool setValue);
extern HQL_API bool isSmallGrouping(IHqlExpression * sortlist);
extern HQL_API void getRecordCountText(StringBuffer & result, IHqlExpression * expr);

extern HQL_API IHqlExpression * queryRecordCountInfo(IHqlExpression * expr);
extern HQL_API IHqlExpression * getRecordCountInfo(IHqlExpression * expr);
extern HQL_API bool hasNoMoreRowsThan(IHqlExpression * expr, __int64 limit);
extern HQL_API bool spillToWorkunitNotFile(IHqlExpression * expr, ClusterType platform);

class CHqlMetaProperty;
extern HQL_API CHqlMetaProperty * queryMetaProperty(IHqlExpression * expr);

#endif
