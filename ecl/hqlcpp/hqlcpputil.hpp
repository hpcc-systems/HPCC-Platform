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
#ifndef HQLCPPUTIL_HPP
#define HQLCPPUTIL_HPP

interface IConstWorkUnit;
class CHqlBoundExpr;

extern ITypeInfo * queryBoolType();
extern IHqlExpression * queryNullChar();
extern IHqlExpression * queryZero();
extern IHqlExpression * getZero();
extern IHqlExpression * getDefaultAttr();
extern IHqlExpression * getSelfAttr();
extern IHqlExpression * queryActivityIdMarker();
extern IHqlExpression * queryConditionalRowMarker();

extern bool storePointerInArray(ITypeInfo * type);
extern bool isCompare3Valued(ITypeInfo * type);
extern ITypeInfo * queryVoidType();

extern ITypeInfo * getArrayElementType(ITypeInfo * itemType);
extern ITypeInfo * getConcatResultType(IHqlExpression * expr);

extern bool isSelectSortedTop(IHqlExpression * selectExpr);
extern ITypeInfo * makeRowReferenceType(IHqlExpression * ds);
extern ITypeInfo * makeRowReferenceType(const CHqlBoundExpr & bound);

extern IHqlExpression * addMemberSelector(IHqlExpression * expr, IHqlExpression * selector);
extern IHqlExpression * addExpressionModifier(IHqlExpression * expr, typemod_t modifier, IInterface * extra=NULL);
extern void expandFieldNames(IErrorReceiver & errorProcessor, StringBuffer & out, IHqlExpression * record, const char * sep, IHqlExpression * formatFunc);
extern IHqlExpression * ensurePositiveOrZeroInt64(IHqlExpression * expr);

extern void getOutputLibraryName(SCMStringBuffer & libraryName, IConstWorkUnit * wu);
extern bool canCreateTemporary(IHqlExpression * expr);
extern IHqlExpression * projectCreateSetDataset(IHqlExpression * createsetExpr);

extern bool mustInitializeField(IHqlExpression * field);
extern bool worthGeneratingRowAsSingleActivity(IHqlExpression * expr);

extern bool isNonConstantAndQueryInvariant(IHqlExpression * expr);

//Common types and expressions...
extern ITypeInfo * boolType;
extern ITypeInfo * sizetType;
extern ITypeInfo * signedType;
extern ITypeInfo * unsignedType;
extern ITypeInfo * defaultIntegralType;
extern ITypeInfo * counterType;
extern ITypeInfo * unknownDataType;
extern ITypeInfo * unknownStringType;
extern ITypeInfo * unknownVarStringType;
extern ITypeInfo * unknownUtf8Type;
extern ITypeInfo * constUnknownVarStringType;
extern ITypeInfo * unknownUnicodeType;
extern ITypeInfo * fposType;
extern ITypeInfo * doubleType;
extern IHqlExpression * skipActionMarker;
extern IHqlExpression * skipReturnMarker;
extern IHqlExpression * subGraphMarker;
extern IHqlExpression * removedAssignTag;
extern IHqlExpression * internalAttrExpr;


#define NO_ACTION_REQUIRES_GRAPH    \
         no_apply:\
    case no_output:\
    case no_buildindex:\
    case no_distribution:\
    case no_newsoapcall:\
    case no_newsoapcall_ds:\
    case no_newsoapaction_ds:\
    case no_keydiff:\
    case no_keypatch:\
    case no_setgraphresult:\
    case no_allnodes:\
    case no_definesideeffect

#endif
