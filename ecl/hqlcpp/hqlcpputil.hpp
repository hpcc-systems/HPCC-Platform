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
#ifndef HQLCPPUTIL_HPP
#define HQLCPPUTIL_HPP

interface IConstWorkUnit;
class CHqlBoundExpr;

extern ITypeInfo * queryBoolType();
extern IHqlExpression * queryBoolExpr(bool value);
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
extern void expandFieldNames(StringBuffer & out, IHqlExpression * record, const char * sep, IHqlExpression * formatFunc);
extern IHqlExpression * ensurePositiveOrZeroInt64(IHqlExpression * expr);

extern void getOutputLibraryName(SCMStringBuffer & libraryName, IConstWorkUnit * wu);
extern bool canCreateTemporary(IHqlExpression * expr);
extern IHqlExpression * projectCreateSetDataset(IHqlExpression * createsetExpr);

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
