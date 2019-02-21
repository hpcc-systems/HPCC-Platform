/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#include "SQLExpression.hpp"

/***********************SQLFieldsExpression START**************************************************************************/

SQLFieldsExpression::SQLFieldsExpression(bool allfiles)
{
    this->all = allfiles;
    isExpanded = false;
}

SQLFieldsExpression::SQLFieldsExpression(const char * table)
{
    this->all = false;
    this->table.set(table);
    isExpanded = false;
}

SQLFieldsExpression::~SQLFieldsExpression()
{
#ifdef _DEBUG
    fprintf(stderr, "\ndestroying SQLFieldsExression.\n");
#endif
}

 void SQLFieldsExpression::toString(StringBuffer & targetstr, bool fullOutput)
 {
     if (table.length() > 0)
         targetstr.append(table.str()).append(".");

     targetstr.append("*");
 }
/***********************SQLFieldsExpression END**************************************************************************/

/***********************SQLFieldValueExpression START**************************************************************************/

 SQLLogicType SQLFieldValueExpression::getLogicType()
 {
     const char * type = field.getColumnType();

     if (strnicmp(type,"STRING",6)==0)
         return String_LogicType;
     else if (strnicmp(type,"QSTRING",7)==0)
         return QSstring_LogicType;
     else if (strnicmp(type,"UNICODE",7)==0)
         return Unicode_LogicType;
     else if (strnicmp(type,"VARUNICODE",10)==0)
         return Unicode_LogicType;
     else if (strnicmp(type,"VARSTRING",9)==0)
         return String_LogicType;
     else if (strnicmp(type,"BOOLEAN",7)==0)
         return Bool_LogicType;
     else if (strnicmp(type,"UNSIGNED",8)==0)
         return Integer_LogicType;
     else if (strnicmp(type,"REAL",4)==0)
         return Decimal_LogicType;
     else if (strnicmp(type,"DECIMAL",7)==0)
         return Decimal_LogicType;
     else
         return Unknown_LogicType;
 }

 void SQLFieldValueExpression::toECLStringTranslateSource(
             StringBuffer & eclStr,
             IProperties * map,
             bool ignoreMisTranslations,
             bool forHaving,
             bool funcParam,
             bool countFuncParam)
 {

     StringBuffer translation;
     map->getProp(getParentTableName(),translation);

     if (translation.length() == 0 && getParentTableName() != NULL)
         return;

     if ( translation.length() > 0)
     {
         if (funcParam && forHaving && translation.length() > 0)
         {
             eclStr.append(" ROWS ( ");
             eclStr.append(translation);
             eclStr.append(" ) ");
             if (!countFuncParam)
             {
                 eclStr.append(", ");
                 eclStr.append(getName());
             }
         }
         else
         {
             eclStr.append(translation);
             eclStr.append(".");
             eclStr.append(getName());
         }
     }
 }
SQLFieldValueExpression::SQLFieldValueExpression(const char * parentTableName, const char * fieldName)
{
    field.setName(fieldName);
    field.setParenttable(parentTableName);
}

SQLFieldValueExpression::SQLFieldValueExpression()
{
    field.setName("");
    field.setParenttable("");
    field.setAlias("");
}

const char * SQLFieldValueExpression::getParentTableName()
{
    return field.getParenttable();
}

void SQLFieldValueExpression::setParentTableName(const char * parentTableName)
{
    field.setParenttable(parentTableName);
}

const char * SQLFieldValueExpression::getName()
{
    return field.getName();
}

const char * SQLFieldValueExpression::getNameOrAlias()
{
    return field.getColumnNameOrAlias();
}
void SQLFieldValueExpression::setName(const char * fieldName)
{
    field.setName(fieldName);
}

bool SQLFieldValueExpression::containsKey(const char * colname)
{
    return field.isFieldNameOrAalias(colname);
}

void SQLFieldValueExpression::toString(StringBuffer & targetstr, bool fullOutput)
{
    field.toString(targetstr, fullOutput);
}

const char * SQLFieldValueExpression::getAlias()
{
    return field.getAlias();
}

void SQLFieldValueExpression::setAlias(const char * alias)
{
    if (alias)
        field.setAlias(alias);
}

bool SQLFieldValueExpression::isAscending()
{
    return field.isAscending();
}

void SQLFieldValueExpression::setAscending(bool ascending)
{
    field.setAscending(ascending);
}

bool SQLFieldValueExpression::isAliasOrName(const char * possiblenameoralias)
{
    return field.isFieldNameOrAalias(possiblenameoralias);
}

void SQLFieldValueExpression::setECLType(const char * type)
{
    field.setColumnType(type);
}

const char * SQLFieldValueExpression::getECLType()
{
    return field.getColumnType();
}

SQLFieldValueExpression::~SQLFieldValueExpression()
{
#ifdef _DEBUG
    fprintf(stderr, "\ndestroying SQLFieldValueExpression.\n");
#endif
}
/***********************SQLFieldValueExpression END**************************************************************************/

/***********************SQLUnaryExpression START**************************************************************************/

SQLUnaryExpression::SQLUnaryExpression() : operand1(nullptr), op(-1)
{}

SQLUnaryExpression::~SQLUnaryExpression()
{
#ifdef _DEBUG
    fprintf(stderr, "\ndestroying SQLUnaryExpression.\n");
#endif
    operand1->Release();
}

void SQLUnaryExpression::getExpressionFromColumnName(const char * colname, StringBuffer & str)
{
    switch (op)
    {
        case ISNOTNULL:
            str.append(" TRUE ");
            break;
        case ISNULL:
            str.append(" FALSE ");
            break;
        case NOT_SYM:
        case NEGATION:
            str.append(" NOT ");
            operand1->getExpressionFromColumnName(colname, str);
            break;
        default:
            break;
        }
}

SQLUnaryExpression::SQLUnaryExpression(ISQLExpression* operand1, int opname)
{
    this->operand1 =  operand1;
    this->op = opname;
}

bool SQLUnaryExpression::containsKey(const char* colname)
{
    return operand1->containsKey(colname);
}

bool SQLUnaryExpression::containsEqualityCondition(IProperties * map, const char * first, const char * second)
{
    return operand1->containsEqualityCondition(map, first, second);
}

void SQLUnaryExpression::toString(StringBuffer & targetstr, bool fullOutput)
{
    switch (op)
    {
        case ISNOTNULL:
            targetstr.append(" TRUE ");
            break;
        case ISNULL:
            targetstr.append(" FALSE ");
            break;
        case NOT_SYM:
        case NEGATION:
            targetstr.append(" NOT ");
            operand1->toString(targetstr, fullOutput);
            break;
        default:
            operand1->toString(targetstr, fullOutput);
            break;
    }
}

void SQLUnaryExpression::toECLStringTranslateSource(
                    StringBuffer & eclStr,
                    IProperties * map,
                    bool ignoreMisTranslations,
                    bool forHaving,
                    bool funcParam,
                    bool countFuncParam)
     {
        switch (op)
        {
            case ISNOTNULL:
                eclStr.append(" TRUE ");
                break;
            case ISNULL:
                eclStr.append(" FALSE ");
                break;
            case NOT_SYM:
            case NEGATION:
                eclStr.append(" NOT ");
                operand1->toECLStringTranslateSource(eclStr, map, ignoreMisTranslations, forHaving, funcParam, countFuncParam);
                break;
            default:
                operand1->toECLStringTranslateSource(eclStr, map, ignoreMisTranslations, forHaving, funcParam, countFuncParam);
                break;
        }
     }

/***********************SQLUnaryExpression END**************************************************************************/

/***********************SQLParenthesisExpression START**************************************************************************/

SQLParenthesisExpression::~SQLParenthesisExpression()
{
#ifdef _DEBUG
    fprintf(stderr, "\ndestroying SQLParenthesisExpression.\n");
#endif
    innerexpression->Release();
}
SQLParenthesisExpression::SQLParenthesisExpression(ISQLExpression* exp)
{
    this->innerexpression =  exp;
}

bool SQLParenthesisExpression::containsKey(const char* colname)
{
    return this->containsKey(colname);
}

void SQLParenthesisExpression::toString(StringBuffer & targetstr, bool fullOutput)
{
    targetstr.append("( ");
    innerexpression->toString(targetstr, fullOutput);
    targetstr.append(" )");
}
ISQLExpression* SQLParenthesisExpression::getInnerExp()
{
    return innerexpression;
}
/***********************SQLParenthesisExpression END**************************************************************************/

/***********************SQLValueExpression START**************************************************************************/

int SQLValueExpression::setParameterizedNames(int currentindex)
{
    if (hasPlaceHolder())
    {
        placeHolderName.setf("%sPlaceHolder%d", getPlaceHolderType(), currentindex);
        return ++currentindex;
    }
    else
        return currentindex;
}

void SQLValueExpression::toECLStringTranslateSource(
            StringBuffer & eclStr,
            IProperties * map,
            bool ignoreMisTranslations,
            bool forHaving,
            bool funcParam,
            bool countFuncParam)
{
     if (hasPlaceHolder())
        eclStr.append(placeHolderName.str());
    else
        eclStr.append(value.str());
}

SQLValueExpression::SQLValueExpression()
{
    type = -1;
    value = "";
    field.setName("");
    field.setParenttable("");
    field.setAlias("");
    isAWildCardPattern = false;
}

SQLValueExpression::~SQLValueExpression()
{
#ifdef _DEBUG
    fprintf(stderr, "\ndestroying SQLValueExpression.\n");
#endif
}

SQLValueExpression::SQLValueExpression(int type, const char * value)
{
    this->type = type;
    this->value = value;
    isAWildCardPattern = false;
}

void SQLValueExpression::trimTextQuotes()
{
    if (this->type == TEXT_STRING)
    {
        if (this->value.charAt(0) == '\'' && this->value.charAt(value.length()-1) == '\'')
        {
            this->value.remove(this->value.length()-1, 1);
            this->value.remove(0, 1);
        }
    }
}

void SQLValueExpression::toString(StringBuffer & targetstr, bool fullOutput)
{
    if (hasPlaceHolder())
        targetstr.append(placeHolderName.str());
    else
        targetstr.append(value.str());
}

const char * SQLValueExpression::getName()
{
    return field.getName();
}

const char * SQLValueExpression::getNameOrAlias()
{
    return field.getColumnNameOrAlias();
}
void SQLValueExpression::setName(const char * fieldName)
{
    field.setName(fieldName);
}

const char * SQLValueExpression::getAlias()
{
    return field.getAlias();
}

void SQLValueExpression::setAlias(const char * alias)
{
    if (alias)
        field.setAlias(alias);
}

void SQLValueExpression::setECLType(const char * type)
{
    field.setColumnType(type);
}

const char * SQLValueExpression::getECLType()
{
    return field.getColumnType();
}

/***********************SQLValueExpression END**************************************************************************/

/***********************SQLBinaryExpression Start**************************************************************************/
int SQLBinaryExpression::setParameterizedNames(int currentindex)
{
    int op1index = operand1->setParameterizedNames(currentindex);
    return operand2->setParameterizedNames(op1index);
}

bool SQLBinaryExpression::containsEqualityCondition(ISQLExpression* operand, IProperties * map, const char * first, const char * second)
{
    switch (operand->getExpType())
    {
        case Unary_ExpressionType:
        {
            SQLUnaryExpression * unary = dynamic_cast<SQLUnaryExpression *>(operand);
            return unary ? unary->containsEqualityCondition(map, first, second) : false;
        }
        case Binary_ExpressionType:
        {
            SQLBinaryExpression * binary = dynamic_cast<SQLBinaryExpression *>(operand);
            return binary ? binary->isEqualityCondition(map, first, second) : false;
        }
        case Parenthesis_ExpressionType:
        {
            SQLParenthesisExpression * paren = dynamic_cast<SQLParenthesisExpression *>(operand);
            return paren ? containsEqualityCondition(paren, map, first, second) : false;
        }
        default:
            return false;
    }
}

bool SQLBinaryExpression::containsEqualityCondition(IProperties * map, const char * first, const char * second)
{
    if (isEqualityCondition(map, first, second))
    {
        return true;
    }
    else
    {
        bool operand1Hasequality = containsEqualityCondition(operand1, map, first, second);
        bool operand2Hasequality = containsEqualityCondition(operand2, map, first, second);

        if (op == OR_SYM)
            return (operand1Hasequality && operand2Hasequality);
        else
            return (operand1Hasequality || operand2Hasequality);
    }
}

bool SQLBinaryExpression::isEqualityCondition(IProperties * map, const char * first, const char * second)
{
    StringBuffer operand1Translate;
    StringBuffer operand2Translate;

    if (operand1->getExpType() == FieldValue_ExpressionType)
    {
        SQLFieldValueExpression * op1field = dynamic_cast<SQLFieldValueExpression *>(operand1);
        if (op1field)
            map->getProp(op1field->getParentTableName(), operand1Translate);
    }

    if (operand2->getExpType() == FieldValue_ExpressionType)
    {
        SQLFieldValueExpression * op2field = dynamic_cast<SQLFieldValueExpression *>(operand2);
        if (op2field)
            map->getProp(op2field->getParentTableName(), operand2Translate);
    }

    if (operand1Translate.length()<=0 || operand2Translate.length() <= 0)
        return false;

    return (
               strcmp(operand1Translate.str(), operand2Translate.str()) != 0 &&
               (
                   (strcmp(operand1Translate.str(), first)==0 || strcmp(operand2Translate.str(), first)==0)
                   &&
                   (strcmp(operand1Translate.str(), second)==0 || strcmp(operand2Translate.str(), second)==0)
               )
           );
}
void SQLBinaryExpression::toECLStringTranslateSource(
            StringBuffer & eclStr,
            IProperties * map,
            bool ignoreMisTranslations,
            bool forHaving,
            bool funcParam,
            bool countFuncParam)
{
    StringBuffer translation1;
    StringBuffer translation2;

    operand1->toECLStringTranslateSource(translation1, map, ignoreMisTranslations, forHaving, false, false);
    operand2->toECLStringTranslateSource(translation2, map, ignoreMisTranslations, forHaving, false, false);

    if (translation1.length() != 0 && translation2.length() != 0)
    {
        if ( op == LIKE_SYM || op == NOT_LIKE)
        {
            eclStr.append(getOpStr());
            eclStr.append("( ");
            eclStr.append(translation1);
            eclStr.append(" , ");
            eclStr.append(translation2);
            eclStr.append(" , true )");
        }
        else
        {
            eclStr.append(translation1);
            eclStr.append(getOpStr());
            eclStr.append(translation2);
        }
    }
    else if (ignoreMisTranslations)
    {
        if (translation1.length() == 0 && translation2.length() == 0)
             return;

        /*
        * If operand1 or operand2 could not be translated using the translation map,
        * and ignoreMisTranslations = true, we're going to attempt to return an valid
        * ECL translation of this binary expression. IF the binary expression is of type
        * OR | AND, we can substitute the mistranslated operand with the appropriate boolean value
        * to complete the expression with out changing the gist of the expression.
        *
        * This is typically done when converting an SQL 'WHERE' clause or 'ON' clause to ECL to
        * be used in an ECL JOIN function. In any one particular ECL Join funtion only two datasets
        * are joined, therefore not all portions of the SQL logic clause might be relevant.
        *
        */
        if (op == OR_SYM || op == AND_SYM)
        {
            StringBuffer convert( op == OR_SYM ? "FALSE" : "TRUE");

            if (translation1.length() != 0)
            {
                UWARNLOG("Operand 1 of binary expression could not be translated.");
                eclStr.append(translation1);
                eclStr.append(getOpStr());
                eclStr.append(convert);
            }
            else
            {
                UWARNLOG("Operand 2 of binary expression could not be translated.");
                eclStr.append(convert);
                eclStr.append(getOpStr());
                eclStr.append(translation2);
            }
        }
        else
        {
            UWARNLOG("Binary expression could not be translated.");
            return;
        }
    }
    else
    {
        UWARNLOG("Binary expression could not be translated.");
        return;
    }
}

SQLBinaryExpression::SQLBinaryExpression(int op,ISQLExpression* operand1,ISQLExpression* operand2)
{
    this->operand1 = operand1;
    this->op = op;
    this->operand2 = operand2;
}

SQLBinaryExpression::~SQLBinaryExpression()
{
#ifdef _DEBUG
    fprintf(stderr, "\ndestroying SQLBinaryExpression.\n");
#endif
    if (operand1)
        operand1->Release();
    if (operand2)
        operand2->Release();
}

bool SQLBinaryExpression::containsKey(const char * colname)
{
    return operand1->containsKey(colname) || operand2->containsKey(colname);
}

void SQLBinaryExpression::toString(StringBuffer & targetstr, bool fullOutput)
{
    if ( op == LIKE_SYM || op == NOT_LIKE)
    {
        targetstr.append(getOpStr());
        targetstr.append("( ");
        operand1->toString(targetstr, fullOutput);
        targetstr.append(" , ");
        operand2->toString(targetstr, fullOutput);
        targetstr.append(" , true )");
    }
    else
    {
        operand1->toString(targetstr, fullOutput);
        targetstr.append(getOpStr());
        operand2->toString(targetstr, fullOutput);
    }
}

const char * SQLBinaryExpression::getOpStr()
{
    switch (op)
    {
        case AND_SYM:
            return " AND ";
        case DIVIDE:
            return " / ";
        case EQ_SYM:
            return " = ";
        case GTH:
            return " > ";
        case GET:
            return " >= ";
        case LTH:
            return " < ";
        case LET:
            return " <= ";
        case MINUS:
            return " - ";
        case MOD:
            return " % ";
        case ASTERISK:
            return " * ";
        case NOT_EQ:
            return " != ";
        case OR_SYM:
            return " OR ";
        case PLUS:
            return " + ";
        case IN_SYM:
            return " IN ";
        case NOT_IN:
            return " NOT IN ";
        case NOT_LIKE:
            return " NOT STD.Str.WildMatch";
        case LIKE_SYM:
            return " STD.Str.WildMatch";
        default:
            return " ";
    }
}
int SQLBinaryExpression::getExpressionsCount()
{
    return operand1->getExpressionsCount() + operand2->getExpressionsCount();
}
/***********************SQLBinaryExpression END**************************************************************************/

/***********************SQLParameterPlaceHolderExpression START**********************************************************/
const char * SQLParameterPlaceHolderExpression::PARAMPREFIX = "PARAM";

int SQLParameterPlaceHolderExpression::setParameterizedNames(int currentindex)
{
    value.set(PARAMPREFIX);
    value.append(currentindex);
    return ++currentindex;
}

SQLParameterPlaceHolderExpression::SQLParameterPlaceHolderExpression() : index(-1)
{
}

SQLParameterPlaceHolderExpression::~SQLParameterPlaceHolderExpression()
{
#ifdef _DEBUG
    fprintf(stderr, "\ndestroying SQLParameterPlaceHolderExpression.\n");
#endif
}

void SQLParameterPlaceHolderExpression::toString(StringBuffer & targetstr, bool fullOutput)
{
    targetstr.append(value.str());
}

/***********************SQLParameterPlaceHolderExpression END********************************************************/

/***********************SQLFunctionExpression START**********************************************************/

int SQLFunctionExpression::setParameterizedNames(int currentindex)
{
    int paramundex = currentindex;
    ForEachItemIn(paramidx , params)
    {
        ISQLExpression & param = params.item(paramidx);
        paramundex = param.setParameterizedNames(currentindex);
    }

    return paramundex;
}
void SQLFunctionExpression::toECLStringTranslateSource(
            StringBuffer & eclStr,
            IProperties * map,
            bool ignoreMisTranslations,
            bool forHaving,
            bool funcParam,
            bool countFuncParam)
{

    eclStr.append(function.eclFunctionName);
    eclStr.append("( ");

    ForEachItemIn(paramidx , params)
    {
       ISQLExpression & param = params.item(paramidx);
       StringBuffer translation;
       param.toECLStringTranslateSource(translation, map, ignoreMisTranslations, forHaving, true, strncmp(function.name,"COUNT", 5)==0);
       if (!ignoreMisTranslations && translation.length()<=0)
           return;
       if ( paramidx != 0 )
           eclStr.append(", ");
       eclStr.append(translation);
    }
    eclStr.append(" )");
}
SQLFunctionExpression::SQLFunctionExpression(const char* funcname)
{
    setFunction(funcname);
    this->alias = "";
    distinct = false;
}

SQLFunctionExpression::SQLFunctionExpression(const char* funcname, const IArrayOf<ISQLExpression> &params)
{
    setFunction(funcname);
    this->params = params;
    this->alias = "";
    distinct = false;
}

const char * SQLFunctionExpression::getAlias()
{
    return alias.str();
}

void SQLFunctionExpression::setAlias(const char * something)
{
    this->alias.append(something);
}

SQLFunctionExpression::~SQLFunctionExpression()
{
#ifdef _DEBUG
    fprintf(stderr, "Leaving SQLFunctionExpression");
#endif
    params.kill(false);
}

void SQLFunctionExpression::getParamsString(StringBuffer & targetstr, bool fullOutput)
{
    int paramcount = params.length();
    for (int i = 0; i < paramcount; i++)
    {
        if ( i != 0 )
            targetstr.append(", ");
        ISQLExpression & exp = params.item(i);
        exp.toString(targetstr, fullOutput);
    }
}

void SQLFunctionExpression::toString(StringBuffer & targetstr, bool fullOutput)
{
    targetstr.append(function.eclFunctionName);
    targetstr.append(" ( ");
    getParamsString(targetstr, fullOutput);
    targetstr.append(" )");
}

bool SQLFunctionExpression::containsKey(const char * colname)
{
    ForEachItemIn(Idx, params)
    {
        if (params.item(Idx).containsKey(colname))
            return true;
    }

    return false;
}

int SQLFunctionExpression::getExpressionsCount()
{
    int count = 0;
    ForEachItemIn(paramidx , params)
    {
        count += params.item(paramidx).getExpressionsCount();
    }
    return count;
}

const char * SQLFunctionExpression::getNameOrAlias()
{
    if (alias.length() > 0)
           return getAlias();
       else
           return getName();
}
/***********************SQLFunctionExpression END********************************************************/


/************************SQLListExpression Start *************************************************************************/

SQLListExpression::SQLListExpression(){}
SQLListExpression::~SQLListExpression()
{
#ifdef _DEBUG
    fprintf(stderr, "\ndestroying SQLListExpression.\n");
#endif
    entries.kill(false);
}

int SQLListExpression::getExpressionsCount()
{
    int count = 0;
    ForEachItemIn(paramidx , entries)
    {
        count += entries.item(paramidx).getExpressionsCount();
    }
    return count;
}

void SQLListExpression::getExpressionFromColumnName(const char * colname, StringBuffer & str)
{
    StringBuffer paramlist;

    StringBuffer paramresult;
    ForEachItemIn(idx, entries)
    {
       entries.item(idx).getExpressionFromColumnName(colname, paramresult.clear());
    
       if (paramresult.length() == 0)
           return;
    
       if ( idx > 0 )
           paramlist.append(", ");
    
       paramlist.append(paramresult);
    }
    
    if (paramlist.length()>0)
        str.appendf(" [ %s ] ", paramlist.str());
}

void SQLListExpression::getUniqueExpressionColumnNames(StringArray &  uniquenames)
{
    ForEachItemIn(paramidx , entries)
    {
       ISQLExpression & param = entries.item(paramidx);
       param.getUniqueExpressionColumnNames(uniquenames);
    }
}

void SQLListExpression::eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype)
{
    ForEachItemIn(paramidx , entries)
    {
        ISQLExpression & param = entries.item(paramidx);
        param.eclDeclarePlaceHolders(eclstr, op, sibtype);
    }
}

SQLLogicType SQLListExpression::getLogicType()
{
        return Bool_LogicType;
}

int SQLListExpression::setParameterizedNames(int currentindex)
{
    int paramundex = currentindex;
    ForEachItemIn(paramidx , entries)
    {
        ISQLExpression & param = entries.item(paramidx);
        paramundex = param.setParameterizedNames(currentindex);
    }

    return paramundex;
}

void SQLListExpression::toECLStringTranslateSource(
                    StringBuffer & eclStr,
                    IProperties * map,
                    bool ignoreMisTranslations,
                    bool forHaving,
                    bool funcParam,
                    bool countFuncParam)
{
    eclStr.append("[ ");
    ForEachItemIn(paramidx , entries)
    {
       ISQLExpression & param = entries.item(paramidx);
       StringBuffer translation;
       param.toECLStringTranslateSource(translation, map, ignoreMisTranslations, forHaving, funcParam, countFuncParam);
       if (!ignoreMisTranslations && translation.length()<=0)
           return;
       if ( paramidx != 0 )
           eclStr.append(", ");
       eclStr.append(translation);
    }
    eclStr.append(" ]");
}


bool SQLListExpression::containsKey(const char* colname)
{
    ForEachItemIn(Idx, entries)
    {
        if (entries.item(Idx).containsKey(colname))
            return true;
    }
    return false;
}
void SQLListExpression::toString(StringBuffer & targetstr, bool fullOutput)
{
    targetstr.append(" [ ");
    int entrycount = entries.length();
    ForEachItemIn(Idx, entries)
    {
        if ( Idx != 0 )
            targetstr.append(", ");

        ISQLExpression & exp = entries.item(Idx);
        exp.toString(targetstr, fullOutput);
    }

    targetstr.append(" ] ");
}

void SQLListExpression::appendEntry(ISQLExpression * entry)
{
    entries.append(*entry);
}

/************************SQLListExpression end *************************************************************************/
