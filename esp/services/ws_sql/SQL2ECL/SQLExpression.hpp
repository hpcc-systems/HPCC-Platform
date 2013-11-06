/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#include "ws_sql.hpp"
#include "SQLColumn.hpp"
#include "ECLFunction.hpp"

/* undef SOCKET definitions to avoid collision in Antlrdefs.h*/
#ifdef INVALID_SOCKET
    //#pragma message( "UNDEFINING INVALID_SOCKET - Will be redefined by ANTLRDEFS.h" )
    #undef INVALID_SOCKET
#endif
#ifdef SOCKET
    //#pragma message( "UNDEFINING SOCKET - Will be redefined by ANTLRDEFS.h" )
    #undef SOCKET
#endif
#include "HPCCSQLLexer.h"
/* undef SOCKET definitions to avoid collision in Antlrdefs.h*/

#ifndef SQLEXPRESSION_HPP_
#define SQLEXPRESSION_HPP_

typedef enum _SQLExpressionType
{
    Unary_ExpressionType,
    FieldValue_ExpressionType,
    Fields_ExpressionType,
    Parenthesis_ExpressionType,
    Value_ExpressionType,
    Binary_ExpressionType,
    ParameterPlaceHolder_ExpressionType,
    Function_ExpressionType,
    List_ExpressionType
} SQLExpressionType;

typedef enum _SQLLogicType
{
    Unknown_LogicType=-1,
    Bool_LogicType,
    String_LogicType,
    QSstring_LogicType,
    Unicode_LogicType,
    Numeric_LogicType,
    Integer_LogicType,
    Decimal_LogicType
} SQLLogicType;

#define parameterizedPrefix "PARAM";

interface ISQLExpression : public CInterface, public IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    /*
    * Reports whether this expression contains reference to field 'colname'.
    * @param String name of column
    *
    * @return bool Reports whether this expression contains reference to field 'colname'.
    */
    virtual bool containsKey(const char * colname) = 0;

    /*
    * appends string representation of this SQLExpression, fully qualifies field values
    * depending on input param's value.
    * @param boolean Fully qualify field values.
    *
    * @return appends string Representation of this SQLExpression, fields fully-qualified baed on in param.
    */
    virtual void toString(StringBuffer & targetstr, bool fullOutput) = 0;

    /*
    * Returns number of field value expressions contained within SQL Expression.
    *
    * @return int number of field value expressions contained within SQL Expression.
    */
    virtual int getExpressionsCount() = 0;


    /*
    * Translates SQLExpression to ECL in flat string form.
    * @param map IProperties containing source table names as keys and its translation target
    * @param ignoreMistranslations option to continue translating expression even if
    * current expression contains table.field where table not found in map
    * @param forHaving option to translate for ECL HAVING function
    * @param funcParam option specifying if expression to be translated is a function parameter
    * @param countFuncParam option specifying if expression to be translated is an ECL Count() parameter
    *
    * @return appends string ECL consumable representation of expression.
    */
    virtual void toECLStringTranslateSource(StringBuffer & eclStr,
            IProperties * map,
            bool ignoreMisTranslations,
            bool forHaving,
            bool funcParam,
            bool countFuncParam)=0;

    /*
    * Reports if this SQL expression contains an equality condition between table first and second.
    *
    * @param map IProperties containing source table names and their possible ECL translation
    * @param String Name of first table.
    * @param String Name of second table.
    * @return bool True if this expression was found to contain equality condition between
    * table 'first' and table 'second'.
    */
    virtual bool containsEqualityCondition(IProperties * map, const char * first, const char * second)
    {
        return false;
    }

    /*
    * Attempts to label each parameterized expression placeholder, increases the
    * running index value by the number of param placeholder processed.
    *
    * @param int currentindex index used to as postfix for placeholder label
    *
    * @return int running index value. To be used for next param placeholder label.
    */
    virtual int setParameterizedNames(int currentindex)=0;

    virtual bool needsColumnExpansion() { return false;}

    virtual void appendField(ISQLExpression * field){}

    virtual void setECLType(const char * type) {UNIMPLEMENTED;}
    virtual const char * getECLType() {UNIMPLEMENTED; return NULL;}
    virtual const char * getName() {UNIMPLEMENTED;}
    virtual void   setName(const char * name) {UNIMPLEMENTED;}
    virtual const char * getNameOrAlias() {UNIMPLEMENTED;}
    virtual void   setAlias(const char * alias) {UNIMPLEMENTED;}
    virtual const char * getAlias() {UNIMPLEMENTED;}
    virtual SQLExpressionType getExpType()=0;

    virtual SQLLogicType getLogicType(){UNIMPLEMENTED;};
    virtual void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype){UNIMPLEMENTED;};
    virtual void getUniqueExpressionColumnNames(StringArray & uniquenames)=0;
    virtual void getExpressionFromColumnName(const char * colname, StringBuffer & str)=0;
};

/*************************************************************************************************/
class SQLListExpression : implements ISQLExpression
{
public:
    IMPLEMENT_IINTERFACE;

    SQLListExpression();
    virtual ~SQLListExpression();
    void getExpressionFromColumnName(const char * colname, StringBuffer & str);
    void getUniqueExpressionColumnNames(StringArray &  uniquenames);
    void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype);
    SQLLogicType getLogicType();
    int setParameterizedNames(int currentindex);
    void toECLStringTranslateSource(StringBuffer & eclStr, IProperties * map, bool ignoreMisTranslations, bool forHaving, bool funcParam, bool countFuncParam);
    SQLExpressionType getExpType() { return List_ExpressionType;}
    bool containsKey(const char* colname);
    void toString(StringBuffer & targetstr, bool fullOutput);
    int getExpressionsCount();
    void appendEntry(ISQLExpression * entry);

private:
    IArrayOf<ISQLExpression> entries;
};

/*************************************************************************************************/
class SQLUnaryExpression : implements ISQLExpression
{
public:
    IMPLEMENT_IINTERFACE;
    void getExpressionFromColumnName(const char * colname, StringBuffer & str);
    void getUniqueExpressionColumnNames(StringArray &  uniquenames)
    {
        operand1->getUniqueExpressionColumnNames(uniquenames);
    }

    void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype)
    {
        operand1->eclDeclarePlaceHolders(eclstr, op, sibtype);
    }

    SQLLogicType getLogicType(){ return operand1->getLogicType();}

    int setParameterizedNames(int currentindex)
    {
        return operand1->setParameterizedNames(currentindex);
    }

    void toECLStringTranslateSource(StringBuffer & eclStr, IProperties * map, bool ignoreMisTranslations, bool forHaving, bool funcParam, bool countFuncParam);
    bool containsEqualityCondition(IProperties * map, const char * first, const char * second);
    SQLExpressionType getExpType() { return Unary_ExpressionType;}
    SQLUnaryExpression();
    SQLUnaryExpression(ISQLExpression* operand1, int opname);
    virtual ~SQLUnaryExpression();
    bool containsKey(const char* colname);
    void toString(StringBuffer & targetstr, bool fullOutput);

    int getOp() const
    {
        return op;
    }

    void setOp(int op)
    {
        this->op = op;
    }

    ISQLExpression* getOperand1() const
    {
        return operand1;
    }

    void setOperand1(ISQLExpression* operand1)
    {
        this->operand1 = operand1;
    }

    int getExpressionsCount() { return operand1->getExpressionsCount();}

private:
    ISQLExpression * operand1;
    int op;
};

/*************************************************************************************************/
class SQLFieldValueExpression : implements ISQLExpression
{
public:
    IMPLEMENT_IINTERFACE;

    void getExpressionFromColumnName(const char * colname, StringBuffer & str)
    {
        if(stricmp(getName(),colname)==0)
            toString(str, false);
    }

    void getUniqueExpressionColumnNames(StringArray & uniquenames)
    {
        uniquenames.appendUniq(getName());
    }

    void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype) {return;}
    SQLLogicType getLogicType();
    virtual int setParameterizedNames(int currentindex)
    {
        return currentindex;
    }

    void toECLStringTranslateSource(StringBuffer & eclStr, IProperties * map, bool ignoreMisTranslations, bool forHaving, bool funcParam, bool countFuncParam);
    SQLExpressionType getExpType() { return FieldValue_ExpressionType;}
    SQLFieldValueExpression();
    SQLFieldValueExpression(const char * parentTableName, const char * fieldName);
    virtual ~SQLFieldValueExpression();

    const char * getParentTableName();
    void setParentTableName(const char * parentTableName);
    const char * getName();
    void setName(const char * name);
    const char * getAlias();
    void setAlias(const char * alias);
    bool isAscending();
    void setAscending(bool ascending);
    bool containsKey(const char * colname);
    void toString(StringBuffer & targetstr, bool fullOutput);
    int getExpressionsCount() { return 1;}
    bool isAliasOrName(const char * possiblenameoralias);
    void setECLType(const char * type);
    const char * getECLType();
    const char * getNameOrAlias();

    SQLColumn * queryField()
    {
        return &field;
    }

private:
    SQLColumn field;
};

/*************************************************************************************************/
class SQLFieldsExpression : implements ISQLExpression
{
public:
    IMPLEMENT_IINTERFACE;
    void getExpressionFromColumnName(const char * colname, StringBuffer & str){ UNIMPLEMENTED_X("This method should never be accessed for SQLFieldsExpression!");}
    void getUniqueExpressionColumnNames(StringArray & uniquenames){ UNIMPLEMENTED_X("This method should never be accessed for SQLFieldsExpression!");}

    void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype) {return;}
    virtual SQLLogicType getLogicType(){return Unknown_LogicType;}
    virtual int setParameterizedNames(int currentindex)
    {
        return currentindex;
    }

    void toECLStringTranslateSource(
                StringBuffer & eclStr,
                IProperties * map,
                bool ignoreMisTranslations,
                bool forHaving,
                bool funcParam,
                bool countFuncParam) { UNIMPLEMENTED_X("This method should never be accessed for SQLFieldsExpression!");}

    SQLExpressionType getExpType() { return Fields_ExpressionType;}

    SQLFieldsExpression(bool allfiles);
    SQLFieldsExpression(const char * table);
    virtual ~SQLFieldsExpression();
    int getExpressionsCount() { return 1;}
    void toString(StringBuffer & targetstr, bool fullOutput);
    bool containsKey(const char * colname) {return false;}

    bool isAll() const
    {
        return all;
    }

    void setAll(bool all)
    {
        this->all = all;
    }

    const char* getTable() const
    {
        return this->table.toCharArray();
    }

    void setTable(const char* table)
    {
        this->table.set(table);
    }

    bool needsColumnExpansion() { return !isExpanded; }

private:
    StringBuffer table;
    bool all;
    bool isExpanded;
};

/*************************************************************************************************/
class SQLParenthesisExpression : implements ISQLExpression
{
public:
    IMPLEMENT_IINTERFACE;

    void getExpressionFromColumnName(const char * colname, StringBuffer & str)
    {
       StringBuffer result1;
       innerexpression->getExpressionFromColumnName(colname, result1);

       //if (result1.length > 0 )
           str.appendf("( %s )", result1.str());
    }

    void getUniqueExpressionColumnNames(StringArray & uniquenames)
    {
        innerexpression->getUniqueExpressionColumnNames(uniquenames);
    }

    void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype)
    {
        innerexpression->eclDeclarePlaceHolders(eclstr, op, sibtype);
    }

    virtual SQLLogicType getLogicType(){return innerexpression->getLogicType();}
    virtual int setParameterizedNames(int currentindex)
    {
        return innerexpression->setParameterizedNames(currentindex);
    }

    void toECLStringTranslateSource(
                        StringBuffer & eclStr,
                        IProperties * map,
                        bool ignoreMisTranslations,
                        bool forHaving,
                        bool funcParam,
                        bool countFuncParam)
        {
            eclStr.append("( ");
            innerexpression->toECLStringTranslateSource(eclStr, map, ignoreMisTranslations, forHaving, funcParam, countFuncParam);
            eclStr.append(" )");
        }

    ISQLExpression* getInnerExp();
    SQLExpressionType getExpType() { return Parenthesis_ExpressionType;}
    SQLParenthesisExpression(ISQLExpression * exp);
    virtual ~SQLParenthesisExpression();

    bool containsKey(const char * colname);
    void toString(StringBuffer & targetstr, bool fullOutput);
    int getExpressionsCount() { return innerexpression->getExpressionsCount();}

private:
    ISQLExpression* innerexpression;
};

/*************************************************************************************************/
class SQLValueExpression : implements ISQLExpression
{
public:
    IMPLEMENT_IINTERFACE;

    void getExpressionFromColumnName(const char * colname, StringBuffer & str)
    {
        str.appendf(" %s ", value.str());
    }

    void getUniqueExpressionColumnNames(StringArray & uniquenames) { return; }

    void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype) {return;}

    virtual SQLLogicType getLogicType()
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
        else if (strnicmp(type,"DECIMAL",78)==0)
            return Decimal_LogicType;
        else
            return Unknown_LogicType;
    }

    virtual int setParameterizedNames(int currentindex)
    {
        return currentindex;
    }

    void toECLStringTranslateSource(
                StringBuffer & eclStr,
                IProperties * map,
                bool ignoreMisTranslations,
                bool forHaving,
                bool funcParam,
                bool countFuncParam);

    SQLExpressionType getExpType() { return Value_ExpressionType;}
    SQLValueExpression();
    SQLValueExpression(int type, const char * value);
    virtual ~SQLValueExpression();

    bool containsKey(const char * colname) {return false;}
    void toString(StringBuffer & targetstr, bool fullOutput);

    int getType() const
    {
        return type;
    }

    void setType(int type)
    {
        this->type = type;
    }

    const char * getValue() const
    {
        return value.str();
    }

    void setValue(const char * value)
    {
        this->value.set(value);
    }

    int getExpressionsCount()
    {
        return 0;
    }

    virtual void         setECLType(const char * type);
    virtual const char * getECLType();
    virtual const char * getName();
    virtual void         setName(const char * name);
    virtual const char * getNameOrAlias();
    virtual void         setAlias(const char * alias);
    virtual const char * getAlias();

private:
    int type; //As defined in HPCCSQLParser.h
    StringBuffer value;
    SQLColumn field;
};

/*************************************************************************************************/
class SQLBinaryExpression : implements ISQLExpression
{
public:
    IMPLEMENT_IINTERFACE;

    void getExpressionFromColumnName(const char * colname, StringBuffer & str)
    {
        StringBuffer result1;
        StringBuffer result2;
        operand1->getExpressionFromColumnName(colname, result1);
        operand2->getExpressionFromColumnName(colname, result2);

        if (result1.length() > 0 && result2.length() > 0)
        {
            str.appendf("%s %s %s", result1.str(), getOpStr(),result2.str());
        }
        else if (op == OR_SYM)
        {
            if (result1.length() > 0)
               str.append(result1);
            else if (result2.length() > 0)
                str.append(result2);
        }
    }

    void getUniqueExpressionColumnNames(StringArray & uniquenames)
    {
        operand1->getUniqueExpressionColumnNames(uniquenames);
        operand2->getUniqueExpressionColumnNames(uniquenames);
    }

    void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype)
    {
        operand1->eclDeclarePlaceHolders(eclstr, this->op, operand2->getLogicType());
        operand2->eclDeclarePlaceHolders(eclstr, this->op, operand1->getLogicType());
    }

    virtual SQLLogicType getLogicType()
    {
        switch (op)
        {
            case AND_SYM:
            case OR_SYM:
                return Bool_LogicType;
            case DIVIDE:
            case GTH:
            case GET:
            case LTH:
            case LET:
            case MINUS:
            case MOD:
            case ASTERISK:
            case PLUS:
                return Numeric_LogicType;
            case EQ_SYM:
            case NOT_EQ:
            case IN_SYM:
            case NOT_IN:
            {
                SQLLogicType op1type =operand1->getLogicType();
                SQLLogicType op2type =operand2->getLogicType();
                if (op1type != Unknown_LogicType)
                    return op1type;
                else if (op2type != Unknown_LogicType)
                    return op2type;
                else
                    return Unknown_LogicType;
            }
            default:
                return Unknown_LogicType;
        }
    }

    virtual int setParameterizedNames(int currentindex);
    bool isEqualityCondition(IProperties * map, const char * first, const char * second);
    virtual bool containsEqualityCondition(IProperties * map, const char * first, const char * second);
    static bool containsEqualityCondition(ISQLExpression* operand, IProperties * map, const char * first, const char * second);
    void toECLStringTranslateSource(
            StringBuffer & eclStr,
            IProperties * map,
            bool ignoreMisTranslations,
            bool forHaving,
            bool funcParam,
            bool countFuncParam);

    SQLExpressionType getExpType() { return Binary_ExpressionType;}
    SQLBinaryExpression(int op,ISQLExpression* operand1,ISQLExpression* operand2);
    virtual ~SQLBinaryExpression();

    bool containsKey(const char * colname);
    void toString(StringBuffer & targetstr, bool fullOutput);
    int getExpressionsCount();

    int getOp() const
    {
        return op;
    }

    void setOp(int op)
    {
        this->op = op;
    }

    ISQLExpression* getOperand1() const
    {
        return operand1;
    }

    void setOperand1(ISQLExpression* operand1)
    {
        this->operand1 = operand1;
    }

    ISQLExpression* getOperand2() const
    {
        return operand2;
    }

    void setOperand2(ISQLExpression* operand2)
    {
        this->operand2 = operand2;
    }

private:
    const char * getOpStr();
    ISQLExpression* operand1;
    ISQLExpression* operand2;
    int op;
};


/*************************************************************************************************/
class SQLParameterPlaceHolderExpression : implements ISQLExpression
{
public:
    static const char * PARAMPREFIX;

    IMPLEMENT_IINTERFACE;

    void getExpressionFromColumnName(const char * colname, StringBuffer & str) { return; }
    void getUniqueExpressionColumnNames(StringArray & uniquenames) { return; }

    void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype)
    {
        StringBuffer defaulteclvalue;
        switch (op)
        {
            case AND_SYM:
            case OR_SYM:
                eclstr.append( "BOOLEAN ");
                defaulteclvalue.set(" FALSE ");
                break;
            case DIVIDE:
            case MINUS:
            case MOD:
            case ASTERISK:
            case GTH:
            case GET:
            case LTH:
            case LET:
            case PLUS:
            case EQ_SYM:
            case NOT_EQ:
            case IN_SYM:
            case NOT_IN:
            {
                switch (sibtype)
                {
                    case Bool_LogicType:
                        eclstr.append( "BOOLEAN ");
                        defaulteclvalue.set(" FALSE ");
                        break;
                    case Numeric_LogicType:
                    case Integer_LogicType:
                        eclstr.append( "INTEGER ");
                        defaulteclvalue.set(" 0 ");
                        break;
                    case Decimal_LogicType:
                        eclstr.append( "DECIMAL ");
                        defaulteclvalue.set(" 0.0 ");
                        break;
                    case QSstring_LogicType:
                        eclstr.append( "QSTRING ");
                        defaulteclvalue.set(" '' ");
                        break;
                    case Unicode_LogicType:
                        eclstr.append( "UNICODE");
                        defaulteclvalue.set(" '' ");
                        break;
                    case String_LogicType:
                    default:
                        eclstr.append( "STRING ");
                        defaulteclvalue.set(" '' ");
                        break;
                }
                break;
            }
            default:
                eclstr.append( "STRING ");
                defaulteclvalue.set(" '' ");
                break;
        }

        eclstr.append(value.str());
        eclstr.append(" := ");
        eclstr.append(defaulteclvalue.str());
        eclstr.append(" : STORED('");
        eclstr.append(value.str());
        eclstr.append("');\n");

    }

    virtual SQLLogicType getLogicType(){return Unknown_LogicType;}
    virtual int setParameterizedNames(int currentindex);
    void toECLStringTranslateSource(
                    StringBuffer & eclStr,
                    IProperties * map,
                    bool ignoreMisTranslations,
                    bool forHaving,
                    bool funcParam,
                    bool countFuncParam)
    {
        eclStr.append( value.toCharArray() );
    }

    SQLExpressionType getExpType()
    {
        return ParameterPlaceHolder_ExpressionType;
    }

    SQLParameterPlaceHolderExpression();
    virtual ~SQLParameterPlaceHolderExpression ();

    bool containsKey(const char * colname) {return false;}
    void toString(StringBuffer & targetstr, bool fullOutput);
    int getExpressionsCount() {return 0;}
private:

    int index;
    StringBuffer value;
};

/*************************************************************************************************/
class SQLFunctionExpression : public ISQLExpression
{
public:
    IMPLEMENT_IINTERFACE;

    void getExpressionFromColumnName(const char * colname, StringBuffer & str)
    {
        StringBuffer paramlist;

        StringBuffer paramresult;
        for (int i = 0; i < params.length(); i++)
        {
            params.item(i).getExpressionFromColumnName(colname, paramresult.clear());

            if (paramresult.length() <= 0)
                return;

            if ( i > 0 )
                paramlist.append(", ");

            paramlist.append(paramresult);
        }

        if (paramlist.length()>0)
        {
            str.append(function.eclFunctionName);
            str.append("( ");
            str.append( paramlist );
            str.append(" )");
        }
    }

    void getUniqueExpressionColumnNames(StringArray & uniquenames)
    {
       ForEachItemIn(paramidx , params)
       {
           ISQLExpression & param = params.item(paramidx);
           param.getUniqueExpressionColumnNames(uniquenames);
       }
    }

    void eclDeclarePlaceHolders(StringBuffer & eclstr, int op, int sibtype)
    {
        ForEachItemIn(paramidx , params)
        {
            ISQLExpression & param = params.item(paramidx);
            param.eclDeclarePlaceHolders(eclstr, op, sibtype);
        }
    }

    SQLLogicType getLogicType()
    {
        if (strcmp(function.returnType,"NUMERIC")==0)
        {
            if (params.length()>0)
                return params.item(0).getLogicType();
            else
                return Numeric_LogicType;
        }
        else if (strcmp(function.returnType,"INTEGER")==0)
            return Integer_LogicType;
        else
            return String_LogicType;
    }

    int setParameterizedNames(int currentindex);

    void toECLStringTranslateSource(
                StringBuffer & eclStr,
                IProperties * map,
                bool ignoreMisTranslations,
                bool forHaving,
                bool funcParam,
                bool countFuncParam);

    SQLExpressionType getExpType() { return Function_ExpressionType;}
    SQLFunctionExpression(const char* funcname);
    SQLFunctionExpression(const char* funcname, const IArrayOf<ISQLExpression> &params);
    virtual ~SQLFunctionExpression();
    bool containsKey(const char* colname);
    void toString(StringBuffer & targetstr, bool fullOutput);

    void setName(const char * funcname)
    {
        name.set(funcname);
    }

    void setNameAndDefaultAlias(const char * funcname)
    {
        name.set(funcname);
        alias.set(funcname);
        alias.append("Out");
    }

    const char * getName()
    {
        return name.str();
    }

    ECLFunctionDefCfg getFunction() const
    {
        return function;
    }

    void setFunction(const char * funcname)
    {
        this->function = ECLFunctions::getEclFuntionDef(funcname);
    }

    IArrayOf<ISQLExpression> * getParams()
    {
        return &params;
    }

    virtual const char * getAlias();
    virtual void setAlias(const char * alias);
    virtual const char * getNameOrAlias();
    void addParams(ISQLExpression * param)
    {
        this->params.append(*param);
    }

    int getExpressionsCount();

    bool isDistinct() {return distinct;}
    void setDistinct(bool d) {distinct = d;}

private:
    bool distinct;
    StringBuffer name;
    StringBuffer alias;
    ECLFunctionDefCfg function;
    IArrayOf<ISQLExpression> params;
    void getParamsString(StringBuffer & targetstr, bool fullOutput);
};

#endif /* SQLEXPRESSION_HPP_ */
