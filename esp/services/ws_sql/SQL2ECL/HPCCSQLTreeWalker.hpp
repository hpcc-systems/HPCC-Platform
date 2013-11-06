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

#ifndef HPCCSQLTREEWALKER_HPP_
#define HPCCSQLTREEWALKER_HPP_
#include <stdlib.h>
#include "ws_sql.hpp"

/* undef SOCKET definitions to avoid collision in Antlrdefs.h*/
#ifdef INVALID_SOCKET
    //#pragma message("UNDEFINING INVALID_SOCKET - Will be redefined by ANTLRDEFS.h" )
    #undef INVALID_SOCKET
#endif
#ifdef SOCKET
    //#pragma message( "UNDEFINING SOCKET - Will be redefined by ANTLRDEFS.h" )
    #undef SOCKET
#endif
/* undef SOCKET definitions to avoid collision in Antlrdefs.h*/

#include "HPCCSQLLexer.h"
#include "HPCCSQLParser.h"


#include "SQLColumn.hpp"
#include "SQLTable.hpp"
#include "SQLExpression.hpp"
#include "HPCCFile.hpp"
#include "HPCCFileCache.hpp"
#include "ECLFunction.hpp"
#include "SQLJoin.hpp"

typedef enum _SQLQueryType
{
    SQLTypeUnknown=-1,
    SQLTypeSelect,
    SQLTypeCall
} SQLQueryType;

class HPCCSQLTreeWalker: public CInterface, public IInterface
{
public:
    IMPLEMENT_IINTERFACE;

private:
    SQLQueryType sqlType;
    IArrayOf<ISQLExpression> selectList;
    IArrayOf<ISQLExpression> groupbyList;
    IArrayOf<ISQLExpression> orderbyList;
    IArrayOf<SQLTable> tableList;

    int limit;
    int offset;
    string indexhint;
    Owned<ISQLExpression> whereClause;
    Owned<ISQLExpression> havingClause;
    Owned<HPCCFileCache> tmpHPCCFileCache;

    bool selectDistinct;

    StringBuffer procedureName;
    StringBuffer querySetName;
    IArrayOf<ISQLExpression> paramList;

    void sqlTreeWalker(pANTLR3_BASE_TREE sqlAST);
    void selectStatementTreeWalker(pANTLR3_BASE_TREE selectsqlAST);
    void callStatementTreeWalker(pANTLR3_BASE_TREE callsqlAST);
    void columnListTreeWalker(pANTLR3_BASE_TREE columnsAST, IArrayOf<SQLColumn>& collist);
    ISQLExpression* expressionTreeWalker(pANTLR3_BASE_TREE exprAST, pANTLR3_BASE_TREE parent);
    void fromTreeWalker(pANTLR3_BASE_TREE fromsqlAST);
    void limitTreeWalker(pANTLR3_BASE_TREE limitAST);
    void processAllColumns(HpccFiles *  availableFiles);
    void verifyColAndDisabiguateName();
    void assignParameterIndexes();
    int parameterizedCount;

    StringBuffer normalizedSQL;

public:

    bool isParameterizedCall();
    void setQuerySetName(const char * qsname)
    {
        querySetName.set(qsname);
    }

    const char * getQuerySetName()
    {
        return querySetName.str();
    }

    const char * getStoredProcName()
    {
        return procedureName.str();
    }

    int getStoredProcParamListCount()
    {
        return paramList.length();
    }

    IArrayOf<ISQLExpression>* getStoredProcParamList()
    {
        return &paramList;
    }

    void getWhereClauseString(StringBuffer & str);
    void getHavingClauseString(StringBuffer & str);
    ISQLExpression * getHavingClause();

    void expandWildCardColumn();
    HPCCSQLTreeWalker();
    HPCCSQLTreeWalker(pANTLR3_BASE_TREE t, IEspContext &context);
    virtual ~HPCCSQLTreeWalker();

    int getParameterizedCount() const
    {
        return parameterizedCount;
    }

    IArrayOf<SQLTable>* getTableList()
    {
        return &tableList;
    }

    IArrayOf<ISQLExpression>* getSelectList()
    {
        return &selectList;
    }

    int getLimit() const
    {
        return limit;
    }

    void setLimit(int limit)
    {
        this->limit = limit;
    }

    int getOffset() const
    {
        return offset;
    }

    void setOffset(int offset)
    {
        this->offset = offset;
    }

    void setSqlType(SQLQueryType type)
    {
        this->sqlType = type;
    }

    SQLQueryType getSqlType()
    {
        return this->sqlType;
    }

    bool hasGroupByColumns()
    {
        return !groupbyList.empty();
    }

    bool hasOrderByColumns()
    {
        return !orderbyList.empty();
    }

    bool hasHavingClause()
    {
        return havingClause != NULL;
    }

    bool hasWhereClause()
    {
        return whereClause != NULL;
    }

    bool isSelectDistinct()
    {
        return selectDistinct;
    }

    void getOrderByString(StringBuffer & str)
    {
        getOrderByString(',', str);
    }

    void getOrderByString(char delimiter, StringBuffer & str)
    {
        int orderbycount = orderbyList.length();
        for (int i = 0; i < orderbycount; i++)
        {
            ISQLExpression* ordercol = &orderbyList.item(i);
            SQLFieldValueExpression* colexp = dynamic_cast<SQLFieldValueExpression*>(ordercol);
            str.append(colexp->isAscending() ? "" : "-");
            str.append(colexp->getNameOrAlias());
            if (i != orderbycount - 1)
                str.append(delimiter);
        }
    }

    void getGroupByString(StringBuffer & str)
    {
        getGroupByString(',', str);
    }

    void getGroupByString(char delimiter, StringBuffer & str)
    {
        int groupbycount = groupbyList.length();
        for (int i = 0; i < groupbycount; i++)
        {
            ISQLExpression * ordercol =  &groupbyList.item(i);
            SQLFieldValueExpression * colexp = dynamic_cast<SQLFieldValueExpression *>(ordercol);
            str.append(colexp->getName());
            if (i != groupbycount - 1)
                str.append(delimiter);
        }
    }

    ISQLExpression * getWhereClause()
    {
        return whereClause.get();
    }

    HPCCFileCache * queryHPCCFileCache()
    {
        return tmpHPCCFileCache.get();
    }

    unsigned getQueryHash();
    const char * getNormalizedSQL();

private:

    bool normalizeSQL();
};

#endif /* HPCCSQLTREEWALKER_HPP_ */
