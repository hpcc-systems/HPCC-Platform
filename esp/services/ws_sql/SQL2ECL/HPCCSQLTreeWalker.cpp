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

#include "HPCCSQLTreeWalker.hpp"

HPCCSQLTreeWalker::HPCCSQLTreeWalker()
{
    sqlType = SQLTypeUnknown;
}

void HPCCSQLTreeWalker::limitTreeWalker(pANTLR3_BASE_TREE limitAST)
{
    char *  limit = NULL;
    char *  offset = NULL;

    if ( limitAST != NULL )
    {
        int childrenCount = limitAST->getChildCount(limitAST);

        if (childrenCount > 0)
        {
            pANTLR3_BASE_TREE limitnode = (pANTLR3_BASE_TREE)(limitAST->getChild(limitAST, 0));
            limit = (char *)limitnode->toString(limitnode)->chars;
            setLimit(atoi(limit));

            if (childrenCount == 2)
            {
                pANTLR3_BASE_TREE offsetnode = (pANTLR3_BASE_TREE)limitAST->getChild(limitAST, 1);
                if (offsetnode != NULL)
                {
                    offset = (char *)offsetnode->toString(offsetnode)->chars;
                    setOffset(atoi(offset));
                }
            }
            else if (childrenCount > 2)
                throw MakeStringException(-1," Extra token found after LIMIT/OFFSET directive.");
        }
        else
            throw MakeStringException(-1," Missing LIMIT value");
    }
}

void HPCCSQLTreeWalker::fromTreeWalker(pANTLR3_BASE_TREE fromsqlAST)
{
    char *  tablename = NULL;
    char *  tablealias = NULL;

    if ( fromsqlAST != NULL )
    {
        int childrenCount = fromsqlAST->getChildCount(fromsqlAST);

        for ( int i = 0; i < childrenCount; i++ )
        {
            Owned<SQLTable> temptable = SQLTable::createSQLTable();

            pANTLR3_BASE_TREE ithchild = (pANTLR3_BASE_TREE)(fromsqlAST->getChild(fromsqlAST, i));
            ANTLR3_UINT32 tokenType = ithchild->getType(ithchild);

            pANTLR3_BASE_TREE onclausenode = NULL;

            SQLJoinType jointype = SQLJoinTypeUnknown;
            if (tokenType != ID && tokenType != TOKEN_INDEX_HINT && tokenType != TOKEN_AVOID_INDEX)
            {
                if (tokenType == TOKEN_OUTTER_JOIN)
                    jointype = SQLJoinTypeOutter;
                else if (tokenType == TOKEN_INNER_JOIN)
                    jointype = SQLJoinTypeInner;
                else
                    throw MakeStringException(-1,"Possible error found in from list.");

                int joinNodeChildcount = ithchild->getChildCount(ithchild);
                if (joinNodeChildcount < 2 )
                    throw MakeStringException(-1, "Join statement appears to be incomplete");

                onclausenode = (pANTLR3_BASE_TREE) ithchild->getFirstChildWithType(ithchild, ON);

                ithchild = (pANTLR3_BASE_TREE)(ithchild->getChild(ithchild, 0));
                tokenType = ithchild->getType(ithchild);
            }

            if (tokenType == ID)
            {
                tablename = (char *)ithchild->toString(ithchild)->chars;
                temptable->setName(tablename);

                int tablechildcount = ithchild->getChildCount(ithchild);

                for ( int tabchildidx = 0; tabchildidx < tablechildcount; tabchildidx++ )
                {

                   pANTLR3_BASE_TREE tablechild = (pANTLR3_BASE_TREE)ithchild->getChild(ithchild, tabchildidx);
                   ANTLR3_UINT32 childType = tablechild->getType(tablechild);

                    if (childType == TOKEN_ALIAS)
                    {
                       pANTLR3_BASE_TREE alias = (pANTLR3_BASE_TREE)tablechild->getChild(tablechild, 0);
                       tablealias = (char *)alias->toString(alias)->chars;
                       temptable->setAlias(tablealias);
                    }
                    else if ( childType == TOKEN_INDEX_HINT)
                    {
                        pANTLR3_BASE_TREE indexhint = (pANTLR3_BASE_TREE)tablechild->getChild(tablechild, 0);
                        tablealias = (char *)indexhint->toString(indexhint)->chars;
                        temptable->setIndexhint(tablealias);

                        tmpHPCCFileCache->cacheHpccFileByName(tablealias);
                    }
                    else if ( childType == TOKEN_AVOID_INDEX)
                    {
                        temptable->setIndexhint("0");
                    }
                    else
                    {
                        ERRLOG("Invalid node found in table node: %s\n", (char *)tablechild->toString(tablechild)->chars );
                    }
                }

                tmpHPCCFileCache->cacheHpccFileByName(tablename);
                tableList.append(*temptable.getLink());

                if (jointype == -1 && i > 0) //multiple tables
                {
                    temptable->setNewJoin(SQLJoinTypeImplicit);
                }

                if (jointype != SQLJoinTypeUnknown)
                {
                    Owned<SQLJoin> join = SQLJoin::creatSQLJoin(jointype);

                    if (onclausenode)
                        join->setOnClause(expressionTreeWalker((pANTLR3_BASE_TREE)onclausenode->getChild(onclausenode, 0), NULL));
                    else
                        throw MakeStringException(-1, "Join statement appears to be missing on clause");

                    temptable->setJoin(join.getLink());
                }
            }
        }
    }
}

ISQLExpression * HPCCSQLTreeWalker::expressionTreeWalker(pANTLR3_BASE_TREE exprAST, pANTLR3_BASE_TREE parent)
{
    Owned<ISQLExpression>  tmpexp;
    ANTLR3_UINT32 exptype = exprAST->getType(exprAST);

    if ( exprAST != NULL )
    {
        switch (exptype)
        {
            case TOKEN_LISTEXP:
            {
                Owned<SQLListExpression> tmpexp2 = new SQLListExpression();
                for (int i = 0; i < exprAST->getChildCount(exprAST); i++)
                {
                    pANTLR3_BASE_TREE ithentry = (pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, i));
                    tmpexp2->appendEntry(expressionTreeWalker(ithentry, exprAST));
                }

                tmpexp.setown(tmpexp2.getLink());
                break;
            }
            case TOKEN_FUNCEXP:
            {
                if (exprAST->getChildCount(exprAST) <= 0)
                {
                    if (parent != NULL)
                        throw MakeStringException(-1, "Error detected while parsing SQL around: %s", (char *)parent->toString(parent)->chars);
                    else
                        throw MakeStringException(-1, "Error detected while parsing SQL.");
                }

                pANTLR3_BASE_TREE tmpNode = (pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 0));
                Owned<SQLFunctionExpression> tmpexp2 = new SQLFunctionExpression((char *)tmpNode->toString(tmpNode)->chars);

                for (int i = 1; i < exprAST->getChildCount(exprAST); i++)
                {
                    pANTLR3_BASE_TREE ithcolumnattributenode = (pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, i));
                    ANTLR3_UINT32 columnattributetype = ithcolumnattributenode->getType(ithcolumnattributenode);

                    if (columnattributetype == TOKEN_ALIAS)
                    {
                        pANTLR3_BASE_TREE ithcolumnaliasnode = (pANTLR3_BASE_TREE)(ithcolumnattributenode->getChild(ithcolumnattributenode, 0));
                        tmpexp2->setAlias((char *)ithcolumnaliasnode->toString(ithcolumnaliasnode)->chars);
                    }
                    else if (columnattributetype == DISTINCT)
                    {
                        tmpexp2->setDistinct(true);
                    }
                    else
                    {
                        tmpexp2->addParams(expressionTreeWalker(ithcolumnattributenode, exprAST));
                    }
                }

                tmpexp2->setName((char *)tmpNode->toString(tmpNode)->chars);
                tmpexp.setown(tmpexp2.getLink());

                break;
            }
            case TOKEN_PARAMPLACEHOLDER:
            {
                tmpexp.setown(new SQLParameterPlaceHolderExpression());
                break;
            }
            case TRUE_SYM:
            case FALSE_SYM:
                tmpexp.setown(new SQLValueExpression(exptype, (char *)exprAST->toString(exprAST)->chars));
                tmpexp->setName("ConstBool");
                tmpexp->setECLType("BOOLEAN");
                break;
            case INTEGER_NUM:
            case REAL_NUMBER:
            case HEX_DIGIT:
                tmpexp.setown(new SQLValueExpression(exptype, (char *)exprAST->toString(exprAST)->chars));
                tmpexp->setName("ConstNum");
                tmpexp->setECLType("INTEGER");
                break;
            //case QUOTED_STRING:
            case TEXT_STRING:
                tmpexp.setown(new SQLValueExpression(exptype, (char *)exprAST->toString(exprAST)->chars));
                tmpexp->setName("ConstStr");
                tmpexp->setECLType("STRING");
                break;
            case IN_SYM:
            case NOT_IN:
            case EQ_SYM:
            case NOT_EQ:
            case AND_SYM:
            case OR_SYM:
            case MINUS:
            case PLUS:
            case MOD_SYM:
            case DIVIDE:
            case ASTERISK:
            case LET:
            case GET:
            case GTH:
            case LTH:
                tmpexp.setown( new SQLBinaryExpression(exptype,
                                expressionTreeWalker((pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 0)),exprAST),
                                expressionTreeWalker((pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 1)),exprAST)
                                ));
                break;
            case ISNOTNULL:
            case ISNULL:
            case NEGATION:
            case NOT_SYM:
            {
                tmpexp.setown(new SQLUnaryExpression(expressionTreeWalker((pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 0)),exprAST), exptype ));
                break;
            }
            //case PARENEXP: ANTLR idiosyncrasy prevented using imaginary token as root node
            case LPAREN:
                tmpexp.setown(new SQLParenthesisExpression(expressionTreeWalker((pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 0)),exprAST)));
                break;
            case TOKEN_COLUMNWILDCARD:
            {
                tmpexp.setown(new SQLFieldsExpression(true));
                break;
            }
            case TOKEN_COLUMN:
            {
                const char * colparent = NULL;
                const char * colname = NULL;
                int nodeChildrenCount = exprAST->getChildCount(exprAST);

                pANTLR3_BASE_TREE tmpNode = (pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 0));
                colname = (char *)tmpNode->toString(tmpNode)->chars;

                Owned<SQLFieldValueExpression> tmpfve = new SQLFieldValueExpression();
                tmpfve->setName(colname);

                for (int i = 1; i < nodeChildrenCount; i++)
                {
                    pANTLR3_BASE_TREE ithcolumnattributenode = (pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, i));
                    ANTLR3_UINT32 columnattributetype = ithcolumnattributenode->getType(ithcolumnattributenode);

                    if (columnattributetype == DESC)
                    {
                        tmpfve->setAscending(false);
                    }
                    else if (columnattributetype == TOKEN_ALIAS)
                    {
                        pANTLR3_BASE_TREE ithcolumnaliasnode = (pANTLR3_BASE_TREE)(ithcolumnattributenode->getChild(ithcolumnattributenode, 0));
                        tmpfve->setAlias((char *)ithcolumnaliasnode->toString(ithcolumnaliasnode)->chars);
                    }
                    else
                    {
                        colparent = (char *)ithcolumnattributenode->toString(ithcolumnattributenode)->chars;
                        tmpfve->setParentTableName(colparent);
                    }
                }

                if (colparent == NULL)
                {
                    if (tableList.length() == 1)
                    {
                        colparent = tableList.item(0).getName();
                        tmpfve->setParentTableName(colparent);
                    }
                    else
                        throw MakeStringException(-1, "AMBIGUOUS SELECT COLUMN FOUND: %s\n", colname);
                }
                else
                {
                    //SEARCH FOR possible table name (this could be an aliased parent)
                    bool found = false;
                    ForEachItemIn(tableidx, tableList)
                    {
                        const char * translated = tableList.item(tableidx).translateIfAlias(colparent);
                        if (translated && *translated)
                        {
                            tmpfve->setParentTableName(translated);
                            found = true;
                            break;
                        }
                    }
                    if (found == false)
                    {
                        StringBuffer msg;
                        tmpfve->toString(msg, true);
                        throw MakeStringException(-1, "INVALID SELECT COLUMN FOUND (parent table unknown): %s\n", msg.str() );
                    }
                }

                HPCCFilePtr file = dynamic_cast<HPCCFile *>(tmpHPCCFileCache->getHpccFileByName(tmpfve->getParentTableName()));

                if (file)
                {
                    tmpfve->setECLType(file->getColumn(colname)->getColumnType());
                }

                tmpexp.setown(tmpfve.getLink());
                break;
            }
            default:
                throw MakeStringException(-1, "\n Unexpected expression node found : %s ", (char *)exprAST->toString(exprAST)->chars);
                break;
        }
    }
    return tmpexp.getLink();
}

void HPCCSQLTreeWalker::callStatementTreeWalker(pANTLR3_BASE_TREE callsqlAST)
{
    char * tokenText = NULL;

    if ( callsqlAST != NULL )
    {
        int childrenCount = callsqlAST->getChildCount(callsqlAST);
        ANTLR3_UINT32 tokenType = callsqlAST->getType(callsqlAST);
        if (childrenCount >=1 && tokenType == TOKEN_CALL_STATEMENT)
        {
            pANTLR3_BASE_TREE procedurePart = (pANTLR3_BASE_TREE)(callsqlAST->getChild(callsqlAST, 0));
            if ( procedurePart->getType(procedurePart) == TOKEN_PROC_NAME)
            {
                int namepartcount = procedurePart->getChildCount(procedurePart);
                if (namepartcount >= 1)
                {
                    pANTLR3_BASE_TREE procedureNameAST = (pANTLR3_BASE_TREE)(procedurePart->getChild(procedurePart, 0));
                    tokenText = (char *)procedureNameAST->toString(procedureNameAST)->chars;
                    procedureName.set(tokenText);

                    if (namepartcount == 2)
                    {
                        procedureNameAST = (pANTLR3_BASE_TREE)(procedurePart->getChild(procedurePart, 1));
                        tokenText = (char *)procedureNameAST->toString(procedureNameAST)->chars;
                        querySetName.set(tokenText);
                    }
                    else if (namepartcount > 2)
                        throw MakeStringException(-1, "Error detected in CALL: Invalid Procedure name.");
                }
                else
                    throw MakeStringException(-1, "Error detected in CALL: Procedure name is empty.");
            }
            else
                throw MakeStringException(-1, "Error detected in CALL: Procedure name not found.");

            if (childrenCount == 2)
            {
                procedurePart = (pANTLR3_BASE_TREE)(callsqlAST->getChild(callsqlAST, 1));
                if ( procedurePart->getType(procedurePart) == TOKEN_PROC_PARAMS)
                {
                    int paramCount = procedurePart->getChildCount(procedurePart);
                    for ( int i = 0; i < paramCount; i++ )
                    {
                        pANTLR3_BASE_TREE ithchild = (pANTLR3_BASE_TREE)(procedurePart->getChild(procedurePart, i));
                        ANTLR3_UINT32 tokenType = ithchild->getType(ithchild);
                        tokenText = (char *)ithchild->toString(ithchild)->chars;

                        paramList.append(*expressionTreeWalker(ithchild, NULL));
                    }
                }
            }
            else if (childrenCount > 2)
                throw MakeStringException(-1, "Error detected in CALL: Error in param list.");
        }
        else
            throw MakeStringException(-1, "\nError detected in CALL.");
    }
}

void HPCCSQLTreeWalker::selectStatementTreeWalker(pANTLR3_BASE_TREE selectsqlAST)
{
    char * tokenText = NULL;

    if ( selectsqlAST != NULL )
    {
        int childrenCount = selectsqlAST->getChildCount(selectsqlAST);

        //processing the table list first helps process other portions in one pass
        pANTLR3_BASE_TREE fromchild = (pANTLR3_BASE_TREE) selectsqlAST->getFirstChildWithType(selectsqlAST, FROM);
        if (fromchild != NULL)
            fromTreeWalker(fromchild);

        for ( int i = 0; i < childrenCount; i++ )
        {
            pANTLR3_BASE_TREE ithchild = (pANTLR3_BASE_TREE)(selectsqlAST->getChild(selectsqlAST, i));
            ANTLR3_UINT32 tokenType = ithchild->getType(ithchild);
            tokenText = (char *)ithchild->toString(ithchild)->chars;

            switch (ithchild->getType(ithchild))
            {
                case SELECT:
                {
                    int columnCount = ithchild->getChildCount(ithchild);
                    for ( int i = 0; i < columnCount; i++ )
                    {
                        pANTLR3_BASE_TREE ithnode = (pANTLR3_BASE_TREE)(ithchild->getChild(ithchild, i));
                        ANTLR3_UINT32 tokenType = ithnode->getType(ithnode);

                        if (tokenType == DISTINCT)
                        {
                            selectDistinct = true;
                        }
                        else
                        {
                            Owned<ISQLExpression> exp = expressionTreeWalker(ithnode,NULL);
                            if (exp.get())
                                selectList.append(*exp.getLink());
                            else
                                throw MakeStringException(-1, "\nError in select list\n");
                        }
                    }

                    break;
                }
                case FROM:
                // FROM list is parsed first. See top of function.
                    break;
                case WHERE:
                {
                    Owned<ISQLExpression> logicexpression = expressionTreeWalker((pANTLR3_BASE_TREE)(ithchild->getChild(ithchild, 0)),NULL);
                    if (logicexpression.get())
                    {
                        SQLExpressionType expressiontype = logicexpression->getExpType();
                        switch (expressiontype)
                        {
                            case Unary_ExpressionType:
                            case Parenthesis_ExpressionType:
                            case Value_ExpressionType:
                            case Binary_ExpressionType:
                            case Function_ExpressionType:
                                whereClause.setown(logicexpression.getLink());
                                break;
                            case FieldValue_ExpressionType:
                            case Fields_ExpressionType:
                            case ParameterPlaceHolder_ExpressionType:
                            default:
                            {
                                StringBuffer tmp;
                                logicexpression->toString(tmp, false);
                                throw MakeStringException(-1, "Invalid expression type detected in Where clause: %s", tmp.str());
                            }
                                break;
                        }
                    }
                    else
                        throw MakeStringException(-1, "Error in Where clause");
                    break;
                }
                case GROUP_SYM:
                {
                    int groupByCount = ithchild->getChildCount(ithchild);
                    for ( int i = 0; i < groupByCount; i++ )
                    {
                        Owned<ISQLExpression> exp = expressionTreeWalker((pANTLR3_BASE_TREE)(ithchild->getChild(ithchild, i)),NULL);

                        if (exp.get())
                            groupbyList.append(*exp.getLink());
                        else
                            throw MakeStringException(-1, "Error in group by list");
                    }
                    break;
                }
                case HAVING:
                    havingClause.setown(expressionTreeWalker((pANTLR3_BASE_TREE)(ithchild->getChild(ithchild, 0)),NULL));
                    break;
                case ORDER_SYM:
                {
                    int orderByCount = ithchild->getChildCount(ithchild);
                    for ( int i = 0; i < orderByCount; i++ )
                    {
                        Owned<ISQLExpression> exp = expressionTreeWalker((pANTLR3_BASE_TREE)(ithchild->getChild(ithchild, i)),NULL);

                        if (exp.get())
                            orderbyList.append(*exp.getLink());
                        else
                            throw MakeStringException(-1, "Error in order by list.");
                    }
                    break;
                }
                case LIMIT:
                    limitTreeWalker(ithchild);
                    break;
                 default:
                     throw MakeStringException(-1, "Error in SQL Statement.");
                    break;
            }
        }
    }
}

void HPCCSQLTreeWalker::sqlTreeWalker(pANTLR3_BASE_TREE sqlAST)
{
    pANTLR3_BASE_TREE firstchild = NULL;
    char *  tokenText = NULL;

    if ( sqlAST != NULL && sqlAST->getChildCount(sqlAST) > 0)
    {
        pANTLR3_BASE_TREE firstchild = (pANTLR3_BASE_TREE)(sqlAST->getChild(sqlAST, 0));

        switch (firstchild->getType(firstchild))
        {
            case TOKEN_SELECT_STATEMENT:
                setSqlType(SQLTypeSelect);
                selectStatementTreeWalker(firstchild);
                break;
            case TOKEN_CALL_STATEMENT:
                setSqlType(SQLTypeCall);
                callStatementTreeWalker(firstchild);
                break;
            default:
                setSqlType(SQLTypeUnknown);
                throw MakeStringException(-1, "Invalid sql tree root node found: %s\n", (char *)firstchild->toString(firstchild)->chars);
                break;
        }
    }
    else
        throw MakeStringException(-1, "Error could not parse SQL Statement.");
}

void HPCCSQLTreeWalker::assignParameterIndexes()
{
    int paramIndex = 1;

    if (sqlType == SQLTypeSelect)
    {
        if (whereClause != NULL)
            paramIndex = whereClause->setParameterizedNames(paramIndex);
    }
    else if (sqlType == SQLTypeCall)
    {
        ForEachItemIn(paramlistidx, paramList)
        {
            ISQLExpression * paramexp = &paramList.item(paramlistidx);
            paramIndex = paramexp->setParameterizedNames(paramIndex);
        }
    }

    parameterizedCount = paramIndex - 1;
}

HPCCSQLTreeWalker::HPCCSQLTreeWalker(pANTLR3_BASE_TREE ast, IEspContext &context)
{
    normalizedSQL.clear();
    setLimit(-1);
    setOffset(-1);
    selectDistinct = false;

    StringBuffer username;
    StringBuffer password;
    context.getUserID(username);
    context.getPassword(password);

    tmpHPCCFileCache.setown(HPCCFileCache::createFileCache(username.str(), password.str()));

    sqlTreeWalker(ast);

    if (sqlType == SQLTypeSelect)
    {
        verifyColAndDisabiguateName();
        assignParameterIndexes();
        expandWildCardColumn();
    }
    else if (sqlType == SQLTypeCall)
    {
        assignParameterIndexes();
    }
}

HPCCSQLTreeWalker::~HPCCSQLTreeWalker()
{
#if defined _DEBUG
    fprintf(stderr, "\nDestroying hpccsql object\n");
#endif

    switch(sqlType)
    {
        case SQLTypeSelect:
            tableList.kill(false);
            whereClause.clear();
            havingClause.clear();
            tmpHPCCFileCache.clear();

            selectList.kill(false);
            groupbyList.kill(false);
            orderbyList.kill(false);
            tableList.kill(false);
            break;
        case SQLTypeCall:
            paramList.kill(false);
            break;
        default:
            break;
    }
}

void HPCCSQLTreeWalker::expandWildCardColumn()
{
    ForEachItemIn(selectcolidx, selectList)
    {
        ISQLExpression * currexp = &selectList.item(selectcolidx);

        if (currexp->needsColumnExpansion())
        {
            if (currexp->getExpType()== Fields_ExpressionType && ((SQLFieldsExpression*)currexp)->isAll())
            {
                ForEachItemIn(tableidx, tableList)
                {
                    SQLTable tab = (SQLTable)tableList.item(tableidx);

                    HPCCFilePtr file = dynamic_cast<HPCCFile *>(tmpHPCCFileCache->getHpccFileByName(tab.getName()));
                    if (file)
                    {
                        IArrayOf<HPCCColumnMetaData> * cols = file->getColumns();
                        ForEachItemIn(colidx, *cols)
                        {
                            HPCCColumnMetaData col = cols->item(colidx);
                            Owned<ISQLExpression> fve = new SQLFieldValueExpression(file->getFullname(),col.getColumnName());
                            if (colidx == 0)
                            {
                                selectList.replace(*fve.getLink(), selectcolidx, true);
                                currexp->Release();
                            }
                            else
                                selectList.add(*fve.getLink(),selectcolidx+ colidx );
                        }
                    }
                    else
                        throw MakeStringException(-1, "INVALID TABLE FOUND");
                }
            }
            else
            {
                const char * tablename = ((SQLFieldsExpression)currexp).getTable();
                HPCCFilePtr file =  dynamic_cast<HPCCFile *>(tmpHPCCFileCache->getHpccFileByName(tablename));
                if (file)
                {
                    IArrayOf<HPCCColumnMetaData> * cols = file->getColumns();
                    ForEachItemIn(colidx, *cols)
                    {
                        HPCCColumnMetaData col = cols->item(colidx);
                        Owned<ISQLExpression> fve = new SQLFieldValueExpression(tablename, col.getColumnName());
                        if (colidx == 0)
                        {
                            selectList.replace(*fve.getLink(), selectcolidx, true);
                            currexp->Release();
                        }
                        else
                            selectList.add(*fve.getLink(),selectcolidx+ colidx );
                    }
                    break;
                }
            }
            break; //only one select all ??
        }
    }
}

void HPCCSQLTreeWalker::getWhereClauseString(StringBuffer & str)
{
    if (whereClause.get())
    {
        whereClause->toString(str, false);
    }
}

void HPCCSQLTreeWalker::getHavingClauseString(StringBuffer & str)
{
    if (havingClause.get())
    {
        havingClause->toString(str, false);
    }
}

ISQLExpression * HPCCSQLTreeWalker::getHavingClause()
{
    return havingClause.get();
}

void HPCCSQLTreeWalker::verifyColAndDisabiguateName()
{
    int orderbycount = orderbyList.length();
    for (int i = 0; i < orderbycount; i++)
    {
        ISQLExpression * ordercol =  &orderbyList.item(i);

        ForEachItemIn(sellistidx, selectList)
        {
            ISQLExpression * selcolexp = &selectList.item(sellistidx);
            const char * selcolname = selcolexp->getName();
            const char * selcolalias = selcolexp->getAlias();
            if (stricmp (ordercol->getName(), selcolname)==0 ||
                    (selcolalias != NULL && stricmp (ordercol->getName(), selcolalias)==0))
            {
                ordercol->setName(selcolname);
                if (selcolexp->getAlias() != NULL)
                    ordercol->setAlias(selcolalias);
                     // found = true;
                  break;
            }
        }
    }
}

bool HPCCSQLTreeWalker::normalizeSQL()
{
    bool success = true;
    if (normalizedSQL.length() <= 0)
    {
        try
        {
            if (sqlType == SQLTypeSelect)
            {
                normalizedSQL.append("SELECT");
                ForEachItemIn(idx1, selectList)
                {
                    if (idx1 > 0)
                        normalizedSQL.append(", ");
                    selectList.item(idx1).toString(normalizedSQL, true);
                }
                normalizedSQL.append(" FROM ");
                ForEachItemIn(idxt, tableList)
                {
                    if (idxt > 0)
                        normalizedSQL.append(", ");

                    SQLTable tab = (SQLTable)tableList.item(idxt);
                    normalizedSQL.append(tab.getName());
                    if (tab.hasJoin())
                    {
                        tab.getJoin()->toString(normalizedSQL);
                    }
                }

                if (whereClause.get())
                {
                    normalizedSQL.append(" WHERE ");
                    whereClause->toString(normalizedSQL, true);
                }

                if (!orderbyList.empty()>0)
                {
                    normalizedSQL.append(" ORDER BY ");
                    ForEachItemIn(idxobl, orderbyList)
                    {
                        if (idxobl > 0)
                            normalizedSQL.append(", ");

                        orderbyList.item(idxobl).toString(normalizedSQL, true);
                    }
                }

                if (!groupbyList.empty())
                {
                    normalizedSQL.append(" GROUP BY ");
                    ForEachItemIn(idxgbl, groupbyList)
                    {
                        if (idxgbl > 0)
                            normalizedSQL.append(", ");

                        groupbyList.item(idxgbl).toString(normalizedSQL, true);
                    }
                }

                if (getLimit()>0)
                    normalizedSQL.append(" LIMIT ").append(getLimit());
                if ( getOffset()>0)
                    normalizedSQL.append(" OFFSET ").append(getOffset());
            }
            else if (sqlType == SQLTypeCall)
            {
                normalizedSQL.append("CALL ");
                normalizedSQL.append(querySetName.str());
                normalizedSQL.append("::");
                normalizedSQL.append(procedureName.str());

                ForEachItemIn(idparams, paramList)
                {
                    if (idparams > 0)
                        normalizedSQL.append(", ");
                    paramList.item(idparams).toString(normalizedSQL, true);
                }
            }
            else
                success = false;
        }
        catch (...)
        {
            success = false;
        }
    }
    return success;
}

bool HPCCSQLTreeWalker::isParameterizedCall()
{
    if (sqlType == SQLTypeCall)
    {
        if (paramList.length() == 0)
            return false;

        ForEachItemIn(idparams, paramList)
        {
            if (paramList.item(idparams).getExpType() != ParameterPlaceHolder_ExpressionType)
                return false;
        }
        return true;
    }

    return false;
}

const char * HPCCSQLTreeWalker::getNormalizedSQL()
{
    normalizeSQL();
    return normalizedSQL.str();
}

unsigned HPCCSQLTreeWalker::getQueryHash()
{
    unsigned hashed;
    if (normalizeSQL())
        hashed = hashc((const byte *)normalizedSQL.str(), normalizedSQL.length(), 0);

    Owned<IProperties> completedGraphs = createProperties(true);
    return hashed;
}

