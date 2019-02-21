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

#include "HPCCSQLTreeWalker.hpp"

void trimSingleQuotes(StringBuffer & quotedString)
{
    int len = quotedString.length();
    if (len)
    {
        quotedString.trim();

        if (quotedString.charAt(0) == '\'' && quotedString.charAt(len-1) == '\'')
        {
            quotedString.remove(len-1, 1);
            quotedString.remove(0, 1);
        }
    }
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
            if (tokenType == TOKEN_OUTTER_JOIN || tokenType == TOKEN_INNER_JOIN)
            {
                if (tokenType == TOKEN_OUTTER_JOIN)
                    jointype = SQLJoinTypeOuter;
                else if (tokenType == TOKEN_INNER_JOIN)
                    jointype = SQLJoinTypeInner;

                int joinNodeChildcount = ithchild->getChildCount(ithchild);
                if (joinNodeChildcount < 2 )
                    throw MakeStringException(-1, "Join statement appears to be incomplete");

                onclausenode = (pANTLR3_BASE_TREE) ithchild->getFirstChildWithType(ithchild, ON);

                ithchild = (pANTLR3_BASE_TREE)(ithchild->getChild(ithchild, 0));
                tokenType = ithchild->getType(ithchild);
            }

            if (tokenType == ID || tokenType == ABSOLUTE_FILE_ID)
            {
                tablename = (char *)ithchild->toString(ithchild)->chars;

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

                        const char * fullindexhintname = tmpHPCCFileCache->cacheHpccFileByName(tablealias);
                        if (!fullindexhintname || !*fullindexhintname)
                            IERRLOG("Invalid index hint found: %s\n", tablealias);
                        else
                            temptable->setIndexhint(fullindexhintname);
                    }
                    else if ( childType == TOKEN_AVOID_INDEX)
                    {
                        temptable->setIndexhint("0");
                    }
                    else if ( childType == TOKEN_TABLE_SCHEMA && tablechild->getChildCount(tablechild) == 1)
                    {
                        pANTLR3_BASE_TREE schema = (pANTLR3_BASE_TREE)tablechild->getChild(tablechild, 0);
                        UWARNLOG("Table schema detected but ignored: %s\n", (char *)schema->toString(schema)->chars);
                    }
                    else
                    {
                        UERRLOG("Invalid node found in table node: %s\n", (char *)tablechild->toString(tablechild)->chars);
                    }
                }

                const char * fullTableName = tmpHPCCFileCache->cacheHpccFileByName(tablename);
                if (!fullTableName || !*fullTableName)
                    throw MakeStringException(-1, "Invalid table name or file type not supported: %s\n", tablename);
                else
                    temptable->setName(fullTableName);

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

    if ( exprAST != NULL )
    {
        Owned<ISQLExpression>  leftexp;
        Owned<ISQLExpression>  rightexp;

        bool checkForAlias = false;
        ANTLR3_UINT32 exptype = exprAST->getType(exprAST);
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
                    else if (columnattributetype == ASTERISK)
                    {
                        ForEachItemIn(tableidx, tableList)
                        {
                            const char * tablename = tableList.item(tableidx).getName();
                            HPCCFilePtr file = tmpHPCCFileCache->getHpccFileByName(tablename);
                            if (file)
                            {
                                IArrayOf<HPCCColumnMetaData> * cols = file->getColumns();
                                ForEachItemIn(colidx, *cols)
                                {
                                    HPCCColumnMetaData col = cols->item(colidx);
                                    Owned<ISQLExpression> fve = new SQLFieldValueExpression(file->getFullname(),col.getColumnName());
                                    tmpexp2->addParams(fve.getLink());
                                }
                            }
                        }
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
                checkForAlias = true;
                break;
            case REAL_NUMBER:
                tmpexp.setown(new SQLValueExpression(exptype, (char *)exprAST->toString(exprAST)->chars));
                tmpexp->setName("ConstNum");
                tmpexp->setECLType("REAL");
                checkForAlias = true;
                break;
            case INTEGER_NUM:
            case HEX_DIGIT:
                tmpexp.setown(new SQLValueExpression(exptype, (char *)exprAST->toString(exprAST)->chars));
                tmpexp->setName("ConstNum");
                tmpexp->setECLType("INTEGER");
                checkForAlias = true;
                break;
            //case QUOTED_STRING:
            case TEXT_STRING:
                tmpexp.setown(new SQLValueExpression(exptype, (char *)exprAST->toString(exprAST)->chars));
                tmpexp->setName("ConstStr");
                tmpexp->setECLType("STRING");
                checkForAlias = true;
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
            case LIKE_SYM:
            case NOT_LIKE:
                leftexp.set(expressionTreeWalker((pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 0)),exprAST));
                rightexp.set(expressionTreeWalker((pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 1)),exprAST));

                tmpexp.setown( new SQLBinaryExpression(exptype,leftexp, rightexp));
                if (parameterizeStaticValues)
                {
                    if ( leftexp->getExpType() == Value_ExpressionType && rightexp->getExpType() != Value_ExpressionType)
                    {
                        leftexp->setValuePlaceHolderType(rightexp->getECLType());
                        paramList.append(*leftexp.getLink());
                    }
                    else if ( rightexp->getExpType() == Value_ExpressionType && leftexp->getExpType() != Value_ExpressionType)
                    {
                        rightexp->setValuePlaceHolderType(leftexp->getECLType());
                        paramList.append(*rightexp.getLink());
                    }
                }
                if (exptype == LIKE_SYM || exptype == NOT_LIKE)
                {
                    if (rightexp->getExpType() == Value_ExpressionType)
                    {
                        SQLValueExpression * valexp = static_cast<SQLValueExpression *>(rightexp.get());
                        valexp->setIsWildCardPattern(true);
                    }
                }

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
                if (exprAST->getChildCount(exprAST) > 0)
                {
                    pANTLR3_BASE_TREE table = (pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 0));
                    const char * tablename = (char *)table->toString(table)->chars;
                    if (tablename == NULL)
                    {
                        tmpexp.setown(new SQLFieldsExpression(true));
                    }
                    else
                    {
                        //SEARCH FOR possible table name (this could be an aliased parent)
                        bool found = false;
                        ForEachItemIn(tableidx, tableList)
                        {
                            const char * translated = tableList.item(tableidx).translateIfAlias(tablename);
                            if (translated && *translated)
                            {
                                tmpexp.setown(new SQLFieldsExpression(translated));
                                found = true;
                                break;
                            }
                        }

                        if (found == false)
                        {
                            throw MakeStringException(-1, "INVALID TABLE NAME FOUND: %s\n", tablename );
                        }
                    }
                }
                else
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
                        throw MakeStringException(-1, "AMBIGUOUS COLUMN FOUND: %s\n", colname);
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
                        throw MakeStringException(-1, "INVALID COLUMN FOUND (parent table unknown): %s\n", msg.str() );
                    }
                }

                //Sets the ecltype of the field.
                verifyColumn(tmpfve);

                tmpexp.setown(tmpfve.getLink());
                break;
            }
            case TOKEN_COLUMN_DEF:
            {
                const char * colname = NULL;
                int nodeChildrenCount = exprAST->getChildCount(exprAST);

                if (nodeChildrenCount != 2)
                    throw MakeStringException(-1, "Invalid column definition encountered");

                pANTLR3_BASE_TREE tmpNode = (pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 0));

                Owned<SQLFieldValueExpression> tmpfve = new SQLFieldValueExpression();
                tmpfve->setName((char *)tmpNode->toString(tmpNode)->chars);

                tmpNode = (pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 1));
                ANTLR3_UINT32 columnattributetype = tmpNode->getType(tmpNode);

                bool isunsigned = false;
                bool isbinary = false;
                const char * length = NULL;
                int lengthi = 0;
                const char * presicion = NULL;

                StringBuffer strList;
                int typechildcount = tmpNode->getChildCount(tmpNode);
                for (int typechildindex = 0; typechildindex < typechildcount; typechildindex++)
                {
                    pANTLR3_BASE_TREE tmpCNode = (pANTLR3_BASE_TREE)(tmpNode->getChild(tmpNode, typechildindex));
                    ANTLR3_UINT32 typechildtype = tmpCNode->getType(tmpCNode);
                    if (typechildtype == UNSIGNED_SYM)
                        isunsigned = true;
                    else if (typechildtype == INTEGER_NUM)
                    {
                        length = (char *)tmpCNode->toString(tmpCNode)->chars;
                        lengthi = atoi(length);
                        if (tmpCNode->getChildCount(tmpCNode)>0)
                        {
                            tmpCNode = (pANTLR3_BASE_TREE)(tmpCNode->getChild(tmpCNode, 0));
                            presicion = (char *)tmpCNode->toString(tmpCNode)->chars;
                        }
                    }
                    else if (typechildtype == TOKEN_PROC_PARAMS)
                    {
                        int strListCount = tmpCNode->getChildCount(tmpCNode);
                        for (int strListIndex = 0; strListIndex < strListCount; strListIndex++)
                        {
                            pANTLR3_BASE_TREE strValue = (pANTLR3_BASE_TREE)(tmpCNode->getChild(tmpCNode, strListIndex));
                            strList.appendf("%s%s", (char *)strValue->toString(strValue)->chars, (strListIndex < strListCount-1) ? "," : "");
                        }
                    }
                    else if (typechildtype == BINARY_SYM)
                    {
                        isbinary = true;
                    }
                    else
                        throw MakeStringException(-1, "\n Unexpected type option encountered: %s ", (char *)tmpCNode->toString(tmpCNode)->chars);
                }

                StringBuffer ecltype;
                switch (columnattributetype)
                {
                   case BIT_SYM:
                        ecltype.set("BOOlEAN");
                        break;
                    case TINYINT:
                        ecltype.setf("%s", isunsigned ? "UNSIGNED1" : "INTEGER1");
                        break;
                    case SMALLINT:
                        ecltype.setf("%s", isunsigned ? "UNSIGNED2" : "INTEGER2");
                        break;
                    case MEDIUMINT:
                        ecltype.setf("%s", isunsigned ? "UNSIGNED3" : "INTEGER3");
                        break;
                    case INTEGER_SYM:
                        ecltype.setf("%s%s", isunsigned ? "UNSIGNED" : "INTEGER", lengthi > 0 && lengthi < 4 ? length : "4");
                        break;
                    case BIGINT_SYM:
                        ecltype.setf("%s%s", isunsigned ? "UNSIGNED" : "INTEGER", lengthi > 0 && lengthi < 8 ? length : "8");
                        break;
                    case REAL_SYM:
                        ecltype.setf("REAL%s", lengthi < 3 ? "4" : "8"); //Ecl real can only be 8 or 4
                        break;
                    case DOUBLE_SYM:
                        ecltype.set("REAL8"); //Ecl real can only be 8 or 4
                        break;
                    case FLOAT_SYM:
                        ecltype.set("REAL4"); //Ecl real can only be 8 or 4
                        break;
                    case DECIMAL_SYM:
                        ecltype.setf("%sDECIMAL", isunsigned ? "UNSIGNED " : "");//A packed decimal value of n total digits (to a maximum of 32).
                                                                                 //If the _y value is present, it defines the number of decimal places
                        if (lengthi > 0)
                        {
                            if (lengthi >= 32)
                                ecltype.append("32");
                            else
                                ecltype.append(length);

                            if (presicion && *presicion)
                                ecltype.appendf("_%s", presicion);
                        }
                        break;
                    case NUMERIC_SYM:
                        throw MakeStringException(-1, "Ambiguous 'NUMERIC' column type encountered, please specify actual type.");
                        break;
                    case DATE_SYM:
                        ecltype.set("std.Date.DATE_T");
                        break;
                    case TIME_SYM:
                        ecltype.set("std.Date.TIME_T");
                        break;
                    case TIMESTAMP_SYM:
                        ecltype.set("DATA8");
                        break;
                    case DATETIME_SYM:
                        ecltype.set("DATETIME_T");
                        break;
                    case YEAR_SYM:
                        throw MakeStringException(-1, "'YEAR' column type not supported in ECL.");
                        break;
                    case CHAR_SYM:
                        ecltype.setf("STRING%s", lengthi > 0 && lengthi < 255 ? length : "255");
                        break;
                    case VARCHAR_SYM:
                        ecltype.setf("STRING%s", lengthi > 0 && lengthi < 65535 ? length : "65535");
                        break;
                    case BINARY_SYM:
                        //ecltype.setf("DATA%s", lengthi > 0 && lengthi < 255 ? length : "255"); //DATA[n] A "packed hexadecimal" data block of n bytes,
                        ecltype.set("DATA");
                        break;
                    case VARBINARY_SYM:
                        //ecltype.setf("DATA%s", lengthi > 0 && lengthi < 255 ? length : "255"); //DATA[n] A "packed hexadecimal" data block of n bytes,
                        ecltype.set("DATA");
                        break;
                    case TINYBLOB_SYM: //255 max size
                        //ecltype.setf("DATA%s", lengthi > 0 && lengthi < 255  ? length : "255"); //DATA[n] A "packed hexadecimal" data block of n bytes,
                        ecltype.set("DATA");
                        break;
                    case BLOB_SYM://65535 max size
                        //ecltype.setf("DATA%s", lengthi > 0 && lengthi < 65535  ? length : "65535"); //DATA[n] A "packed hexadecimal" data block of n bytes,
                        ecltype.set("DATA");
                        break;
                    case MEDIUMBLOB_SYM: //16777215 max size
                        //ecltype.setf("DATA%s", lengthi > 0 && lengthi < 16777215  ? length : "16777215");//DATA[n] A "packed hexadecimal" data block of n bytes,
                        ecltype.set("DATA");
                        break;
                    case LONGBLOB_SYM://4294967295 max size
                        //ecltype.setf("DATA%s", lengthi > 0 && lengthi < 4294967295 ? length : "4294967295");//DATA[n] A "packed hexadecimal" data block of n bytes,
                        ecltype.set("DATA");
                        break;
                    case TINYTEXT_SYM:
                        //ecltype.setf("%s%s", isbinary ? "DATA" : "STRING", lengthi > 0 && lengthi < 255? length : "255");
                        ecltype.setf("%s", isbinary ? "DATA" : "STRING");
                        break;
                    case TEXT_SYM:
                        //ecltype.setf("%s%s", isbinary ? "DATA" : "STRING", lengthi > 0 && lengthi < 65535 ? length : "65535");
                        ecltype.setf("%s", isbinary ? "DATA" : "STRING");
                        break;
                    case MEDIUMTEXT_SYM:
                        //ecltype.setf("%s%s", isbinary ? "DATA" : "STRING", lengthi > 0 && lengthi < 16777215 ? length : "16777215");
                        ecltype.setf("%s", isbinary ? "DATA" : "STRING");
                        break;
                    case LONGTEXT_SYM:
                        //ecltype.set("DATA"); // Should really be allowed to be a string - max len 4Gig
                        ecltype.setf("%s", isbinary ? "DATA" : "STRING");
                        break;
                    case ENUM_SYM:
                        if (strList.length())
                            ecltype.setf("ENUM ( %s )", strList.str());
                        else
                            throw MakeStringException(-1, "\n Enumeration definition must contain at least one entry");
                        break;
                    case SET_SYM:
                        if (strList.length())
                            ecltype.setf("SET OF STRING");
                        else
                            throw MakeStringException(-1, "\n SET definition must contain at least one entry");
                        break;
                    default:
                        throw MakeStringException(-1, "\n Unexpected/Unsupported SQL field type encountered");
                        break;
                }
                tmpfve->setECLType(ecltype.str());
                tmpexp.setown(tmpfve.getLink());
                break;
            }
            default:
                throw MakeStringException(-1, "\n Unexpected expression node found : %s ", (char *)exprAST->toString(exprAST)->chars);
                break;
        }

        if (checkForAlias)
        {
            if(exprAST->getChildCount(exprAST) == 1)//Alias
            {
                pANTLR3_BASE_TREE tmpNode = (pANTLR3_BASE_TREE)(exprAST->getChild(exprAST, 0));
                ANTLR3_UINT32 columnattributetype = tmpNode->getType(tmpNode);

                if (columnattributetype == TOKEN_ALIAS && tmpNode->getChildCount(tmpNode) == 1)
                {
                    pANTLR3_BASE_TREE ithcolumnaliasnode = (pANTLR3_BASE_TREE)(tmpNode->getChild(tmpNode, 0));
                    tmpexp->setAlias((char *)ithcolumnaliasnode->toString(ithcolumnaliasnode)->chars);
                }
                else
                {
                    throw MakeStringException(-1, "INVALID NODE: '%s' found while processing possible expression alias \n", (char *)tmpNode->toString(tmpNode)->chars);
                }
            }
        }
    }
    return tmpexp.getLink();
}

void HPCCSQLTreeWalker::createAndLoadStatementTreeWalker(pANTLR3_BASE_TREE clsqlAST)
{
    if ( clsqlAST != NULL )
    {
        char * tokenText = NULL;

        if (clsqlAST->getChildCount(clsqlAST) != 2 || clsqlAST->getType(clsqlAST) != TOKEN_CREATE_LOAD_TABLE_STATEMENT)
            throw MakeStringException(-1, "\nError in Create and Load command(s). WsSQL requires CREATE command to be accompanied by a LOAD command.\n");

        pANTLR3_BASE_TREE createPart = (pANTLR3_BASE_TREE)(clsqlAST->getChild(clsqlAST, 0));
        if ( createPart->getType(createPart) == TOKEN_CREATE_TABLE)
        {
            int createPartCount = createPart->getChildCount(createPart);
            if (createPartCount >= 1)
            {
                pANTLR3_BASE_TREE newTableName = (pANTLR3_BASE_TREE)(createPart->getChild(createPart, 0));
                tokenText = (char *)newTableName->toString(newTableName)->chars;
                if (!tokenText || !*tokenText)
                    throw MakeStringException(-1, "Error detected in CREATE and LOAD: New table name cannot be empty.");
                tableName.set(tokenText);

                for (int createAttributesIndex = 1; createAttributesIndex < createPartCount; createAttributesIndex++)
                {
                    pANTLR3_BASE_TREE ithTableAttribute = (pANTLR3_BASE_TREE)(createPart->getChild(createPart, createAttributesIndex));
                    ANTLR3_UINT32 ithTableAttributeType = ithTableAttribute->getType(ithTableAttribute);
                    if ( ithTableAttributeType == TOKEN_DONOT_OVERWRITE)
                    {
                        overwrite = false;
                    }
                    else if (ithTableAttributeType == COMMENT_SYM)
                    {
                        pANTLR3_BASE_TREE commentNode = (pANTLR3_BASE_TREE)(ithTableAttribute->getChild(ithTableAttribute, 0));
                        comment = (char *)commentNode->toString(commentNode)->chars;
                        trimSingleQuotes(comment);
                    }
                    else if (ithTableAttributeType == TOKEN_COLUMN_DEF_LIST)
                    {
                        int fieldsCount = ithTableAttribute->getChildCount(ithTableAttribute);
                        for (int fieldIndex = 0; fieldIndex < fieldsCount; fieldIndex++)
                        {
                            pANTLR3_BASE_TREE ithField = (pANTLR3_BASE_TREE)(ithTableAttribute->getChild(ithTableAttribute, fieldIndex));
                            Owned<ISQLExpression> exp = expressionTreeWalker(ithField,NULL);
                            if (exp.get())
                                recordDefinition.appendf("%s\t%s;\n", exp->getECLType(), exp->getName());
                            else
                                throw MakeStringException(-1, "\nError in call list\n");
                        }
                    }
                }
            }
            else
                throw MakeStringException(-1, "Error detected in CREATE and LOAD: Missing CREATE information.");

            pANTLR3_BASE_TREE loadPart = (pANTLR3_BASE_TREE)(clsqlAST->getChild(clsqlAST, 1));
            if ( loadPart->getType(loadPart) == TOKEN_LOAD_TABLE)
            {
                int loadPartCount = loadPart->getChildCount(loadPart);

                if (loadPartCount < 2)
                    throw MakeStringException(-1, "Error detected in CREATE and LOAD: Missing LOAD information.");

                pANTLR3_BASE_TREE loadPartIthChild = (pANTLR3_BASE_TREE)(loadPart->getChild(loadPart, 0));

                if (strcmp((char *)loadPartIthChild->toString(loadPartIthChild)->chars, tableName.str()) != 0)
                    throw MakeStringException(-1, "Error detected in CREATE and LOAD: LOAD must target newly created table.");

                loadPartIthChild = (pANTLR3_BASE_TREE)(loadPart->getChild(loadPart, 1));
                sourceDataTableName = (char *)loadPartIthChild->toString(loadPartIthChild)->chars;
                trimSingleQuotes(sourceDataTableName);

                if (loadPartCount > 2)
                {
                    for (int loadPartIndex = 2; loadPartIndex < loadPartCount; loadPartIndex++)
                    {
                        loadPartIthChild = (pANTLR3_BASE_TREE)(loadPart->getChild(loadPart, loadPartIndex));
                        ANTLR3_UINT32 loadPartIthChildType = loadPartIthChild->getType(loadPartIthChild);
                        if (loadPartIthChildType == TOKEN_LANDING_ZONE)
                        {
                            int sourcetypechildcount = loadPartIthChild->getChildCount(loadPartIthChild);
                            if (sourcetypechildcount != 2)
                                throw MakeStringException(-1, "Error detected in CREATE and LOAD: LOAD Landing Zone clause requires IP and Directory.");

                            pANTLR3_BASE_TREE lzinfo = (pANTLR3_BASE_TREE)(loadPartIthChild->getChild(loadPartIthChild, 0));
                            landingZoneIP.set((char *)lzinfo->toString(lzinfo)->chars);
                            trimSingleQuotes(landingZoneIP);

                            lzinfo = (pANTLR3_BASE_TREE)(loadPartIthChild->getChild(loadPartIthChild, 1));
                            landingZonePath.set((char *)lzinfo->toString(lzinfo)->chars);
                            trimSingleQuotes(landingZonePath);
                        }
                        else if (loadPartIthChildType == TYPE_SYM)
                        {
                            pANTLR3_BASE_TREE typeDeclaration = (pANTLR3_BASE_TREE)(loadPartIthChild->getChild(loadPartIthChild, 0));
                            sourceDataType.set((char *)typeDeclaration->toString(typeDeclaration)->chars);

                            int sourcetypechildcount = typeDeclaration->getChildCount(typeDeclaration);
                            if (sourcetypechildcount > 0)
                            {
                                sourceDataType.append('(');

                                for (int sourcetypechildindex = 0; sourcetypechildindex < sourcetypechildcount; sourcetypechildindex++)
                                {
                                    pANTLR3_BASE_TREE sourcetypechild = (pANTLR3_BASE_TREE)(loadPartIthChild->getChild(loadPartIthChild, sourcetypechildindex));
                                    sourceDataType.append((char *)sourcetypechild->toString(sourcetypechild)->chars);
                                    sourceDataType.append(" = ");
                                    sourcetypechild = (pANTLR3_BASE_TREE)(loadPartIthChild->getChild(loadPartIthChild, ++sourcetypechildindex));
                                    StringBuffer value = (char *)sourcetypechild->toString(sourcetypechild)->chars;
                                    trimSingleQuotes(value);
                                    sourceDataType.append(value.str());

                                    if (sourcetypechildindex < sourcetypechildcount - 1)
                                        sourceDataType.append(", ");
                                }

                                sourceDataType.append(" )");
                            }
                        }
                        else if (loadPartIthChildType == TOKEN_VARIABLE_FILE)
                        {
                            sourceDataType.setf("CSV");
                            pANTLR3_BASE_TREE typeDeclaration = (pANTLR3_BASE_TREE)(loadPartIthChild->getChild(loadPartIthChild, 0));

                            int sourcetypechildcount = typeDeclaration->getChildCount(typeDeclaration);
                            if (sourcetypechildcount > 0)
                            {
                                sourceDataType.append('(');

                                for (int sourcetypechildindex = 0; sourcetypechildindex < sourcetypechildcount; sourcetypechildindex++)
                                {
                                    pANTLR3_BASE_TREE sourcetypechild = (pANTLR3_BASE_TREE)(loadPartIthChild->getChild(loadPartIthChild, sourcetypechildindex));
                                    ANTLR3_UINT32 csvoption = sourcetypechild->getType(sourcetypechild);

                                    if (csvoption == TOKEN_VAR_SEPERATOR)
                                    {
                                        sourceDataType.append("SEPARATOR( ");
                                    }
                                    else if (csvoption == TOKEN_VAR_ESCAPED)
                                    {
                                        sourceDataType.append("ESCAPE( ");
                                    }
                                    else if (csvoption == TOKEN_VAR_ENCLOSED)
                                    {
                                        sourceDataType.append("QUOTE( ");
                                    }
                                    else if (csvoption == TOKEN_VAR_TERMINATOR)
                                    {
                                        sourceDataType.append("TERMINATOR( ");
                                    }
                                    else
                                        throw MakeStringException(-1, "Unknown variable file data type option encountered.");

                                    sourcetypechild = (pANTLR3_BASE_TREE)(sourcetypechild->getChild(sourcetypechild, 0));
                                    sourceDataType.append((char *)sourcetypechild->toString(sourcetypechild)->chars);
                                    sourceDataType.append(" )");

                                    if (sourcetypechildindex < sourcetypechildcount - 1)
                                        sourceDataType.append(", ");

                                }

                                sourceDataType.append(" )");
                            }
                        }
                    }
                }
            }
            else
                throw MakeStringException(-1, "Error detected in CREATE and LOAD: LOAD clause not found.");
        }
        else
            throw MakeStringException(-1, "Error detected in CREATE and LOAD: CREATE clause not found.");
    }
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

                        Owned<ISQLExpression> exp = expressionTreeWalker(ithchild,NULL);
                        if (exp.get())
                        {
                            //Call parameter strings have to be single quoted in order to be distinguished from
                            //Identifiers, however, the string parameters passed to the stored procedures are not quoted.
                            //If the content needs to be quoted, they have to be escaped, within the first set of single quotes.
                            //The first set of single quotes are stripped here.
                            if (tokenType == TEXT_STRING)
                            {
                                SQLValueExpression * valexp = static_cast<SQLValueExpression *>(exp.get());
                                if (valexp)
                                    valexp->trimTextQuotes();
                            }
                            paramList.append(*exp.getLink());
                        }
                        else
                            throw MakeStringException(-1, "\nError in call list\n");
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

                        if (!exp)
                            throw MakeStringException(-1, "Error in order by list.");

                        if (exp->getExpType() == FieldValue_ExpressionType)
                        {
                            SQLFieldValueExpression * fielvalexp = static_cast<SQLFieldValueExpression *>(exp.getLink());
                            groupbyList.append(*fielvalexp);
                        }
                        else
                        {
                            StringBuffer fieldvalue;
                            exp->toString(fieldvalue, true);
                            throw MakeStringException(-1, "Encountered invalid entry '%s' in 'GROUP BY' clause", fieldvalue.str());
                        }
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

                        if (!exp)
                            throw MakeStringException(-1, "Error in order by list.");

                        if (exp->getExpType() == FieldValue_ExpressionType)
                        {
                            SQLFieldValueExpression * fielvalexp = static_cast<SQLFieldValueExpression *>(exp.getLink());
                            orderbyList.append(*fielvalexp);
                        }
                        else
                        {
                            StringBuffer fieldvalue;
                            exp->toString(fieldvalue, true);
                            throw MakeStringException(-1, "Encountered invalid entry '%s' in 'ORDER BY' clause", fieldvalue.str());
                        }
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
            case TOKEN_CREATE_LOAD_TABLE_STATEMENT:
                setSqlType(SQLTypeCreateAndLoad);
                createAndLoadStatementTreeWalker(firstchild);
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

        if (havingClause != NULL)
            paramIndex = havingClause->setParameterizedNames(paramIndex);
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

HPCCSQLTreeWalker::HPCCSQLTreeWalker() :  sqlType(SQLTypeUnknown), parameterizeStaticValues(true), limit(-1)
                                         ,offset(-1), selectDistinct(false)
                                         ,overwrite(true), sourceDataType(""), parameterizedCount(-1)
{
    normalizedSQL.clear();
}

HPCCSQLTreeWalker::HPCCSQLTreeWalker(pANTLR3_BASE_TREE ast, IEspContext &context, bool attemptParameterization) :
                            limit(-1), offset(-1), selectDistinct(false), overwrite(true), sourceDataType("FLAT")
{
    parameterizeStaticValues = attemptParameterization;
    normalizedSQL.clear();

    StringBuffer username;
    StringBuffer password;
    context.getUserID(username);
    context.getPassword(password);

    tmpHPCCFileCache.setown(HPCCFileCache::createFileCache(username.str(), password.str()));

    sqlTreeWalker(ast);

    if (sqlType == SQLTypeSelect)
    {
        assignParameterIndexes();
        expandWildCardColumn();
        verifyColAndDisambiguateName();
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
            paramList.kill(false);
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
        bool replaced = false;

        if (currexp && currexp->needsColumnExpansion())
        {
            if (currexp->getExpType()== Fields_ExpressionType && ((SQLFieldsExpression*)currexp)->isAll())
            {
                ForEachItemIn(tableidx, tableList)
                {
                    SQLTable tab = (SQLTable)tableList.item(tableidx);

                    HPCCFilePtr file = tmpHPCCFileCache->getHpccFileByName(tab.getName());
                    if (file)
                    {
                        IArrayOf<HPCCColumnMetaData> * cols = file->getColumns();
                        ForEachItemIn(colidx, *cols)
                        {
                            HPCCColumnMetaData col = cols->item(colidx);
                            Owned<ISQLExpression> fve = new SQLFieldValueExpression(file->getFullname(),col.getColumnName());
                            if (tableidx == 0 && colidx == 0)
                            {
                                selectList.replace(*fve.getLink(), selectcolidx, true);
                                replaced = true;
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
                const char * tablename = ((SQLFieldsExpression * )currexp)->getTable();
                HPCCFilePtr file = tmpHPCCFileCache->getHpccFileByName(tablename);
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
                            replaced = true;
                        }
                        else
                            selectList.add(*fve.getLink(),selectcolidx+ colidx );
                    }
                    break;
                }
            }
            break; //only one select all ??
        }

        if (replaced && currexp)
            currexp->Release();
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

void HPCCSQLTreeWalker::verifyAndDisambiguateNameFromList(IArrayOf<SQLFieldValueExpression> * explist)
{
    if (explist)
    {
        int bycount = explist->length();
        for (int i = 0; i < bycount; i++)
        {
            bool found = false;
            SQLFieldValueExpression * coltoverify =  &explist->item(i);

            //we're trying to verify the list (groupby or sortby) contains only coloumns which appear in the select list
            ForEachItemIn(sellistidx, selectList)
            {
                ISQLExpression * selcolexp = &selectList.item(sellistidx);
                if (selcolexp && selcolexp->getExpType() == Function_ExpressionType)
                {
                    SQLFunctionExpression * currentfunccol = (SQLFunctionExpression *)selcolexp;

                    IArrayOf<ISQLExpression> * funcparams = currentfunccol->getParams();
                    ForEachItemIn(paramidx, *funcparams)
                    {
                        ISQLExpression * param = &(funcparams->item(paramidx));
                        if (param)
                        {
                            const char * paramname = param->getName();
                            if (stricmp (coltoverify->getName(), paramname)==0)
                            {
                                found = true;
                                break;
                            }
                        }
                    }
                }
                else if (selcolexp && selcolexp->getExpType() == Value_ExpressionType)
                    continue;
                else
                {
                    const char * selcolname = selcolexp->getName();
                    if (selcolname && *selcolname)
                    {
                        const char * selcolalias = selcolexp->getAlias();

                        if (stricmp (coltoverify->getName(), selcolname)==0 ||
                           (selcolalias != NULL && stricmp (coltoverify->getName(), selcolalias)==0))
                        {
                            coltoverify->setName(selcolname);
                            if (selcolalias && *selcolalias)
                            {
                                coltoverify->setAlias(selcolalias);
                            }
                            found = true;
                            break;
                        }
                    }
                }
            }
            if (!found)
                throw MakeStringException(-1, "Could not verify field: %s. It does not appear in SELECT list.", coltoverify->getName());
        }
    }
}

void HPCCSQLTreeWalker::verifyColumn(SQLFieldValueExpression * col)
{
    if (col)
    {
        const char * selcolname = col->getName();
        const char * selcolparent = col->getParentTableName();

        if (selcolname && *selcolname)
        {
            HPCCFilePtr file = tmpHPCCFileCache->getHpccFileByName(selcolparent);
            if (file)
            {
                if (selcolname && *selcolname)
                {
                    HPCCColumnMetaData * fcol = file->getColumn(selcolname);
                    if (fcol)
                        col->setECLType(fcol->getColumnType());
                    else //This exception doesn't allows us to validate direct references to aliases from select list
                        throw MakeStringException(-1, "INVALID COLUMN FOUND: %s.%s\n", selcolparent, selcolname );
                }
                else
                    throw MakeStringException(-1, "Could not verify a column\n");
            }
            else
                throw MakeStringException(-1, "INVALID COLUMN PARENT FOUND: %s.%s\n", selcolparent, selcolname );
        }
        else
            throw MakeStringException(-1, "Could not verify a column\n");
    }
    else
        throw MakeStringException(-1, "Could not verify a column\n");
}

void HPCCSQLTreeWalker::verifyColAndDisambiguateName()
{
    ForEachItemIn(sellistidx, selectList)
    {
        ISQLExpression * selcolexp = &selectList.item(sellistidx);
        if (selcolexp && selcolexp->getExpType() == FieldValue_ExpressionType)
        {
            verifyColumn((SQLFieldValueExpression * )selcolexp);
        }
        else if (selcolexp && selcolexp->getExpType() == Function_ExpressionType)
        {
            SQLFunctionExpression * currentfunccol = (SQLFunctionExpression *)selcolexp;

            IArrayOf<ISQLExpression> * funcparams = currentfunccol->getParams();
            ForEachItemIn(paramidx, *funcparams)
            {
                ISQLExpression * param = &(funcparams->item(paramidx));
                if (param && param->getExpType() == FieldValue_ExpressionType)
                   verifyColumn((SQLFieldValueExpression *)param);
            }
        }
        else if (selcolexp && selcolexp->getExpType() == Value_ExpressionType)
        {
            continue;
        }
        else
            throw MakeStringException(-1, "Could not process an entry on the select list");
    }

    if (orderbyList.length())
        verifyAndDisambiguateNameFromList(&orderbyList);

    if (groupbyList.length())
        verifyAndDisambiguateNameFromList(&groupbyList);
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
                normalizedSQL.append("SELECT ");
                if (isSelectDistinct())
                    normalizedSQL.append("DISTINCT ");

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

                    if (tab.hasIndexHint())
                        normalizedSQL.append(" USE INDEX ( ").append(tab.getIndexhint()).append(" ) ");

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

                if (!orderbyList.empty())
                {
                    normalizedSQL.append(" ORDER BY ");
                    getOrderByString(normalizedSQL);
                }

                if (!groupbyList.empty())
                {
                    normalizedSQL.append(" GROUP BY ");
                    getGroupByString(normalizedSQL);

                    if ( havingClause.get())
                    {
                        normalizedSQL.append(" HAVING ");
                        havingClause->toString(normalizedSQL, true);
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
            if (sqlType == SQLTypeCreateAndLoad)
            {
                normalizedSQL.appendf("CREATE %s TABLE %s \n( %s )\n%s;\n", isOverwrite() ? "" : "IF NOT EXISTS", tableName.str(), recordDefinition.str(), comment.length() ? comment.str() : "");
                normalizedSQL.appendf("LOAD DATA INFILE %s %s %s %s INTO TABLE %s", sourceDataTableName.str(), landingZoneIP.length() ? landingZoneIP.str() : "", landingZonePath.length() ? landingZonePath.str() : "", sourceDataType.str(), tableName.str());
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

/*
unsigned HPCCSQLTreeWalker::getQueryHash()
{
    unsigned hashed;
    if (normalizeSQL())
        hashed = hashc((const byte *)normalizedSQL.str(), normalizedSQL.length(), 0);

    Owned<IProperties> completedGraphs = createProperties(true);
    return hashed;
}
*/

