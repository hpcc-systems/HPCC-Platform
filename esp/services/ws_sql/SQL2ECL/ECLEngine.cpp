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

#include "ECLEngine.hpp"
#include <limits>       // std::numeric_limits

const char * ECLEngine::SELECTOUTPUTNAME = "JDBCSelectQueryResult";

ECLEngine::ECLEngine(){}

ECLEngine::~ECLEngine(){}

void ECLEngine::generateECL(HPCCSQLTreeWalker * sqlobj, StringBuffer & out)
{
    if (sqlobj)
    {
        switch (sqlobj->getSqlType())
        {
            case SQLTypeSelect:
                generateSelectECL(sqlobj, out);
                break;
            case SQLTypeCall:
                break;
            case SQLTypeUnknown:
            default:
                break;
        }
    }
}

void ECLEngine::generateIndexSetupAndFetch(SQLTable * table, int tableindex, HPCCSQLTreeWalker * selectsqlobj, IProperties* eclEntities)
{
    bool isPayloadIndex = false;
    bool avoidindex = false;
    StringBuffer indexname;

    const char * tname = table->getName();

    if (table->hasIndexHint())
    {
        StringBuffer indexhint;
        indexhint.set(table->getIndexhint());
        if (strncmp(indexhint.trim().str(), "0", 1)==0)
        {
            avoidindex = true;
            WARNLOG("Will not use any index.");
            indexhint.clear();
        }
        else if (indexhint.length() <= 0)
        {
            WARNLOG("Empty index hint found!");
        }

        if (!avoidindex)
        {
            findAppropriateIndex(indexhint.str(), selectsqlobj, indexname);
            if (indexname.length() <= 0)
                WARNLOG("Cannot use USE INDEX hint: %s", indexname.str());
        }
    }

    if (indexname.length()>0)
    {
        HPCCFilePtr datafile = dynamic_cast<HPCCFile *>(selectsqlobj->queryHPCCFileCache()->getHpccFileByName(tname));
        HPCCFilePtr indexfile = dynamic_cast<HPCCFile *>(selectsqlobj->queryHPCCFileCache()->getHpccFileByName(indexname));
        if (indexfile && datafile)
        {
            StringBuffer idxsetupstr;
            StringBuffer idxrecdefname;

            idxrecdefname.set("TblDS").append(tableindex).append("RecDef");

            StringBuffer indexPosField;
            indexPosField.set(indexfile->getIdxFilePosField());
            HPCCColumnMetaData * poscol = indexfile->getColumn(indexPosField);

            datafile->getFileRecDefwithIndexpos(poscol, idxsetupstr, idxrecdefname.str());
            eclEntities->appendProp("INDEXFILERECDEF", idxsetupstr.str());

            StringBuffer keyedAndWild;
            isPayloadIndex = processIndex(indexfile, keyedAndWild, selectsqlobj);

            eclEntities->appendProp("KEYEDWILD", keyedAndWild.str());

            if (isPayloadIndex)
                eclEntities->appendProp("PAYLOADINDEX", "true");

            idxsetupstr.clear();
            idxsetupstr.append("Idx");
            idxsetupstr.append(tableindex);
            idxsetupstr.append(" := INDEX(TblDS");
            idxsetupstr.append(tableindex);
            idxsetupstr.append(", {");
            indexfile->getKeyedFieldsAsDelmitedString(',', "", idxsetupstr);
            idxsetupstr.append("}");

            if (indexfile->getNonKeyedColumnsCount() > 0)
            {
                idxsetupstr.append(",{ ");
                indexfile->getNonKeyedFieldsAsDelmitedString(',', "", idxsetupstr);
                idxsetupstr.append(" }");
            }

            idxsetupstr.append(",\'~").append(indexfile->getFullname()).append("\');\n");

            eclEntities->appendProp("IndexDef", idxsetupstr.str());

            idxsetupstr.clear();

            if (isPayloadIndex)
            {
                WARNLOG(" as PAYLOAD");
                idxsetupstr.append("IdxDS")
                                  .append(tableindex)
                                  .append(" := Idx")
                                  .append(tableindex)
                                  .append("(")
                                  .append(keyedAndWild.str());
            }
            else
            {
                WARNLOG(" Not as PAYLOAD");
                idxsetupstr.append("IdxDS")
                                  .append(tableindex)
                                  .append(" := FETCH(TblDS")
                                  .append(tableindex)
                                  .append(", Idx")
                                  .append(tableindex)
                                  .append("( ")
                                  .append(keyedAndWild.str())
                                  .append("), RIGHT.")
                                  .append(indexfile->getIdxFilePosField());
            }
            idxsetupstr.append(");\n");

            eclEntities->appendProp("IndexRead", idxsetupstr.str());
        }
        else
            WARNLOG("NOT USING INDEX!");
    }
}

void ECLEngine::generateSelectECL(HPCCSQLTreeWalker * selectsqlobj, StringBuffer & out)
{
    StringBuffer latestDS = "TblDS0";

    Owned<IProperties> eclEntities = createProperties(true);
    Owned<IProperties> eclDSSourceMapping = createProperties(true);
    Owned<IProperties> translator = createProperties(true);

    out.clear();
    out.append("import std;\n"); /* ALL Generated ECL will import std, even if std lib not used */

    //Prepared statement parameters are handled by ECL STORED service workflow statements
    if (selectsqlobj->hasWhereClause())
    {
        selectsqlobj->getWhereClause()->eclDeclarePlaceHolders(out, 0,0);
    }

    IArrayOf<SQLTable> * tables = selectsqlobj->getTableList();

    ForEachItemIn(tableidx, *tables)
    {
        SQLTable table = tables->item(tableidx);
        const char * tname = table.getName();

        HPCCFilePtr file = dynamic_cast<HPCCFile *>(selectsqlobj->queryHPCCFileCache()->getHpccFileByName(tname));
        if (file)
        {
            translator->setProp(tname, "LEFT");

            StringBuffer currntTblDS("TblDS");
            currntTblDS.append(tableidx);

            StringBuffer currntTblRecDef(currntTblDS);
            currntTblRecDef.append("RecDef");

            StringBuffer currntJoin("JndDS");
            currntJoin.append(tableidx);

            out.append("\n");
            if (tableidx == 0)
            {

                //Currently only utilizing index fetch/read for single table queries
                if (table.hasIndexHint() && tables->length() == 1 && !file->isFileKeyed())
                    generateIndexSetupAndFetch(&table, tableidx, selectsqlobj, eclEntities);

                if (eclEntities->hasProp("INDEXFILERECDEF"))
                {
                    eclDSSourceMapping->appendProp(tname, "IdxDS0");
                    eclEntities->getProp("INDEXFILERECDEF", out);
                }
                else
                    file->getFileRecDef(out, currntTblRecDef);
            }
            else
                file->getFileRecDef(out, currntTblRecDef);
            out.append("\n");

            if (!file->isFileKeyed())
            {
                out.append(currntTblDS);
                out.append(" := DATASET(\'~");
                out.append(file->getFullname());
                out.append("\', ");
                out.append(currntTblRecDef);
                out.append(", ");
                out.append(file->getFormat());
                out.append(");\n");
            }
            else
            {
                out.append(currntTblDS);
                out.append(" := INDEX( {");
                file->getKeyedFieldsAsDelmitedString(',', "TblDS0RecDef", out);
                out.append("},{");
                file->getNonKeyedFieldsAsDelmitedString(',', "TblDS0RecDef", out);
                out.append("},");
                out.append("\'~").append(file->getFullname()).append("\');\n");
            }

            if (tableidx > 0)
            {
                out.append("\n").append(currntJoin).append(" := JOIN( ");
                translator->setProp(tname, "RIGHT");

                if (tableidx == 1)
                {
                    //First Join, previous DS is TblDS0
                    out.append("TblDS0");
                    latestDS.set("JndDS1");
                }
                else
                {
                    //Nth Join, previous DS is JndDS(N-1)
                    out.append("JndDS");
                    out.append(tableidx-1);
                    latestDS.set("JndDS");
                    latestDS.append(tableidx);
                }

                StringBuffer translatedAndFilteredOnClause;
                SQLJoin * tablejoin = table.getJoin();
                if (tablejoin && tablejoin->doesHaveOnclause())
                {
                    //translatedAndFilteredOnClause = tablejoin->getOnClause()->toECLStringTranslateSource(translator,true, false, false, false);
                    tablejoin->getOnClause()->toECLStringTranslateSource(translatedAndFilteredOnClause, translator,true, false, false, false);
                    if (selectsqlobj->hasWhereClause())
                    {
                        translatedAndFilteredOnClause.append(" AND ");
                        //translatedAndFilteredOnClause.append(selectsqlobj->getWhereClause()->toECLStringTranslateSource(translator, true, true, false, false));
                        selectsqlobj->getWhereClause()->toECLStringTranslateSource(translatedAndFilteredOnClause, translator, true, true, false, false);
                    }
                }
                else if ( tablejoin && tablejoin->getType() == SQLJoinTypeImplicit && selectsqlobj->hasWhereClause())
                {
                    if (translatedAndFilteredOnClause.length() > 0)
                        translatedAndFilteredOnClause.append(" AND ");
                    //translatedAndFilteredOnClause.append(selectsqlobj->getWhereClause()->toECLStringTranslateSource(translator, true, true, false, false));
                    selectsqlobj->getWhereClause()->toECLStringTranslateSource(translatedAndFilteredOnClause, translator, true, true, false, false);
                }
                else
                    throw MakeStringException(-1,"No join condition between tables %s, and earlier table", tname);

                if (translatedAndFilteredOnClause.length() <= 0)
                    throw MakeStringException(-1,"Join condition does not contain proper join condition between tables %s, and earlier table", tname);

                out.append(", ");
                out.append(currntTblDS);
                out.append(", ");
                out.append(translatedAndFilteredOnClause.length() > 0 ? translatedAndFilteredOnClause.str() : "TRUE");

                out.append(", ");
                tablejoin->getECLTypeStr(out);

                if (tablejoin->getOnClause() != NULL && !tablejoin->getOnClause()->containsEqualityCondition(translator, "LEFT", "RIGHT"))
                {
                    WARNLOG("Warning: No Join EQUALITY CONDITION detected!, using ECL ALL option");
                    out.append(", ALL");
                }

                out.append(" );\n");

                //move this file to LEFT for possible next iteration
                translator->setProp(tname, "LEFT");

                //eclDSSourceMapping.put(hpccJoinFileName, "JndDS"+joinsCount);
            }
            //eclDSSourceMapping.put(queryFileName, "JndDS"+joinsCount);
            eclEntities->setProp("JoinQuery", "1");
        }
    }

    int limit=selectsqlobj->getLimit();
    int offset=selectsqlobj->getOffset();

    if (!eclEntities->hasProp("IndexDef"))
    {
        if (eclEntities->hasProp("SCALAROUTNAME"))
        {
            out.append("OUTPUT(ScalarOut ,NAMED(\'");
            eclEntities->getProp("SCALAROUTNAME", out);
            out.append("\'));");
        }
        else
        {
            //Create filtered DS if there's a where clause, and no join clause,
            //because filtering is applied while performing join.
            //if (sqlParser.getWhereClause() != null && !eclEntities.containsKey("JoinQuery"))
            if (selectsqlobj->hasWhereClause())
            {
                out.append(latestDS).append("Filtered := ");
                out.append(latestDS);

                addFilterClause(selectsqlobj, out);
                out.append(";\n");
                latestDS.append("Filtered");
            }

            generateSelectStruct(selectsqlobj, eclEntities.getLink(), *selectsqlobj->getSelectList(),latestDS.str());

            const char *selectstr = eclEntities->queryProp("SELECTSTRUCT");
            out.append(selectstr);

            out.append(latestDS).append("Table").append(" := TABLE( ");
            out.append(latestDS);

            out.append(", SelectStruct ");

            if (selectsqlobj->hasGroupByColumns() && !selectsqlobj->hasHavingClause())
            {
                out.append(", ");
                selectsqlobj->getGroupByString(out);
            }

            out.append(");\n");

            latestDS.append("Table");

            if (selectsqlobj->isSelectDistinct())
            {
                out.append(latestDS)
                .append("Deduped := Dedup( ")
                .append(latestDS)
                .append(", HASH);\n");
                latestDS.append("Deduped");
            }

            out.append("OUTPUT(");
            if (limit>0 || (!eclEntities->hasProp("NONSCALAREXPECTED") && !selectsqlobj->hasGroupByColumns()))
                out.append("CHOOSEN(");

            if (selectsqlobj->hasOrderByColumns())
                out.append("SORT(");

            out.append(latestDS);
            if (selectsqlobj->hasOrderByColumns())
            {
                out.append(",");
                selectsqlobj->getOrderByString(out);
                out.append(")");
            }
        }
    }
    else //PROCESSING FOR INDEX BASED FETCH
    {
        //Not creating a filtered DS because filtering is applied while
        //performing index read/fetch.
        eclEntities->getProp("IndexDef",out);
        eclEntities->getProp("IndexRead",out);
        StringBuffer latestDS = "IdxDS0";


        //if (eclEntities.containsKey("COUNTDEDUP"))
        //    eclCode.append(eclEntities.get("COUNTDEDUP"));

        if (eclEntities->hasProp("SCALAROUTNAME"))
        {
            out.append("OUTPUT(ScalarOut ,NAMED(\'");
            eclEntities->getProp("SCALAROUTNAME", out);
            out.append("\'));\n");
        }
        else
        {
            // If group by contains HAVING clause, use ECL 'HAVING' function,
            // otherwise group can be done implicitly in table step.
            // since the implicit approach has better performance.
            if (eclEntities->hasProp("GROUPBY") && selectsqlobj->hasHavingClause())
            {
                out.append(latestDS).append("Grouped").append(" := GROUP( ");
                out.append(latestDS);
                out.append(", ");
                eclEntities->getProp("GROUPBY", out);
                out.append(", ALL);\n");

                latestDS.append("Grouped");

                if (appendTranslatedHavingClause(selectsqlobj, out, latestDS.str()))
                    latestDS.append("Having");
            }

            generateSelectStruct(selectsqlobj, eclEntities.get(), *selectsqlobj->getSelectList(),latestDS.str());

            const char *selectstr = eclEntities->queryProp("SELECTSTRUCT");
            out.append(selectstr);


            out.append(latestDS)
            .append("Table := TABLE(")
            .append(latestDS)
            .append(", SelectStruct ");

            if (eclEntities->hasProp("GROUPBY") && !selectsqlobj->hasHavingClause())
            {
                out.append(", ");
                eclEntities->getProp("GROUPBY", out);
            }
            out.append(");\n");
            latestDS.append("Table");

            if (selectsqlobj->isSelectDistinct())
            {
                out.append(latestDS)
                .append("Deduped := Dedup( ")
                .append(latestDS)
                .append(", HASH);\n");

                latestDS.append("Deduped");
            }

            if (eclEntities->hasProp("ORDERBY"))
            {
                out.append(latestDS).append("Sorted := SORT( ").append(latestDS).append(", ");
                eclEntities->getProp("ORDERBY", out);
                out.append(");\n");
                latestDS.append("Sorted");
            }

            out.append("OUTPUT(CHOOSEN(");
            out.append(latestDS);
        }
    }

    if (!eclEntities->hasProp("SCALAROUTNAME"))
    {
        if (!eclEntities->hasProp("NONSCALAREXPECTED") && !selectsqlobj->hasGroupByColumns())
        {
            out.append(", 1)");
        }
        else if (limit>0)
        {
            out.append(",");
            out.append(limit);
            if (offset>0)
            {
                out.append(",");
                out.append(offset);
            }
            out.append(")");
        }

        out.append(",NAMED(\'");
        out.append(SELECTOUTPUTNAME);
        out.append("\'));");
    }
}

void ECLEngine::generateSelectStruct(HPCCSQLTreeWalker * selectsqlobj, IProperties* eclEntities,  IArrayOf<ISQLExpression> & expectedcolumns, const char * datasource)
{
    StringBuffer selectStructSB = "SelectStruct := RECORD\n";

    ForEachItemIn(i, expectedcolumns)
    {
        selectStructSB.append(" ");
        ISQLExpression * col = &expectedcolumns.item(i);

        if (col->getExpType() == Value_ExpressionType)
        {
            selectStructSB.append(col->getECLType());
            selectStructSB.append(" ");
            selectStructSB.append(col->getNameOrAlias());
            selectStructSB.append(i);//should only do this if the alias was not set
            selectStructSB.append(" := ");
            col->toString(selectStructSB, false);
            selectStructSB.append("; ");

            if (i == 0 && expectedcolumns.length() == 1)
                eclEntities->setProp("SCALAROUTNAME", col->getNameOrAlias());
        }
        else if (col->getExpType() == Function_ExpressionType)
        {
            SQLFunctionExpression * funcexp = dynamic_cast<SQLFunctionExpression *>(col);
            IArrayOf<ISQLExpression> * funccols = funcexp->getParams();

            ECLFunctionDefCfg func = ECLFunctions::getEclFuntionDef(funcexp->getName());

            if (func.functionType == CONTENT_MODIFIER_FUNCTION_TYPE )
            {
                if (funccols->length() > 0)
                {
                    ISQLExpression * param = &funccols->item(0);
                    int paramtype = param->getExpType();

                    if (strlen(col->getAlias())>0)
                        selectStructSB.append(col->getAlias());
                    else
                        selectStructSB.append(param->getNameOrAlias());
                    selectStructSB.append(" := ");
                    selectStructSB.append(func.eclFunctionName).append("( ");
                    if (paramtype == FieldValue_ExpressionType)
                    {
                        eclEntities->setProp("NONSCALAREXPECTED", "TRUE");

                        selectStructSB.append(datasource);
                        selectStructSB.append(".");
                        selectStructSB.append(param->getNameOrAlias());
                    }
                    else
                        param->toString(selectStructSB, false);
                }
            }
            else
            {
                if (strlen(col->getAlias())>0)
                    selectStructSB.append(col->getAlias());
                else
                {
                    selectStructSB.append(col->getName());
                    selectStructSB.append("out");
                    selectStructSB.append(i+1);
                }
                selectStructSB.append(" := ");

                selectStructSB.append(func.eclFunctionName).append("( ");

                if (selectsqlobj->hasGroupByColumns())
                {
                    selectStructSB.append("GROUP ");
                }
                else
                {
                    if (funcexp->isDistinct())
                    {
                        selectStructSB.append("DEDUP( ");
                        selectStructSB.append(datasource);
                        addFilterClause(selectsqlobj, selectStructSB);

                        for (int j = 0; j < funccols->length(); j++)
                        {
                            StringBuffer paramname = funccols->item(j).getName();
                            selectStructSB.append(", ");
                            selectStructSB.append(paramname);
                        }
                        selectStructSB.append(", HASH)");
                    }
                    else
                    {
                        selectStructSB.append(datasource);
                        addFilterClause(selectsqlobj, selectStructSB);
                    }
                }

                if ((strcmp(func.name,"COUNT"))!=0  && funccols->length() > 0)
                {
                    const char * paramname = funccols->item(0).getName();
                    if (paramname[0]!='*' && funccols->item(0).getExpType() != Value_ExpressionType)
                    {
                        selectStructSB.append(", ");
                        selectStructSB.append(datasource);
                        selectStructSB.append(".");
                        selectStructSB.append(paramname);
                    }
                }
            }

            //AS OF community_3.8.6-4 this is causing error:
            // (0,0): error C3000: assert(!cond) failed - file: /var/jenkins/workspace/<build number>/HPCC-Platform/ecl/hqlcpp/hqlhtcpp.cpp, line XXXXX
            //Bug reported: https://track.hpccsystems.com/browse/HPCC-8268
            //Leaving this code out until fix is produced.
            //UPDATE: Issue has been resolved as of 3.10.0

            //RODRIGO below if condition not completed yet
            //if (eclEntities.containsKey("PAYLOADINDEX") && !sqlParser.hasGroupByColumns() && !col.isDistinct())
            if (false && !selectsqlobj->hasGroupByColumns() && !funcexp->isDistinct())
            {
                    selectStructSB.append(", KEYED");
            }

            selectStructSB.append(" );");
        }
        else
        {
            eclEntities->setProp("NONSCALAREXPECTED", "TRUE");
            selectStructSB.append(col->getECLType());
            selectStructSB.append(" ");
            selectStructSB.append(col->getNameOrAlias());
            selectStructSB.append(" := ");
            selectStructSB.append(datasource);
            selectStructSB.append(".");
            selectStructSB.append(col->getName());
            selectStructSB.append(";");
        }

        selectStructSB.append("\n");
    }
    selectStructSB.append("END;\n");

    eclEntities->setProp("SELECTSTRUCT", selectStructSB.toCharArray());
}

bool containsPayload(HPCCFile * indexfiletotest, HPCCSQLTreeWalker * selectsqlobj)
{
    if (selectsqlobj)
    {
        IArrayOf <ISQLExpression> * selectlist = selectsqlobj->getSelectList();

        for (int j = 0; j < selectlist->length(); j++)
        {
            ISQLExpression * exp = &selectlist->item(j);
            if (exp->getExpType() == FieldValue_ExpressionType)
            {
                SQLFieldValueExpression * currentselectcol = dynamic_cast<SQLFieldValueExpression *>(exp);
                if (!indexfiletotest->containsField(currentselectcol->queryField(), true))
                    return false;
            }
            else if (exp->getExpType() == Function_ExpressionType)
            {
                SQLFunctionExpression * currentfunccol = dynamic_cast<SQLFunctionExpression *>(exp);

                IArrayOf<ISQLExpression> * funcparams = currentfunccol->getParams();
                ForEachItemIn(paramidx, *funcparams)
                {
                    ISQLExpression * param = &(funcparams->item(paramidx));
                    if (param->getExpType() == FieldValue_ExpressionType)
                    {
                        SQLFieldValueExpression * currentselectcol = dynamic_cast<SQLFieldValueExpression *>(param);
                        if (!indexfiletotest->containsField(currentselectcol->queryField(), true))
                            return false;
                    }
                }
            }
        }
    }
    return true;
}

bool ECLEngine::processIndex(HPCCFile * indexfiletouse, StringBuffer & keyedandwild, HPCCSQLTreeWalker * selectsqlobj)
{
    bool isPayloadIndex = containsPayload(indexfiletouse, selectsqlobj);

    StringArray keyed;
    StringArray wild;
    StringArray uniquenames;

    ISQLExpression * whereclause = selectsqlobj->getWhereClause();

    if (!whereclause)
        return false;

    // Create keyed and wild string
    IArrayOf<HPCCColumnMetaData> * cols = indexfiletouse->getColumns();
    for (int i = 0; i < cols->length(); i++)
    {
        HPCCColumnMetaData currcol = cols->item(i);
        if (currcol.isKeyedField())
        {
            const char * keyedcolname = currcol.getColumnName();
            StringBuffer keyedorwild;

            if (whereclause->containsKey(keyedcolname))
            {
                keyedorwild.set(" ");
                whereclause->getExpressionFromColumnName(keyedcolname, keyedorwild);
                keyedorwild.append(" ");
                keyed.append(keyedorwild);
            }
            else
            {
                keyedorwild.setf(" %s ", keyedcolname);
                wild.append(keyedorwild);
            }
        }
    }

    if (isPayloadIndex)
    {
        if (keyed.length() > 0)
        {
            keyedandwild.append("KEYED( ");
            for (int i = 0; i < keyed.length(); i++)
            {
                keyedandwild.append(keyed.item(i));
                if (i < keyed.length() - 1)
                    keyedandwild.append(" AND ");
            }
            keyedandwild.append(" )");
        }

        if (wild.length() > 0)
        {
            // TODO should I bother making sure there's a KEYED entry ?
            for (int i = 0; i < wild.length(); i++)
            {
                keyedandwild.append(" and WILD( ");
                keyedandwild.append(wild.item(i));
                keyedandwild.append(" )");
            }
        }
        keyedandwild.append(" and ( ");
        whereclause->toString(keyedandwild, false);
        keyedandwild.append(" )");
    }
    else
    {
        // non-payload just AND the keyed expressions
        keyedandwild.append("( ");
        whereclause->toString(keyedandwild, false);
        keyedandwild.append(" )");
    }

    return isPayloadIndex;
}

void ECLEngine::findAppropriateIndex(const char * indexhint, HPCCSQLTreeWalker * selectsqlobj, StringBuffer & indexname)
{
    StringArray indexhints;
    indexhints.append(indexhint);

    findAppropriateIndex(&indexhints, selectsqlobj, indexname);
}

void ECLEngine::findAppropriateIndex(StringArray * relindexes, HPCCSQLTreeWalker * selectsqlobj, StringBuffer & indexname)
{
    StringArray uniquenames;
    ISQLExpression * whereclause = selectsqlobj->getWhereClause();

    if (whereclause)
        whereclause->getUniqueExpressionColumnNames(uniquenames);

    int totalparamcount = uniquenames.length();

    if (relindexes->length() <= 0 || totalparamcount <= 0)
        return ;

    bool payloadIdxWithAtLeast1KeyedFieldFound = false;

    IntArray scores;
    for (int indexcounter = 0; indexcounter < relindexes->length(); indexcounter++)
    {
        const char * indexname = relindexes->item(indexcounter);
        HPCCFilePtr indexfile = dynamic_cast<HPCCFile *>(selectsqlobj->queryHPCCFileCache()->getHpccFileByName(indexname));

        IArrayOf<ISQLExpression> * expectedretcolumns =selectsqlobj->getSelectList();
        if (indexfile && indexfile->isFileKeyed() && indexfile->hasValidIdxFilePosField())
        {
            //The more fields this index has in common with the select columns higher score
            int commonparamscount = 0;
            for (int j = 0; j < expectedretcolumns->length(); j++)
            {
                ISQLExpression * exp = &expectedretcolumns->item(j);
                if (exp->getExpType() == FieldValue_ExpressionType)
                {
                    SQLFieldValueExpression * fieldexp = dynamic_cast<SQLFieldValueExpression *>(exp);
                    if (indexfile->containsField(fieldexp->queryField(), true))
                        commonparamscount++;
                }
                else if (exp->getExpType() == Function_ExpressionType)
                {
                    SQLFunctionExpression * currentfunccol = dynamic_cast<SQLFunctionExpression *>(exp);

                    IArrayOf<ISQLExpression> * funcparams = currentfunccol->getParams();
                    ForEachItemIn(paramidx, *funcparams)
                    {
                        ISQLExpression * param = &(funcparams->item(paramidx));
                        if (param->getExpType() == FieldValue_ExpressionType)
                        {
                            SQLFieldValueExpression * currentselectcol = dynamic_cast<SQLFieldValueExpression *>(param);
                            if (indexfile->containsField(currentselectcol->queryField(), true))
                                commonparamscount++;
                        }
                    }
                }
            }
            int commonparamsscore = commonparamscount * NumberOfCommonParamInThisIndex_WEIGHT;
            scores.add(commonparamsscore, indexcounter);

            if (payloadIdxWithAtLeast1KeyedFieldFound && commonparamscount == 0)
                break; // Don't bother with this index

            //The more keyed fields this index has in common with the where clause, the higher score
            //int localleftmostindex = -1;
            int keycolscount = 0;
            IArrayOf<HPCCColumnMetaData> * columns = indexfile->getColumns();
            ForEachItemIn(colidx, *columns)
            {
                HPCCColumnMetaData currcol = columns->item(colidx);
                if (currcol.isKeyedField())
                {
                    ForEachItemIn(uniqueidx, uniquenames)
                    {
                        if(strcmp( uniquenames.item(uniqueidx), currcol.getColumnName())==0)
                        {
                            keycolscount++;
                            //RODRIGO need to verify this
                            //int paramindex = indexfile.getKeyColumnIndex(currentparam);
                            //if (localleftmostindex > paramindex)
                            //    localleftmostindex = paramindex;
                        }
                    }
                }
            }
            if (keycolscount == 0)
            {
                scores.add(std::numeric_limits<int>::min(), indexcounter);
                continue;
            }

            int keycolsscore = keycolscount * NumberofColsKeyedInThisIndex_WEIGHT;
            scores.add(keycolsscore + scores.item(indexcounter), indexcounter);
            if (commonparamscount == expectedretcolumns->length() && keycolscount > 0
                //&& !parser.whereClauseContainsOrOperator()
               )
                    payloadIdxWithAtLeast1KeyedFieldFound = true; // during scoring, give this priority

            //int leftmostindexscore = 0;
            //if (localleftmostindex != -1 )
            //{
            //    leftmostindexscore = ((localleftmostindex / totalparamcount) - 1 ) * LeftMostKeyIndexPosition_WEIGHT;
            //    scores.add( scores.item(indexcounter) - leftmostindexscore, indexcounter);
            //}
        }
    }

    int highscore = std::numeric_limits<int>::min();
    int highscoreidx = -1;
    for (int i = 0; i < scores.length(); i++)
    {
        if (highscore < scores.item(i))
        {
           highscore = scores.item(i);
           highscoreidx = i;
        }
    }
    if (highscoreidx != -1 && highscoreidx < relindexes->length())
        indexname.set(relindexes->item(highscoreidx));
}

void ECLEngine::addFilterClause(HPCCSQLTreeWalker * sqlobj, StringBuffer & sb)
{
    if (sqlobj->hasWhereClause())
    {
        StringBuffer where;
        sqlobj->getWhereClauseString(where);
        if (where.length()>0)
        {
            sb.append("( ").append(where.str()).append(" )");
        }
    }
}

void ECLEngine::addHavingCluse(HPCCSQLTreeWalker * sqlobj, StringBuffer & sb)
{
    StringBuffer having;
    sqlobj->getHavingClauseString(having);
    if (having.length()>0)
    {
        sb.append("( ").append(having.str()).append(" )");
    }
}

bool ECLEngine::appendTranslatedHavingClause(HPCCSQLTreeWalker * sqlobj, StringBuffer sb, const char * latesDSName)
{
    bool success = false;
    if (sqlobj)
    {
        if (sqlobj->hasHavingClause())
        {
            Owned<IProperties> translator = createProperties(true);

            IArrayOf<SQLTable> * tables = sqlobj->getTableList();
            ForEachItemIn(tableidx, *tables)
            {
                SQLTable table = tables->item(tableidx);
                translator->appendProp(table.getName(), "LEFT");
            }

            ISQLExpression * having = sqlobj->getHavingClause();
            StringBuffer havingclause;
            having->toECLStringTranslateSource(havingclause, translator, false, true, false, false);

            if (havingclause.length() > 0)
            {
                sb.append(latesDSName).append("Having").append(" := HAVING( ");
                sb.append(latesDSName);
                sb.append(", ");
                sb.append(havingclause);
                sb.append(" );\n");
            }
            success = true;
        }
    }
    return success;
}
