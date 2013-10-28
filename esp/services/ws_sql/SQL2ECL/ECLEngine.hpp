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
#include "HPCCFile.hpp"
#include "SQLColumn.hpp"

#ifndef ECLENGINE_HPP_
#define ECLENGINE_HPP_

#define NumberOfCommonParamInThisIndex_WEIGHT    5;
#define LeftMostKeyIndexPosition_WEIGHT          3;
#define NumberofColsKeyedInThisIndex_WEIGHT      2;

class ECLEngine
{
public:
    ECLEngine();
    virtual ~ECLEngine();

    static void generateECL(HPCCSQLTreeWalker * sqlobj, StringBuffer & out);

private:
    static void generateSelectECL(HPCCSQLTreeWalker * selectsqlobj, StringBuffer & out);
    //static void generateCallECL(HPCCSQLTreeWalker * callsqlobj, StringBuffer & out);

    static void findAppropriateIndex(const char * indexhint, HPCCSQLTreeWalker * selectsqlobj, StringBuffer & indexname);
    static void findAppropriateIndex(StringArray * relindexes, HPCCSQLTreeWalker * selectsqlobj, StringBuffer & indexname);
    static bool processIndex(HPCCFile * indexfiletouse, StringBuffer & keyedandwild, HPCCSQLTreeWalker * selectsqlobj);

    static void generateSelectStruct(HPCCSQLTreeWalker * selectsqlobj, IProperties* eclEntities, IArrayOf<ISQLExpression> & expectedcolumns, const char * datasource);
    static void addFilterClause(HPCCSQLTreeWalker * sqlobj, StringBuffer & sb);
    static void addHavingCluse(HPCCSQLTreeWalker * sqlobj, StringBuffer & sb);
    static bool appendTranslatedHavingClause(HPCCSQLTreeWalker * sqlobj, StringBuffer sb, const char * latesDSName);
    static void generateIndexSetupAndFetch(SQLTable * table, int tableindex, HPCCSQLTreeWalker * selectsqlobj, IProperties* eclEntities);

    static const char * SELECTOUTPUTNAME;
};


#endif /* ECLENGINE_HPP_ */
