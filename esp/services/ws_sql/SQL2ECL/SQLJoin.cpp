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

#include "SQLJoin.hpp"

SQLJoinType SQLJoin::defaultType = SQLJoinTypeInner;

SQLJoin::SQLJoin()
{
    type = defaultType;
    hasOnclause = false;
}

SQLJoin::SQLJoin(SQLJoinType jointype)
{
    switch (jointype)
    {
        case SQLJoinTypeImplicit:
        case SQLJoinTypeInner:
        case SQLJoinTypeOutter:
            type = jointype;
            break;
        default:
            type = defaultType;
            break;
    }
    hasOnclause = false;
}

SQLJoin::~SQLJoin()
{
#ifdef _DEBUG
    fprintf(stderr, "Leaving SQLJoin");
#endif
}

void SQLJoin::getSQLTypeStr(StringBuffer & outstr)
{
    switch (type)
    {
        case SQLJoinTypeImplicit:
        case SQLJoinTypeInner:
            outstr.append(" INNER JOIN ");
            break;
        case SQLJoinTypeOutter:
            outstr.append(" OUTER JOIN ");
            break;
        default:
            outstr.append(" JOIN ");
            break;
    }
}

void SQLJoin::getECLTypeStr(StringBuffer & outstr)
{
    switch (type)
    {
        case SQLJoinTypeImplicit:
        case SQLJoinTypeInner:
            outstr.append(" INNER ");
            break;
        case SQLJoinTypeOutter:
            outstr.append(" FULL OUTER ");
            break;
        default:
            outstr.append(" ");
            break;
    }
}

void SQLJoin::toString(StringBuffer & str)
{
    getSQLTypeStr(str);

    if (type != SQLJoinTypeImplicit)
    {
        str.append(" ON ");
        onClause->toString(str, true);
    }
}
