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
#ifndef SQLJOIN_HPP_
#define SQLJOIN_HPP_

#include "ws_sql.hpp"
#include "SQLExpression.hpp"

typedef enum _SQLJoinType
{
    SQLJoinTypeUnknown=-1,
    SQLJoinTypeInner,
    SQLJoinTypeOutter,
    SQLJoinTypeImplicit
} SQLJoinType;

class SQLJoin : public CInterface, public IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    static SQLJoin * creatSQLJoin()
    {
        return new SQLJoin();
    }

    static SQLJoin * creatSQLJoin(SQLJoinType jointype)
    {
        return new SQLJoin(jointype);
    }

    SQLJoin();
    SQLJoin(SQLJoinType jointype);
    virtual ~SQLJoin();

    ISQLExpression* getOnClause() const
    {
        if (!hasOnclause)
            return NULL;
        else
            return onClause.get();
    }

    void setOnClause(ISQLExpression* onClause)
    {
        hasOnclause = true;
        this->onClause.setown(onClause);
    }

    SQLJoinType getType() const
    {
        return type;
    }

    void setType(SQLJoinType type)
    {
        this->type = type;
    }

    void toString(StringBuffer & str);
    void getSQLTypeStr(StringBuffer & outstr);
    void getECLTypeStr(StringBuffer & outstr);

    bool doesHaveOnclause() const
    {
        return hasOnclause;
    }

private:
    bool hasOnclause;
    SQLJoinType type;
    Owned<ISQLExpression> onClause;
    static SQLJoinType defaultType;
};

#endif /* SQLJOIN_HPP_ */
