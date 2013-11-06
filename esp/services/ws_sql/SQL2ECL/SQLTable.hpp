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

#ifndef SQLTABLE_HPP_
#define SQLTABLE_HPP_

#include "ws_sql.hpp"
#include "SQLJoin.hpp"

class SQLTable : public CInterface, public IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    static SQLTable * createSQLTable()
    {
        return new SQLTable();
    }

    SQLTable(){}

    virtual ~SQLTable()
    {
#ifdef _DEBUG
        fprintf(stderr, "Leaving SQLTable");
#endif
    }

    SQLJoin* getJoin() const
    {
        return join.get();
    }

    void setJoin(SQLJoin* join)
    {
        this->join.setown(join);
    }

    void setNewJoin(SQLJoinType jointype)
    {
        this->join.setown(SQLJoin::creatSQLJoin(jointype));
    }

    bool hasIndexHint()
    {
        return indexhint.length() > 0;
    }
    const char* getIndexhint() const
    {
        return indexhint.str();
    }

    void setIndexhint(const char* indexhint)
    {
        this->indexhint.set(indexhint);
    }

    const char * getAlias() const
    {
        return alias.c_str();
    }

    void setAlias(const char * alias)
    {
        this->alias = alias;
    }

    const char * getName() const
    {
        return name.c_str();
    }

    void setName(const char * name)
    {
        this->name = name;
    }

    bool hasJoin()
    {
        return (join.get() != NULL);
    }

    const char * translateIfAlias(const char* possibleAlias)
    {
        if ((!alias.empty() && alias.compare(possibleAlias) == 0) || (!name.empty() && name.compare(possibleAlias) == 0))
            return name.c_str();
        else
            return "";
    }
private:
    string name;
    string alias;
    StringBuffer indexhint;
    Owned<SQLJoin> join;
};

#endif /* SQLTABLE_HPP_ */
