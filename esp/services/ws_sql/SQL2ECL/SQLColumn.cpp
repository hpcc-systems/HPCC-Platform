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

#include "SQLColumn.hpp"

SQLColumn::SQLColumn() {}
SQLColumn::SQLColumn(const char * parentname, const char * columnname, const char * alias, int position)
{
    this->parenttable.clear();
    this->name.clear();
    this->alias.clear();

    if (parentname)
        this->parenttable.append(parentname);
    if (columnname)
        this->name.append(columnname);
    if (alias)
        this->alias.append(alias);

    this->position = position;

    setAscending(true);
}


SQLColumn::~SQLColumn(){}

bool SQLColumn::isFieldNameOrAalias( const char * possiblenameoralias)
{
    if (
        (name.length() > 0 && strcmp(name.str(), possiblenameoralias)==0)
        ||
        (alias.length() > 0 && strcmp(alias.str(), possiblenameoralias)==0)
       )
        return true;
    else
        return false;
}

void SQLColumn::toString(StringBuffer & str, bool fullOutput)
{
    if (fullOutput)
    {
        if (parenttable.length() > 0)
        {
            str.append(parenttable.str());
            str.append(".");
        }
    }
    str.append(name.str());
}
