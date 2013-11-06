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

#ifndef SQLCOLUMN_HPP_
#define SQLCOLUMN_HPP_

#include "ws_sql.hpp"

class  SQLColumn : public CInterface, public IInterface
{
private:
    StringBuffer name;
    StringBuffer alias;
    StringBuffer parenttable;
    int position;
    bool ascending;
    StringBuffer columnType;

public:
    IMPLEMENT_IINTERFACE;
    SQLColumn();
    SQLColumn(const char* parentname, const char* columnname, const char* alias, int position);
    virtual ~SQLColumn();

    bool isAscending() const
    {
        return ascending;
    }

    void setAscending(bool ascending)
    {
        this->ascending = ascending;
    }

    const char * getAlias() const
    {
        return alias.toCharArray();
    }

    void setAlias(const char * alias)
    {
        this->alias.set(alias);
    }

    const char * getName()
    {
        return name.toCharArray();
    }

    void setName(const char * name)
    {
        this->name.set(name);
    }

    const char *  getParenttable() const
    {
        return parenttable.toCharArray();
    }

    void setParenttable(const char * parenttable)
    {
        this->parenttable.set(parenttable);
    }

    int getPosition() const
    {
        return position;
    }

    void setPosition(int position)
    {
        this->position = position;
    }

    bool isFieldNameOrAalias(const char* possiblenameoralias);
    void toString(StringBuffer & str, bool fullOutput);

    const char * getColumnNameOrAlias()
    {
        if (alias.length() > 0)
            return alias.toCharArray();
        else
            return name.toCharArray();
    }

    const char * getColumnType() const
    {
       return columnType.toCharArray();
    }

    void setColumnType(const char* columnType)
    {
       this->columnType.set(columnType);
    }
};


#define DEFAULTDECIMALCHARS     32;
#define DEFAULTINTBYTES         8;
#define DEFAULTREALBYTES        8;
#define DEFAULTCOLCHARS         0;
#define DEFAULTDECDIGITS        0;

class HPCCColumnMetaData : public CInterface, public IInterface
{
private:
    StringBuffer columnName;
    StringBuffer tableName;
    int index;
    int columnChars;
    int decimalDigits;
    StringBuffer columnType;
    bool keyedField;

public:
    IMPLEMENT_IINTERFACE;

    static HPCCColumnMetaData * createHPCCColumnMetaData(const char * name)
    {
        return new HPCCColumnMetaData(name);
    }

    HPCCColumnMetaData()
    {
        columnName.clear();
        keyedField = false;
    }

    HPCCColumnMetaData(const char * colname)
    {
        columnName.set(colname);
        keyedField = false;
    }

    ~HPCCColumnMetaData()
    {
#ifdef _DEBUG
        fprintf(stderr, "leaving columnmetadata.");
#endif
    }

    StringBuffer toEclRecString()
    {
        StringBuffer result;
        result.append(this->columnType.toCharArray());
        result.append(" ");
        result.append(this->columnName.toCharArray());

        return result;
    }

    const char * getColumnType() const
    {
        return columnType.str();
    }

    void setColumnType(const char* columnType)
    {
        this->columnType.set(columnType);
    }

    int getDecimalDigits() const
    {
        return decimalDigits;
    }

    void setDecimalDigits(int decimalDigits = 0)
    {
        this->decimalDigits = decimalDigits;
    }

    int getIndex() const
    {
        return index;
    }

    void setIndex(int index)
    {
        this->index = index;
    }

    const char * getTableName() const
    {
        return tableName.str();
    }

    void setTableName(const char* tableName)
    {
        this->tableName.set(tableName);
    }

    bool isKeyedField() const
    {
        return keyedField;
    }

    void setKeyedField(bool keyedField)
    {
        this->keyedField = keyedField;
    }

    const char * getColumnName() const
    {
        return columnName.str();
    }
};

#endif /* SQLCOLUMN_HPP_ */
