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
        return alias.str();
    }

    void setAlias(const char * alias)
    {
        this->alias.set(alias);
    }

    const char * getName()
    {
        return name.str();
    }

    void setName(const char * name)
    {
        this->name.set(name);
    }

    const char *  getParenttable() const
    {
        return parenttable.str();
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
            return alias.str();
        else
            return name.str();
    }

    const char * getColumnType() const
    {
       return columnType.str();
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
    int decimalDigits;
    StringBuffer columnType;
    bool keyedField;
    IArrayOf<HPCCColumnMetaData> childColumns;

public:
    IMPLEMENT_IINTERFACE;

    static HPCCColumnMetaData * createHPCCColumnMetaData(const char * name)
    {
        return new HPCCColumnMetaData(name);
    }

    HPCCColumnMetaData() : keyedField(false), decimalDigits(0), index(-1)
    {
        columnName.clear();
    }

    HPCCColumnMetaData(const char * colname) : keyedField(false), decimalDigits(0), index(-1)
    {
        columnName.set(colname);
    }

    virtual ~HPCCColumnMetaData()
    {
#ifdef _DEBUG
        fprintf(stderr, "leaving %s columnmetadata.\n", columnName.str());
#endif
        childColumns.kill(false);
    }

    StringBuffer &toEclRecString(StringBuffer &result)
    {
        result.append(this->columnType.str());
        result.append(" ");
        result.append(this->columnName.str());

        return result;
    }

    const char * getColumnType() const
    {
        return columnType.str();
    }

    void setColumnType(const char* columnType)
    {
        if (strncmp(columnType, "table of", 8)==0)
        {
            StringBuffer result;
            result.append("DATASET({");
            ForEachItemIn(childIndex, childColumns)
            {
               this->childColumns.item(childIndex).toEclRecString(result);
               if (childIndex < childColumns.length()-1)
                   result.append(", ");
            }
            result.append("})");
            this->columnType.set(result);
        }
        else
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

    void setChildCol(HPCCColumnMetaData * child)
    {
        childColumns.append(*LINK(child));
    }

    IArrayOf<HPCCColumnMetaData> * getChildColumns()
    {
        return &childColumns;
    }

};

#endif /* SQLCOLUMN_HPP_ */
