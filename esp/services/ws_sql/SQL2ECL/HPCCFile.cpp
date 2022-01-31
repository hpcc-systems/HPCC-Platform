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

#include "HPCCFile.hpp"

HPCCFileFormat HPCCFile::DEFAULTFORMAT = HPCCFileFormatFlat;

HPCCFile * HPCCFile::createHPCCFile()
{
    return new HPCCFile();
}

HPCCFile::HPCCFile() : formatEnum(HPCCFileFormatUnknown), iskeyfile(false), issuperfile(false), keyedCount(-1),
                       nonKeyedCount(-1), hasNestedColumns(false)
{}

HPCCFile::~HPCCFile()
{
#ifdef _DEBUG
    fprintf(stderr, "\nLeaving HPCCFile\n\t");
#endif

    columns.kill(false);
}

bool HPCCFile::validateFileName(const char * fullname)
{
    if (!fullname || !*fullname)
        return false;

    int len = strlen(fullname);
    for (int strindex = 0; strindex < len; strindex++)
    {
        switch (fullname[strindex])
        {
            case '\'':
            case '\"':
                return false;
            case '~':
                if (strindex > 0)
                    return false;
                break;
            default:
                break;
        }
    }

    return true;
}

bool HPCCFile::getFileRecDef(StringBuffer & out, const char * structname, const char * linedelimiter, const char * recordindent)
{
    if (this->columns.length() > 0)
    {
        out.append(structname);
        out.append(" := RECORD ");
        out.append(linedelimiter);

        ForEachItemIn(rowindex, this->columns)
        {
           out.append(recordindent);
           this->columns.item(rowindex).toEclRecString(out);
           out.append(";");
           out.append(linedelimiter);
        }

        out.append("END;");
    }
    else
        return false;

    return true;
}

void HPCCFile::setKeyedColumn(const char * name)
{
    ForEachItemIn(colidx, columns)
    {
        if (strcmp(columns.item(colidx).getColumnName(), name)==0)
        {
            columns.item(colidx).setKeyedField(true);
            break;
        }
    }
}

bool HPCCFile::getFileRecDefwithIndexpos(HPCCColumnMetaData * fieldMetaData, StringBuffer & out, const char * structname)
{
    if (fieldMetaData)
    {
        out.append(structname);
        out.append(" := RECORD ");
        out.append("\n");

        ForEachItemIn(rowindex, this->columns)
        {
           out.append("\t");
           this->columns.item(rowindex).toEclRecString(out);
           out.append(";");
           out.append("\n");
        }

        out.append("\t")
           .append(fieldMetaData->getColumnType())
           .append(" ")
           .append(fieldMetaData->getColumnName())
           .append(" {virtual(fileposition)};\n");

        out.append("END;");
        return true;
    }

    return false;
}

bool setChildColumns(HPCCColumnMetaData * parent, IPropertyTree * fieldtree)
{
    StringBuffer ecltype;
    StringBuffer colname;

    if (parent == nullptr || fieldtree == nullptr)
    {
        LOG(LegacyMsgCatMin, "Could not set HPCC file childcolumns!");
        return false;
    }

    Owned<IPropertyTreeIterator> fields = fieldtree->getElements("Field");
    ForEach(*fields)
    {
        IPropertyTree & curField = fields->query();
        curField.getProp("@ecltype", ecltype.clear());
        curField.getProp("@name", colname.clear());

        Owned<HPCCColumnMetaData> col = HPCCColumnMetaData::createHPCCColumnMetaData(colname.str());
        col->setIndex(curField.getPropInt("@position", -1));

        if (strncmp(ecltype, "table of", 8)==0)
        {
            setChildColumns(col.get(), &curField);
        }
        col->setColumnType(ecltype.str());
        parent->setChildCol(col);
    }
    return true;
}

bool HPCCFile::setFileColumns(const char * eclString)
{
    StringBuffer text(eclString);

    MultiErrorReceiver errs;
    OwnedHqlExpr record = parseQuery(text.str(), &errs);
    if (errs.errCount())
    {
       StringBuffer errtext;
       IError *first = errs.firstError();
       first->toString(errtext);
       LOG(LegacyMsgCatNormal, "Could not set HPCC file columns: %s", errtext.str());
       return false;
    }

    if(!record)
        return false;

    Owned<IPropertyTree> rectree = createPTree("Table", ipt_caseInsensitive);

    exportData(rectree, record);

    StringBuffer ecltype;
    StringBuffer colname;

    Owned<IPropertyTreeIterator> fields = rectree->getElements("Field");
    ForEach(*fields)
    {
        IPropertyTree & curField = fields->query();

        curField.getProp("@ecltype", ecltype.clear());
        curField.getProp("@name", colname.clear());

        Owned<HPCCColumnMetaData> col = HPCCColumnMetaData::createHPCCColumnMetaData(colname.str());
        col->setIndex(curField.getPropInt("@position", -1));
        col->setTableName(this->fullname.str());

        if (strncmp(ecltype, "table of", 8)==0)
        {
            setHasNestedColumns(true);
            if(!setChildColumns(col.get(), &curField))
                return false;
        }
        col->setColumnType(ecltype.str());
        columns.append(*LINK(col));
    }

    return true;
}

HPCCColumnMetaData * HPCCFile::getColumn(const char * colname)
{
    ForEachItemIn(colidx, columns)
    {
        if (strcmp(columns.item(colidx).getColumnName(), colname)==0)
        {
            return &(columns.item(colidx));
        }
    }
    return NULL;
}

void HPCCFile::getKeyedFieldsAsDelimitedString(char delim, const char * prefix, StringBuffer & out)
{
    getFieldsAsDelmitedString(delim, prefix, out, true);
}

void HPCCFile::getNonKeyedFieldsAsDelmitedString(char delim, const char * prefix, StringBuffer & out)
{
    getFieldsAsDelmitedString(delim, prefix, out, false);
}
void HPCCFile::getFieldsAsDelmitedString(char delim, const char * prefix, StringBuffer & out, bool onlykeyed)
{
    bool isFirst = true;
    ForEachItemIn(colidx, columns)
    {
        HPCCColumnMetaData currcol = columns.item(colidx);
        if ((onlykeyed && currcol.isKeyedField()) || (!onlykeyed && !currcol.isKeyedField()))
        {
            if (!isFirst)
            {
                out.append(delim);
                out.append(" ");
            }

            if (prefix && *prefix)
            {
                out.append(prefix);
                out.append('.');
            }
            out.append(currcol.getColumnName());
            isFirst = false;
        }
    }
}

bool HPCCFile::containsField(SQLColumn * field, bool verifyEclType) const
{
    const char *  fieldName = field->getName();

    for (int i = 0; i < columns.length(); i++)
    {
        const char * name = columns.item(i).getColumnName();
        if (stricmp(name, fieldName)==0)
        {
            if (!verifyEclType ||  strcmp(columns.item(i).getColumnType(), field->getColumnType())==0)
                return true;
            return false;
        }
    }
    return false;
}
void HPCCFile::setKeyCounts()
{
    nonKeyedCount = 0;
    keyedCount = 0;
    ForEachItemIn(colidx, columns)
    {
        HPCCColumnMetaData currcol = columns.item(colidx);
        if (!currcol.isKeyedField())
            nonKeyedCount++;
        else
            keyedCount++;
    }
}

int HPCCFile::getNonKeyedColumnsCount()
{
    if (nonKeyedCount == -1)
        setKeyCounts();

    return nonKeyedCount;
}
int HPCCFile::getKeyedColumnsCount()
{
    if (keyedCount == -1)
        setKeyCounts();

    return keyedCount;
}

int main()
{
    HPCCFile * file = new HPCCFile();
    file->setCluster("cluster");
    file->setName("name");

    delete file;

    return 0;
}
