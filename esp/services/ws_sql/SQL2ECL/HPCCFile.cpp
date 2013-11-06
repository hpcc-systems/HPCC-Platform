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

#include "HPCCFile.hpp"

HPCCFileFormat HPCCFile::DEFAULTFORMAT = HPCCFileFormatFlat;

HPCCFile * HPCCFile::createHPCCFile()
{
    return new HPCCFile();
}

HPCCFile::HPCCFile()
{
    iskeyfile=false;
    issuperfile=false;
    keyedCount = -1;
    nonKeyedCount = -1;
}

HPCCFile::~HPCCFile()
{
#ifdef _DEBUG
    fprintf(stderr, "Leaving HPCCFile");
#endif

    columns.kill(false);

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
           out.append(this->columns.item(rowindex).toEclRecString().toCharArray());
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
           out.append(this->columns.item(rowindex).toEclRecString().toCharArray());
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

bool HPCCFile::setFileColumns(const char * eclString)
{
    StringBuffer text = eclString;

    MultiErrorReceiver errs;
    OwnedHqlExpr record = parseQuery(text.str(), &errs);
    if (errs.errCount())
    {
       StringBuffer errtext;
       IECLError *first = errs.firstError();
       first->toString(errtext);
       return false;
    }

    if(!record)
        return false;

    Owned<IPropertyTree> rectree = createPTree("Table", ipt_caseInsensitive);

    exportData(rectree, record);

    StringBuffer ecltype;
    StringBuffer colname;
    int colsize;
    int colindex;

    Owned<IPropertyTreeIterator> fields = rectree->getElements("Field");
    ForEach(*fields)
    {
      fields->query().getProp("@ecltype", ecltype.clear());
      fields->query().getProp("@name", colname.clear());
      colsize = fields->query().getPropInt("@size", -1);
      colindex = fields->query().getPropInt("@position", -1);

      Owned<HPCCColumnMetaData> col = HPCCColumnMetaData::createHPCCColumnMetaData(colname.toCharArray());
      col->setColumnType(ecltype.toCharArray());
      col->setIndex(colindex);
      col->setTableName(this->fullname.toCharArray());

      columns.append(*LINK(col));
    }

    return true;
}

HPCCColumnMetaData * HPCCFile::getColumn(const char * colname)
{
    ForEachItemIn(colidx, columns)
    {
       // HPCCColumnMetaData currcol = columns.item(colidx);
        if (strcmp(columns.item(colidx).getColumnName(), colname)==0)
        {
            return &(columns.item(colidx));
        }
    }
    return NULL;
}

void HPCCFile::getKeyedFieldsAsDelmitedString(char delim, const char * prefix, StringBuffer & out)
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
bool HPCCFile::containsField(SQLColumn * field, bool verifyEclType)
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
