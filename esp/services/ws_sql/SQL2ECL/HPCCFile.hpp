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

#ifndef HPCCFILE_HPP_
#define HPCCFILE_HPP_

#include "ws_sql.hpp"
#include "SQLColumn.hpp"

#include "ws_sql_esp.ipp"
#include "hqlerror.hpp"

typedef enum _HPCCFileFormat
{
    HPCCFileFormatUnknown=-1,
    HPCCFileFormatFlat,
    HPCCFileFormatCSV,
    HPCCFileFormatXML,
    HPCCFileFormatKey,
} HPCCFileFormat;

class HPCCFile : public CInterface, public IInterface
{
public:
    static HPCCFileFormat DEFAULTFORMAT;

    IMPLEMENT_IINTERFACE;
    HPCCFile();
    virtual ~HPCCFile();

    const char * getCluster() const
    {
        return cluster.str();
    }

    void setCluster(const char * cluster)
    {
        this->cluster.set(cluster);
    }

    HPCCColumnMetaData * getColumn(const char * colname);

    IArrayOf<HPCCColumnMetaData> * getColumns()
    {
        return &columns;
    }

    const char * getEcl() const
    {
        return ecl;
    }

    bool setEcl(const char * ecl)
    {
        if (setFileColumns(ecl))
            this->ecl = ecl;
        else
            return false;

        return true;
    }

    const char * getFormat() const
    {
        switch(formatEnum)
        {
            case HPCCFileFormatFlat:
                return "FLAT";
            case HPCCFileFormatCSV:
                return "CSV";
            case HPCCFileFormatXML:
                return "XML";
            case HPCCFileFormatKey:
                return "KEYED";
            case HPCCFileFormatUnknown:
            default:
                return "UNKNOWN";
        }
    }

    void setFormat(const char * format)
    {
        if (!format || !*format)
            this->formatEnum = DEFAULTFORMAT;
        else
        {
            if (stricmp(format, "FLAT")==0)
                this->formatEnum = HPCCFileFormatFlat;
            else if (stricmp(format, "utf8n")==0)
                this->formatEnum = HPCCFileFormatCSV;
            else if (stricmp(format, "CSV")==0)
                this->formatEnum = HPCCFileFormatCSV;
            else if (stricmp(format, "XML")==0)
                this->formatEnum = HPCCFileFormatXML;
            else if (stricmp(format, "KEY")==0)
                this->formatEnum = HPCCFileFormatKey;
            else
                this->formatEnum = DEFAULTFORMAT;
        }
    }

    const char * getFullname() const
    {
        return fullname.str();
    }

    void setFullname(const char * fullname)
    {
        this->fullname = fullname;
    }

    bool isFileKeyed() const
    {
        return iskeyfile;
    }

    void setIsKeyedFile(bool iskeyfile)
    {
        this->iskeyfile = iskeyfile;
    }

    bool isFileSuper() const
    {
        return issuperfile;
    }

    void setIsSuperfile(bool issuperfile)
    {
        this->issuperfile = issuperfile;
    }

    const char * getName() const
    {
        return name.str();
    }

    void setName(const char * name)
    {
        this->name = name;
    }

    void setKeyedColumn(const char * name);
    bool getFileRecDefwithIndexpos(HPCCColumnMetaData * fieldMetaData, StringBuffer & out, const char * structname);
    bool getFileRecDef(StringBuffer & out, const char * structname, const char * linedelimiter  = "\n", const char * recordindent  = "\t");
    int getNonKeyedColumnsCount();
    int getKeyedColumnsCount();
    void getKeyedFieldsAsDelmitedString(char delim, const char * prefix, StringBuffer & out);
    void getNonKeyedFieldsAsDelmitedString(char delim, const char * prefix, StringBuffer & out);
    const char * getIdxFilePosField()
    {
       return idxFilePosField.length() > 0 ? idxFilePosField.str() : getLastNonKeyedNumericField();
    }

    bool hasValidIdxFilePosField()
    {
        const char * posfield = getIdxFilePosField();
        return (posfield && *posfield);
    }

    static HPCCFile * createHPCCFile();

    bool containsField(SQLColumn * field, bool verifyEclType);
    void setIdxFilePosField(const char * idxfileposfieldname)
    {
        idxFilePosField.set(idxfileposfieldname);
    }

private:
    void getFieldsAsDelmitedString(char delim, const char * prefix, StringBuffer & out, bool onlykeyed);
    bool setFileColumns(const char * eclString);
    void setKeyCounts();
    const char * getLastNonKeyedNumericField()
    {
        // TODO get numeric field
        for (int i = columns.length() - 1; i >= 0; i--)
        {
           if (!columns.item(i).isKeyedField())
           {
               const char * type = columns.item(i).getColumnType();
               return columns.item(i).getColumnName();
           }
        }
        return NULL;
    }

private:
    HPCCFileFormat formatEnum;
    StringBuffer name;
    StringBuffer fullname;
    StringBuffer cluster;
    StringBuffer idxFilePosField;
    bool iskeyfile;
    bool issuperfile;
    StringBuffer ecl;
    IArrayOf<HPCCColumnMetaData> columns;
    int keyedCount;
    int nonKeyedCount;
};

#endif /* HPCCFILE_HPP_ */
