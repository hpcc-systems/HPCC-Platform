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

#ifndef HPCCFILE_HPP_
#define HPCCFILE_HPP_

#include "ws_sql.hpp"
#include "SQLColumn.hpp"

#include "ws_sql_esp.ipp"
#include "hqlerror.hpp"
#include "hqlexpr.hpp"

/* undef SOCKET definitions to avoid collision in Antlrdefs.h*/
#ifdef INVALID_SOCKET
    //#pragma message( "UNDEFINING INVALID_SOCKET - Will be redefined by ANTLRDEFS.h" )
    #undef INVALID_SOCKET
#endif
#ifdef SOCKET
    //#pragma message( "UNDEFINING SOCKET - Will be redefined by ANTLRDEFS.h" )
    #undef SOCKET
#endif

#include "HPCCSQLLexer.h"
#include "HPCCSQLParser.h"

typedef enum _HPCCFileFormat
{
    HPCCFileFormatUnknown=-1,
    HPCCFileFormatFlat,
    HPCCFileFormatCSV,
    HPCCFileFormatXML,
    HPCCFileFormatKey,
    HPCCFileFormatJSON,
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

    static const char * formatToString(HPCCFileFormat format)
    {
        switch(format)
        {
            case HPCCFileFormatFlat:
                return "FLAT";
            case HPCCFileFormatCSV:
                return "CSV";
            case HPCCFileFormatXML:
                return "XML";
            case HPCCFileFormatKey:
                return "KEYED";
            case HPCCFileFormatJSON:
                return "JSON";
            case HPCCFileFormatUnknown:
            default:
                return "UNKNOWN";
        }
    }

    const char * getFormat() const
    {
        return formatToString(formatEnum);
    }

    static HPCCFileFormat formatStringToEnum(const char * formatstr)
    {
        if (!formatstr || !*formatstr)
            return HPCCFileFormatUnknown;
        else
        {
            StringBuffer toUpper = formatstr;
            toUpper.trim().toUpperCase();

            if (strcmp(toUpper.str(), "FLAT")==0)
                return HPCCFileFormatFlat;
            else if (strcmp(toUpper.str(), "UTF8N")==0)
                return HPCCFileFormatCSV;
            else if (strcmp(toUpper.str(), "CSV")==0)
                return HPCCFileFormatCSV;
            else if (strcmp(toUpper.str(), "XML")==0)
                return HPCCFileFormatXML;
            else if (strcmp(toUpper.str(), "KEY")==0)
                return HPCCFileFormatKey;
            else if (strcmp(toUpper.str(), "JSON")==0)
                return HPCCFileFormatJSON;
            else
                return HPCCFileFormatUnknown;
        }
    }

    void setFormat(const char * format)
    {
        HPCCFileFormat formatenum = formatStringToEnum(format);
        if (formatenum == HPCCFileFormatUnknown)
            this->formatEnum = DEFAULTFORMAT;
        else
            this->formatEnum = formatenum;
    }

    const char * getFullname() const
    {
        return fullname.str();
    }

    void setFullname(const char * fullname)
    {
        this->fullname.set(fullname);
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
        this->name.set(name);
    }

    static bool validateFileName(const char * fullname);
    void setKeyedColumn(const char * name);
    bool getFileRecDefwithIndexpos(HPCCColumnMetaData * fieldMetaData, StringBuffer & out, const char * structname);
    bool getFileRecDef(StringBuffer & out, const char * structname, const char * linedelimiter  = "\n", const char * recordindent  = "\t");
    int getNonKeyedColumnsCount();
    int getKeyedColumnsCount();
    void getKeyedFieldsAsDelimitedString(char delim, const char * prefix, StringBuffer & out);
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

    bool containsField(SQLColumn * field, bool verifyEclType) const;

    const char * getOwner() const
    {
        return owner.str();
    }

    void setOwner(const char * owner)
    {
        this->owner.set(owner);
    }

    const char* getDescription() const
    {
        return description.str();
    }

    void setDescription(const char* description)
    {
        if (description && *description)
        {
            this->description.set(description);
            const char * pos = strstr(description, "XDBC:RelIndexes");
            if (pos)
            {
                pos = pos + 15;//advance to end of "XDBC:RelIndexes"
                while(pos && *pos) //find the = char
                {
                    if (!isspace(*pos))
                    {
                        if (*pos == '=' )
                        {
                            pos++;
                            while(pos && *pos) //find the beginning bracket
                            {
                                if (!isspace(*pos))
                                {
                                    if (*pos == '[' )
                                    {
                                        pos++;
                                        break;
                                    }
                                    else
                                        return;//found invalid char before [
                                }
                                pos++;
                            }
                            break;
                        }
                        else
                            return;//found invalid char before = char
                    }
                    pos++;
                }

                if ( pos && *pos) //found keyword
                    setRelatedIndexes(pos);
            }
        }
    }

    static bool parseOutRelatedIndexes(StringBuffer & description, StringBuffer & releatedIndexes)
    {
        if (description.length() > 0)
        {
            const char * head = strstr(description.str(), "XDBC:RelIndexes");
            if (head && *head)
            {
                const char * tail = strchr(head, ']');
                if (tail && *tail)
                {
                    description.remove(head-description.str(), tail-description.str()).trim();
                    return true;
                }
            }
        }
        return false;
    }

    void getRelatedIndexes(StringArray & indexes)
    {
        ForEachItemIn(c, relIndexFiles)
        {
            indexes.append(relIndexFiles.item(c));
        }
    }

    const char * getRelatedIndex(int relindexpos)
    {
        if (relindexpos > -1 && relindexpos < relIndexFiles.length())
            return relIndexFiles.item(relindexpos);
        else
            return NULL;
    }

    int getRelatedIndexCount()
    {
        return relIndexFiles.length();
    }
  /*
    tutorial::yn::peoplebyzipindex;
    Tutorial.IDX_PeopleByName;
    Tutorial.IDX_PeopleByPhonetic]
*/
    void setRelatedIndexes(const char * str)
    {
        StringBuffer index;
        while (str && *str)
        {
            if (!isspace(*str))
            {
                if  (*str == ';' || *str == ']')
                {
                    relIndexFiles.append(index.str());
                    if (*str == ']')
                        break;
                    else
                    index.clear();
                }
                else
                    index.append(*str);
            }
            str++;
        }
    }

    void setIdxFilePosField(const char* idxfileposfieldname)
    {
        idxFilePosField.set(idxfileposfieldname);
    }

    bool containsNestedColumns() const
    {
        return hasNestedColumns;
    }

    void setHasNestedColumns(bool hasNestedColumns)
    {
        this->hasNestedColumns = hasNestedColumns;
    }

private:
    void getFieldsAsDelmitedString(char delim, const char* prefix, StringBuffer& out, bool onlykeyed);
    bool setFileColumns(const char* eclString);
    void setKeyCounts();

    const char* getLastNonKeyedNumericField()
    {
        for (int i = columns.length() - 1;i >= 0;i--)
        {
            if (!columns.item(i).isKeyedField())
                return columns.item(i).getColumnName();
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
    bool hasNestedColumns;
    StringBuffer description;
    StringBuffer owner;
    StringArray relIndexFiles;
}
;

#endif /* HPCCFILE_HPP_ */
