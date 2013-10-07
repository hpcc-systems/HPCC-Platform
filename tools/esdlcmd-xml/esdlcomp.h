/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef __ESDLCOMP_H__
#define __ESDLCOMP_H__

#include <stdarg.h>
#include <assert.h>

#include "esdl_utils.hpp"
#include "../../system/include/platform.h"


#undef YYSTYPE
#define YYSTYPE attribute

#define ESDLVER "1.2"

#define MAX_IDENT 2048

#define RETURNNAME "_return"

#ifndef _WIN32
#define stricmp strcasecmp
#endif

inline bool es_streq(const char* s, const char* t) {  return strcmp(s,t)==0; }

#define PF_IN             0x1
#define PF_OUT            0x2
#define PF_VARSIZE        0x4
#define PF_PTR            0x8
#define PF_REF           0x10
#define PF_CONST         0x40
#define PF_SIMPLE        0x80
#define PF_RETURN       0x100
#define PF_STRING       0x200
#define PF_TEMPLATE     0x400
#define PF_ESPSTRUCT    0x800
#define PF_MASK        0x7fff


enum type_kind
{
    TK_null,
    TK_CHAR,
    TK_UNSIGNEDCHAR,
    TK_BYTE,
    TK_BOOL,
    TK_SHORT,
    TK_UNSIGNEDSHORT,
    TK_INT,
    TK_UNSIGNED,
    TK_LONG,
    TK_UNSIGNEDLONG,
    TK_LONGLONG,
    TK_UNSIGNEDLONGLONG,
    TK_DOUBLE,
    TK_FLOAT,
    TK_STRUCT,
    TK_ENUM,
    TK_VOID,
    TK_ESPSTRUCT,
    TK_ESPENUM
};

enum EAMType
{
    EAM_basic,
    EAM_jsbuf,
    EAM_jmbuf,
    EAM_jmbin
};

typedef struct esp_xlate_info_
{
    const char *meta_type;
    const char *xsd_type;
    const char *store_type;
    const char *array_type;
    const char *access_type;
    enum type_kind access_kind;
    unsigned access_flags;
    enum EAMType eam_type;
} esp_xlate_info;

esp_xlate_info *esp_xlat(const char *from, bool defaultToString=true);


enum  clarion_special_type_enum { cte_normal,cte_longref,cte_constcstr,cte_cstr };

void out(const char*, size_t);
void outs(const char*);
void outf(const char*,...) __attribute__((format(printf, 1, 2)));
void outs(int indent, const char*);
void outf(int indent, const char*,...) __attribute__((format(printf, 2, 3)));

char *appendstr(char *text,const char *str);

struct attribute
{
private:
    union
    {
        char *str_val;
        int int_val;
        double double_val;
    };

    enum { t_none, t_string, t_int, t_double } atr_type;
    char name_[MAX_IDENT];

public:

    attribute()
    {
        atr_type=t_none;
        str_val=NULL;
    }

    ~attribute()
    {
        release();
    }

    void release()
    {
        if (atr_type==t_string)
            free(str_val);
        atr_type=t_none;
    }

    void setVal(int val)
    {
        release();

        int_val=val;
        atr_type = t_int;
    }

    void setVal(double val)
    {
        release();

        double_val=val;
        atr_type = t_double;
    }

    void setVal(char *val)
    {
        release();

        str_val=val;
        atr_type = t_int;
    }

    const char *getString()
    {
        return str_val;
    }

    int getInt()
    {
        return int_val;
    }

    double getDouble()
    {
        return double_val;
    }

    const char *getName(){return name_;}
    void setName(const char *value)
    {
        strcpy(name_,(value)? value : (char *)"");
    }

    void setNameF(const char *format, ...) __attribute__((format(printf, 2, 3)))
    {
        va_list args;
        va_start(args, format);
        _vsnprintf(name_, MAX_IDENT,format, args);
        va_end(args);
    }
};

class LayoutInfo
{
public:
    LayoutInfo();
    ~LayoutInfo();

    char * size;
    char * count;
    LayoutInfo * next;
};


class MetaTagInfo
{
private:
    char *name_;

    union
    {
        char *str_val_;
        int int_val_;
        double double_val_;
    };

public:
    enum {mt_none, mt_int, mt_string, mt_double, mt_const_id} mttype_;
    MetaTagInfo     *next;

public:
    MetaTagInfo(const char *name, const char *strval)
    {
        mttype_=mt_string;
        str_val_=strdup(strval);
        name_ = strdup(name);
    }
    MetaTagInfo(const char *name, const char *strval,bool)
    {
        mttype_=mt_const_id;
        str_val_=strdup(strval);
        name_ = strdup(name);
    }
    MetaTagInfo(const char *name, int intval)
    {
        mttype_=mt_int;
        int_val_=intval;
        name_ =strdup(name);
    }
    MetaTagInfo(const char *name, double doubleval)
    {
        mttype_=mt_double;
        double_val_=doubleval;
        name_ =strdup(name);
    }
    ~MetaTagInfo()
    {
        release();
    }

    void release()
    {
        if (mttype_==mt_string && str_val_!=NULL)
            free(str_val_);

        if (name_)
            free(name_);

        mttype_=mt_none;
    }

    const char *getName()
    {
        return name_;
    }
    const char* getConstId()
    {
        if (mttype_==mt_const_id)
            return str_val_;
        return NULL;
    }
    void setName(const char *name)
    {
        if (name_)
            free(name_);
        name_ =strdup(name);
    }

    void setString(const char *val)
    {
        if (mttype_==mt_string && str_val_!=NULL)
            free(str_val_);

        str_val_=strdup(val);

        mttype_=mt_string;
    }

    const char *getString()
    {
        if (mttype_==mt_string)
            return str_val_;
        return NULL;
    }

    int getInt()
    {
        if (mttype_==mt_int)
            return int_val_;
        return -1;
    }

    double getDouble()
    {
        if (mttype_==mt_double)
            return double_val_;
        return -1;
    }

    void write_xml_attr()
    {
        switch (mttype_)
        {
        case mt_string:
        case mt_const_id:
        {
            if (!str_val_)
                outf(" %s=\"\"", getName());
            else
            {
                int len=strlen(str_val_);
                StrBuffer val;
                if (len>=2 && *str_val_=='\"')
                    val.append(str_val_, 1, strlen(str_val_)-2);
                else
                    val.append(str_val_);
                StrBuffer encoded;
                encodeXML(val.str(), encoded);
                outf(" %s=\"%s\"", getName(), encoded.str());
            }
            break;
        }
        case mt_double:
            outf(" %s=\"%f\"", getName(), double_val_);
            break;
        case mt_int:
            outf(" %s=\"%d\"", getName(), int_val_);
            break;
        case mt_none:
        default:
            outf(" %s=\"\"", getName());
            break;
        }
    }


};

inline MetaTagInfo* findMetaTag(MetaTagInfo *list, const char *name)
{
    MetaTagInfo *mti;
    for (mti=list; mti; mti=mti->next)
    {
        if (stricmp(mti->getName(), name)==0)
            return mti;
    }
    return NULL;
}

inline const char *getMetaString(MetaTagInfo *list, const char *tag, const char *def_val)
{
    MetaTagInfo *mti=findMetaTag(list, tag);
    return (mti!=NULL) ? mti->getString() : def_val;
}

inline bool getMetaStringValue(MetaTagInfo *list, StrBuffer &val, const char *tag)
{
    MetaTagInfo *mti=findMetaTag(list, tag);
    if (!mti)
        return false;
    const char *mtval=mti->getString();
    if (!mtval || strlen(mtval)<2)
        return false;
    val.append(strlen(mtval)-2, mtval+1);
    return true;
}

inline int getMetaInt(MetaTagInfo *list, const char *tag, int def_val=0)
{
    MetaTagInfo *mti=findMetaTag(list, tag);
    return (mti!=NULL) ? mti->getInt() : def_val;
}

inline double getMetaDouble(MetaTagInfo *list, const char *tag, double def_val=0)
{
    MetaTagInfo *mti=findMetaTag(list, tag);
    return (mti!=NULL) ? mti->getDouble() : def_val;
}

inline const char* getMetaConstId(MetaTagInfo *list, const char *tag, const char* def_val=NULL)
{
    MetaTagInfo *mti=findMetaTag(list, tag);
    return (mti!=NULL) ? mti->getConstId() : def_val;
}

// return: true if the ver tag is defined
bool hasMetaVerInfo(MetaTagInfo *list, const char* tag);
bool getMetaVerInfo(MetaTagInfo *list, const char* tag, StrBuffer& s);

class ParamInfo
{
public:
    ParamInfo();
    ~ParamInfo();

    char *bytesize(int deref=0);
    bool simpleneedsswap();
    void cat_type(char *s,int deref=0,int var=0);
    clarion_special_type_enum clarion_special_type();
    void out_clarion_parameter();
    void out_clarion_type(bool ret);
    void out_parameter(const char *pfx,int forclarion=0);
    void out_type(int deref=0,int var=0);
    void typesizeacc(char *accstr,size_t &acc);
    size_t typesizealign(size_t &ofs);
    void write_body_struct_elem(int ref);
    void write_param_convert(int deref=0);

    bool isEspArrayOf(const char *elem_type=NULL, bool def=true)
    {
        if (flags & PF_TEMPLATE && !strcmp(templ, "ESParray"))
        {
            if (!elem_type || !typname)
                return def;
            return (!strcmp(typname, elem_type));
        }
        return false;
    }

    bool isEspStringArray()
    {
        if (flags & PF_TEMPLATE && !strcmp(templ, "ESParray"))
            return (!typname||!strcmp(typname, "string")||!strcmp(typname, "EspTextFile"));
        return false;
    }

    bool isPrimitiveArray()
    {
        if (flags & PF_TEMPLATE && !strcmp(templ, "ESParray"))
            return (kind != TK_STRUCT && kind != TK_null && kind != TK_ESPENUM && kind != TK_ESPSTRUCT) || !typname;
        return false;
    }

    type_kind getArrayItemType()
    {
        assert(isPrimitiveArray());
        return kind;
    }

    const char* getArrayItemXsdType();

    const char* getArrayImplType();

    bool hasNameTag(){return (typname && !stricmp(typname, "EspTextFile"));}

    void write_clarion_attr_method(bool isSet);

    bool hasMapInfo();
    bool write_mapinfo_check(int indents, const char* ctxvar);

    bool hasMetaTag(const char* tag)
    {
        return findMetaTag(tags,tag)!=NULL;
    }

    const char *getMetaString(const char *tag, const char *def_val)
    {
        return ::getMetaString(tags, tag, def_val);
    }

    bool getMetaStringValue(StrBuffer &val, const char *tag)
    {
        return ::getMetaStringValue(tags, val, tag);
    }

    int getMetaInt(const char *tag, int def_val=0)
    {
        return ::getMetaInt(tags, tag, def_val);
    }

    double getMetaDouble(const char *tag, double def_val=0)
    {
        return ::getMetaDouble(tags, tag, def_val);
    }

    bool hasMetaVerInfo(const char* tag)
    {
        return ::hasMetaVerInfo(tags,tag);
    }

    bool getMetaVerInfo(const char* tag, StrBuffer& s)
    {
        return ::getMetaVerInfo(tags,tag,s);
    }

    void setXsdType(const char *value);
    const char *getXsdType();
    const char *getMetaXsdType() {  const char* xsd = getMetaString("xsd_type",NULL);  return xsd ? xsd : getMetaString("format_as",NULL); }

    // should be call once at a time. Better: use CStringBuffer
    const char* getXmlTag()
    {
        static char buffer[256];
        const char* xmlTag = getMetaString("xml_tag", NULL);
        if (xmlTag) {
            if (*xmlTag == '"' || *xmlTag == '\'')
                xmlTag++;
            int len = strlen(xmlTag);
            if (*(xmlTag+len-1)=='"' || *(xmlTag+len-1)=='\'')
                len--;
            strncpy(buffer, xmlTag, len);
            buffer[len] = 0;
        }
        else
            strcpy(buffer,name);
        return buffer;
    }

    void write_xml_type_attr()
    {
        const char *xsd_type = getMetaString("xsd_type", NULL);
        if (xsd_type)
        {
            if (*xsd_type=='\"')
                xsd_type++;
            const char *finger = strchr(xsd_type, ':');
            if (finger)
                xsd_type=finger+1;
            StrBuffer TypeName(xsd_type);
            TypeName.replace('\"', 0);
            outf(" complex_type=\"%s\"", TypeName.str());
        }
        else
        {
            char typestr[256]={0};
            cat_type(typestr, 0, 0);

            outf(" %s=\"%s\"", (kind==TK_ESPSTRUCT) ? "complex_type" : "type", typestr);
        }
    }

    void write_esxdl()
    {
        if (flags & PF_TEMPLATE && !strcmp(templ, "ESParray"))
        {
            outf(2, "<EsdlArray name=\"%s\" ", name);
            write_xml_type_attr();
            for (MetaTagInfo *mtag=tags; mtag; mtag=mtag->next)
                mtag->write_xml_attr();
            outs("/>\n");
        }
        else if (kind==TK_ENUM)
        {
            outf(2, "<EsdlEnumItem name=\"%s\"", name);
            for (MetaTagInfo *mtag=tags; mtag; mtag=mtag->next)
                mtag->write_xml_attr();
            outs("/>\n");
        }
        else if (kind==TK_ESPENUM)
        {
            outf(2, "<EsdlEnum name=\"%s\" enum_type=\"%s\"", name, typname);
            for (MetaTagInfo *mtag=tags; mtag; mtag=mtag->next)
                mtag->write_xml_attr();
            outs("/>\n");
        }
        else
        {
            outf(2, "<EsdlElement name=\"%s\"", name);
            write_xml_type_attr();
            for (MetaTagInfo *mtag=tags; mtag; mtag=mtag->next)
                mtag->write_xml_attr();
            outs("/>\n");
        }
    };

    type_kind kind;
    char      *name;
    char      *templ;
    char      *typname;
    char      *size;
    char      *sizebytes;
    unsigned   flags;
    LayoutInfo *layouts;
    ParamInfo  *next;
    MetaTagInfo     *tags;

private:
    char      *xsdtype;
    StrBuffer *m_arrayImplType;
};

class ProcInfo
{
public:
    ProcInfo();
    ~ProcInfo();

    char      * name;
    ParamInfo * rettype;
    ParamInfo * params;
    ProcInfo  * next;
    char      * conntimeout;
    char      * calltimeout;
    int         async;
    int         callback;
    ParamInfo * firstin;
    ParamInfo * lastin;
    int         virt;
    int         constfunc;
};

class ModuleInfo
{
public:
    ModuleInfo(const char *n);
    ~ModuleInfo();

    char        *name;
    char        *base;
    int          version;
    ProcInfo    *procs;
    ModuleInfo  *next;
    bool            isSCMinterface;
};

class ExportDefInfo
{
public:
    ExportDefInfo(const char *name)
    {
        name_=strdup(name);
        next=NULL;
    }

    ~ExportDefInfo()
    {
        if (name_)
            free(name_);
    }


    char                *name_;
    ExportDefInfo  *next;
};


class ApiInfo
{
public:
    ApiInfo(const char *grp="");
    ~ApiInfo();

   void write_header_method();
   void write_clarion_include_method();

    char            *group;
    char        *name;
    ProcInfo    *proc;
    ApiInfo  *next;
};

class IncludeInfo
{
public:
    IncludeInfo(const char *path_) : pathstr(path_), next(NULL)
    {
    };
    ~IncludeInfo(){}

   void write_esxdl()
   {
       outf(1, "<EsdlInclude file=\"%s\"/>\n", pathstr.str());
   }

   StrBuffer pathstr;
   IncludeInfo  *next;
};

class VersionInfo
{
public:
    //VersionInfo(const char *version_name_, double version_value_) : version_name(version_name_), version_value(version_value_)
    VersionInfo(const char *version_name_, double version_value_)
    {
        version_name.append(version_name_);
        version_value = version_value_;
        next=NULL;
    };
    ~VersionInfo(){}

   void write_esxdl()
   {
       outf(1, "<EsdlVersion name=\"%s\" version=\"%f\" />\n", version_name.str(), version_value);
   }

   StrBuffer version_name;
   double version_value;
   VersionInfo *next;
};

class EnumValInfo
{
public:
    EnumValInfo(const char *_name,int _val)
    {
        name = strdup(_name);
        val = _val;
        next = NULL;
    }

    ~EnumValInfo()
    {
        free(name);
    }

    char             *name;
    int               val;
    EnumValInfo      *next;
};


class EnumInfo
{
public:
    EnumInfo(const char *_name)
    {
        name = strdup(_name);
        vals = NULL;
        next = NULL;
    }

    ~EnumInfo()
    {
        free(name);
    }

    char             *name;
    EnumValInfo  *vals;
    EnumInfo         *next;
};



class EspMessageInfo
{
public:
    enum espm_type {espm_none, espm_struct, espm_enum, espm_request, espm_response};
    enum espaxm_type {espaxm_getters, espaxm_setters, espaxm_both};

    EspMessageInfo(const char *name, enum espm_type type=espm_none)
    {
        espm_type_=type;
        name_ =strdup(name);
        base_=NULL;
        attrs_=NULL;
        tags=NULL;
        next=NULL;
        parent=NULL;
        xsdgrouptype=NULL;
    }

    EspMessageInfo(enum espm_type type, ProcInfo *procInfo)
    {
        espm_type_=type;

        name_ =strdup(procInfo->name);

        if (espm_type_==espm_struct)
        {
            name_ =(char *)malloc(strlen(procInfo->name)+6);
            strcpy(name_, procInfo->name);
            strcat(name_, "Struct");

            attrs_ = procInfo->params;
            procInfo->params=NULL;
        }
        else if (espm_type_==espm_request)
        {
            name_ =(char *)malloc(strlen(procInfo->name)+8);
            strcpy(name_, procInfo->name);
            strcat(name_, "Request");

            attrs_ = procInfo->params;
            procInfo->params=NULL;
        }
        else
        {
            name_ =(char *)malloc(strlen(procInfo->name)+9);
            strcpy(name_, procInfo->name);
            strcat(name_, "Response");

            setParams(procInfo->rettype);
            procInfo->rettype=NULL;
        }

        tags=NULL;
        next=NULL;
        parent=NULL;
        xsdgrouptype=NULL;
    }

    void setType(espm_type type){espm_type_=type;}
    espm_type getType(){return espm_type_;}

    const char *getName(){return name_;}
    void setName(const char *name)
    {
        if (name_)
            free(name_);
        name_=strdup(name);
    }

    const char *getBase(){return base_;}
    void setBase(const char *base)
    {
        if (base_)
            free(base_);
        base_=strdup(base);
    }

    const char *getParentName()
    {
        if (parent)
            return parent;
        if (!getMetaString("extends", NULL))
            return NULL;
        parent=((char *)strdup(getMetaString("extends", NULL))+1);
        char *finger=strchr(parent, '\"');
        if (finger)
            *finger=0;
        return parent;
    }
    void setParentName(const char* name)
    {
        parent = strdup(name);
    }

    const char *getXsdGroupType()
    {
        if (!xsdgrouptype)
        {
            xsdgrouptype = strdup(getMetaString("xsd_group_type", "\"all\""));
            if (*xsdgrouptype == '\"')
                xsdgrouptype++;
            char *finger = strchr(xsdgrouptype, '\"');
            if (finger)
                *finger=0;
        }
        return xsdgrouptype;
    }


    const char *getMetaString(const char *tag, const char *def_val)
    {
        return ::getMetaString(tags, tag, def_val);
    }

    bool getMetaStringValue(StrBuffer &val, const char *tag)
    {
        return ::getMetaStringValue(tags, val, tag);
    }

    int getMetaInt(const char *tag, int def_val=0)
    {
        return ::getMetaInt(tags, tag, def_val);
    }

    bool hasMapInfo();

    void setParams(ParamInfo *param)
    {
        attrs_=param;
    }
    ParamInfo *getParams(){return attrs_;}

    const char * query_esxdl_type()
    {
        switch (espm_type_)
        {
        case espm_struct:
            return "EsdlStruct";
        case espm_enum:
            return "EsdlEnumType";
        case espm_request:
            return "EsdlRequest";
        case espm_response:
            return "EsdlResponse";
        case espm_none:
        default:
            break;
        }
        return NULL;
    }

    EspMessageInfo *find_parent();

    void write_esxdl_children()
    {
        EspMessageInfo *parmsg = find_parent();
        if (parmsg)
            parmsg->write_esxdl_children();
        for (ParamInfo *param=attrs_; param; param=param->next)
            param->write_esxdl();
    }

    void write_esxdl()
    {
        const char *esdltype = query_esxdl_type();
        if (esdltype)
        {
            outf(1, "<%s name=\"%s\"", esdltype, name_);
            if (base_ && *base_)
                outf(" base=\"%s\"", base_);
            if (parent)
                outf(" base_type=\"%s\"", parent);
            for (MetaTagInfo *mtag=tags; mtag; mtag=mtag->next)
                mtag->write_xml_attr();
            outs(">\n");

            for (ParamInfo *param=attrs_; param; param=param->next)
                param->write_esxdl();

            outf(1, "</%s>\n", esdltype);
        }
    }

    MetaTagInfo     *tags;
    EspMessageInfo  *next;

private:
    ParamInfo       *attrs_;
    espm_type       espm_type_;
    char            *name_;
    char            *base_;
    char            *parent;
    char            *xsdgrouptype;
};

class EspMethodInfo
{
private:
    char                *name_;
    char                *request_;
    char                *response_;
    ProcInfo            *proc_;

public:
    EspMethodInfo(const char *name, const char *req, const char *resp)
    {
        name_ =strdup(name);
        request_ =strdup(req);
        response_ =strdup(resp);
        proc_=NULL;
        tags=NULL;
        next=NULL;
    }

    EspMethodInfo(ProcInfo *procInfo)
    {
        proc_=procInfo;

        name_ =strdup(procInfo->name);

        request_ =(char *)malloc(strlen(name_)+8);
        strcpy(request_, name_);
        strcat(request_, "Request");

        response_ =(char *)malloc(strlen(name_)+9);
        strcpy(response_, name_);
        strcat(response_, "Response");

        tags=NULL;
        next=NULL;
    }

    const char *getName(){return name_;}
    void setName(const char *name)
    {
        if (name_)
            free(name_);
        name_ =strdup(name);
    }

    const char *getReq(){return request_;}
    void setReq(const char *name)
    {
        if (request_)
            free(request_);
        request_ =strdup(name);
    }

    const char *getResp(){return response_;}
    void setResp(const char *name)
    {
        if (response_)
            free(response_);
        response_ =strdup(name);
    }

    const char *getMetaString(const char *tag, const char *def_val)
    {
        return ::getMetaString(tags, tag, def_val);
    }

    bool getMetaStringValue(StrBuffer &val, const char *tag)
    {
        return ::getMetaStringValue(tags, val, tag);
    }


    int getMetaInt(const char *tag, int def_val=0)
    {
        return ::getMetaInt(tags, tag, def_val);
    }

    bool getMetaVerInfo(const char* tag, StrBuffer& s)
    {
        return ::getMetaVerInfo(tags,tag,s);
    }

    EspMessageInfo *getRequestInfo();

    void write_esxdl()
    {
        outf(1, "<EsdlMethod name=\"%s\" request_type=\"%s\" response_type=\"%s\" ", name_, request_, response_);
        for (MetaTagInfo *mtag=tags; mtag; mtag=mtag->next)
            mtag->write_xml_attr();
        outs("/>\n");
    }


    MetaTagInfo     *tags;
    EspMethodInfo   *next;
};


class EspMountInfo
{
private:
    char *name_;
    char *localPath_;

public:
    EspMountInfo(const char *name, const char *localPath)
    {
        name_ =strdup(name);
        localPath_ =strdup(localPath);
        tags=NULL;
        next=NULL;
    }

    const char *getName(){return name_;}
    void setName(const char *name)
    {
        if (name_)
            free(name_);
        name_ =strdup(name);
    }

    const char *getLocalPath(){return localPath_;}
    void setLocalPath(const char *path)
    {
        if (localPath_)
            free(localPath_);
        localPath_ =strdup(path);
    }

    const char *getMetaString(const char *tag, const char *def_val)
    {
        return ::getMetaString(tags, tag, def_val);
    }

    bool getMetaStringValue(StrBuffer &val, const char *tag)
    {
        return ::getMetaStringValue(tags, val, tag);
    }

    int getMetaInt(const char *tag, int def_val=0)
    {
        return ::getMetaInt(tags, tag, def_val);
    }

    double getMetaDouble(const char* tag, double def_val=0)
    {
        return ::getMetaDouble(tags,tag,def_val);
    }

    MetaTagInfo     *tags;
    EspMountInfo    *next;
};


class EspStructInfo
{
private:
    char *name_;

public:
    EspStructInfo(const char *name)
    {
        name_ =strdup(name);
        tags=NULL;
        next=NULL;
    }

    const char *getName(){return name_;}
    void setName(const char *name)
    {
        if (name_)
            free(name_);
        name_ =strdup(name);
    }

    const char *getMetaString(const char *tag, const char *def_val)
    {
        return ::getMetaString(tags, tag, def_val);
    }

    bool getMetaStringValue(StrBuffer &val, const char *tag)
    {
        return ::getMetaStringValue(tags, val, tag);
    }

    int getMetaInt(const char *tag, int def_val=0)
    {
        return ::getMetaInt(tags, tag, def_val);
    }

    MetaTagInfo     *tags;
    EspStructInfo   *next;
};

typedef enum _catch_type
{
    ct_httpresp,
    ct_soapresp,
} catch_type;

class EspServInfo
{
private:
    char        *name_;
    char        *base_;
    bool needsXslt;


public:
    EspServInfo(const char *name)
    {
        name_ =strdup(name);
        base_=NULL;
        methods=NULL;
        mounts=NULL;
        tags=NULL;
        structs=NULL;
        next=NULL;
        needsXslt = false;
    }

    ~EspServInfo()
    {
        if (name_)
            free(name_);
        if (base_)
            free(base_);
    }

    const char *getName(){return name_;}
    void setName(const char *name)
    {
        if (name_)
            free(name_);
        name_ =strdup(name);
    }

    const char *getBase(){return base_;}
    void setBase(const char *base)
    {
        if (base_)
            free(base_);
        base_ =strdup(base);
    }

    const char *getMetaString(const char *tag, const char *def_val)
    {
        return ::getMetaString(tags, tag, def_val);
    }

    bool getMetaStringValue(StrBuffer &val, const char *tag)
    {
        return ::getMetaStringValue(tags, val, tag);
    }

    int getMetaInt(const char *tag, int def_val=0)
    {
        return ::getMetaInt(tags, tag, def_val);
    }

    void write_esxdl()
    {
        outf(2, "<EsdlService name=\"%s\" ", name_);
        for (MetaTagInfo *mtag=tags; mtag; mtag=mtag->next)
            mtag->write_xml_attr();
        outs(">\n");

        for (EspMethodInfo *mth=methods; mth; mth=mth->next)
            mth->write_esxdl();
        outs(2, "</EsdlService>");
    }

    EspStructInfo  *structs;
    EspMethodInfo   *methods;
    EspMountInfo    *mounts;
    MetaTagInfo     *tags;
    EspServInfo     *next;
    void sortMethods();
};


class ESDLcompiler
{
public:
    ESDLcompiler(const char * SourceFile,const char *OutDir);
    ~ESDLcompiler();

    void Process();
    void write_esxdl();

    const char *getPackageName(){return packagename;}
    char *       filename;
    StrBuffer name;

private:
    int esxdlo;
    char *       packagename;

public:
    ModuleInfo * modules;
    EnumInfo * enums;
    ApiInfo *apis;
    EspMessageInfo *msgs;
    EspServInfo *servs;
    EspMethodInfo *methods;
    IncludeInfo *includes;
    VersionInfo *versions;
};


//extern char * clarion;
extern int nCommentStartLine;
extern void yyerror(const char *s);
inline char upperchar(char val){return ((val<97 || val>122) ? val : (val-32));}

#endif
