/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef __HIDLCOMP_H__
#define __HIDLCOMP_H__

#include <stdarg.h>
#include <assert.h>
#include "hidl_utils.hpp"

#undef YYSTYPE
#define YYSTYPE attribute

#define HIDLVER "1.3"

#define RETURNNAME "_return"
#define FEATEACCESSATTRIBUTE "auth_feature"

#ifndef _WIN32
#define stricmp strcasecmp
#endif

inline bool streq(const char* s, const char* t) {  return strcmp(s,t)==0; }

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

struct attribute
{
private:
    union 
    {
        const char*  str_val;
        int    int_val;
        double double_val;
    };

    enum { t_none, t_string, t_int, t_double, t_name } atr_type;
    
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

    attribute(attribute& o)
    {
        // should not be called
        assert(false);
    }

    attribute& operator = (attribute& o)
    {
        if ((atr_type==t_string || atr_type==t_name) && str_val)
            free((char*)str_val);

        atr_type = o.atr_type;
        switch(atr_type)
        {
        case t_string: str_val = strdup(o.str_val); break;
        case t_int: int_val = o.int_val; break;
        case t_double: double_val = o.double_val; break;
        case t_name: str_val = strdup(o.str_val); break;
        default: break;
        }
        return *this;
    }

    const char* attrTypeDesc()
    {
        switch(atr_type)
        {
        case t_none: return "NONE";
        case t_string: return "STRING";
        case t_int: return "INT";
        case t_double: return "DOUBLE";
        case t_name: return "NAME";
        default: return "UNKNOWN";
        }
    }

    void release()
    {
        if ((atr_type==t_string || atr_type==t_name) && str_val!=NULL)
        {
            free((char*)str_val);
            str_val = NULL;
        }
        atr_type=t_none;
    }

    void setVal(int val)
    {
        release();

        int_val = val;
        atr_type = t_int;
    }

    void setVal(double val)
    {
        release();

        double_val = val;
        atr_type = t_double;
    }

    // NOTE: the caller allocates space for val; this obj frees the space
    void setVal(char *val)
    {
        release();

        str_val = val;
        atr_type = t_string;
    }

    const char *getString()
    {
        assert(atr_type==t_string);
        return str_val;
    }

    int getInt()
    {
        assert(atr_type==t_int);
        return int_val;
    }

    double getDouble()
    {
        assert(atr_type==t_double);
        return double_val;
    }

    const char *getName()
    {
        assert(atr_type==t_name);
        return str_val;
    }

    // NOTE: this function copy over the buf
    void setName(const char *value)
    {
        release();
        str_val = strdup(value);
        atr_type = t_name;
    }
    
    void setNameF(const char *format, ...) __attribute__((format(printf, 2, 3)))
    {
        release();
        
        va_list args;
        StrBuffer buf;
        
        va_start(args, format);
        buf.va_append(format,args);
        str_val = buf.detach();
        va_end(args);

        atr_type = t_name;
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
        name_ = strdup(name);
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
        delete next;
    }

    void release()
    {
        if (mttype_==mt_string || mttype_==mt_const_id)
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
        name_ = strdup(name);
    }

    void setString(const char *val)
    {
        if (mttype_==mt_string && str_val_!=NULL)
            free(str_val_);
        
        str_val_=strdup(val);

        mttype_ = mt_string;
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
    val.append((unsigned)(strlen(mtval)-2), mtval+1);
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
    
    void write_esp_param();
    void write_esp_declaration();
    void write_esp_ng_declaration(int pos);
    void write_esp_init(bool &isFirst, bool removeNil);
    void write_esp_marshall(bool isRpc, bool encodeXml, bool checkVer=false, int indent=1, bool encodeJson=true);
    void write_esp_unmarshall(const char *rpcvar, bool useBasePath=false, int indents=1);
    void write_esp_unmarshall_properties(const char *propvar, const char *attachvar, int indents=1);
    void write_esp_unmarshall_soapval(const char *var, int indents=1);
    void write_esp_unmarshall_attachments(const char *propvar, const char *attachvar, int indents=1);

    void write_esp_attr_method(const char *msgname, bool isSet, bool parNilRemove, bool isDecl, bool isPure, bool parTrim,const char* xsdType);
    void write_esp_attr_method_ng(const char *msgname, int pos, bool isSet, bool hasNilRemove);

    void write_esp_client_impl();

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

    const char* getArrayItemTag();
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

    // should be call once at a time. Better: use CStrBuffer
    const char* getXmlTag() 
    {
        static char buffer[256];
        const char* xmlTag = getMetaString("xml_tag", NULL);
        if (xmlTag) {
            if (*xmlTag == '"' || *xmlTag == '\'')
                xmlTag++;
            size_t len = strlen(xmlTag);
            if (*(xmlTag+len-1)=='"' || *(xmlTag+len-1)=='\'')
                len--;
            strncpy(buffer, xmlTag, len);
            buffer[len] = 0;
        }
        else
            strcpy(buffer,name);
        return buffer;
    }

    const char* getOptionalParam();

    type_kind kind;
    char      *name;
    char      *templ;
    char      *typname;
    char      *size;
    char      *sizebytes;
    unsigned   flags;
    LayoutInfo *layouts;
    ParamInfo   *next;  
    MetaTagInfo *tags;

private:
    char      *xsdtype;
    StrBuffer *m_arrayImplType;
};  

class ProcInfo
{
public:
    ProcInfo();
    ~ProcInfo();

    void out_clarion_parameter_list();
    void out_method(const char * classpfx=NULL,int omitvirt=0);
    void out_parameter_list(const char *pfx,int forclarion=0);
    void write_body_method_structs2(const char * modname);
    void write_body_popparam(int swapp);
    int write_body_swapparam();
    void write_body_popreturn();
    void write_body_pushparam(int swapp);
    void write_body_pushreturn();
    void write_head_size();

    void out_clarion_method();
    
    char      * name;
    ParamInfo * rettype;
    ParamInfo * params;
    char      * conntimeout;
    char      * calltimeout;
    int         async;
    int         callback;
    ParamInfo * firstin;    
    ParamInfo * lastin; 
    int         virt;
    int         constfunc;
    ProcInfo  * next;
};

class ModuleInfo
{
public:
    ModuleInfo(const char *n);
    ~ModuleInfo();

    void write_body_class();
    void write_body_class_proxy(int cb);
    void write_body_class_stub(int cb);
    void write_clarion_include_module();
    void write_clarion_interface_class();
    void write_clarion_scm_stub_class();
    void write_define();
    void write_example_module();
    void write_header_class();

    char        *name;
    char        *base;
    int          version;
    ProcInfo    *procs;
    ModuleInfo  *next;
    bool         isSCMinterface;
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
        delete next;
    }

   void write_cpp_header()
    {
        outf("#ifndef %s_API\n", name_);
        outf("#define %s_API\n", name_);
        outf("#endif //%s_API\n\n", name_);
    }

    char          *name_;
    ExportDefInfo *next;
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
        delete next;
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
        delete vals;
        delete next;
    }

    void write_header_enum();
    void write_clarion_enum();

    char         *name;
    EnumValInfo  *vals;
    EnumInfo     *next;
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

    ~EspMessageInfo() 
    {
        if (name_) 
            free(name_);
        if (base_)
            free(base_);
        if (xsdgrouptype)
            free(xsdgrouptype);
        if (parent)
            free(parent);

        delete tags;
        delete attrs_;
        delete next;
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

    void write_esp();
    void write_esp_ipp();
    void write_esp_ng_ipp();
    void write_esp_methods(enum espaxm_type=espaxm_both, bool isDecl=false, bool isPure=false);
    void write_esp_methods_ng(enum espaxm_type axstype=espaxm_both);

    void write_esp_mapinfo(bool isDecl);
    void write_cpp_interfaces();
    void write_factory_decl();
    void write_factory_impl();
    void write_esp_parms(bool isClientImpl);
    void write_esp_client_method(const char *serv, const char *respname, const char *methname, bool isDecl, bool isPure);

    void write_clarion_methods(enum espaxm_type axstype);
    void write_clarion_include_interface();

    const char *getParentName()
    {
        if (parent)
            return parent;
        if (!getMetaString("extends", NULL))
            return NULL;
        StrBuffer tmp;
        if (getMetaStringValue(tmp,"extends"))
            parent = tmp.detach();
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
            const char* s = getMetaString("xsd_group_type", "\"all\"");
            if (*s == '\"')
            {
                s++;  // Skip leading " and (assumed) trailing "
                size_t len = strlen(s);
                xsdgrouptype = (char*) malloc(len);
                memcpy(xsdgrouptype,s,len-1);
                xsdgrouptype[len-1]=0;
            }
            else
                xsdgrouptype = strdup(s);
        }
        return xsdgrouptype;
    }

    bool hasNonAttributeChild()
    {
        for (ParamInfo* pi=getParams();pi!=NULL;pi=pi->next)
            if (!pi->getMetaInt("attribute"))
                return true;
        return false;
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

    ~EspMethodInfo()
    {
        if (name_)
            free(name_);
        if (request_)
            free(request_);
        if (response_)
            free(response_);
        delete tags;
        delete proc_;
        delete next;
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

    bool hasMetaTag(const char *tag)
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

    bool getMetaVerInfo(const char* tag, StrBuffer& s)
    {
        return ::getMetaVerInfo(tags,tag,s);
    }

    EspMessageInfo *getRequestInfo();
    void write_esp_method(const char *servname, bool isDecl, bool isPure);

    bool write_mapinfo_check(const char* ctxvar);

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
    
    ~EspMountInfo() 
    {
        if (name_) 
            free(name_);
        if (localPath_) 
            free(localPath_);
        delete tags;
        delete next;
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
    
    ~EspStructInfo() 
    {
        if (name_) 
            free(name_);
        delete tags;
        delete next;
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
        next=NULL;
        needsXslt = false;
    }
    
    ~EspServInfo()
    {
        if (name_)
            free(name_);
        if (base_)
            free(base_);
        
        delete methods;
        delete mounts;
        delete tags;
        delete next;
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

    void write_event_interface();
    void write_esp_interface();
    void write_client_interface();
    void write_factory_impl();

    void write_esp_binding();
    void write_esp_binding_ipp();
    void write_esp_service_ipp();
    void write_esp_binding_ng_ipp(EspMessageInfo *);
    void write_esp_binding_ng_cpp(EspMessageInfo *);
    void write_esp_service_ng_ipp();
    void write_esp_service();
    void write_esp_client();
    void write_esp_client_ipp();
    void write_catch_blocks(EspMethodInfo* mthi, catch_type ct, int indents);
    void write_clarion_include_interface();

    EspMethodInfo   *methods;
    EspMountInfo    *mounts;
    MetaTagInfo     *tags;
    EspServInfo     *next;
    void sortMethods();
};

class IncludeInfo
{
public:
    IncludeInfo(const char *path)
    {
        path_ = strdup(path);
        next = NULL;
    };
    
    ~IncludeInfo() 
    { 
        if (path_)
            free(path_);
        delete next;
    }

    void write_cpp_interfaces()
    {
        outf(0, "#include \"%s_esp.ipp\"", path_);
    }

    char        *path_; 
    IncludeInfo *next;
};


class HIDLcompiler
{
public:
    HIDLcompiler(const char * SourceFile,const char *OutDir);
    ~HIDLcompiler();

    void Process();
    void write_source_file_classes();
    void write_clarion_HRPC_interfaces();
    void write_example_implementation_module();
    void write_header_class_intro();
    void write_header_class_outro();
    void write_esp();
    void write_esp_ng();
    void write_esp_ng_cpp();
    void write_esp_ex_ipp();

    void write_clarion_esp_interfaces();

    const char *getPackageName(){return packagename;}
    char *       filename;

private:
    int          espng;
    int          espngc;
    int          espx;
    int          espi;
    int          espc;
    int          clwo;
    int          cppo;
    int          ho;
    int          xsvo;
    char *       packagename;

public:
    ModuleInfo * modules;
    EnumInfo * enums;
    ApiInfo *apis;
    EspMessageInfo *msgs;
    EspServInfo *servs;
    IncludeInfo *includes;
};


extern bool isSCM;
extern bool isESP;
extern bool isESPng;
extern char *esp_def_export_tag;

extern StrBuffer clarion;
extern char srcFileExt[4];
extern int nCommentStartLine;
extern void yyerror(const char *s);
inline char upperchar(char val){return ((val<97 || val>122) ? val : (val-32));}

#endif
