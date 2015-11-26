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

#pragma warning(disable:4786)

#include "esdl_utils.hpp"

#include "platform.h"
#include "esdlcomp.h"
#undef new
#include <map>
#include <set>
#include <string>

//-------------------------------------------------------------------------------------------------------------
#define NEW_INSTANT_QUERY
inline bool es_strieq(const char* s,const char* t) { return stricmp(s,t)==0; }

//-------------------------------------------------------------------------------------------------------------
extern FILE *yyin;
extern int yyparse();

extern void yyrestart  (FILE * input_file );
extern int  yylex_destroy  (void);
extern void yyInitESDLGlobals(ESDLcompiler * esdlcompiler);
extern void yyCleanupESDLGlobals();

extern ESDLcompiler * hcp;
extern char *esp_def_export_tag;

// --- globals -----
int gOutfile = -1;

CriticalSection ESDLcompiler::m_critSect;
//-------------------------------------------------------------------------------------------------------------
// Utility struct and function

static const char* getTypeKindName(type_kind kind)
{
    switch (kind)
    {
    case TK_null: return "TK_null";
    case TK_CHAR: return "TK_CHAR";
    case TK_UNSIGNEDCHAR: return "TK_UNSIGNEDCHAR";
    case TK_BYTE: return "TK_BYTE";
    case TK_BOOL: return "TK_BOOL";
    case TK_SHORT: return "TK_SHORT";
    case TK_UNSIGNEDSHORT: return "TK_UNSIGNEDSHORT";
    case TK_INT: return "TK_INT";
    case TK_UNSIGNED: return "TK_UNSIGNED";
    case TK_LONG: return "TK_LONG";
    case TK_UNSIGNEDLONG: return "TK_UNSIGNEDLONG";
    case TK_LONGLONG: return "TK_LONGLONG";
    case TK_UNSIGNEDLONGLONG: return "TK_UNSIGNEDLONGLONG";
    case TK_DOUBLE: return "TK_DOUBLE";
    case TK_FLOAT: return "TK_FLOAT";
    case TK_STRUCT: return "TK_STRUCT";
    case TK_ENUM: return "TK_ENUM";
    case TK_VOID: return "TK_VOID";
    case TK_ESPSTRUCT: return "TK_ESPSTRUCT";
    case TK_ESPENUM: return "TK_ESPENUM";
    default: return "<unknown kind>";
    }
};

const char *type_name[] =
{
    "??",
    "char",
    "unsigned char",
    "byte",
    "bool",
    "short",
    "unsigned short",
    "int",
    "unsigned",
    "long",
    "unsigned long",
    "__int64",
    "unsigned __int64",
    "double",
    "float",
    "",  // STRUCT
    "",  // ENUM
    "void",
    "??", // ESPSTRUCT
    "??" // ESPENUM
};

const int type_size[] =
{
    1,
    1,
    1,
    1,
    0,  // pretend we don't know (To be fixed at some later date)
    2,
    2,
    4,
    4,
    4,
    4,
    8,
    8,
    8,
    4, // STRUCT
    4, // ENUM
    0, // void
    1, // ESP_STRUCT
    1  // ESP_ENUM
};

static const char *xlattable[] =
{
    "abs","_abs",
    "add","_add",
    "address","_address",
    "age","_age",
    "any","_any",
    "append","_append",
    "at","_at",
    "band","_band",
    "bfloat4","_bfloat4",
    "bfloat8","_bfloat8",
    "binary","_binary",
    "bind","_bind",
    "blob","_blob",
    "bor","_bor",
    "bshift","_bshift",
    "bxor","_bxor",
    "byte","_byte",
    "chr","_chr",
    "clear","_clear",
    "column","_column",
    "create","_create",
    "decimal","_decimal",
    "deformat","_deformat",
    "device","_device",
    "dim","_dim",
    "dispose","_dispose",
    "dock","_dock",
    "docked","_docked",
    "dup","_dup",
    "encrypt","_encrypt",
    "entry","_entry",
    "equate","_equate",
    "errorcode","_errorcode",
    "format","_format",
    "get","_get",
    "hlp","_hlp",
    "icon","_icon",
    "imm","_imm",
    "in","_in",
    "index","_index",
    "inlist","_inlist",
    "inrange","_inrange",
    "ins","_ins",
    "int","_int",
    "key","_key",
    "length","_length",
    "like","_like",
    "logout","_logout",
    "long","_long",
    "maximum","_maximum",
    "memo","_memo",
    "nocase","_nocase",
    "omitted","_omitted",
    "opt","_opt",
    "out","_out",
    "over","_over",
    "ovr","_ovr",
    "owner","_owner",
    "page","_page",
    "pageno","_pageno",
    "pdecimal","_pdecimal",
    "peek","_peek",
    "poke","_poke",
    "pre","_pre",
    "press","_press",
    "print","_print",
    "project","_project",
    "put","_put",
    "range","_range",
    "real","_real",
    "reclaim","_reclaim",
    "req","_req",
    "round","_round",
    "scroll","_scroll",
    "short","_short",
    "size","_size",
    "sort","_sort",
    "step","_step",
    "string","_string",
    "text","_text",
    "upr","_upr",
    "use","_use",
    "val","_val",
    "width","_width",
    NULL,NULL
};


static const char *xlat(const char *from)
{
    for (unsigned i=0;xlattable[i];i+=2) {
        if (stricmp(from,xlattable[i])==0)
            return xlattable[i+1];
    }
    return from;
}


bool toClaInterface(char * dest, const char * src)
{
    if(*src == 'I' && strlen(src) > 1)
    {
        strcpy(dest, "cpp");
        strcpy(dest + 3, src + 1);
        return true;
    }
    strcpy(dest, src);
    return false;
}

#define ForEachParam(pr,pa,flagsset,flagsclear) for (pa=pr->params;pa;pa=pa->next) \
if (((pa->flags&(flagsset))==(flagsset))&((pa->flags&(flagsclear))==0))

#define INDIRECTSIZE(p) ((p->flags&(PF_PTR|PF_REF))==(PF_PTR|PF_REF))

void indent(int indents)
{
    for (int i=0;i<indents; i++)
        out("\t",1);
}

void out(const char *s,size_t l)
{
    if(gOutfile != -1)
        write(gOutfile,s,l);
}

void outs(const char *s)
{
    out(s,strlen(s));
}

void outs(int indents, const char *s)
{
    indent(indents);
    out(s,strlen(s));
}

char *appendstr(char *text,const char *str)
{
    // not quick!
    if (text==NULL)
        return strdup(str);
    size_t l=strlen(text);
    text = (char *)realloc(text, l+strlen(str)+1);
    strcpy(text+l,str);
    return text;
}

static void voutf(const char* fmt,va_list args) __attribute__((format(printf,1,0)));

void voutf(const char* fmt,va_list args)
{
    const int BUF_LEN = 0x4000;
    static char buf[BUF_LEN+1];

    // Better to use StringBuffer.valist_appendf, but unfortunately, project dependencies
    // disallow us to use StringBuffer (defined in jlib).
    if (_vsnprintf(buf, BUF_LEN, fmt, args)<0)
        fprintf(stderr,"Warning: outf() gets too many long buffer (>%d)", BUF_LEN);
    va_end(args);

    outs(buf);
}

void outf(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    voutf(fmt,args);
    va_end(args);
}

void outf(int indents, const char *fmt, ...)
{
    indent(indents);

    va_list args;
    va_start(args, fmt);
    voutf(fmt,args);
    va_end(args);
}

// ------------------------------------
// "auto" indenting

int gIndent = 0;

void indentReset(int indent=0) { gIndent = indent; }
void indentInc(int inc) {  gIndent += inc; }

void indentOuts(const char* s)
{
    indent(gIndent);
    out(s,strlen(s));
}

void indentOuts(int inc, const char* s)
{
    gIndent += inc;
    indentOuts(s);
}

void indentOuts1(int inc, const char* s)
{
    indent(gIndent+inc);
    out(s,strlen(s));
}

static void indentOutf(const char* fmt, ...) __attribute__((format(printf,1,2)));
void indentOutf(const char* fmt, ...)
{
    indent(gIndent);

    va_list args;
    va_start(args, fmt);
    voutf(fmt,args);
    va_end(args);
}

static void indentOutf(int inc, const char* fmt, ...) __attribute__((format(printf,2,3)));
void indentOutf(int inc, const char* fmt, ...)
{
    gIndent += inc;
    indent(gIndent);

    va_list args;
    va_start(args, fmt);
    voutf(fmt,args);
    va_end(args);
}

static void indentOutf1(int inc, const char* fmt, ...) __attribute__((format(printf,2,3)));
void indentOutf1(int inc, const char* fmt, ...)
{
    indent(gIndent+inc);

    va_list args;
    va_start(args, fmt);
    voutf(fmt,args);
    va_end(args);
}

//-------------------------------------------------------------------------------------------------------------
// class LayoutInfo

LayoutInfo::LayoutInfo()
{
    size = 0;
    count = 0;
    next = NULL;
}

LayoutInfo::~LayoutInfo()
{
}

//-------------------------------------------------------------------------------------------------------------
// class ParamInfo

ParamInfo::ParamInfo()
{
    name = NULL;
    templ=NULL;
    typname=NULL;
    size = NULL;
    flags = 0;
    next = NULL;
    kind = TK_null;
    sizebytes = NULL;
    layouts = NULL;
    tags=NULL;
    xsdtype=NULL;
    m_arrayImplType = NULL;
}

ParamInfo::~ParamInfo()
{
    free(name);
    free(typname);
    free(size);
    free(sizebytes);
    if (templ)
        free(templ);
    if (xsdtype)
        free(xsdtype);
    if (m_arrayImplType)
        delete m_arrayImplType;

    while (layouts)
    {
        LayoutInfo *l=layouts;
        layouts = l->next;
        delete l;
    }

    while (tags)
    {
        MetaTagInfo *t=tags;
        tags = t->next;
        delete t;
    }
}

char * ParamInfo::bytesize(int deref)
{
    if (!size)
        return NULL;
    if (sizebytes)
        return sizebytes;
    char str[1024];
    if (type_size[kind]==1)
    {
        if (deref)
        {
            strcpy(str,"*");
            strcat(str,size);
            sizebytes = strdup(str);
            return sizebytes;
        }
        else
            return size;
    }

    strcpy(str,"sizeof(");
    if (kind==TK_STRUCT)
        strcat(str,typname);
    else
        strcat(str,type_name[kind]);
    strcat(str,")*(");
    if (deref)
        strcat(str,"*");
    strcat(str,size);
    strcat(str,")");
    sizebytes = strdup(str);
    return sizebytes;
}

bool ParamInfo::simpleneedsswap()
{
    switch(kind) {
    case TK_SHORT:
    case TK_UNSIGNEDSHORT:
    case TK_INT:
    case TK_UNSIGNED:
    case TK_LONG:
    case TK_UNSIGNEDLONG:
    case TK_LONGLONG:
    case TK_UNSIGNEDLONGLONG:
        return true;
    default:
        return false;
    }
}

void ParamInfo::cat_type(char *s,int deref,int var)
{
    if ((flags&PF_CONST)&&!var)
        strcat(s,"const ");
    if (typname)
        strcat(s,typname);
    else {
        if (kind!=TK_null)
            strcat(s,type_name[kind]);
        else
            strcat(s,"string"); // TODO: why this happens?
    }
    if (!deref) {
        if (flags&PF_PTR)
            strcat(s," *");
        if (flags&PF_REF)
            strcat(s," &");
    }
}

clarion_special_type_enum ParamInfo::clarion_special_type()
{
    if ((type_size[kind]==1)&&((flags&(PF_PTR|PF_REF))==PF_PTR)) {
        if ((flags&PF_CONST)==0)
            return cte_cstr;
        return cte_constcstr;
    }
    else if ((flags&(PF_PTR|PF_REF))==(PF_PTR|PF_REF)) { // no support - convert to long
        return cte_longref;
    }
    return cte_normal;
}

void ParamInfo::out_parameter(const char * pfx, int forclarion)
{
    if (forclarion && (clarion_special_type()==cte_cstr))
        outs("int, ");
    out_type();
    outf(" %s%s",pfx,name);
}

void ParamInfo::out_type(int deref,int var)
{
    char s[256];
    s[0] = 0;
    cat_type(s,deref,var);
    outs(s);
}

void ParamInfo::typesizeacc(char *accstr,size_t &acc)
{
    if ((kind==TK_STRUCT)||(flags&(PF_PTR|PF_REF))) {
        acc = (acc+3)&~3;
        if (*accstr)
            strcat(accstr,"+");
        strcat(accstr,"sizeof(");
        cat_type(accstr);
        strcat(accstr,")");
    }
    else {
        size_t sz=type_size[kind];
        if (sz==2)
            acc = (acc+1)&~1;
        else if (sz>=4)
            acc = (acc+3)&~3;
        acc += type_size[kind];
    }
}

size_t ParamInfo::typesizealign(size_t &ofs)
{
    size_t ret=0;
    if ((kind==TK_STRUCT)||(flags&(PF_PTR|PF_REF))) {
        if (ofs) {
            ret = 4-ofs;
            ofs = 0;
        }
    }
    else {
        size_t sz=type_size[kind];
        if (sz==1) {
            ret = 0;
            ofs = (ofs+1)%4;
        }
        else if (sz==2) {
            ret = (ofs&1);
            ofs = (ofs+ret+2)%4;
        }
        else {
            if (ofs) {
                ret = 4-ofs;
                ofs = 0;
            }
        }
    }
    return ret;
}

void ParamInfo::write_body_struct_elem(int ref)
{
    outs("\t");
    out_type(ref,1);
    if (ref&&(flags&(PF_REF|PF_PTR)))
    {
        outs(" *");
        if ((flags&(PF_REF|PF_PTR))==(PF_REF|PF_PTR))
        {
            outs(" *");
        }
    }
    outf(" %s;\n",name);
}


bool ParamInfo::hasMapInfo()
{
    if (hasMetaVerInfo("min_ver") || hasMetaVerInfo("max_ver") || hasMetaVerInfo("depr_ver"))
        return true;

    if (getMetaString("optional", NULL))
        return true;

    return false;
}

static esp_xlate_info esp_xlate_table[]=
{
    //meta type                 xsd type                implementation      array impl      access type             type_kind           flags               method
    //------------------        ---------------         --------------      --------------  --------------          -----------         ------------        ----------

//  {"string",                  "string",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"string",                  "string",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"StringBuffer",            "string",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
//  {"hexBinary",               "base64Binary",         "MemoryBuffer",     "???",          "unsigned char *",      TK_UNSIGNEDCHAR,    (PF_PTR),           EAM_jmbuf},
    {"binary",                  "base64Binary",         "MemoryBuffer",     "???",          "const MemoryBuffer &", TK_STRUCT,          (PF_REF),           EAM_jmbin},
    {"bool",                    "boolean",              "bool",             "BoolArray",    "bool",                 TK_BOOL,            0,                  EAM_basic},
    {"boolean",                 "boolean",              "bool",             "BoolArray",    "bool",                 TK_BOOL,            0,                  EAM_basic},
    {"decimal",                 "decimal",              "float",            "???",          "float",                TK_FLOAT,           0,                  EAM_basic},
    {"float",                   "float",                "float",            "FloatArray",   "float",                TK_FLOAT,           0,                  EAM_basic},
    {"double",                  "double",               "double",           "DoubleArray",  "double",               TK_DOUBLE,          0,                  EAM_basic},
    {"integer",                 "integer",              "int",              "???",          "int",                  TK_INT,             0,                  EAM_basic},
    {"int64",                   "long",                 "__int64",          "Int64Array",   "__int64",              TK_LONGLONG,        0,                  EAM_basic},
    {"long",                    "long",                 "long",             "Int64Array",   "__int64",              TK_LONG,            0,                  EAM_basic},
    {"int",                     "int",                  "int",              "IntArray",     "int",                  TK_INT,             0,                  EAM_basic},
    {"short",                   "short",                "short",            "ShortArray",   "short",                TK_SHORT,           0,                  EAM_basic},
    {"nonPositiveInteger",      "nonPositiveInteger",   "int",              "???",          "int",                  TK_INT,             0,                  EAM_basic},
    {"negativeInteger",         "negativeInteger",      "unsigned int",     "???",          "unsigned int",         TK_UNSIGNED,        0,                  EAM_basic},
    {"nonNegativeInteger",      "nonNegativeInteger",   "unsigned int",     "???",          "unsigned int",         TK_UNSIGNED,        0,                  EAM_basic},
    {"unsignedLong",            "unsignedLong",         "unsigned long",    "???",          "unsigned long",        TK_UNSIGNEDLONG,    0,                  EAM_basic},
    {"unsignedInt",             "unsignedInt",          "unsigned int",     "???",          "unsigned int",         TK_UNSIGNED,        0,                  EAM_basic},
    {"unsignedShort",           "unsignedShort",        "unsigned short",   "???",          "unsigned short",       TK_UNSIGNEDSHORT,   0,                  EAM_basic},
    {"unsignedByte",            "unsignedByte",         "unsigned char",    "???",          "unsigned char",        TK_UNSIGNEDCHAR,    0,                  EAM_basic},
    {"positiveInteger",         "positiveInteger",      "unsigned int",     "???",          "unsigned int",         TK_UNSIGNED,        0,                  EAM_basic},
    {"base64Binary",            "base64Binary",         "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"normalizedString",        "normalizedString",     "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdString",               "string",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdBinary",               "binary",               "MemoryBuffer",     "???",          "const MemoryBuffer &", TK_STRUCT,          (PF_REF),           EAM_jmbin},
    {"xsdBoolean",              "boolean",              "bool",             "???",          "bool",                 TK_BOOL,            0,                  EAM_basic},
    {"xsdDecimal",              "decimal",              "float",            "???",          "float",                TK_FLOAT,           0,                  EAM_basic},
    {"xsdInteger",              "integer",              "int",              "???",          "int",                  TK_INT,             0,                  EAM_basic},
    {"xsdByte",                 "byte",                 "unsigned char",    "???",          "unsigned char",        TK_UNSIGNEDCHAR,    0,                  EAM_basic},
    {"xsdDuration",             "duration",             "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdDateTime",             "dateTime",             "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdTime",                 "time",                 "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdDate",                 "date",                 "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdYearMonth",            "gYearMonth",           "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdYear",                 "gYear",                "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdMonthDay",             "gMonthDay",            "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdDay",                  "gDay",                 "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdMonth",                "gMonth",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdAnyURI",               "anyURI",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdQName",                "QName",                "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdNOTATION",             "NOTATION",             "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdToken",                "token",                "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdLanguage",             "language",             "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdNMTOKEN",              "NMTOKEN",              "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdNMTOKENS",             "NMTOKENS",             "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdName",                 "Name",                 "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdNCName",               "NCName",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdID",                   "ID",                   "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdIDREF",                "IDREF",                "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdIDREFS",               "IDREFS",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdENTITY",               "ENTITY",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdENTITIES",             "ENTITIES",             "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdNonPositiveInteger",   "nonPositiveInteger",   "int",              "???",          "int",                  TK_INT,             0,                  EAM_basic},
    {"xsdNegativeInteger",      "negativeInteger",      "unsigned int",     "???",          "unsigned int",         TK_UNSIGNED,        0,                  EAM_basic},
    {"xsdNonNegativeInteger",   "nonNegativeInteger",   "unsigned int",     "???",          "unsigned int",         TK_UNSIGNED,        0,                  EAM_basic},
    {"xsdUnsignedLong",         "unsignedLong",         "unsigned long",    "???",          "unsigned long",        TK_UNSIGNEDLONG,    0,                  EAM_basic},
    {"xsdUnsignedInt",          "unsignedInt",          "unsigned int",     "???",          "unsigned int",         TK_UNSIGNED,        0,                  EAM_basic},
    {"xsdUnsignedShort",        "unsignedShort",        "unsigned short",   "???",          "unsigned short",       TK_UNSIGNEDSHORT,   0,                  EAM_basic},
    {"xsdUnsignedByte",         "unsignedByte",         "unsigned char",    "???",          "unsigned char",        TK_UNSIGNEDCHAR,    0,                  EAM_basic},
    {"xsdPositiveInteger",      "positiveInteger",      "unsigned int",     "???",          "unsigned int",         TK_UNSIGNED,        0,                  EAM_basic},
    {"xsdBase64Binary",         "base64Binary",         "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"xsdNormalizedString",     "normalizedString",     "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"EspTextFile",             "string",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"EspResultSet",            "string",               "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {"AdoDataSet",              "tns:AdoDataSet",       "StringBuffer",     "StringArray",  "const char *",         TK_CHAR,            (PF_PTR|PF_CONST),  EAM_jsbuf},
    {NULL,                      NULL,                   NULL,               NULL,           NULL,                   TK_null,            0,                  EAM_basic}
};


esp_xlate_info *esp_xlat(const char *from, bool defaultToString)
{
    if (from)
    {
        for (unsigned i=0; esp_xlate_table[i].meta_type!=NULL; i++)
        {
            if (stricmp(from,esp_xlate_table[i].meta_type)==0)
                return &esp_xlate_table[i];
        }
    }
    return (defaultToString) ? &esp_xlate_table[0] : NULL;
}

//TODO: this is not bullet proof. better idea??
// Return: NULL if no need to be defined, otherwise the type to be defined
// Note: the caller needs to free memory
static char* getToBeDefinedType(const char* type)
{
    const char* colon = strchr(type, ':');
    const char* bareType = colon ? colon+1 : type;

    if (strnicmp(type, "xsd",colon-type)==0)
        return NULL;

    /*
    for (unsigned i=0; esp_xlate_table[i].meta_type!=NULL; i++)
    {
        if (stricmp(bareType,esp_xlate_table[i].xsd_type)==0)
            return NULL;
    }
    */

    if (strnicmp(type, "tns", colon-type)!=0)
    {
        char msg[128];
        sprintf(msg, "*** unhandled type: %s", type);
        outs(msg);
        yyerror(msg);
        return NULL;
    }

    if (strncmp(bareType, "ArrayOf", sizeof("ArrayOf")-1)==0)
        return strdup(bareType+sizeof("ArrayOf")-1);
    else
        return strdup(bareType);
}

static const char *MetaTypeToXsdType(const char *val)
{
    esp_xlate_info *xlation=esp_xlat(val);
    return (xlation) ? xlation->xsd_type : (const char *)"string";
}

// return: true if the ver tag is defined
bool hasMetaVerInfo(MetaTagInfo *list, const char* tag)
{
    double ver = getMetaDouble(list,tag,-1);
    if (ver>0)
        return true;

    const char* vs = getMetaString(list,tag, NULL);
    if (vs!=NULL)
        return true;

    const char* id = getMetaConstId(list,tag,NULL);
    if (id)
        return true;

    return false;
}

bool getMetaVerInfo(MetaTagInfo *list, const char* tag, StringBuffer& s)
{
    double ver = getMetaDouble(list,tag,-1);
    if (ver>0) {
        s.append(ver);
        return true;
    }

    const char* vs = getMetaString(list,tag, NULL);
    if (vs!=NULL) {
        if (*vs=='"' || *vs=='\'')
            vs++;
        double v = atof(vs);
        s.append(v);
        return true;
    }

    const char* id = getMetaConstId(list,tag,NULL);
    if (id) {
        s.append(id);
        return true;
    }

    return false;
}

static esp_xlate_info *esp_xlat(ParamInfo *pi)
{
    char metatype[256];
    *metatype=0;

    pi->cat_type(metatype);

    return esp_xlat(metatype);
}

void ParamInfo::setXsdType(const char *value)
{
    if (xsdtype)
        free(xsdtype);

    const char *newValue=value;
    if (strncmp(value, "xsd", 3)==0)
        newValue=MetaTypeToXsdType(value);

    xsdtype = (newValue!=NULL) ? strdup(newValue) : NULL;
}

const char *ParamInfo::getXsdType()
{
    if (xsdtype==NULL)
    {
        char metatype[256];
        *metatype=0;
        cat_type(metatype);

        setXsdType(MetaTypeToXsdType(metatype));
    }

    return xsdtype;
}

const char* ParamInfo::getArrayImplType()
{
    if (m_arrayImplType)
        return m_arrayImplType->str();

    if (isPrimitiveArray())
    {
        char metatype[256];
        metatype[0] = 0;
        cat_type(metatype);
        esp_xlate_info *xlation=esp_xlat(metatype, false);
        m_arrayImplType = new StringBuffer(xlation->array_type);
    }
    else
    {
        if (kind == TK_ESPENUM)
            m_arrayImplType = new VStringBuffer("%sArray", typname);
        else
            m_arrayImplType = new VStringBuffer("IArrayOf<IConst%s>", typname);
    }

    return m_arrayImplType->str();
}

const char* ParamInfo::getArrayItemXsdType()
{
    switch (kind)
    {
    case TK_CHAR: return "string";
    case TK_UNSIGNEDCHAR: return "string"; //?
    case TK_BYTE: return "byte";
    case TK_BOOL: return "boolean";
    case TK_SHORT: return "short";
    case TK_UNSIGNEDSHORT: return "unsignedShort";
    case TK_INT: return "int";
    case TK_UNSIGNED: return "unsignedInt";
    case TK_LONG: return "long";
    case TK_UNSIGNEDLONG: return "unsignedLong";
    case TK_LONGLONG: return "long";
    case TK_UNSIGNEDLONGLONG: return "unsignedLong";
    case TK_DOUBLE: return "double";
    case TK_FLOAT: return "float";

    case TK_null: return "string";

    case TK_STRUCT:
    case TK_VOID:
    case TK_ESPSTRUCT:
    case TK_ESPENUM:
    default: throw "Unimplemented";
    }
}

//-------------------------------------------------------------------------------------------------------------
// class ProcInfo

ProcInfo::ProcInfo()
{
    name = NULL;
    rettype = NULL;
    params = NULL;
    next = NULL;
    conntimeout = NULL;
    calltimeout = NULL;
    async = 0;
    callback = 0;
    virt = 0;
    constfunc = 0;
}

ProcInfo::~ProcInfo()
{
    free(name);
    free(conntimeout);
    free(calltimeout);
    delete rettype;
    while (params)
    {
        ParamInfo *p=params;
        params = p->next;
        delete p;
    }
}

//-------------------------------------------------------------------------------------------------------------
// class ApiInfo

ApiInfo::ApiInfo(const char *n)
{
    name = NULL;
    group = strdup(n);
    proc = NULL;
    next = NULL;
}

ApiInfo::~ApiInfo()
{
    free (group);
    free (name);

    while (proc)
    {
        ProcInfo *pr=proc;
        proc = pr->next;
        delete pr;
    }
}

//-------------------------------------------------------------------------------------------------------------
// class ModuleInfo

ModuleInfo::ModuleInfo(const char *n)
{
    name = strdup(n);
    base = NULL;
    version = 0;
    procs = NULL;
    next = NULL;
    isSCMinterface=false;
}

ModuleInfo::~ModuleInfo()
{
    while (procs)
    {
       ProcInfo *proc=procs;
       procs = proc->next;
       delete proc;
    }

    free (name);
    free (base);
}

//-------------------------------------------------------------------------------------------------------------
// class EspMessageInfo

bool EspMessageInfo::hasMapInfo()
{
    for (ParamInfo* pi=getParams();pi!=NULL;pi=pi->next)
        if (pi->hasMapInfo())
            return true;
    return false;
}

EspMessageInfo *EspMessageInfo::find_parent()
{
    if (parent && *parent)
    {
        EspMessageInfo *msg = hcp->msgs;
        for(;msg!=NULL; msg=msg->next)
        {
            if (!strcmp(msg->getName(), parent))
                return msg;
        }
    }
    return NULL;
}

char* makeXsdType(const char* s)
{
    if (!s)
        return NULL;

    if (*s == '"')
        s++;
    if (strncmp(s,"tns:",4)==0)
    {
        s+=4;
        int len = strlen(s);
        if (*(s+len-1)=='"')
            len--;
        char* t = (char*)malloc(len+1);
        memcpy(t,s,len);
        t[len] = 0;
        return t;
    }
    else
        return NULL;
}

EspMessageInfo *EspMethodInfo::getRequestInfo()
{
    EspMessageInfo *msg = hcp->msgs;

    for(;msg!=NULL; msg=msg->next)
    {
        if (!strcmp(msg->getName(), request_))
            return msg;
    }
    return NULL;
}

static EspMethodInfo* sortMethods(EspMethodInfo* ms)
{
    if (ms==NULL)
        return ms;

    // find the smallest node
    EspMethodInfo* smallest = ms;
    EspMethodInfo* prev = NULL; // the node right before the smallest node
    for (EspMethodInfo* p = ms; p->next!=NULL; p = p->next)
    {
        if (strcmp(p->next->getName(), smallest->getName())<0)
        {
            prev = p;
            smallest = p->next;
        }
    }

    // move the smallest to the head
    if (smallest != ms)
    {
        if (prev == ms)
        {
            ms->next = smallest->next;
            smallest->next = ms;
        }
        else
        {
            EspMethodInfo* tmp = smallest->next;
            smallest->next = ms->next;
            prev->next = ms;
            ms->next = tmp;
        }
    }

    // recursively sort
    smallest->next = sortMethods(smallest->next);

    return smallest;
}

void EspServInfo::sortMethods()
{
    methods = ::sortMethods(methods);
}

//-------------------------------------------------------------------------------------------------------------
// class ESDLcompiler

char* getTargetBase(const char* outDir, const char* src)
{
    if (outDir && *outDir)
    {
        int dirlen = strlen(outDir);
        int srclen = strlen(src);
        char* buf = (char*)malloc(dirlen+srclen+5);

        // get file name only
        const char* p = src+srclen-1;
        while(p>src && *p!='/' && *p!='\\') p--;
        if (*p == '/' || *p == '\\') p++;

        // dir: outDir+'/'+fileName
        strcpy(buf,outDir);

        int len = strlen(buf);
        if (buf[len-1]=='/' || buf[len-1]=='\\')
        {
            buf[len-1]=0;
            len--;
        }

        // now buf has the directory name for output: make the directory if not exist
        es_createDirectory(buf);

        // copy the file name
        buf[len] = '/';
        strcpy(buf+len+1, p);
        return buf;
    }
    else
        return strdup(src);
}

ESDLcompiler::ESDLcompiler(const char * sourceFile, bool generatefile, const char *outDir, bool outputIncludes_, bool isIncludedEsdl)
{
    outputIncludes = outputIncludes_;
    modules = NULL;
    enums = NULL;
    apis=NULL;
    servs=NULL;
    msgs=NULL;
    includes=NULL;
    methods=NULL;
    versions = NULL;
    esxdlo = -1;
    StringBuffer ext;
    StringBuffer prot;
    splitFilename(sourceFile, &prot, &srcDir, &name, &ext);

    filename = strdup(sourceFile);
    size_t l = strlen(filename);

    yyin = fopen(sourceFile, "rt");
    if (!yyin)
    {
        if (isIncludedEsdl)
        {
            StringBuffer alternateExtFilename;
            alternateExtFilename.setf("%s%s%s", (prot.length()>0) ? prot.str() : "", srcDir.str(), name.str());

            if (stricmp(ext.str(), ESDL_FILE_EXTENSION)==0)
                alternateExtFilename.append(LEGACY_FILE_EXTENSION);
            else
                alternateExtFilename.append(ESDL_FILE_EXTENSION);

            yyin = fopen(alternateExtFilename.str(), "rt");
            if (!yyin)
            {
                fprintf(stderr, "Fatal Error: Could not load included ESDL grammar %s\n", filename);
                exit(1);
            }
        }
        else
        {
            fprintf(stderr, "Fatal Error: Could not load ESDL grammar %s\n", sourceFile);
            exit(1);
        }

    }

    packagename = es_gettail(sourceFile);

    if (generatefile)
    {
        if (!outDir || !*outDir)
            outDir = srcDir.str();

        char* targetBase = getTargetBase(outDir, sourceFile);

        esxdlo = es_createFile(targetBase,"xml");

        free(targetBase);
    }
}

ESDLcompiler::~ESDLcompiler()
{
    free(packagename);
    free(filename);

    while (modules)
    {
        ModuleInfo *mo=modules;
        modules = mo->next;
        delete mo;
    }

    while (enums)
    {
        EnumInfo *en=enums;
        enums = en->next;
        delete en;
    }

    while (apis)
    {
        ApiInfo *api=apis;
        apis = api->next;
        delete api;
    }

    while (servs)
    {
        EspServInfo *ser=servs;
        servs = ser->next;
        delete ser;
    }

    while (includes)
    {
        IncludeInfo *in=includes;
        includes = in->next;
        delete in;
    }

    while (methods)
    {
        EspMethodInfo *meth=methods;
        methods = meth->next;
        delete meth;
    }

    while (versions)
    {
        VersionInfo *mes=versions;
        versions = mes->next;
        delete mes;
    }

    while (msgs)
    {
        EspMessageInfo *mi=msgs;
        msgs = mi->next;
        delete mi;
    }

    while (includes)
    {
        IncludeInfo *in=includes;
        includes = in->next;
        delete in;
    }
}

void ESDLcompiler::Process()
{
    CriticalBlock block(m_critSect);

    yyInitESDLGlobals(this);
    yyrestart (yyin);
    yyparse();

    if (nCommentStartLine > -1)
    {
        char tempBuf[256];
        sprintf(tempBuf, "The comment that started at line %d is not ended yet", nCommentStartLine);
        yyerror(tempBuf);
    }
    write_esxdl();

    fclose(yyin);
    if (gOutfile > 0)
        close (gOutfile);

    yyCleanupESDLGlobals();
    yylex_destroy();
}

void ESDLcompiler::write_esxdl()
{
    esxdlcontent.clear();

    VersionInfo * vi;
    for (vi=versions;vi;vi=vi->next)
    {
        vi->toString(esxdlcontent);
    }

    if(outputIncludes)
    {
        IncludeInfo * ii;
        for (ii=hcp->includes;ii;ii=ii->next)
        {
            ii->toString(esxdlcontent);
        }
    }

    EspMessageInfo * mi;
    for (mi=msgs;mi;mi=mi->next)
    {
        mi->toString(esxdlcontent);
    }

    EspServInfo *si;
    for (si=servs;si;si=si->next)
    {
        si->toString(esxdlcontent);
    }

    if (methods)
    {
        EspMethodInfo *sm;
        for (sm=methods;sm;sm=sm->next)
        {
            sm->toString(esxdlcontent);
        }
    }


    if (esxdlo > 0)
    {
        //Populate the file
        StringBuffer tmp;
        tmp.setf("<esxdl name=\"%s\">\n", name.str()).append(esxdlcontent.str()).append("</esxdl>");
        gOutfile = esxdlo;
        outs(tmp.str());
        gOutfile = -1;
    }
}

// end
//-------------------------------------------------------------------------------------------------------------
