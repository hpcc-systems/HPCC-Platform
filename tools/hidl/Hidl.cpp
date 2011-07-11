/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

//
// HIDL compiler

#define HIDLVER "1.0"

#include "platform.h"

#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdarg.h> 


// TBD should have used yacc/lex - will do so when get time

enum type_kind
{
    TK_null,
    TK_CHAR,
    TK_UNSIGNEDCHAR,
    TK_SHORT,
    TK_UNSIGNEDSHORT,
    TK_INT,
    TK_UNSIGNED,
    TK_LONG,
    TK_UNSIGNEDLONG,
    TK_LONGLONG,
    TK_UNSIGNEDLONGLONG,
    TK_STRUCT,
    TK_VOID
};

#define PF_IN       1
#define PF_OUT      2
#define PF_VARSIZE  4
#define PF_PTR      8
#define PF_REF      0x10
#define PF_CONST    0x40
#define PF_SIMPLE   0x80
#define PF_RETURN   0x100
#define PF_STRING   0x200
#define PF_MASK     0x7fff


/*
Allowed Parameter profiles

in T                - simple
in T&               - ref
in size() T*        - sized ptr
inout T&            - ref in/out
inout size() T*     - size is in only
out T&              - ref out only
out size() T*       - size is in only
out size() T*&      - size out only caller frees
in string const char* - in string
out string char *&    - out string (caller frees)

return (implied out):

  T             - simple return
  size() T*     - size out only caller frees

*/



const char *type_name[] =
{
    "??",
    "char",
    "unsigned char",
    "short",
    "unsigned short",
    "int",
    "unsigned",
    "long",
    "unsigned long",
    "long long",
    "unsigned long long",
    "",
    "void"

};

const char *clarion_type_name[] =
{
    "??",
    "BYTE",
    "BYTE",
    "SHORT",
    "USHORT",
    "LONG",
    "ULONG",
    "LONG",
    "ULONG",
    "LONGLONG",
    "ULONGLONG",
    "",
    "BYTE"

};

const int type_size[] =
{
    1,
    1,
    1,
    2,
    2,
    4,
    4,
    4,
    4,
    8,
    8,
    0,
    1
};

#define RETURNNAME "_return"



class ParamInfo
{
public:
    ParamInfo()
    {
        name = NULL;
        typname=NULL;
        size = NULL;
        flags = 0;      
        next = NULL;
        kind = TK_null;
        sizebytes = NULL;
    }
    ~ParamInfo()
    {
        free(name);
        free(typname);
        free(size);
        free(sizebytes);
    }
    char *bytesize(int deref=0)
    {
        if (!size)
            return NULL;
        if (sizebytes)
            return sizebytes;
        char str[1024];
        if (type_size[kind]==1)
            if (deref) {
                strcpy(str,"*");
                strcat(str,size);
                sizebytes = strdup(str);
                return sizebytes;

            }
            else
                return size;
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

    int simpleneedsswap()
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
            return 1;
        }
        return 0;
    }



    type_kind kind;
    char      *name;
    char      *typname;
    char      *size;
    char      *sizebytes;
    unsigned   flags;
    ParamInfo  *next;   
};  
    



#define ForEachParam(pr,pa,flagsset,flagsclear) for (pa=pr->params;pa;pa=pa->next) \
                                                 if (((pa->flags&(flagsset))==(flagsset))&((pa->flags&(flagsclear))==0))

#define INDIRECTSIZE(p) ((p->flags&(PF_PTR|PF_REF))==(PF_PTR|PF_REF))

    
class ProcInfo
{
public:
    ProcInfo()
    {
        name = NULL;
        rettype = NULL;
        params = NULL;
        next = NULL;
        async = 0;
        virt = 0;
        callback = 0;
    }
    ~ProcInfo()
    {
        free(name);
        delete rettype;
        while (params) {
            ParamInfo *p=params;
            params = p->next;
            delete p;
        }
    }

    char      * name;
    ParamInfo * rettype;
    ParamInfo * params;
    ProcInfo  * next;
    int         async;
    int         virt;
    int         callback;
    ParamInfo * firstin;    
    ParamInfo * lastin; 

};

class ModuleInfo
{
public:
    ModuleInfo(const char *n,size32_t l)
    {
        name = (char *)malloc(l+1);
        memcpy(name,n,l);
        name[l] = 0;
        version = 0;
        procs = NULL;
        next = NULL;
    }
    ~ModuleInfo()
    {
        free(name);
        while (procs) {
            ProcInfo *p=procs;
            procs = p->next;
            delete p;
        }
    }
    char        *name;
    int          version;
    ProcInfo    *procs;
    ModuleInfo  *next;

};
        


enum tok_kind {
    TOK_null,
    TOK_IDENT,
    TOK_NUMBER,
    TOK_CHARCONST,
    TOK_STRCONST,
    TOK_LPAREN,
    TOK_RPAREN,
    TOK_LBRACE,
    TOK_RBRACE,
    TOK_LBRACKET,
    TOK_RBRACKET,
    TOK_LANGLE,
    TOK_RANGLE,
    TOK_STAR,
    TOK_AMPERSAND,
    TOK_COMMA,
    TOK_EQUAL,
    TOK_COLON,
    TOK_SEMICOLON,
    TOK_CONST,
    TOK_INT,
    TOK_SHORT,
    TOK_LONG,
    TOK_UNSIGNED,
    TOK_FLOAT,
    TOK_DOUBLE,
    TOK_CHAR,
    TOK_VOID,
    TOK_MODULE,
    TOK_IN,
    TOK_INOUT,
    TOK_OUT,
    TOK_STRING,
    TOK_SIZE,
    TOK_ASYNC,
    TOK_CALLBACK,
    TOK_VIRTUAL,
    TOK_EOF
};

static const struct tokdef { tok_kind k; const char *n; } symbols[] =
{
    {TOK_MODULE,"module"},
    {TOK_CONST, "const"},
    {TOK_VOID, "void"},
    {TOK_CHAR, "char"},
    {TOK_INT, "int"},
    {TOK_UNSIGNED, "unsigned"},
    {TOK_SHORT, "short"},
    {TOK_LONG, "long"},
    {TOK_FLOAT, "float"},
    {TOK_DOUBLE, "double"},
    {TOK_IN, "in"},
    {TOK_OUT, "out"},
    {TOK_INOUT, "inout"},
    {TOK_STRING, "string"},
    {TOK_SIZE,"size"},
    {TOK_ASYNC,"async"},
    {TOK_CALLBACK,"callback"},
    {TOK_VIRTUAL,"virtual"},
    {TOK_EOF, "??????"},
};


class token {
public:
    tok_kind kind;
    const char *str;        //in buffer
    size32_t        len;
    char *copystr() { char *ret=(char *)malloc(len+1); memcpy(ret,str,len); ret[len] = 0; return ret; }
    int integer()
    {
        size32_t l=len;
        if (l>15)
            l = 15;
        char num[16];
        memcpy(num,str,l);
        num[l] = 0;
        return atoi(num);
    }
};


class HIDLcompiler
{

protected:
    const char *start;
    const char *s;
    int lineno;
    token t;
    int outfile;

    void error(const char *err)
    {
        printf("ERROR: line %d:  %s\n",lineno,err);
        const char *sp=s;
        while ((sp!=start)&&((s-sp<10)||(*(sp-1)!='\n')))
            sp--;
        printf("Near the end of:\n-----\n");
        char *ln = (char *)malloc(s-sp+1);
        memcpy(ln,sp,s-sp);
        ln[s-sp] = 0;
        printf("%s\n-----\n",ln);
        free(ln);
        exit(1);
    }

    

    int parse_strconst()
    {
        t.str = s+1;
        do {
            s++;
            if (*s=='\\')
                s++;
            if (*s==0) {
                error("\" expected");
                return 0;
            }
        } while (*s!='"');
        t.kind = TOK_STRCONST;
        t.len = s-t.str;
        s++;
        return 1;
    }
    int parse_chrconst()
    {
        t.str = s+1;
        do {
            s++;
            if (*s=='\\')
                s++;
            if (*s==0)
                return 0;
        } while (*s!='\'');
        t.kind = TOK_CHARCONST;
        t.len = s-t.str;
        s++;
        return 1;
    }
    int parse_int()
    {
        t.str = s;
        do {
            s++;
            if (*s=='\\')
                s++;
            if (*s==0)
                return 0;
        } while (isalnum(*s));
        t.kind = TOK_NUMBER;
        t.len = s-t.str;
        return 1;
    }
    int parse_id()
    {
        t.str = s;
        do {
            s++;
            if (*s=='\\')
                s++;
            if (*s==0)
                return 0;
        } while (isalnum(*s)||(*s=='_'));
        t.len = s-t.str;
        for (int i=0;symbols[i].k!=TOK_EOF;i++) {
            if (memcmp(t.str,symbols[i].n,t.len)==0) {
                if (symbols[i].n[t.len]==0) {
                    t.kind = symbols[i].k;
                    return 1;
                }
            }
        }
        t.kind = TOK_IDENT;
        return 1;
    }       

    char *parse_paren_str()
    {
        int pc = 1;
        const char *st=s;
        while (1) {
            if (*s==')')
                if (--pc==0)
                    break;
            else if (*s=='(')
                pc++;
            else if (*s==0) {
                error(") expected");
                return NULL;
            }
            s++;
        }
        char *ret = (char *)malloc(s-st+1);
        memcpy(ret,st,s-st);
        ret[s-st] = 0;
        return ret;
    }

    int nexttoken()
    {
        t.kind = TOK_null;
        t.str = NULL;
        t.len = 0;
        while (1) {
            switch (*s) {
            case 0:         return 0;
            case ':':       t.kind = TOK_COLON; s++; return 1;
            case ';':       t.kind = TOK_SEMICOLON; s++; return 1;
            case ',':       t.kind = TOK_COMMA; s++; return 1;
            case '=':       t.kind = TOK_EQUAL; s++; return 1;
            case '*':       t.kind = TOK_STAR; s++; return 1;
            case '[':       t.kind = TOK_LBRACKET; s++; return 1;
            case ']':       t.kind = TOK_RBRACKET; s++; return 1;
            case '{':       t.kind = TOK_LBRACE; s++; return 1;
            case '}':       t.kind = TOK_RBRACE; s++; return 1;
            case '(':       t.kind = TOK_LPAREN; s++; return 1;
            case ')':       t.kind = TOK_RPAREN; s++; return 1;
            case '<':       t.kind = TOK_LANGLE; s++; return 1;
            case '>':       t.kind = TOK_RANGLE; s++; return 1;
            case '&':       t.kind = TOK_AMPERSAND; s++; return 1;
            case '"':       return parse_strconst();
            case '\'':      return parse_chrconst();
            case ' ':
            case '\t':
            case '\r':
                            break;
            case '\n':
                            lineno++;
                            break;
            case '/':       if (s[1]=='/') {
                                do {
                                    s++;
                                } while ((s[1]!=0)&&(s[1]!='\n'));
                            }
                            else if (s[1]=='*') {
                                do {
                                    s++;
                                    if (*s=='\n')
                                        lineno++;
                                    if (*s==0) {
                                        error("*/ expected");
                                        return 0;
                                    }
                                } while ((s[0]!='*')||(s[1]!='/'));
                            }
                            break;
            case '-':
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':       return parse_int();
            default:
                            if (isalpha(*s)||(*s=='_'))
                                return parse_id();
                            error("unexpected character");
            }
            s++;
        }
        return 0;
    }

    tok_kind gettoken()
    {
        if (nexttoken())
            return t.kind;
        error("Premature EOF");
        return TOK_null;
    }

    int expects(tok_kind k)
    {
        nexttoken();
        return (t.kind==k);
    }

    int skip_to_module()
    {
        int i=0;
        for (;symbols[i].k!=TOK_MODULE;i++);
        const char *m=symbols[i].n;
        size32_t l = strlen(m);
        while (*s) {
            while (isspace(*s))
                s++;
            if (memcmp(s,m,l)==0) {
                if (isspace(s[l])) {
                    return 1;
                }
            }
            while (*s!='\n') {
                if (!*s) {
                    return 0;
                }
                s++;
            }
            lineno++;
            s++;
        }
        return 0;
    }

    
    int check_param(ProcInfo *pi,ParamInfo *p)
    {
        if ((pi->async)&&(p->flags&(PF_OUT|PF_RETURN))) {
            error("out parameters not allowed on async procedure");
            return 0;
        }
        if ((p->flags&(PF_CONST&PF_OUT))==(PF_CONST|PF_OUT)) {
            error("const not allowed on out parameter");
            return 0;
        }
        switch (p->flags&(PF_IN|PF_OUT|PF_STRING|PF_VARSIZE|PF_PTR|PF_REF|PF_RETURN)) {
        case PF_IN:                                 // int T
        case (PF_IN|PF_REF):                        // in T&
        case (PF_IN|PF_OUT|PF_REF):                 // inout T&
        case (PF_OUT|PF_REF):                       // out T&
            return 1;
        case (PF_IN|PF_PTR|PF_VARSIZE):             // in size() T*
        case (PF_OUT|PF_PTR|PF_VARSIZE):            // out size() T*
        case (PF_IN|PF_OUT|PF_PTR|PF_VARSIZE):      // inout size() T*
            // should check size is in here
            return 1;
        case (PF_OUT|PF_PTR|PF_REF|PF_VARSIZE):     // inout size() T*&
            // should check size is out here
            return 1;
        case (PF_IN|PF_PTR|PF_STRING):              // in string const char *
        case (PF_OUT|PF_PTR|PF_REF|PF_STRING):      // out string char *&
            // should check char or unsigned char
            return 1;
        case (PF_OUT|PF_RETURN):                    // return simple    
            return 1;
        case (PF_OUT|PF_RETURN|PF_STRING|PF_PTR):           // return char *
            return 1;
        case (PF_OUT|PF_PTR|PF_VARSIZE|PF_RETURN):  // return size() T*
            // should check size is out here
            return 1;
        }
        printf("Parameter flags %x\n",p->flags);
        if (p->flags&PF_RETURN)
            error("Invalid return type");
        else
            error("Invalid parameter combination");
        return 0; // TBD
    }

    ParamInfo *parse_param(ProcInfo *pi,int rettype)
    {
        ParamInfo *p = new ParamInfo;
        while (1) {
            switch (t.kind) {
            case TOK_IN:
                p->flags |= PF_IN;
                break;
            case TOK_OUT:
                p->flags |= PF_OUT;
                break;
            case TOK_INOUT:
                p->flags |= (PF_IN|PF_OUT);
                break;
            case TOK_STRING:
                p->flags |= PF_STRING;
                break;
            case TOK_SIZE:
                if (!expects(TOK_LPAREN)) {
                   error("( expected");
    Error:          
                   delete p;
                   return NULL;
                }
                p->size = parse_paren_str();
                p->flags |= PF_VARSIZE;
                if (!expects(TOK_RPAREN)) {
                   error(") expected");
                   goto Error;
                }
                break;
            case TOK_IDENT:
                // assume it is a name until the second ID
                if (rettype) {
                    if (p->kind==TK_null) {
                        p->kind = TK_STRUCT;
                        p->typname = t.copystr();
                    }
                    else if ((p->kind==TK_VOID)&&((p->flags&PF_PTR)==0)) {
                        delete p;
                        return NULL;
                    }
                    else {
                        p->flags |= (PF_OUT|PF_RETURN);
                        p->name = strdup(RETURNNAME);
                        goto Exit;
                    }
                    break;
                }
                if (p->name) {
                    if (p->kind==TK_null) {
                        p->kind = TK_STRUCT;
                        p->typname = p->name;
                    }
                    else {
                        error("unknown/unexpected ID");
                        goto Error;
                    }
                }
                p->name = t.copystr();
                break;

            case TOK_COMMA:
            case TOK_RPAREN:
                if (p->kind==TK_null) {
                    error("no type specified");
                    goto Error;
                }
                if (!p->name) {
                    error("no name specified");
                    goto Error;
                }
                if ((p->flags&(PF_OUT|PF_IN))==0) {
                    p->flags |= PF_IN;
                }
Exit:
                if (!check_param(pi,p)) {
                    goto Error;
                }
                return p;
            case TOK_CHAR:
                switch (p->kind) {
                case TK_UNSIGNED:   p->kind = TK_UNSIGNEDCHAR; break;
                case TK_null:       p->kind = TK_CHAR; break;
                default: {
    Error2:                 
                        error("invalid type");
                        goto Error;
                    }
                }
                break;
            case TOK_SHORT:
                switch (p->kind) {
                case TK_UNSIGNED:   p->kind = TK_UNSIGNEDSHORT; break;
                case TK_null:       p->kind = TK_SHORT; break;
                default:
                    goto Error2;
                }
                break;
            case TOK_VOID:
                p->kind = TK_VOID;
                break;
            case TOK_INT:
                switch (p->kind) {
                case TK_UNSIGNED:   break;
                case TK_SHORT:      p->kind = TK_SHORT; break;
                case TK_LONG:       p->kind = TK_LONG; break;
                case TK_null:       p->kind = TK_INT; break;
                default:
                    goto Error2;
                }
                break;
            case TOK_UNSIGNED:
                switch (p->kind) {
                case TK_null:       p->kind = TK_UNSIGNED; break;
                default:
                    goto Error2;
                }
                break;
            case TOK_LONG:
                switch (p->kind) {
                case TK_LONG:       p->kind = TK_LONGLONG; break;
                case TK_UNSIGNED:   p->kind = TK_UNSIGNEDLONG; break;
                case TK_UNSIGNEDLONG:   p->kind = TK_UNSIGNEDLONGLONG; break;
                case TK_null:       p->kind = TK_LONG; break;
                default:
                    goto Error2;
                }
                break;
            case TOK_STAR:
                if (p->flags&(PF_PTR|PF_REF)) {
Error3:                 
                    error("parameter type not supported");
                    goto Error;
                }
                p->flags|=PF_PTR;
                break;
            case TOK_AMPERSAND:
                if (p->flags&PF_REF) {
                    goto Error3;
                }
                p->flags|=PF_REF;
                break;
            case TOK_CONST:
                p->flags|=PF_CONST;
                break;
            default:
                error("unexpected symbol");
                goto Error;
            }
            gettoken();
        }       
        return NULL;
    }
    
        
    ProcInfo *parse_proc()
    {
        ProcInfo *pi=new ProcInfo;
        while (1) {
            switch (t.kind) {
            case TOK_ASYNC:
                pi->async = 1;
                gettoken();
                continue;
            case TOK_CALLBACK:
                pi->callback = 1;
                gettoken();
                continue;
            case TOK_VIRTUAL:
                pi->virt = 1;
                gettoken();
                continue;
            }
            break;
        }
        pi->rettype = parse_param(pi,1);
        if (pi->async && pi->rettype) {
            error("Return not allowed");
    Error:          
            delete pi;
            return NULL;
        }
        if (t.kind!=TOK_IDENT) {
            error("Identifier expected");
            goto Error;
        }
        pi->name = t.copystr();
        ParamInfo *last = NULL;
        if (!expects(TOK_LPAREN)) {
            error("parameter definition expected");
            goto Error;
        }
        gettoken();
        while (t.kind!=TOK_RPAREN) {
            ParamInfo *p=parse_param(pi,0);
            if (last)
                last->next = p;
            else
                pi->params = p;
            last = p;
            if (t.kind==TOK_COMMA)
                gettoken();
        }
        nexttoken();
        if (pi->virt&&(t.kind==TOK_EQUAL)) {
            if (!expects(TOK_NUMBER)) {         // 0 actually
                goto Error;
            }
            nexttoken();
            pi->virt = 2;
        }
        if (t.kind!=TOK_SEMICOLON) {
            goto Error;
        }
        gettoken();
        return pi;
    }


    
    void out(const char *s,size32_t l)
    {
        write(outfile,s,l);
    }

    void outs(const char *s)
    {
        out(s,strlen(s));
    }

    void outf(const char *fmt, ...)
    {
        static char buf[0x4000];
        va_list args;
        va_start(args, fmt);
        vsprintf(buf, fmt, args);
        va_end(args);
        outs(buf);
    }

    void cat_type(char *s,ParamInfo *p,int deref=0,int var=0)
    {
        if (p==NULL) {
            strcat(s,"void");
            return;
        }
        if ((p->flags&PF_CONST)&&!var)
            strcat(s,"const ");
        if (p->typname)
            strcat(s,p->typname);
        else
            strcat(s,type_name[p->kind]);
        if (!deref) {
            if (p->flags&PF_PTR)
                strcat(s," *");
            if (p->flags&PF_REF)
                strcat(s," &");
        }
    }

    void out_type(ParamInfo *p,int deref=0,int var=0)
    {
        char s[256];
        s[0] = 0;
        cat_type(s,p,deref,var);
        outs(s);
    }

    void typesizeacc(ParamInfo *p,char *accstr,size32_t &acc)
    {
        if ((p->kind==TK_STRUCT)||(p->flags&(PF_PTR|PF_REF))) {
            acc = (acc+3)&~3;
            if (*accstr)
                strcat(accstr,"+");
            strcat(accstr,"sizeof(");
            cat_type(accstr,p);
            strcat(accstr,")");
        }
        else {
            size32_t sz=type_size[p->kind];
            if (sz==2)
                acc = (acc+1)&~1;
            else if (sz>=4)
                acc = (acc+3)&~3;
            acc += type_size[p->kind];
        }
    }

    size32_t typesizealign(ParamInfo *p,size32_t &ofs)
    {
        
        size32_t ret=0;
        if ((p->kind==TK_STRUCT)||(p->flags&(PF_PTR|PF_REF))) {
            if (ofs) {
                ret = 4-ofs;
                ofs = 0;
            }
        }
        else {
            size32_t sz=type_size[p->kind];
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


    enum  clarion_special_type_enum { cte_normal,cte_longref,cte_constcstr,cte_cstr };
    clarion_special_type_enum clarion_special_type(ParamInfo *p)
    {
        if ((type_size[p->kind]==1)&&((p->flags&(PF_PTR|PF_REF))==PF_PTR)) {
            if ((p->flags&PF_CONST)==0)
                return cte_cstr;
            return cte_constcstr;
        }
        else if (p->flags&PF_PTR) { // no support - convert to long
            return cte_longref;
        }
        return cte_normal;
    }

    void out_clarion_type(ParamInfo *p)
    {
        clarion_special_type_enum cte = clarion_special_type(p);
        if (p->flags&PF_REF) {
            outs("*");
        }
        if (cte==cte_longref) {
            outs("LONG");
        }
        else if (cte==cte_cstr) {
            outs("*CSTRING");
        }
        else if (cte==cte_constcstr) {
            outs("CONST *CSTRING");
        }
        else if (p->typname) {
            outs(p->typname);
        }
        else {
            outs(clarion_type_name[p->kind]);
        }
    }



    void out_parameter_list(ParamInfo *p,const char *pfx,int forclarion=0)
    {
        outs("(");
        while (p) {
            if (forclarion && (clarion_special_type(p)==cte_cstr))
                outs("int, ");
            out_type(p);
            outf(" %s%s",pfx,p->name);
            p = p->next;
            if (p)
                outs(", ");
        }
        outs(")");
    }

    void out_clarion_parameter_list(ParamInfo *p)
    {
        outs("(");
        while (p) {
            out_clarion_type(p);
            outs(" ");
            if (clarion_special_type(p)==cte_longref)
                outs("REF_");
            outs(p->name);
            p = p->next;
            if (p)
                outs(", ");
        }
        outs(")");
    }

    void out_method(ProcInfo *pi,const char *classpfx=NULL)
    {
        if (pi->virt&&!classpfx)
            outf("HRPCvirtual%s ",pi->callback?"callback":"");
        out_type(pi->rettype);
        if (classpfx)
            outf(" %s::%s",classpfx,pi->name);
        else
            outf(" %s",pi->name);
        out_parameter_list(pi->params,"");
        if ((pi->virt==2)&&!classpfx)
            outf(" HRPCpure%s",pi->callback?"callback":"");
    }

    void write_header_class(ModuleInfo *mi)
    {
        int hasvirts = 0;
        ProcInfo *pi;
        for (pi=mi->procs; pi; pi=pi->next) {
            if (pi->virt) {
                hasvirts = 1;
                break;
            }
        }
        outf("#ifdef LOCAL_%s\n",mi->name);
        outf("#define %s  STUB_%s\n",mi->name,mi->name);
        if (hasvirts) {
            outs("#define HRPCvirtual virtual\n");
            outs("#define HRPCpure    =0\n");
            outs("#define HRPCvirtualcallback\n");
            outs("#define HRPCpurecallback\n");
        }
        outf("class %s : public HRPCstub\n",mi->name);
        outs("#else\n");
        if (hasvirts) {
            outs("#define HRPCvirtual\n");
            outs("#define HRPCpure\n");
            outs("#define HRPCvirtualcallback virtual\n");
            outs("#define HRPCpurecallback    =0\n");
        }
        outf("class %s : public HRPCmodule\n",mi->name);
        outs("#endif\n");
        outs("{\npublic:\n");
    
        outf("\t%s();\n",mi->name);
        for (pi=mi->procs; pi; pi=pi->next) {
            outs("\t");
            out_method(pi);
            outs(";\n");
        }
        outf("private:\n");
        outf("#ifdef LOCAL_%s\n",mi->name);
        outf("\tvoid _stub(HRPCbuffer &_b,HRPCbuffer &_rb,int fn);\n");
        outs("#else\n");
        outf("\tvoid _callbackstub(HRPCbuffer &_b,HRPCbuffer &_rb,int fn);\n");
        outs("#endif\n");
        if (hasvirts) {
            outs("#undef HRPCvirtual\n");
            outs("#undef HRPCpure\n");
            outs("#undef HRPCvirtualcallback\n");
            outs("#undef HRPCpurecallback\n");
        }
        outs("};\n");
    }

    void write_body_struct_elem(ParamInfo *p,int ref)
    {
        outs("\t");
        out_type(p,ref,1);
        if (ref&&(p->flags&(PF_REF|PF_PTR))) {
            outs(" *");
            if ((p->flags&(PF_REF|PF_PTR))==(PF_REF|PF_PTR)) {
                outs(" *");
            }
        }
        outf(" %s;\n",p->name);
    }



    void writeheadsize(ProcInfo *pi)
    // used for simple types only at the head of the packet
    {
        if (pi->lastin) {
            char sz[2048];
            sz[0] = 0;
            size32_t sza=0;
            ParamInfo *p=pi->params;
            while (1) {
                if (p->flags&PF_SIMPLE) {
                    typesizeacc(p,sz,sza);
                }
                if(p==pi->lastin)
                    break;
                p = p->next;
            }
            if (*sz) {
                outs(sz);
                if (sza)
                    outf("+%d",sza);
            }
            else
                outf("%d",sza);
        }
        else
            outs("0");
    }


    void write_offsetof(ModuleInfo *mi,ProcInfo *pi,ParamInfo *p,int inclusive,const char *pfx=NULL)
    {
        if (p) {
            outf("offsetof(const struct HRPC_d_%s__%s,%s%s",mi->name,pi->name,pfx?pfx:"",p->name);
            if (inclusive) {
              outs(")+sizeof(");
              out_type(p);
            }
            outs(")");
        }
        else
            outf("0");
    }


    void write_param_convert(ParamInfo *p,int deref=0)
    {
        outs("(");
        out_type(p,1,1);                
        if (p->flags&(PF_REF|PF_PTR)) {
            if (!deref)
                outs(" *");
            if ((p->flags&(PF_REF|PF_PTR))==(PF_REF|PF_PTR)) {
                outs(" *");
            }
        }
        outs(")");
    }

    int write_body_swapparam(ModuleInfo *mi,ProcInfo *pi)
    {
        int ret=0;
        ParamInfo *p;
        ForEachParam(pi,p,PF_IN|PF_SIMPLE,PF_VARSIZE|PF_STRING) {
            if(p->simpleneedsswap()) {
                if (!ret) {
                    outs("\tvoid swapparams()\n\t{\n");
                    ret = 1;
                }
                outf("\t\t_WINREV%d(%s);\n",type_size[p->kind],p->name);
            }
        }
        if (ret)
            outs("\t}\n\n");
        return ret;
    }


    void write_body_pushparam(ModuleInfo *mi,ProcInfo *pi,int swapp)
    {
        outs("\tvoid pushparams(HRPCbuffer &_b)\n\t{\n");
        if (swapp)
            outs("\t\t//swapparams();\n");
        if (pi->lastin) {
            outf("\t\t_b.write(&%s,",pi->firstin->name);
            writeheadsize(pi);
            outs(");\n");
        }
        ParamInfo *p;
        ForEachParam(pi,p,PF_IN,PF_SIMPLE|PF_VARSIZE|PF_STRING) {
            if (p->simpleneedsswap()) {
                outf("\t\t//_b.writerev(%s,sizeof(*%s));\n",p->name,p->name);
                outf("\t\t_b.write(%s,sizeof(*%s));\n",p->name,p->name);
            }
            else 
                outf("\t\t_b.write(%s,sizeof(*%s));\n",p->name,p->name);
        }
        ForEachParam(pi,p,PF_IN|PF_STRING,PF_SIMPLE|PF_VARSIZE) {
            outf("\t\t_b.writestr(%s);\n",p->name);
        }
        // now dynamic sizes
        ForEachParam(pi,p,PF_VARSIZE|PF_IN,0) {
            if (INDIRECTSIZE(p)) {
                // should handle size32_t* as well as ref
                outf("\t\t_b.write(%s,%s);\n",p->name,p->bytesize());
            }
            else {
                outf("\t\t_b.write(%s,%s);\n",p->name,p->bytesize());
            }
        }
        outs("\t}\n\n");
    }
    
    void write_body_popparam(ModuleInfo *mi,ProcInfo *pi,int swapp)
    {

        outs("\tvoid popparams(HRPCbuffer &_b)\n\t{\n");
        if (pi->lastin) {
            outf("\t\t_b.read(&%s,",pi->firstin->name);
            writeheadsize(pi);
            outs(");\n");
        }
        int needensure=0;
        ParamInfo *p;
        ForEachParam(pi,p,PF_OUT,PF_IN|PF_SIMPLE) {
            if (needensure) {
                outs("\t\t\t+");
            }
            else {
                outs("\t\t_b.ensure(\n");
                outs("\t\t\t");
                needensure = 1;
            }
            if ((p->flags&PF_VARSIZE)&&!(INDIRECTSIZE(p))) {
                outf("(%s)\n",p->bytesize());
            }
            else {
                outf("sizeof(*%s)\n",p->name);
            }
        }
        if (needensure) {
            outs("\t\t);\n");
        }
        ForEachParam(pi,p,PF_OUT,PF_IN|PF_SIMPLE|PF_VARSIZE|PF_STRING) {
            outf("\t\t%s = ",p->name);
            write_param_convert(p);
            outf("_b.writeptr(sizeof(*%s));\n",p->name);

        }
        ForEachParam(pi,p,PF_OUT|PF_STRING,0) {
            outf("\t\t%s = ",p->name);
            write_param_convert(p);
            outf("_b.writeptr(sizeof(*%s));\n",p->name);
            outf("\t\t*%s = 0;\n",p->name);
        }
        ForEachParam(pi,p,PF_IN,PF_SIMPLE|PF_VARSIZE|PF_STRING) {
            outf("\t\t%s = ",p->name);
            write_param_convert(p);
            outf("_b.readptr(sizeof(*%s));\n",p->name);
            outf("\t\t\t//_b.readptrrev(sizeof(*%s));\n",p->name);
        }
        ForEachParam(pi,p,PF_IN|PF_STRING,PF_SIMPLE|PF_VARSIZE) {
            outf("\t\t%s = _b.readstrptr();\n",p->name);
        }
        // now dynamic sizes
        ForEachParam(pi,p,PF_VARSIZE|PF_IN,0) {
            outf("\t\t%s = ",p->name);
            write_param_convert(p);
            if (INDIRECTSIZE(p)) {
                // should handle size32_t* as well as ref
                outs("_b.readptr(sizeof");
                write_param_convert(p);
                outs(")\n");
            }
            else {
                outf("_b.readptr(%s);\n",p->bytesize());
            }
        }
        ForEachParam(pi,p,PF_OUT|PF_VARSIZE,PF_IN|PF_SIMPLE) {
            outf("\t\t%s = ",p->name);
            write_param_convert(p);
            outs("_b.writeptr(");
            if ((p->flags&PF_VARSIZE)&&!(INDIRECTSIZE(p))) {
                outf("%s);\n",p->bytesize());
            }
            else {
                outf("sizeof(*%s));\n",p->name);
            }
        }
        if (swapp)
            outs("\t\t//swapparams();\n");
        outs("\t}\n\n");
    }

    void write_body_pushreturn(ModuleInfo *mi,ProcInfo *pi)
    {
        if (!pi->async) {
            outs("\tvoid pushreturn(HRPCbuffer &_b)\n\t{\n");
            ParamInfo *p;
            ForEachParam(pi,p,PF_OUT,PF_SIMPLE|PF_VARSIZE|PF_STRING) {
                outf("\t\t_b.write(%s,sizeof(*%s));\n",p->name,p->name);
            }
            ForEachParam(pi,p,PF_OUT|PF_STRING,0) {
                outf("\t\t_b.writestr(*%s);\n",p->name);
                outf("\t\tfree(*%s);\n",p->name);
            }
            // now dynamic sizes
            ForEachParam(pi,p,PF_VARSIZE|PF_OUT,0) {
                if (INDIRECTSIZE(p)) {
                    // should handle size32_t* as well as ref
                    outf("\t\t_b.write(*%s,%s);\n",p->name,p->bytesize(1));
                    outf("\t\tfree(*%s);\n",p->name);
        
                }
                else {
                    outf("\t\t_b.write(%s,%s);\n",p->name,p->bytesize());
                }
            }
            p = pi->rettype;
            if (p) {
                if ((p->flags&(PF_PTR|PF_STRING))==(PF_PTR|PF_STRING)) {
                    outf("\t\t_b.writestr(%s);\n",p->name);
                    outf("\t\tfree(%s);\n",p->name);
                }
                else if (p->flags&PF_PTR) {
                    outf("\t\t_b.write(%s,%s);\n",p->name,p->bytesize(1));
                    outf("\t\tfree(%s);\n",p->name);
                }
                else {
                    outf("\t\t_b.write(&%s,sizeof(%s));\n",p->name,p->name);
                }
            }
            outs("\t}\n\n");
        }
    }

    void write_body_popreturn(ModuleInfo *mi,ProcInfo *pi)
    {
        if (!pi->async) {
            outs("\tvoid popreturn(HRPCbuffer &_b)\n\t{\n");
            ParamInfo *p;
            ForEachParam(pi,p,PF_OUT,PF_SIMPLE|PF_VARSIZE|PF_RETURN|PF_STRING) {
                outf("\t\t_b.read(%s,sizeof(*%s));\n",p->name,p->name);
            }
            ForEachParam(pi,p,PF_OUT|PF_STRING,0) {
                outf("\t\t*%s = _b.readstrdup();\n",p->name);
            }
            // now dynamic sizes
            ForEachParam(pi,p,PF_VARSIZE|PF_OUT,0) {
                if (INDIRECTSIZE(p)) {
                    outf("\t\t*%s = ",p->name);
                    write_param_convert(p,1);
                    outf("malloc(%s);\n",p->bytesize(1));
                    outf("\t\t_b.read(*%s,%s);\n",p->name,p->bytesize(1));
        
                }
                else {
                    outf("\t\t_b.read(%s,%s);\n",p->name,p->bytesize());
                }
            }
            p = pi->rettype;
            if (p) {
                if ((p->flags&(PF_PTR|PF_STRING))==(PF_PTR|PF_STRING)) {
                    outf("\t\t%s = _b.readstrdup();\n",p->name);
                }
                else if (p->flags&PF_PTR) {
                    outf("\t\t%s = ",p->name);
                    write_param_convert(p);
                    outf("malloc(%s);\n",p->bytesize(1));
                    outf("\t\t_b.read(%s,%s);\n",p->name,p->bytesize(1));
                }
                else {
                    outf("\t\t_b.read(&%s,sizeof(%s));\n",p->name,p->name);
                }
            }
            outs("\t}\n\n");
        }
    }

    void write_body_method_structs2(int fn,ModuleInfo *mi,ProcInfo *pi) {
        // buffer structure
        outf("struct HRPC_d_%s__%s\n{\n",mi->name,pi->name);
        ParamInfo *p;
        pi->lastin=NULL;
        pi->firstin=NULL;
        ForEachParam(pi,p,0,0)
            p->flags &= ~PF_SIMPLE;
        size32_t align=0;
        int dummy = 0;
        ForEachParam(pi,p,PF_IN,PF_OUT|PF_REF|PF_PTR|PF_VARSIZE) {
            p->flags |= PF_SIMPLE;
            pi->lastin = p;
            if (!pi->firstin)
                pi->firstin = p;
            size32_t a=typesizealign(p,align);
            if (a>0) {
                dummy++;
                if (a>1)
                    outf("\tchar __dummy%d[%d];\n",dummy,a);
                else
                    outf("\tchar __dummy%d;\n",dummy,a);
            }
            write_body_struct_elem(p,0);
        }
        if (align>0) {
            dummy++;
            outf("\tchar _dummy%d[%d];\n",dummy,4-align);
            align = 0;
        }
        ForEachParam(pi,p,PF_IN,PF_OUT|PF_SIMPLE) {
            write_body_struct_elem(p,1);
        }
        ForEachParam(pi,p,PF_IN|PF_OUT,PF_SIMPLE) {
            write_body_struct_elem(p,1);
        }
        ForEachParam(pi,p,PF_OUT,PF_IN|PF_SIMPLE) {
            write_body_struct_elem(p,1);
        }
        if (pi->rettype) {
            typesizealign(pi->rettype,align);
            write_body_struct_elem(pi->rettype,0);
            if (align>0) {
                dummy++;
                outf("\tchar _dummy%d[%d];\n",dummy,4-align);
                align = 0;
            }
        }
        
        int swapp=write_body_swapparam(mi,pi);
        write_body_pushparam(mi,pi,swapp);
        write_body_popparam(mi,pi,swapp);
        if (!pi->async) {
            write_body_pushreturn(mi,pi);
            write_body_popreturn(mi,pi);
        }

        // now constructors
        outf("\tHRPC_d_%s__%s() {}\n",mi->name,pi->name);

        if (pi->params) {
            outf("\tHRPC_d_%s__%s",mi->name,pi->name);
            out_parameter_list(pi->params,"_");
            outs(": ");
            ForEachParam(pi,p,0,0) {
                outf("%s(",p->name);
                if (p->flags&PF_REF) {
                    outs("&");
                }
                else if ((p->flags&(PF_PTR&PF_CONST))==(PF_PTR&PF_CONST)) {
                    write_param_convert(p);
                }
                outf("_%s)",p->name);
                if (p->next)
                    outs(", ") ;
            }
            outs("\n\t{\n");
            outs("\t};");
        }
        outs("\n};\n");

    }


    void write_body_class_stub(ModuleInfo *mi,int cb)
    {
        outf("void %s::_%sstub(HRPCbuffer &_b,HRPCbuffer &_br,int fn)\n{\n",mi->name,cb?"callback":"");
        int fn=0;
        int switchdone = 0;
        ProcInfo *pi;
        for (pi=mi->procs; pi; pi=pi->next) {
            fn++;
            if (cb!=pi->callback)
                continue;
            if (!switchdone) {
                outs("\tswitch(fn) {\n");
                switchdone = 1;
            }
            outf("\tcase %d: {\n",fn);
            outf("\t\t\tHRPC_d_%s__%s _params;\n",mi->name,pi->name,mi->name,pi->name);
            outs("\t\t\t_params.popparams(_b);\n");
            if (pi->async) {
                outs("\t\t\t_returnasync(_br);\n");
            }
            outs("\t\t\t");
            if (pi->rettype) {
                outf("_params.%s = ",RETURNNAME);
            }
            outf("%s(",pi->name);
            ParamInfo *p;
            ForEachParam(pi,p,0,0) {
                if (p->flags&PF_REF)
                    outs("*");
                outf("_params.%s",p->name);
                if (p->next)
                    outs(", ");
            }
            outs(");\n");
            if (!pi->async) {
                outs("\t\t\t_returnOK(_br);\n");
                outs("\t\t\t_params.pushreturn(_br);\n");
            }
            outs("\t\t\tbreak;\n");
            outs("\t\t}\n");
        }
        if (switchdone) {
            outs("\t}\n");
        }
        outs("}\n\n");
    }
    void write_body_class_proxy(ModuleInfo *mi,int cb)
    {
        int fn = 0;
        ProcInfo *pi;
        for (pi=mi->procs; pi; pi=pi->next) {
            fn++;
            if (cb!=pi->callback)
                continue;
            out_method(pi,mi->name);
            outs("\n{\n");
            if (pi->callback) {
                outs("\tHRPCcallframe _callframe(&_server->Sync(),cbbuff);\n");
            }
            else {
                outs("\tHRPCcallframe _callframe(sync,inbuff);\n");
            }
            outf("\tHRPC_d_%s__%s _params",mi->name,pi->name);
            if (pi->params) {
                outs("(");
                ParamInfo *p;
                ForEachParam(pi,p,0,0) {
                    outf("%s",p->name);
                    if (p->next)
                        outs(", ");
                }
                outs(")");
            }
            outs(";\n");
            if (pi->callback) {
                outs("\t_params.pushparams(cbbuff);\n");
                outf("\t_callbackproxy(_callframe,%d);\n",fn);
                if (!pi->async)
                    outs("\t_params.popreturn(cbbuff);\n");
            }
            else {
                outs("\t_params.pushparams(inbuff);\n");
                outf("\t_proxy(_callframe,%d);\n",fn);
                if (!pi->async)
                    outs("\t_params.popreturn(inbuff);\n");
            }
            if (pi->rettype) {
                outf("\treturn _params.%s;\n",RETURNNAME);
            }
            outs("}\n\n");
        }
        outs("\n\n");
    }
    void write_body_class(ModuleInfo *mi)
    {
        outf("// class %s \n\n",mi->name);

        outf("static struct HRPCmoduleid _id_%s = { { ",mi->name);
        char *mn = mi->name;
        for (int i=0;i<8;i++) {
            if (i)
                outs(", ");
            if (*mn) {
                outf("'%c'",*mn);
                mn++;
            }
            else
                outs("0");
        }
        outf("}, %d };\n\n",mi->version);
        
        ProcInfo *pi;
        int fn=0;
        for (pi=mi->procs; pi; pi=pi->next) {
            fn++;
            write_body_method_structs2(fn,mi,pi);
        }       
        outs("\n");
        outf("%s::%s() { _id = &_id_%s; }\n\n",mi->name,mi->name,mi->name);
        outf("#ifdef LOCAL_%s  // Stub(%s):\n\n",mi->name,mi->name);
        write_body_class_stub(mi,0);
        write_body_class_proxy(mi,1);
        outf("#else   // Proxy(%s):\n\n",mi->name);
        write_body_class_proxy(mi,0);
        write_body_class_stub(mi,1);
        outf("#endif  // end class %s\n",mi->name);
    }
    
    void write_clarion_interface_class(ModuleInfo *mi)
    {
        outs("extern \"C\" {\n\n");
        outs("\n");
        outf("// clarion interface class CIC_%s \n",mi->name);
        outf("struct HRPCI_%s: public HRPCI_Clarion_Module // interface\n",mi->name);
        ProcInfo *pi;
        outs("{\n");
        for (pi=mi->procs; pi; pi=pi->next) {
            if (pi->callback)
                continue;
            outs("\tvirtual ");
            out_type(pi->rettype);
            outf(" _stdcall %s",pi->name);
            out_parameter_list(pi->params,"",1);
            outs("=0;\n");
        }
        outs("};\n");
        outf("#ifndef LOCAL_%s\n\n",mi->name);
        outf("class CIC_%s : public HRPCI_%s\n",mi->name,mi->name);
        outs("{\n");
        outs("public:\n");
        outf("\t%s _o;\n",mi->name);
        outs("\tmutable unsigned xxcount;\n");
        for (pi=mi->procs; pi; pi=pi->next) {
            if (pi->callback)
                continue;
            outs("\t");
            out_type(pi->rettype);
            outf(" _stdcall %s",pi->name);
            out_parameter_list(pi->params,"",1);
            outs("\n");
            outs("\t{\n");
            outs("\t\t");
            if (pi->rettype) {
                outs("return ");
            }
            outf("_o.%s(",pi->name);
            ParamInfo *p;
            ForEachParam(pi,p,0,0) {
                outf("%s",p->name);
                if (p->next)
                    outs(", ");
            }
            outs(");\n");
            outs("\t}\n");
        }
        outf("\tCIC_%s() { xxcount = 0; }\n",mi->name);
        outs("\tvoid _stdcall Link() const  { ++xxcount; }\n");
        outs("\tint _stdcall Release() const {if (xxcount == 0) { delete this; return 1; } --xxcount; return 0; }\n");
        outs("\tvoid _stdcall FreeMem(void *p) { free(p); }\n");
        outs("};\n");
        outf("CIC_%s* PASCAL HRPC_Make_%s(HRPCI_Clarion_Transport *t)\n",mi->name,mi->name);
        outs("{\n");
        outf("\tCIC_%s *ret=new CIC_%s;\n",mi->name,mi->name);
        outs("\tret->_o.UseTransport(t->GetTransport());\n");
        outs("\treturn ret;\n");
        outs("}\n");

        outs("#endif\n");
        outs("}\n");
    }
    
    void write_example_module(ModuleInfo *mi)
    {
        outf("void %s_Server()\n",mi->name);
        outs("{\n");
        outs("\tHRPCserver server(MakeTcpTransport(NULL,PORTNO)); // PORTNO TBD\n");
        outf("\t%s stub;\n",mi->name);
        outs("\tserver.AttachStub(&stub);   // NB a server can service more than one module\n");
        outs("\tserver.Listen();\n");
        outs("}\n\n");
        ProcInfo *pi;
        for (pi=mi->procs; pi; pi=pi->next) {
            out_method(pi,mi->name);
            outs("\n{\n");
            outf("\t // TBD\n");
            if (pi->rettype) {
                outs("\treturn TBD;\n");
            }
            outs("}\n\n");
        }
    }

    void write_clarion_include_module(ModuleInfo *mi)
    {
        outf("HRPCI_%s  INTERFACE(HRPCI_Clarion_Module)\r\n",mi->name);
        ProcInfo *pi;
        for (pi=mi->procs; pi; pi=pi->next) {
            if (pi->callback)
                continue;
            
            outf("%-15s PROCEDURE",pi->name);
            out_clarion_parameter_list(pi->params);
            if (pi->rettype) {
                outs(",");
                out_clarion_type(pi->rettype);
            }
            outs(",PASCAL\r\n");
        }
        outs("    END\r\n\r\n");
    }


public:

    void ProcessHIDL(char *sp,int ho,int cppo,int xsvo,int clwo,const char *packagename)
    {
        lineno = 1;
        outfile = ho;
        ModuleInfo *first=NULL;
        ModuleInfo *last;
        ModuleInfo *mi;
        outf("// *** Include file generated by HIDL Version %s from %s.hid ***\n",HIDLVER,packagename);
        outf("// *** Not to be hand edited (changes will be lost on re-generation) ***\n\n");
        outf("#include \"hrpc.hpp\"\n\n");
        start = sp;
        s = sp;
        while (*s) {
            const char *p=s;
            int eof = !skip_to_module();
            out(p,s-p);
            if (eof)
                break;
            gettoken(); // skip module
            if (!expects(TOK_IDENT)) {
                error("Module name expected");
                break;
            }
            mi = new ModuleInfo(t.str,t.len);
            if (first)
                last->next = mi;
            else {
                first = mi;
            }
            last = mi;
            while (1) {
                if (gettoken()==TOK_LBRACE)
                    break;
                if (t.kind==TOK_LPAREN) {
                    if (expects(TOK_NUMBER)) {
                        mi->version = t.integer();
                        if ((mi->version>255)||(mi->version<0))
                            error("version must be in range 0-255");
                        if (!expects(TOK_RPAREN)) {
                            error(") expected");
                        }
                    }
                    else
                        error("Version number expected");
                }
                else {
                    error("{ expected");
                    return;
                }
            }
            ProcInfo *last=NULL;
            gettoken();
            while (t.kind!=TOK_RBRACE) {
                ProcInfo *pi = parse_proc();
                if (!pi)
                    break;
                if (last)
                    last->next = pi;
                else
                    mi->procs = pi;
                last = pi;
            }
            if (!expects(TOK_SEMICOLON)) {
                error("; expected after }");
                break;
            }
            write_header_class(mi);
        }
        outs("//end\r\n"); // CR for Clarion
        outfile = cppo;
        outf("// *** Source file generated by HIDL Version %s from %s.hid ***\n",HIDLVER,packagename);
        outf("// *** Not to be hand edited (changes will be lost on re-generation) ***\n\n");
        outf("#include \"%s.hpp\"\n\n",packagename);
        for (mi=first;mi;mi=mi->next) {
            write_body_class(mi);
            write_clarion_interface_class(mi);
        }
        outs("//end\r\n"); // CR for Clarion
        outfile = xsvo;
        outs("// Example Server Implementation Template\n");
        outf("// Source file generated by HIDL Version %s from %s.hid\n",HIDLVER,packagename);
        outs("// *** You should copy this file before changing, as it will be overwritten next time HIDL is run ***\n\n");
        outs("#include <stddef.h>\n");
        outs("#include <stdlib.h>\n");
        outs("#include <assert.h>\n\n");
        outs("#include \"hrpcsock.hpp\"// default use TCP/IP sockets\n\n");
        outs("// TBD - Add other includes here\n\n");
        for (mi=first;mi;mi=mi->next) {
            outf("#define LOCAL_%s       // implementation of %s\n",mi->name,mi->name);
        }
        outf("#include \"%s.cpp\"\n\n",packagename);
        for (mi=first;mi;mi=mi->next) {
            write_example_module(mi);
        }
        outfile = clwo;
        outs("! Clarion HRPC Interfaces\r\n");
        outf("! Include file generated by HIDL Version %s from %s.hid\r\n",HIDLVER,packagename);
        outs("! *** Not to be hand edited (changes will be lost on re-generation) ***\r\n\r\n");
        outf("  OMIT('EndOfInclude',_%s_I_)\r\n",packagename);
        outf("_%s_I_ EQUATE(1)\r\n\r\n",packagename);
        outs("  INCLUDE('HRPC.INC'),ONCE\r\n\r\n");
        for (mi=first;mi;mi=mi->next) {
            write_clarion_include_module(mi);
        }
        outs("\r\n");
        outs("  MAP\r\n");
        outf("    MODULE('%s')\r\n",packagename);
        for (mi=first;mi;mi=mi->next) {
            outf("      HRPC_Make_%s(HRPCI_Clarion_Transport transport),*HRPCI_%s,PASCAL,NAME('_HRPC_Make_%s@4')\r\n",mi->name,mi->name,mi->name);
        }
        outs("    END\r\n\r\n");
        outs("  END\r\n\r\n");
        outf("\r\nEndOfInclude\r\n");

        while (first) {
            mi = first;
            first = mi->next;
            delete mi;
        }

    }


};

char *gettail(const char *fn)
{
    const char *e=NULL;
    const char *e1=fn;
    while((e1=strchr(e1,'.'))!=NULL)
        e = e1++;
    const char *s=fn;
    const char *s1;
#ifdef _WIN32
    if (*s&&s[1]==':')
        s+=2;
#endif
    for (s1 = s;*s1&&(s1!=e);s1++)
#ifdef _WIN32
        if (*s1=='\\')
#else
        if (*s1=='/')
#endif
            s = s1+1;
    size32_t l = s1-s;
    char *ret = (char *)malloc(l+1);
    memcpy(ret,s,l);
    ret[l] = 0;
    return ret;
}


char *changeext(const char *fn,const char *ext)
{
    char *ret=gettail(fn);
    size32_t l = strlen(ret);
    ret = (char *)realloc(ret,l+strlen(ext)+2);
    ret[l] = '.';
    strcpy(ret+l+1,ext);
    return ret;
}
    

int main(int argc, char* argv[])
{
    if (argc<2) {
        printf("HIDL Compiler Version %s\n\n",HIDLVER);
        printf("Usage:  HIDL  filename.hid  [<outdir>] \n\n");
        printf("Output:       filename.cpp\n");
        printf("              filename.hpp\n");
        printf("              filename.xsv\n");
        printf(" (the xsv file is an example server implementation template)\n");
        return 1;
    }
    char *path=changeext(argv[1],"hid");
    int hi = open(path,_O_RDONLY | _O_BINARY);
    if (hi==-1) {
        printf("Could not read %s\n",path);
        return 1;
    }
    free(path);
    size32_t l = lseek(hi,0,SEEK_END);
    lseek(hi,0,SEEK_SET);
    char *s = (char *)malloc(l+1);
    l = read(hi,s,l);
    close(hi);
    if (argc>2)
        _chdir(argv[2]);
    s[l] = 0;
    path=changeext(argv[1],"hpp");
    int ho = open(path, _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY , S_IREAD|S_IWRITE);
    if (ho==-1) {
        printf("Could not write %s\n",path);
        return 1;
    }
    free(path);
    path=changeext(argv[1],"cpp");
    int cppo = open(path,_O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY , S_IREAD|S_IWRITE);
    if (cppo==-1) {
        printf("Could not write %s\n",path);
        return 1;
    }
    free(path);
    path=changeext(argv[1],"xsv");
    int xsvo = open(path,_O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY , S_IREAD|S_IWRITE);
    if (xsvo==-1) {
        printf("Could not write %s\n",path);
        return 1;
    }
    free(path);
    path=changeext(argv[1],"int");
    int clwo = open(path,_O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY , S_IREAD|S_IWRITE);
    if (clwo==-1) {
        printf("Could not write %s\n",path);
        return 1;
    }
    free(path);
    HIDLcompiler hc;
    char *package = gettail(argv[1]);
    hc.ProcessHIDL(s,ho,cppo,xsvo,clwo,package);
    free(package);
    free(s);
    close(ho);
    close(cppo);
    close(xsvo);
    close(clwo);
    return 0;
}
    


