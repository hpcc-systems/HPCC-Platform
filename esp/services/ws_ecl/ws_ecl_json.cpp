#include "ws_ecl_service.hpp"

//#include <stdlib.h>
//#include <stdio.h>
//#include <string.h>
//#include <assert.h>
//#include <locale.h>

#include <vector>

#include "JSON_parser.h"

static int ptree_builder(void* ctx, int type, const JSON_value* value);

class jsonkey
{
public:
    StringBuffer name;
    bool isArray;

    jsonkey(const char *name_) : name(name_), isArray(false){}
};

class JSonToXmlStringContext
{
public:
    StringBuffer &xml;
    std::vector<jsonkey> stack;
    StringBuffer tail;

    const char *tagname(){if (stack.size()>0) return stack.back().name.str(); return NULL;}
    bool isArray(){return stack.back().isArray;}
    void setIsArray(bool isArray=true){stack.back().isArray = isArray;}
    void push(const char *name){stack.push_back(name);}
    void pop(){stack.pop_back();}
    bool isEmpty(){return !stack.size();}

    JSonToXmlStringContext(StringBuffer &xml_, const char *tail_) : xml(xml_), tail(tail_) {}
    void startKey(const char *name)
    {
        const char *curname = tagname();
        if (tail.length() && isEmpty())
        {
            StringBuffer fullname(name);
            push(fullname.append(tail).str());
        }
        else
            push(name);
    }
    void endKey()
    {
        const char *curname = tagname();
        pop();
    }
    void startObject()
    {
        const char *curname = tagname();
        if (!isEmpty()) 
            xml.appendf("<%s>", tagname());
    }
    void endObject()
    {
        const char *curname = tagname();
        if (!isEmpty()) 
        { 
            xml.appendf("</%s>", tagname()); 
            if (!isArray())
                pop();
        }
    }
    void startArray()
    {
        const char *curname = tagname();
        setIsArray();
    }
    void endArray()
    {
        const char *curname = tagname();
        pop();
    }

    void setValueStr(const char *val){xml.appendf("<%s>%s</%s>", tagname(), val, tagname()); if (!isArray()) pop();}
    void setValueBool(bool val){xml.appendf("<%s>%s</%s>", tagname(), val ? "true" : "false", tagname()); if (!isArray()) pop();}
    void setValueInt(int val){xml.appendf("<%s>%d</%s>", tagname(), val, tagname()); if (!isArray()) pop();}
    void setValueDouble(double val){xml.appendf("<%s>%f</%s>", tagname(), val, tagname()); if (!isArray()) pop();}
    void setNULL(){xml.appendf("<%s></%s>", tagname(), tagname()); if (!isArray()) pop();}
};


void createPTreeFromJsonString(const char *json, bool caseInsensitive, StringBuffer &xml, const char *tail)
{
    int count = 0, result = 0;
        
    JSonToXmlStringContext jcontext(xml, tail);
    
    struct JSON_parser_struct* jc = NULL;

    JSON_config config;
    init_JSON_config(&config);
    
    config.depth                  = 19;
    config.callback               = &ptree_builder;
    config.allow_comments         = 1;
    config.handle_floats_manually = 0;
    config.callback_ctx = &jcontext;
    
    /* Important! Set locale before parser is created.*/
    setlocale(LC_ALL, "english");
    
    jc = new_JSON_parser(&config);
    
    const char *finger = json;
    for (; *finger ; finger++) {
        if (!JSON_parser_char(jc, *finger)) {
            throw MakeStringException(-1, "JSON_parser_char: syntax error, byte %d\n", finger - json);
        }
    }
    if (!JSON_parser_done(jc)) {
        throw MakeStringException(-1, "JSON_parser_end: syntax error\n");
    }

    //TBD: worry about exception cleanup later
    delete_JSON_parser(jc);
}



static size_t s_Level = 0;
static const char* s_pIndention = "  ";
static int s_IsKey = 0;

static void print_indention(StringBuffer &xml)
{
    xml.appendN(s_Level * 2, ' ');
}
 

static int ptree_builder(void* ctx, int type, const JSON_value* value)
{
    JSonToXmlStringContext* jcx = (JSonToXmlStringContext*) ctx;

    switch(type) {
    case JSON_T_ARRAY_BEGIN:    
        jcx->startArray();
        ++s_Level;
        break;
    case JSON_T_ARRAY_END:
        jcx->endArray();
        if (s_Level > 0) --s_Level;
        break;
   case JSON_T_OBJECT_BEGIN:
       jcx->startObject();
        ++s_Level;
        break;
    case JSON_T_OBJECT_END:
        if (s_Level > 0) --s_Level;
        jcx->endObject();
        break;
    case JSON_T_INTEGER:
        jcx->setValueInt(value->vu.integer_value);
        break;
    case JSON_T_FLOAT:
        jcx->setValueDouble(value->vu.float_value);
        break;
    case JSON_T_NULL:
        jcx->setNULL();
        break;
    case JSON_T_TRUE:
        s_IsKey = 0;
        jcx->setValueBool(true);
        break;
    case JSON_T_FALSE:
        jcx->setValueBool(false);
        break;
    case JSON_T_KEY:
        jcx->startKey(value->vu.str.value);
        break;   
    case JSON_T_STRING:
        jcx->setValueStr(value->vu.str.value);
        break;
    default:
        assert(0);
        break;
    }
    
    return 1;
}


