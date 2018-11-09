/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

// jsonhelpers.hpp:
//
//////////////////////////////////////////////////////////////////////
#pragma warning( disable : 4786)

#ifndef _JSONHELPERS_HPP__
#define _JSONHELPERS_HPP__
#include "jliball.hpp"
#include "wsexcept.hpp"

#define REQSF_ROOT         0x0001
#define REQSF_SAMPLE_DATA  0x0002
#define REQSF_TRIM         0x0004
#define REQSF_ESCAPEFORMATTERS 0x0008
#define REQSF_FORMAT       0x0010
#define REQSF_EXCLUSIVE (REQSF_SAMPLE_DATA | REQSF_TRIM)

namespace JsonHelpers
{
    static StringBuffer &appendJSONExceptionItem(StringBuffer &s, int code, const char *msg, const char *objname="Exceptions", const char *arrayName = "Exception")
    {
        if (objname && *objname)
            appendJSONName(s, objname).append('{');
        if (arrayName && *arrayName)
            appendJSONName(s, arrayName).append('[');
        delimitJSON(s);
        s.append('{');
        appendJSONValue(s, "Code", code);
        appendJSONValue(s, "Message", msg);
        s.append('}');
        if (arrayName && *arrayName)
            s.append(']');
        if (objname && *objname)
            s.append('}');
        return s;
    }

    static StringBuffer &appendJSONException(StringBuffer &s, IException *e, const char *objname="Exceptions", const char *arrayName = "Exception")
    {
        if (!e)
            return s;
        StringBuffer temp;
        return appendJSONExceptionItem(s, e->errorCode(), e->errorMessage(temp).str(), objname, arrayName);
    }

    static StringBuffer &appendJSONException(StringBuffer &s, IWsException *e, const char *objname="Exceptions", const char *arrayName = "Exception")
    {
        if (!e)
            return s;

        StringBuffer temp;
        IArrayOf<IException>& exceptions = e->getArray();
        for (unsigned i = 0 ; i < exceptions.ordinality(); i++)
        {
            appendJSONExceptionItem(s, e->errorCode(), e->errorMessage(temp.clear()).str(), objname, arrayName);
        }

        return s;
    }

    static StringBuffer &appendJSONExceptions(StringBuffer &s, IMultiException *e, const char *objname="Exceptions", const char *arrayName = "Exception")
    {
        if (!e)
            return s;
        if (objname && *objname)
            appendJSONName(s, objname).append('{');
        if (arrayName && *arrayName)
            appendJSONName(s, arrayName).append('[');
        ForEachItemIn(i, *e)
            appendJSONException(s, &e->item(i), NULL, NULL);
        if (arrayName && *arrayName)
            s.append(']');
        if (objname && *objname)
            s.append('}');
        return s;
    }

    static IException *MakeJSONValueException(int code, const char *start, const char *pos, const char *tail, const char *intro="Invalid json format: ")
    {
         StringBuffer s(intro);
         s.append(pos-start, start).append('^').append(pos);
         if (tail && *tail)
             s.append(" - ").append(tail);
         return MakeStringException(code, "%s", s.str());
    }

    static inline StringBuffer &jsonNumericNext(StringBuffer &s, const char *&c, bool &allowDecimal, bool &allowExponent, const char *start)
    {
        if (isdigit_char(*c))
            s.append(*c++);
        else if ('.'==*c)
        {
            if (!allowDecimal || !allowExponent)
                throw MakeJSONValueException(-1, start, c, "Unexpected decimal");
            allowDecimal=false;
            s.append(*c++);
        }
        else if ('e'==*c || 'E'==*c)
        {
            if (!allowExponent)
                throw MakeJSONValueException(-1, start, c, "Unexpected exponent");

            allowDecimal=false;
            allowExponent=false;
            s.append(*c++);
            if ('-'==*c || '+'==*c)
                s.append(*c++);
            if (!isdigit_char(*c))
                throw MakeJSONValueException(-1, start, c, "Unexpected token");
        }
        else
            throw MakeJSONValueException(-1, start, c, "Unexpected token");

        return s;
    }

    static inline StringBuffer &jsonNumericStart(StringBuffer &s, const char *&c, const char *start)
    {
        if ('-'==*c)
            return jsonNumericStart(s.append(*c++), c, start);
        else if ('0'==*c)
        {
            s.append(*c++);
            if (*c && '.'!=*c)
                throw MakeJSONValueException(-1, start, c, "Unexpected token");
        }
        else if (isdigit_char(*c))
            s.append(*c++);
        else
            throw MakeJSONValueException(-1, start, c, "Unexpected token");
        return s;
    }

    static StringBuffer &appendJSONNumericString(StringBuffer &s, const char *value, bool allowDecimal)
    {
        if (!value || !*value)
            return s.append("null");

        bool allowExponent = allowDecimal;

        const char *pos = value;
        jsonNumericStart(s, pos, value);
        while (*pos)
            jsonNumericNext(s, pos, allowDecimal, allowExponent, value);
        return s;
    }

    typedef enum _JSONFieldCategory
    {
        JSONField_String,
        JSONField_Integer,
        JSONField_Real,
        JSONField_Boolean,
        JSONField_Present  //true or remove
    } JSONField_Category;

    static JSONField_Category xsdTypeToJSONFieldCategory(const char *xsdtype)
    {
        if (!xsdtype || !*xsdtype)
            return JSONField_String;
        switch (*xsdtype)
        {
        case 'b':
            if (streq(xsdtype, "boolean"))
                return JSONField_Boolean;
            if (streq(xsdtype, "byte"))
                return JSONField_Integer;
            break;
        case 'd':
            if (!strncmp(xsdtype, "decimal", 7)) //ecl creates derived types of the form decimal#_#
                return JSONField_Real;
            if (streq(xsdtype, "double"))
                return JSONField_Real;
            break;
        case 'f':
            if (streq(xsdtype, "float"))
                return JSONField_Real;
            break;
        case 'i':
            if (streq(xsdtype, "int") || streq(xsdtype, "integer"))
                return JSONField_Integer;
            break;
        case 'l':
            if (streq(xsdtype, "long"))
                return JSONField_Integer;
            break;
        case 'n':
            if (streq(xsdtype, "negativeInteger") || streq(xsdtype, "nonNegativeInteger") || streq(xsdtype, "nonPositiveInteger"))
                return JSONField_Integer;
            if (streq(xsdtype, "none")) //maps to an xml schema element with no type.  set to true or don't add
                return JSONField_Present;
            break;
        case 'p':
            if (streq(xsdtype, "positiveInteger"))
                return JSONField_Integer;
            break;
        case 's':
            if (streq(xsdtype, "short"))
                return JSONField_Integer;
            break;
        case 'u':
            if (strncmp(xsdtype, "unsigned", 8))
                break;
            xsdtype+=8;
            if (streq(xsdtype, "Byte") || streq(xsdtype, "Int") || streq(xsdtype, "Long") || streq(xsdtype, "Short"))
                return JSONField_Integer;
            break;
        }
        return JSONField_String;
    }

    static void buildJsonAppendValue(IXmlType* type, StringBuffer& out, const char* tag, const char *value, unsigned flags)
    {
        JSONField_Category ct = xsdTypeToJSONFieldCategory(type->queryName());

        if (ct==JSONField_Present && (!value || !*value))
            return;

        if (tag && *tag)
            out.appendf("\"%s\": ", tag);
        StringBuffer sample;
        if ((!value || !*value) && (flags & REQSF_SAMPLE_DATA))
        {
            type->getSampleValue(sample, NULL);
            value = sample.str();
        }

        if (value)
        {
            switch (ct)
            {
            case JSONField_String:
                appendJSONValue(out, NULL, value);
                break;
            case JSONField_Integer:
                appendJSONNumericString(out, value, false);
                break;
            case JSONField_Real:
                appendJSONNumericString(out, value, true);
                break;
            case JSONField_Boolean:
                if (strieq(value, "default"))
                    out.append("null");
                else
                    appendJSONValue(out, NULL, strToBool(value));
                break;
            case JSONField_Present:
                appendJSONValue(out, NULL, true);
                break;
            }
        }
        else
            out.append("null");
    }
    inline void checkNewlineJsonMsg(StringBuffer &out, unsigned &level, unsigned flags, int increment)
    {
        if (!(flags & REQSF_FORMAT))
            return;
        out.newline();
        level+=increment;
        if (level>0)
            out.pad(level);
    }
    inline void checkDelimitJsonMsg(StringBuffer &out, unsigned level, unsigned flags)
    {
        delimitJSON(out, (flags & REQSF_FORMAT));
        if (out.length() && out.charAt(out.length()-1)=='\n')
            out.pad(level);
    }
    static void buildJsonMsg(unsigned &level, StringArray& parentTypes, IXmlType* type, StringBuffer& out, const char* tag, IPropertyTree *reqTree, unsigned flags)
    {
        assertex(type!=NULL);

        if (flags & REQSF_ROOT)
        {
            out.append("{");
            checkNewlineJsonMsg(out, level, flags, 2);
        }

        const char* typeName = type->queryName();
        if (type->isComplexType())
        {
            if (typeName && !parentTypes.appendUniq(typeName))
                return; // recursive

            int startlen = out.length();
            if (tag)
                appendJSONName(out, tag);
            out.append('{');
            checkNewlineJsonMsg(out, level, flags, 2);
            int taglen=out.length()+1;
            if (type->getSubType()==SubType_Complex_SimpleContent)
            {
                if (reqTree)
                {
                    const char *attrval = reqTree->queryProp(NULL);
                    out.appendf("\"%s\" ", (attrval) ? attrval : "");
                }
                else if (flags & REQSF_SAMPLE_DATA)
                {
                    out.append("\"");
                    type->queryFieldType(0)->getSampleValue(out,tag);
                    out.append("\" ");
                }
            }
            else
            {
                if (flags & REQSF_ROOT)
                {
                    if (reqTree)
                    {
                        bool log = reqTree->getPropBool("@log", false); //not in schema
                        if (log)
                            appendJSONValue(out, "@log", true);
                        int tracelevel = reqTree->getPropInt("@traceLevel", -1);
                        if (tracelevel>=0)
                            appendJSONValue(out, "@traceLevel", tracelevel);
                    }
                }
                int flds = type->getFieldCount();
                for (int idx=0; idx<flds; idx++)
                {
                    checkDelimitJsonMsg(out, level, flags);
                    IXmlType *childType = type->queryFieldType(idx);
                    const char *childname = type->queryFieldName(idx);
                    bool repeats = type->queryFieldRepeats(idx);
                    if (repeats)
                    {
                        Owned<IPropertyTreeIterator> children;
                        if (reqTree)
                            children.setown(reqTree->getElements(childname));
                        appendJSONName(out, childname);
                        out.append('[');
                        checkNewlineJsonMsg(out, level, flags, 2);
                        if (!children)
                            buildJsonMsg(level, parentTypes, childType, out, NULL, NULL, flags & ~REQSF_ROOT);
                        else
                        {
                            ForEach(*children)
                                buildJsonMsg(level, parentTypes, childType, out, NULL, &children->query(), flags & ~REQSF_ROOT);
                        }
                        checkNewlineJsonMsg(out, level, flags, -2);
                        out.append(']');
                    }
                    else
                    {
                        IPropertyTree *childtree = NULL;
                        if (reqTree)
                            childtree = reqTree->queryPropTree(childname);
                        buildJsonMsg(level, parentTypes, childType, out, childname, childtree, flags & ~REQSF_ROOT);
                    }
                }
            }

            if (typeName)
                parentTypes.pop();
            checkNewlineJsonMsg(out, level, flags, -2); //end of children
            out.append("}");
        }
        else if (type->isArray())
        {
            bool skipContent = (typeName && !parentTypes.appendUniq(typeName)); // recursive
            const char* itemName = type->queryFieldName(0);
            IXmlType*   itemType = type->queryFieldType(0);
            if (!itemName || !itemType)
                throw MakeStringException(-1,"*** Invalid array definition: tag=%s, itemName=%s", tag, itemName?itemName:"NULL");

            int startlen = out.length();
            if (tag)
                out.appendf("\"%s\": ", tag);
            out.append('{');
            checkNewlineJsonMsg(out, level, flags, 2);
            out.appendf("\"%s\": [", itemName);
            checkNewlineJsonMsg(out, level, flags, 2);
            if (!skipContent)
            {
                if (reqTree)
                {
                    Owned<IPropertyTreeIterator> items = reqTree->getElements(itemName);
                    ForEach(*items)
                    {
                        checkDelimitJsonMsg(out, level, flags);
                        buildJsonMsg(level, parentTypes, itemType, out, NULL, &items->query(), flags & ~REQSF_ROOT);
                    }
                }
                else
                    buildJsonMsg(level, parentTypes, itemType, out, NULL, NULL, flags & ~REQSF_ROOT);
                if (typeName)
                    parentTypes.pop();
            }

            checkNewlineJsonMsg(out, level, flags, -2);
            out.append(']');
            checkNewlineJsonMsg(out, level, flags, -2);
            out.append("}");
        }
        else // simple type
        {
            const char *parmval = (reqTree) ? reqTree->queryProp(NULL) : NULL;
            buildJsonAppendValue(type, out, tag, parmval, flags);
        }

        if (flags & REQSF_ROOT)
        {
            checkNewlineJsonMsg(out, level, flags, -2);
            out.append('}');
        }
    }
    static void buildJsonMsg(StringArray& parentTypes, IXmlType* type, StringBuffer& out, const char* tag, IPropertyTree *reqTree, unsigned flags)
    {
        unsigned level = 0;
        buildJsonMsg(level, parentTypes, type, out, tag, reqTree, flags);
    }
};
#endif // _JSONHELPERS_HPP__
