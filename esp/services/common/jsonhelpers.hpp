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

// jsonhelpers.hpp:
//
//////////////////////////////////////////////////////////////////////
#pragma warning( disable : 4786)

#ifndef _JSONHELPERS_HPP__
#define _JSONHELPERS_HPP__
#include "jliball.hpp"

#define REQSF_ROOT         0x0001
#define REQSF_SAMPLE_DATA  0x0002
#define REQSF_TRIM         0x0004
#define REQSF_ESCAPEFORMATTERS 0x0008
#define REQSF_EXCLUSIVE (REQSF_SAMPLE_DATA | REQSF_TRIM)

class JsonHelpers
{
public:
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
        for (int i = 0 ; i < exceptions.ordinality(); i++)
        {
            appendJSONExceptionItem(s, e->errorCode(), e->errorMessage(temp).str(), objname, arrayName);
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
        if (isdigit(*c))
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
            if (!isdigit(*c))
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
        else if (isdigit(*c))
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
        //map XML Schema types used in ECL generated schemas to basic JSON formatting types
        if (streq(xsdtype, "integer") || streq(xsdtype, "nonNegativeInteger"))
            return JSONField_Integer;
        if (streq(xsdtype, "boolean"))
            return JSONField_Boolean;
        if (streq(xsdtype, "double"))
            return JSONField_Real;
        if (!strncmp(xsdtype, "decimal", 7)) //ecl creates derived types of the form decimal#_#
            return JSONField_Real;
        if (streq(xsdtype, "none")) //maps to an eml schema element with no type.  set to true or don't add
            return JSONField_Present;
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

    static const char *nextParameterTag(StringBuffer &tag, const char *path)
    {
        while (*path=='.')
            path++;
        const char *finger = strchr(path, '.');
        if (finger)
        {
            tag.clear().append(finger - path, path);
            finger++;
        }
        else
            tag.set(path);
        return finger;
    }

    static void ensureParameter(IPropertyTree *pt, StringBuffer &tag, const char *path, const char *value, const char *fullpath)
    {
        if (!tag.length())
            return;

        unsigned idx = 1;
        if (path && isdigit(*path))
        {
            StringBuffer pos;
            path = nextParameterTag(pos, path);
            idx = (unsigned) atoi(pos.str())+1;
            if (idx>25) //adf
                throw MakeStringException(-1, "Array items above 25 not supported in HPCC WS HTTP parameters: %s", fullpath);
        }

        if (tag.charAt(tag.length()-1)=='$')
        {
            if (path && *path)
                throw MakeStringException(-1, "'$' not allowed in parent node of parameter path: %s", fullpath);
            tag.setLength(tag.length()-1);
            StringArray values;
            values.appendList(value, "\r");
            ForEachItemIn(pos, values)
            {
                const char *itemValue = values.item(pos);
                while (*itemValue=='\n')
                    itemValue++;
                pt->addProp(tag, itemValue);
            }
            return;
        }
        unsigned count = pt->getCount(tag);
        while (count++ < idx)
            pt->addPropTree(tag, createPTree(tag));
        StringBuffer xpath(tag);
        xpath.append('[').append(idx).append(']');
        pt = pt->queryPropTree(xpath);

        if (!path || !*path)
        {
            pt->setProp(NULL, value);
            return;
        }

        StringBuffer nextTag;
        path = nextParameterTag(nextTag, path);
        ensureParameter(pt, nextTag, path, value, fullpath);
    }

    static void ensureParameter(IPropertyTree *pt, const char *path, const char *value)
    {
        const char *fullpath = path;
        StringBuffer tag;
        path = nextParameterTag(tag, path);
        ensureParameter(pt, tag, path, value, fullpath);
    }

    static IPropertyTree *createPTreeFromHttpParameters(const char *name, IProperties *parameters)
    {
        Owned<IPropertyTree> pt = createPTree(name);
        Owned<IPropertyIterator> props = parameters->getIterator();
        ForEach(*props)
        {
            const char *key = props->getPropKey();
            const char *value = parameters->queryProp(key);
            ensureParameter(pt, key, value);
        }
        return pt.getClear();
    }
};
#endif // _JSONHELPERS_HPP__
