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
#include "wsexcept.hpp"

#define REQSF_ROOT         0x0001
#define REQSF_SAMPLE_DATA  0x0002
#define REQSF_TRIM         0x0004
#define REQSF_ESCAPEFORMATTERS 0x0008
#define REQSF_EXCLUSIVE (REQSF_SAMPLE_DATA | REQSF_TRIM)
#define NOT_FOUND UINT_MAX

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
    static bool isNotJSONDelim(int source, int delim)
    {
    	return (source != delim);
    }
    static void trimJSONString(StringBuffer& buffer, unsigned rpos=NOT_FOUND)
    {
    	unsigned rlen = buffer.length();
    	if (!rlen)
    		return;

    	const char *p = buffer.str();
    	if(rpos == NOT_FOUND)
    		rpos = rlen -1;
    	p += rpos;
    	assertex(rpos!=NOT_FOUND);

    	switch (*p)
    	{
    		case '}':
    		{
    			unsigned ccurlyPos = rpos;


    			for(p--,--rpos;isspace(*p)&&rpos>=0;p--&&--rpos);
    			if (ccurlyPos == rpos) { p--;--rpos; }
    			for(;isspace(*p)&&rpos>=0;p--&&--rpos);

    			if (*p == ':')
    			{
    				unsigned colonPos = rpos;

    				for(;isNotJSONDelim(*p, '"')&&rpos>=0;p--&&--rpos);
    				for(p--,--rpos;isNotJSONDelim(*p, '"')&&rpos>=0;p--&&--rpos);

    				unsigned dqPos1 = rpos; //'"'

    				for(p--,--rpos;isspace(*p)&&rpos>=0;p--&&--rpos);
    				if (*(p-1) == '{')
    					buffer.remove(rpos-1, ccurlyPos-rpos+2).clip();
    				else
    					buffer.remove(dqPos1, colonPos-dqPos1+1).clip();

    				if (buffer.length())
    					trimJSONString(buffer);
    			}
    			else if (*p == '{')
    			{
    				unsigned ocurlyPos = rpos;
    				buffer.remove(ocurlyPos, ccurlyPos-ocurlyPos+1).clip();

    				if (buffer.length())
    					trimJSONString(buffer);
    			}
    			else if (*p == ',')
    			{
    				unsigned commaPos = rpos;
    				for(p--,--rpos;isspace(*p)&&rpos>=0;p--&&--rpos);
    				if (commaPos == rpos) { p--;--rpos; }
    				if (*p != ':')
    				{
    					buffer.remove(commaPos, ccurlyPos-commaPos).clip();

    					if (buffer.length())
    						trimJSONString(buffer);
    				}
    				else if (*p == ':')
    				{
    					unsigned colonPos = rpos;

    					for(;isNotJSONDelim(*p, '"')&&rpos>=0;p--&&--rpos);//goto to the second double-quote.
    					for(p--,--rpos;isNotJSONDelim(*p, '"')&&rpos>=0;p--&&--rpos);//goto to the first double-quote.

    					unsigned dqPos1 = rpos - 1; //'"'

    					for(p--,--rpos;isspace(*p)&&rpos>=0;p--&&--rpos);
    					if (*(p-1) == '{')
    						buffer.remove(rpos-1, ccurlyPos-rpos+1).clip();
    					else
    						buffer.remove(dqPos1, colonPos-dqPos1+1).clip();

    					if (buffer.length())
    						trimJSONString(buffer);
    				}
    				else if (*p == '}')
    				{
    					buffer.remove(commaPos, 1).clip();

    					if(buffer.length())
    						trimJSONString(buffer);
    				}
    			}
    			else
    				trimJSONString(buffer, rpos);

    			break;
    		}
    		case ':':
    		{
    			unsigned colonPos = rpos;

    			for(;isNotJSONDelim(*p, '"')&&rpos>=0;p--&&--rpos);//goto to the second double-quote.
    			for(p--,--rpos;isNotJSONDelim(*p, '"')&&rpos>=0;p--&&--rpos);//goto to the first double-quote.

    			unsigned dqPos1 = rpos; //'"'

    			for(p--,--rpos;isspace(*p)&&rpos>=0;p--&&--rpos);
    			buffer.remove(dqPos1, colonPos-dqPos1+1).clip();

    			break;
    		}
    		case ']':
    		{
    			unsigned csquarePos = rpos;

    			for(;isNotJSONDelim(*p, '[')&&rpos>=0;p--&&--rpos) ;

    			unsigned osquarePos = rpos;
    			unsigned len = csquarePos - osquarePos + 1;
    			if (len == 2) // '[]'. Remove it.
    				buffer.remove(osquarePos, len).clip();

    			len = buffer.length();
    			if (len)
    			{
    				const char *p = buffer.str();
    				unsigned rpos = len -1;
    				p += rpos;
    				if (*p == '}')
    				{
    					for(p--,--rpos;isspace(*p)&&rpos>=0;p--&&--rpos);
    					if ((*p == ']')) // ]}
    					{
    						for(p--,--rpos;isspace(*p)&&rpos>=0;p--&&--rpos);
    						if (*p == '[') // []}
    							trimJSONString(buffer, rpos);
    					}
    					else if (*p == ':') // {"QuoteBack": }
    						trimJSONString(buffer);
    				}
    			}

    			break;
    		}
    	}
    }
    static void buildJsonMsg(StringArray& parentTypes, IXmlType* type, StringBuffer& out, const char* tag, const IPropertyTree *reqTree, unsigned flags)
    {
        assertex(type!=NULL);

        if (flags & REQSF_ROOT)
            out.append('{');

        const char* typeName = type->queryName();
        if (type->isComplexType())
        {
            if (typeName && !parentTypes.appendUniq(typeName))
                return; // recursive

            if (((flags & REQSF_TRIM) && (tag&&*tag)) || (!(flags & REQSF_TRIM)))
                appendJSONName(out, tag);
            out.append('{');
            if (type->getSubType()==SubType_Complex_SimpleContent)
            {
                if (reqTree)
                {
                    const char *attrval = reqTree->queryProp(NULL);
                    if ((flags & REQSF_TRIM) && (attrval&&*attrval))
                    		out.appendf("\"%s\" ", attrval);
                    else if (!(flags & REQSF_TRIM))
                    out.appendf("\"%s\" ", (attrval) ? attrval : "");
                }
                else if (flags & REQSF_SAMPLE_DATA)
                {
                    if (((flags & REQSF_TRIM) && (tag&&*tag)) || (!(flags & REQSF_TRIM)))
                    {
                    out.append("\"");
                    type->queryFieldType(0)->getSampleValue(out,tag);
                    out.append("\" ");
                    }
                }
            }
            else
            {
                int flds = type->getFieldCount();
                for (int idx=0; idx<flds; idx++)
                {
                    delimitJSON(out);
                    IPropertyTree *childtree = NULL;
                    const char *childname = type->queryFieldName(idx);
                    if (reqTree)
                        childtree = reqTree->queryPropTree(childname);
                    buildJsonMsg(parentTypes, type->queryFieldType(idx), out, childname, childtree, flags & ~REQSF_ROOT);
                }
            }

            if (typeName)
                parentTypes.pop();
            out.append('}');
        }
        else if (type->isArray())
        {
            if (typeName && !parentTypes.appendUniq(typeName))
                return; // recursive

            const char* itemName = type->queryFieldName(0);
            IXmlType*   itemType = type->queryFieldType(0);
            if (!itemName || !itemType)
                throw MakeStringException(-1,"*** Invalid array definition: tag=%s, itemName=%s", tag, itemName?itemName:"NULL");

        	if (((flags & REQSF_TRIM) && (tag&&*tag)) || (!(flags & REQSF_TRIM) && (tag)))
                out.appendf("\"%s\": ", tag);
            out.append('{');
        	if (((flags & REQSF_TRIM) && (itemName&&*itemName)) || (!(flags & REQSF_TRIM)))
            out.appendf("\"%s\": [", itemName);
            if (reqTree)
            {
                Owned<IPropertyTreeIterator> items = reqTree->getElements(itemName);
                ForEach(*items)
                    buildJsonMsg(parentTypes, itemType, delimitJSON(out), NULL, &items->query(), flags & ~REQSF_ROOT);
            }
            else
                buildJsonMsg(parentTypes, itemType, out, NULL, NULL, flags & ~REQSF_ROOT);

            out.append(']');

            if (typeName)
                parentTypes.pop();
            out.append('}');
        }
        else // simple type
        {
            const char *parmval = (reqTree) ? reqTree->queryProp(NULL) : NULL;
            if (((flags & REQSF_TRIM) && (parmval&&*parmval)) || (!(flags & REQSF_TRIM)))
            buildJsonAppendValue(type, out, tag, parmval, flags);
        }

        if (flags & REQSF_ROOT)
            out.append('}');
        if (flags & REQSF_TRIM)
        	trimJSONString(out);
    }
};
#endif // _JSONHELPERS_HPP__
