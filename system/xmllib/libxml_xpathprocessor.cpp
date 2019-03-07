/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "jstring.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"

#include <libxml/xmlmemory.h>
#include <libxml/parserInternals.h>
#include <libxml/debugXML.h>
#include <libxml/HTMLtree.h>
#include <libxml/xmlIO.h>
#include <libxml/xinclude.h>
#include <libxml/catalog.h>
#include <libxml/xpathInternals.h>
#include <libxml/xpath.h>
#include <libxml/xmlschemas.h>
#include <libxml/hash.h>

#include "xpathprocessor.hpp"
#include "xmlerror.hpp"

class CLibCompiledXpath : public CInterface, public ICompiledXpath
{
private:
    xmlXPathCompExprPtr m_compiledXpathExpression = nullptr;
    StringBuffer m_xpath;
    ReadWriteLock m_rwlock;

public:
    IMPLEMENT_IINTERFACE;

    CLibCompiledXpath(const char * xpath)
    {
        m_xpath.set(xpath);
        m_compiledXpathExpression = xmlXPathCompile(BAD_CAST m_xpath.str());
    }
    ~CLibCompiledXpath()
    {
        xmlXPathFreeCompExpr(m_compiledXpathExpression);
    }
    const char * getXpath()
    {
        return m_xpath.str();
    }
    xmlXPathCompExprPtr getCompiledXPathExpression()
    {
        return m_compiledXpathExpression;
    }
};

class CLibXpathContext : public CInterface, public IXpathContext
{
private:
    xmlDocPtr m_xmlDoc = nullptr;
    xmlXPathContextPtr m_xpathContext = nullptr;
    StringBuffer m_xpath;
    ReadWriteLock m_rwlock;

public:
    IMPLEMENT_IINTERFACE;

    CLibXpathContext(const char * xmldoc) //not thread safe
    {
        setXmlDoc(xmldoc);
    }
    ~CLibXpathContext()
    {
        xmlXPathFreeContext(m_xpathContext);
        xmlFreeDoc(m_xmlDoc);
    }

    static void tableScanCallback(void *payload, void *data, xmlChar *name)
    {
        DBGLOG("k/v == [%s,%s]\n", (char *) name, (char *) payload);
    }

    virtual const char * getXpath() override
    {
        return m_xpath.str();
    }

    virtual bool addVariable(const char * name,  const char * val) override
    {
        WriteLockBlock wblock(m_rwlock);
        if (m_xpathContext && val)
        {
            return xmlXPathRegisterVariable(m_xpathContext, (xmlChar *)name, xmlXPathNewCString(val)) == 0;
        }
        return false;
    }

    virtual const char * getVariable(const char * name, StringBuffer & variable) override
    {
        ReadLockBlock rblock(m_rwlock);
        if (m_xpathContext)
        {
            xmlXPathObjectPtr ptr = xmlXPathVariableLookup(m_xpathContext, (const xmlChar *)name);
            variable.append((const char *) ptr->stringval);
            xmlXPathFreeObject(ptr);
            return variable;
        }
        else
            return nullptr;
    }

    virtual bool evaluateAsBoolean(const char * xpath)
    {
        if (!xpath || !*xpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsBoolean: Error: Could not evaluate empty XPATH");
        return evaluateAsBoolean(evaluate(xpath), xpath);
    }
    virtual bool evaluateAsString(const char * xpath, StringBuffer & evaluated)
    {
        if (!xpath || !*xpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsString: Error: Could not evaluate empty XPATH");
        return evaluateAsString(evaluate(xpath), evaluated, xpath);
    }

    virtual bool evaluateAsBoolean(ICompiledXpath * compiledXpath) override
    {
        CLibCompiledXpath * clibCompiledXpath = static_cast<CLibCompiledXpath *>(compiledXpath);
        if (!clibCompiledXpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsBoolean: Error: Missing compiled XPATH");
        return evaluateAsBoolean(evaluate(clibCompiledXpath->getCompiledXPathExpression(), compiledXpath->getXpath()), compiledXpath->getXpath());
    }

    virtual const char * evaluateAsString(ICompiledXpath * compiledXpath, StringBuffer & evaluated) override
    {
        CLibCompiledXpath * clibCompiledXpath = static_cast<CLibCompiledXpath *>(compiledXpath);
        if (!clibCompiledXpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsString: Error: Missing compiled XPATH");
        return evaluateAsString(evaluate(clibCompiledXpath->getCompiledXPathExpression(), compiledXpath->getXpath()), evaluated, compiledXpath->getXpath());
    }

private:
    virtual bool setXmlDoc(const char * xmldoc) override
    {
        if (xmldoc && * xmldoc)
        {
            m_xmlDoc = xmlParseDoc((const unsigned char *)xmldoc);
            if (m_xmlDoc == nullptr)
            {
                ERRLOG("XpathProcessor:setxmldoc Error: Unable to parse XMLLib document");
                return false;
            }

            // Create xpath evaluation context
            m_xpathContext = xmlXPathNewContext(m_xmlDoc);
            if(m_xpathContext == nullptr)
            {
                ERRLOG("XpathProcessor:setxmldoc: Error: Unable to create new XMLLib XPath context");
                return false;
            }
            return true;
        }
        return false;
    }

    bool evaluateAsBoolean(xmlXPathObjectPtr evaluatedXpathObj, const char* xpath)
    {
        if (!evaluatedXpathObj)
        {
            throw MakeStringException(XPATHERR_InvalidInput, "XpathProcessor:evaluateAsBoolean: Error: Could not evaluate XPATH '%s'", xpath);
        }
        if (XPATH_BOOLEAN != evaluatedXpathObj->type)
        {
            xmlXPathFreeObject(evaluatedXpathObj);
            throw MakeStringException(XPATHERR_UnexpectedInput, "XpathProcessor:evaluateAsBoolean: Error: Could not evaluate XPATH '%s' as Boolean", xpath);
        }

        bool bresult = evaluatedXpathObj->boolval;

        xmlXPathFreeObject(evaluatedXpathObj);
        return bresult;
    }

    const char* evaluateAsString(xmlXPathObjectPtr evaluatedXpathObj, StringBuffer& evaluated, const char* xpath)
    {
        if (!evaluatedXpathObj)
            throw MakeStringException(XPATHERR_InvalidInput,"XpathProcessor:evaluateAsString: Error: Could not evaluate XPATH '%s'", xpath);

        evaluated.clear();
        switch (evaluatedXpathObj->type)
        {
            case XPATH_NODESET:
            {
                xmlNodeSetPtr nodes = evaluatedXpathObj->nodesetval;
                for (int i = 0; i < nodes->nodeNr; i++)
                {
                    xmlNodePtr nodeTab = nodes->nodeTab[i];
                    auto nodeContent = xmlNodeGetContent(nodeTab);
                    evaluated.append((const char *)nodeContent);
                    xmlFree(nodeContent);
                }
                break;
            }
            case XPATH_BOOLEAN:
            case XPATH_NUMBER:
            case XPATH_STRING:
            case XPATH_POINT:
            case XPATH_RANGE:
            case XPATH_LOCATIONSET:
            case XPATH_USERS:
            case XPATH_XSLT_TREE:
            {
                evaluatedXpathObj = xmlXPathConvertString (evaluatedXpathObj); //existing object is freed
                if (!evaluatedXpathObj)
                    throw MakeStringException(XPATHERR_UnexpectedInput,"XpathProcessor:evaluateAsString: Error: Could not evaluate XPATH '%s'; could not convert result to string", xpath);
                evaluated.append(evaluatedXpathObj->stringval);
                break;
            }
            default:
            {
                xmlXPathFreeObject(evaluatedXpathObj);
                throw MakeStringException(XPATHERR_UnexpectedInput,"XpathProcessor:evaluateAsString: Error: Could not evaluate XPATH '%s' as string; unexpected type %d", xpath, evaluatedXpathObj->type);
                break;
            }
        }
        xmlXPathFreeObject(evaluatedXpathObj);
        return evaluated.str();
    }

    virtual xmlXPathObjectPtr evaluate(xmlXPathCompExprPtr compiledXpath, const char* xpath)
    {
        xmlXPathObjectPtr evaluatedXpathObj = nullptr;
        if (compiledXpath)
        {
            ReadLockBlock rlock(m_rwlock);
            if ( m_xpathContext)
            {
                evaluatedXpathObj = xmlXPathCompiledEval(compiledXpath, m_xpathContext);
            }
            else
            {
                throw MakeStringException(XPATHERR_InvalidState,"XpathProcessor:evaluate: Error: Could not evaluate XPATH '%s'; ensure xmldoc has been set", xpath);
            }
        }

        return evaluatedXpathObj;
    }

    virtual xmlXPathObjectPtr evaluate(const char * xpath)
    {
        xmlXPathObjectPtr evaluatedXpathObj = nullptr;
        if (xpath && *xpath)
        {
            ReadLockBlock rlock(m_rwlock);
            if ( m_xpathContext)
            {
                evaluatedXpathObj = xmlXPathEval((const xmlChar *)xpath, m_xpathContext);
            }
            else
            {
                throw MakeStringException(XPATHERR_InvalidState,"XpathProcessor:evaluate: Error: Could not evaluate XPATH '%s'; ensure xmldoc has been set", xpath);
            }
        }

        return evaluatedXpathObj;
    }
};

extern ICompiledXpath* compileXpath(const char * xpath)
{
    return new CLibCompiledXpath(xpath);
}

extern IXpathContext* getXpathContext(const char * xmldoc)
{
    return new CLibXpathContext(xmldoc);
}
