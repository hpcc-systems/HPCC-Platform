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
#include <libexslt/exslt.h>

#include "xpathprocessor.hpp"
#include "xmlerror.hpp"

#include <map>
#include <stack>
#include <string>
#include <memory>

static inline char *skipWS(char *s)
{
    while (isspace(*s)) s++;
    return s;
}
static char *markEndGetNext(char *line)
{
    char *end = (char *)strchr(line, '\n');
    if (!end)
        return nullptr;
    *end=0;
    if (isEmptyString(++end))
        return nullptr;
    return end;
}
static char *extractFromLineGetNext(StringArray &functions, StringArray &variables, char *line)
{
    line = skipWS(line);
    if (isEmptyString(line))
        return nullptr;
    char *next = markEndGetNext(line);
    if (strncmp(line, "FUNCTION", 8)==0)
    {
        char *paren = (char *)strchr(line, '(');
        if (paren)
            *paren=0;
        functions.append(skipWS(line+8+1));
    }
    else if (strncmp(line, "VARIABLE", 8)==0)
    {
        variables.append(skipWS(line+8+1));
    }
    return next;
}

class CLibCompiledXpath : public CInterfaceOf<ICompiledXpath>
{
private:
    xmlXPathCompExprPtr m_compiledXpathExpression = nullptr;
    StringBuffer m_xpath;
    ReadWriteLock m_rwlock;

public:
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

    virtual void extractReferences(StringArray &functions, StringArray &variables) override
    {
        char *buf = nullptr;
        size_t len = 0;

        FILE *stream = open_memstream(&buf, &len);
        if (stream == nullptr)
            return;

        xmlXPathDebugDumpCompExpr(stream, m_compiledXpathExpression, 0);
        fputc(0, stream);
        fflush(stream);
        fclose (stream);
        char *line = buf;
        while (line)
            line = extractFromLineGetNext(functions, variables, line);
        free (buf);
    }
};
static xmlXPathObjectPtr variableLookupFunc(void *data, const xmlChar *name, const xmlChar *ns_uri);

typedef std::map<std::string, xmlXPathObjectPtr> XPathObjectMap;

class CLibXpathScope
{
public:
    StringAttr name; //in future allow named parent access?
    XPathObjectMap variables;

public:
    CLibXpathScope(const char *_name) : name(_name){}
    ~CLibXpathScope()
    {
        for (XPathObjectMap::iterator it=variables.begin(); it!=variables.end(); ++it)
            xmlXPathFreeObject(it->second);
    }
    bool setObject(const char *key, xmlXPathObjectPtr obj)
    {
        std::pair<XPathObjectMap::iterator,bool> ret = variables.emplace(key, obj);
        if (ret.second==true)
            return true;
        //within scope, behave exactly like xmlXPathContext variables are added now, which seems to be that they are replaced
        //if we're preventing replacing variables we need to handle elsewhere
        //  and still replace external values when treated as xsdl:variables, but not when treated as xsdl:params
        if (ret.first->second)
            xmlXPathFreeObject(ret.first->second);
        ret.first->second = obj;
        return true;
    }
    xmlXPathObjectPtr getObject(const char *key)
    {
        XPathObjectMap::iterator it = variables.find(key);
        if (it == variables.end())
            return nullptr;
        return it->second;
    }
};

typedef std::vector<std::unique_ptr<CLibXpathScope>> XPathScopeVector;
typedef std::map<std::string, ICompiledXpath*> XPathInputMap;

class XpathContextState
{
private:
    xmlDocPtr doc = nullptr;
    xmlNodePtr node = nullptr;
    int contextSize = 0;
    int proximityPosition = 0;
public:
    XpathContextState(xmlXPathContextPtr ctx)
    {
        doc = ctx->doc;
        node = ctx->node;
        contextSize = ctx->contextSize;
        proximityPosition = ctx->proximityPosition;
    }

    void restore(xmlXPathContextPtr ctx)
    {
        ctx->doc = doc;
        ctx->node = node;
        ctx->contextSize = contextSize;
        ctx->proximityPosition = proximityPosition;
    }
};

typedef std::vector<XpathContextState> XPathContextStateVector;

class CLibXpathContext : public CInterfaceOf<IXpathContext>
{
public:
    XPathInputMap provided;
    xmlDocPtr m_xmlDoc = nullptr;
    xmlXPathContextPtr m_xpathContext = nullptr;
    ReadWriteLock m_rwlock;
    XPathScopeVector scopes;
    bool strictParameterDeclaration = true;
    bool removeDocNamespaces = false;

    //saved state
    XPathContextStateVector saved;

public:
    CLibXpathContext(const char * xmldoc, bool _strictParameterDeclaration, bool removeDocNs) : strictParameterDeclaration(_strictParameterDeclaration), removeDocNamespaces(removeDocNs)
    {
        beginScope("/");
        setXmlDoc(xmldoc);
        exsltDateXpathCtxtRegister(m_xpathContext, (xmlChar*)"date");
        exsltMathXpathCtxtRegister(m_xpathContext, (xmlChar*)"math");
        exsltSetsXpathCtxtRegister(m_xpathContext, (xmlChar*)"set");
        exsltStrXpathCtxtRegister(m_xpathContext, (xmlChar*)"str");
    }

    ~CLibXpathContext()
    {
        for (XPathInputMap::iterator it=provided.begin(); it!=provided.end(); ++it)
            it->second->Release();
        xmlXPathFreeContext(m_xpathContext);
        xmlFreeDoc(m_xmlDoc);
    }

    void pushLocation()
    {
        WriteLockBlock wblock(m_rwlock);
        saved.emplace_back(XpathContextState(m_xpathContext));
    }

    void setLocation(xmlDocPtr doc, xmlNodePtr node, int contextSize, int proximityPosition)
    {
        WriteLockBlock wblock(m_rwlock);
        m_xpathContext->doc = doc;
        m_xpathContext->node = node;
        m_xpathContext->contextSize = contextSize;
        m_xpathContext->proximityPosition = proximityPosition;
    }

    void setLocation(xmlXPathContextPtr ctx)
    {
        WriteLockBlock wblock(m_rwlock);
        m_xpathContext->doc = ctx->doc;
        m_xpathContext->node = ctx->node;
        m_xpathContext->contextSize = ctx->contextSize;
        m_xpathContext->proximityPosition = ctx->proximityPosition;
    }

    void popLocation()
    {
        WriteLockBlock wblock(m_rwlock);
        saved.back().restore(m_xpathContext);
        saved.pop_back();
    }

    void beginScope(const char *name) override
    {
        WriteLockBlock wblock(m_rwlock);
        scopes.emplace_back(new CLibXpathScope(name));
    }

    void endScope() override
    {
        WriteLockBlock wblock(m_rwlock);
        if (scopes.size()>1) //preserve root scope
            scopes.pop_back();
    }

    static void tableScanCallback(void *payload, void *data, xmlChar *name)
    {
        DBGLOG("k/v == [%s,%s]\n", (char *) name, (char *) payload);
    }

    virtual void registerNamespace(const char *prefix, const char * uri) override
    {
        if (m_xpathContext)
        {
            WriteLockBlock wblock(m_rwlock);
            xmlXPathRegisterNs(m_xpathContext, (const xmlChar *) prefix, (const xmlChar *) uri);
        }
    }

    virtual const char *queryNamespace(const char *prefix) override
    {
        if (!m_xpathContext)
            return nullptr;
        WriteLockBlock wblock(m_rwlock);
        return (const char *) xmlXPathNsLookup(m_xpathContext, (const xmlChar *)prefix);
    }

    virtual void registerFunction(const char *xmlns, const char * name, void *f) override
    {
        if (m_xpathContext)
        {
            WriteLockBlock wblock(m_rwlock);
            xmlXPathRegisterFuncNS(m_xpathContext, (const xmlChar *) name, (const xmlChar *) xmlns, (xmlXPathFunction) f);
        }
    }

    virtual void setUserData(void *userdata) override
    {
        if (m_xpathContext)
        {
            WriteLockBlock wblock(m_rwlock);
            m_xpathContext->userData = userdata;
        }
    }

    virtual void *getUserData() override
    {
        if (m_xpathContext)
        {
            ReadLockBlock wblock(m_rwlock);
            return m_xpathContext->userData;
        }
        return nullptr;
    }

    inline CLibXpathScope *getCurrentScope()
    {
        ReadLockBlock wblock(m_rwlock);
        assertex(scopes.size());
        return scopes.back().get();
    }
    xmlXPathObjectPtr findVariable(const char *name, const char *ns_uri, CLibXpathScope *scope)
    {
        const char *fullname = name;
        StringBuffer s;
        if (!isEmptyString(ns_uri))
            fullname = s.append(ns_uri).append(':').append(name).str();

        ReadLockBlock wblock(m_rwlock);
        xmlXPathObjectPtr obj = nullptr;
        if (scope)
            return scope->getObject(fullname);

        for (XPathScopeVector::const_reverse_iterator it=scopes.crbegin(); !obj && it!=scopes.crend(); ++it)
            obj = it->get()->getObject(fullname);

        //check libxml2 level variables, shouldn't happen currently but we may want to wrap existing xpathcontexts in the future
        if (!obj)
            obj = (xmlXPathObjectPtr)xmlHashLookup2(m_xpathContext->varHash, (const xmlChar *)name, (const xmlChar *)ns_uri);
        return obj;
    }

    xmlXPathObjectPtr getVariableObject(const char *name, const char *ns_uri, CLibXpathScope *scope)
    {
        return xmlXPathObjectCopy(findVariable(name, ns_uri, scope));
    }

    bool hasVariable(const char *name, const char *ns_uri, CLibXpathScope *scope)
    {
        return (findVariable(name, ns_uri, scope)!=nullptr);
    }

    virtual bool addObjectVariable(const char * name, xmlXPathObjectPtr obj, CLibXpathScope *scope)
    {
        if (isEmptyString(name))
            return false;
        if (m_xpathContext)
        {
            if (!obj)
                throw MakeStringException(-1, "addObjectVariable %s error", name);
            WriteLockBlock wblock(m_rwlock);
            if (!scope && !scopes.empty())
                scope = scopes.back().get();
            if (scope)
                return scope->setObject(name, obj);
            return xmlXPathRegisterVariable(m_xpathContext, (xmlChar *)name, obj) == 0;
        }
        return false;
    }

    bool addStringVariable(const char * name,  const char * val, CLibXpathScope *scope)
    {
        if (!val)
            return false;
        return addObjectVariable(name, xmlXPathNewCString(val), scope);
    }

    virtual bool addXpathVariable(const char * name, const char * xpath, CLibXpathScope *scope)
    {
        if (isEmptyString(xpath))
            addVariable(name, "");
        if (m_xpathContext)
        {
            xmlXPathObjectPtr obj = evaluate(xpath);
            if (!obj)
                throw MakeStringException(-1, "addXpathVariable xpath error %s", xpath);
            return addObjectVariable(name, obj, scope);
        }
        return false;
    }

    bool addCompiledVariable(const char * name, ICompiledXpath * compiled, CLibXpathScope *scope)
    {
        if (!compiled)
            addVariable(name, "");
        if (m_xpathContext)
        {
            CLibCompiledXpath * clibCompiledXpath = static_cast<CLibCompiledXpath *>(compiled);
            xmlXPathObjectPtr obj = evaluate(clibCompiledXpath->getCompiledXPathExpression(), clibCompiledXpath->getXpath());
            if (!obj)
                throw MakeStringException(-1, "addEvaluateVariable xpath error %s", clibCompiledXpath->getXpath());
            return addObjectVariable(name, obj, scope);
        }

        return false;
    }

    virtual bool addInputValue(const char * name, const char * value) override
    {
        if (isEmptyString(name)||isEmptyString(value))
            return false;
        VStringBuffer xpath("'%s'", value);
        return addInputXpath(name, xpath);
    }
    virtual bool addInputXpath(const char * name, const char * xpath) override
    {
        if (isEmptyString(name)||isEmptyString(xpath))
            return false;
        Owned<ICompiledXpath> compiled = compileXpath(xpath);
        if (compiled)
        {
            WriteLockBlock wblock(m_rwlock);
            provided.emplace(name, compiled.getClear());
            return true;
        }
        return false;
    }

    inline ICompiledXpath *findInput(const char *name)
    {
        ReadLockBlock rblock(m_rwlock);
        XPathInputMap::iterator it = provided.find(name);
        if (it == provided.end())
            return nullptr;
         return it->second;
    }

    virtual bool declareCompiledParameter(const char * name, ICompiledXpath * compiled) override
    {
        if (hasVariable(name, nullptr, getCurrentScope()))
            return false;

        //use input value
        ICompiledXpath *inputxp = findInput(name);
        if (inputxp)
            return addCompiledVariable(name, inputxp, getCurrentScope());

        //use default provided
        return addCompiledVariable(name, compiled, getCurrentScope());
    }

    virtual void declareRemainingInputs() override
    {
        for (XPathInputMap::iterator it=provided.begin(); it!=provided.end(); ++it)
            declareCompiledParameter(it->first.c_str(), it->second);
    }

    virtual bool declareParameter(const char * name, const char *value) override
    {
        if (hasVariable(name, nullptr, getCurrentScope()))
            return false;

        //use input value
        ICompiledXpath *input = findInput(name);
        if (input)
            return addCompiledVariable(name, input, getCurrentScope());

        //use default provided
        return addStringVariable(name, value, getCurrentScope());
    }

    virtual bool addXpathVariable(const char * name, const char * xpath) override
    {
        return addXpathVariable(name, xpath, nullptr);
    }


    virtual bool addVariable(const char * name,  const char * val) override
    {
        return addStringVariable(name, val, nullptr);
    }

    virtual bool addCompiledVariable(const char * name, ICompiledXpath * compiled) override
    {
        return addCompiledVariable(name, compiled, nullptr);
    }

    virtual const char * getVariable(const char * name, StringBuffer & variable) override
    {
        if (m_xpathContext)
        {
            ReadLockBlock rblock(m_rwlock);
            xmlXPathObjectPtr ptr = xmlXPathVariableLookupNS(m_xpathContext, (const xmlChar *)name, nullptr);
            if (!ptr)
                return nullptr;
            variable.append((const char *) ptr->stringval);
            xmlXPathFreeObject(ptr);
            return variable;
        }
        return nullptr;
    }
    virtual  IXpathContextIterator *evaluateAsNodeSet(ICompiledXpath * compiled) override
    {
        CLibCompiledXpath * clCompiled = static_cast<CLibCompiledXpath *>(compiled);
        if (!clCompiled)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsNodeSet: Error: Could not evaluate XPATH");
        return evaluateAsNodeSet(evaluate(clCompiled->getCompiledXPathExpression(), compiled->getXpath()), compiled->getXpath());
    }

    IXpathContextIterator *evaluateAsNodeSet(xmlXPathObjectPtr evaluated, const char* xpath);

    virtual bool evaluateAsBoolean(const char * xpath) override
    {
        if (!xpath || !*xpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsBoolean: Error: Could not evaluate empty XPATH");
        return evaluateAsBoolean(evaluate(xpath), xpath);
    }

    virtual bool evaluateAsString(const char * xpath, StringBuffer & evaluated) override
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

    virtual double evaluateAsNumber(ICompiledXpath * compiledXpath) override
    {
        CLibCompiledXpath * clibCompiledXpath = static_cast<CLibCompiledXpath *>(compiledXpath);
        if (!clibCompiledXpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsNumber: Error: Missing compiled XPATH");
        return evaluateAsNumber(evaluate(clibCompiledXpath->getCompiledXPathExpression(), compiledXpath->getXpath()), compiledXpath->getXpath());
    }

    virtual bool setXmlDoc(const char * xmldoc) override
    {
        if (isEmptyString(xmldoc))
            return false;
        xmlDocPtr doc = xmlParseDoc((const unsigned char *)xmldoc);
        if (doc == nullptr)
        {
            ERRLOG("XpathProcessor:setXmlDoc Error: Unable to parse XMLLib document");
            return false;
        }
        return setContextDocument(doc, xmlDocGetRootElement(doc));
    }

private:
    xmlNodePtr checkGetSoapNamespace(xmlNodePtr cur, const char *expected, const xmlChar *&soap)
    {
        if (!cur)
            return nullptr;
        if (!streq((const char *)cur->name, expected))
            return cur;
        if (!soap && cur->ns && cur->ns->href)
            soap = cur->ns->href;
        return xmlFirstElementChild(cur);
    }
    void removeNamespace(xmlNodePtr cur, const xmlChar *remove)
    {
        //we need to recognize different namespace behavior at each entry point
        //
        //for backward compatibility we need to be flexible with what we receive, from the user
        //but other entry points are dealing with standardized content
        //hopefully even calls out to a given 3rd party services will be self consistent
        //
        if (!remove)
            cur->ns=nullptr; //just a "reference", no free
        else if (cur->ns && cur->ns->href && streq((const char *)cur->ns->href, (const char *)remove))
            cur->ns=nullptr;
        for (xmlNodePtr child = xmlFirstElementChild(cur); child!=nullptr; child=xmlNextElementSibling(child))
            removeNamespace(child, remove);
        for (xmlAttrPtr att = cur->properties; att!=nullptr; att=att->next)
        {
            if (!remove)
                att->ns=nullptr;
            else if (att->ns && att->ns->href && streq((const char *)att->ns->href, (const char *)remove))
                att->ns=nullptr;
        }
    }
    void processNamespaces()
    {
        const xmlChar *soap = nullptr;
        const xmlChar *n = nullptr;

        xmlNodePtr cur = checkGetSoapNamespace(xmlDocGetRootElement(m_xmlDoc), "Envelope", soap);
        cur = checkGetSoapNamespace(cur, "Body", soap);
        if (soap)
            xmlXPathRegisterNs(m_xpathContext, (const xmlChar *)"soap", soap);
        if (cur->ns && cur->ns->href)
            n = cur->ns->href;
        if (n)
            xmlXPathRegisterNs(m_xpathContext, (const xmlChar *)"n", n);
        if (removeDocNamespaces)
            removeNamespace(xmlDocGetRootElement(m_xmlDoc), nullptr);
    }
    bool setContextDocument(xmlDocPtr doc, xmlNodePtr node)
    {
        WriteLockBlock rblock(m_rwlock);

        m_xmlDoc = doc;
        m_xpathContext = xmlXPathNewContext(m_xmlDoc);
        if(m_xpathContext == nullptr)
        {
            ERRLOG("XpathProcessor:setContextDocument: Error: Unable to create new XMLLib XPath context");
            return false;
        }

        //relative paths need something to be relative to
        if (node)
            m_xpathContext->node = node;
        xmlXPathRegisterVariableLookup(m_xpathContext, variableLookupFunc, this);
        processNamespaces();
        return true;
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
                for (int i = 0; nodes!=nullptr && i < nodes->nodeNr; i++)
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

    double evaluateAsNumber(xmlXPathObjectPtr evaluatedXpathObj, const char* xpath)
    {
        if (!evaluatedXpathObj)
            throw MakeStringException(XPATHERR_InvalidInput,"XpathProcessor:evaluateAsNumber: Error: Could not evaluate XPATH '%s'", xpath);
        double ret = xmlXPathCastToNumber(evaluatedXpathObj);
        xmlXPathFreeObject(evaluatedXpathObj);
        return ret;
    }

    virtual xmlXPathObjectPtr evaluate(xmlXPathCompExprPtr compiled, const char *xpath)
    {
        xmlXPathObjectPtr evaluatedXpathObj = nullptr;
        if (compiled)
        {
            ReadLockBlock rlock(m_rwlock);
            if ( m_xpathContext)
            {
                evaluatedXpathObj = xmlXPathCompiledEval(compiled, m_xpathContext);
            }
            else
            {
                throw MakeStringException(XPATHERR_InvalidState,"XpathProcessor:evaluate: Error: Could not evaluate XPATH '%s'", xpath);
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

class XPathNodeSetIterator : public CInterfaceOf<IXpathContextIterator>
{
public:
    CLibXpathContext *context;
    xmlNodeSetPtr list;
    unsigned pos = 0;

public:
    XPathNodeSetIterator(CLibXpathContext *xpctx, xmlNodeSetPtr nodeset) : context(xpctx), list(nodeset)
    {
        context->pushLocation();
        context->m_xpathContext->contextSize = xmlXPathNodeSetGetLength(list);
    }

    virtual ~XPathNodeSetIterator()
    {
        context->popLocation();
        if (list)
            xmlXPathFreeNodeSet(list);
    }

    bool update(int newpos)
    {
        pos = newpos;
        if (!isValid())
            return false;
        xmlNodePtr node = xmlXPathNodeSetItem(list, pos);;
        context->m_xpathContext->node = node;
        if ((node->type != XML_NAMESPACE_DECL && node->doc != nullptr))
            context->m_xpathContext->doc = node->doc;
        context->m_xpathContext->proximityPosition = pos + 1;
        return true;
    }
    virtual bool first()
    {
        if (xmlXPathNodeSetGetLength(list)==0)
            return false;
        return update(0);
    }

    virtual bool next()
    {
        return update(pos+1);
    }
    virtual bool isValid()
    {
        return (pos < xmlXPathNodeSetGetLength(list));
    }
    virtual IXpathContext  & query()
    {
        return *context;
    }
};

IXpathContextIterator *CLibXpathContext::evaluateAsNodeSet(xmlXPathObjectPtr evaluated, const char* xpath)
{
    if (!evaluated)
    {
        throw MakeStringException(XPATHERR_InvalidInput, "XpathProcessor:evaluateAsNodeSet: Error: Could not evaluate XPATH '%s'", xpath);
    }

    if (XPATH_NODESET != evaluated->type)
    {
        xmlXPathFreeObject(evaluated);
        throw MakeStringException(XPATHERR_UnexpectedInput, "XpathProcessor:evaluateAsNodeSet: Error: Could not evaluate XPATH '%s' as NodeSet", xpath);
    }


    xmlNodeSetPtr ns = evaluated->nodesetval;
    evaluated->nodesetval = nullptr;
    xmlXPathFreeObject(evaluated);

    return new XPathNodeSetIterator(this, ns);
}

static xmlXPathObjectPtr variableLookupFunc(void *data, const xmlChar *name, const xmlChar *ns_uri)
{
    CLibXpathContext *ctxt = (CLibXpathContext *) data;
    if (!ctxt)
        return nullptr;
    return ctxt->getVariableObject((const char *)name, (const char *)ns_uri, nullptr);
}

extern ICompiledXpath* compileXpath(const char * xpath)
{
    return new CLibCompiledXpath(xpath);
}

extern IXpathContext* getXpathContext(const char * xmldoc, bool strictParameterDeclaration, bool removeDocNamespaces)
{
    return new CLibXpathContext(xmldoc, strictParameterDeclaration, removeDocNamespaces);
}
