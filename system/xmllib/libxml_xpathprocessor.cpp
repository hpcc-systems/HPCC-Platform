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
#ifndef _WIN32
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
#else
        UNIMPLEMENTED;
#endif
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

class CLibXpathContext : public CInterfaceOf<IXpathContext>, implements IVariableSubstitutionHelper
{
public:
    XPathInputMap provided;
    xmlDocPtr m_xmlDoc = nullptr;
    xmlXPathContextPtr m_xpathContext = nullptr;
    ReadWriteLock m_rwlock;
    XPathScopeVector scopes;
    Owned<CLibXpathContext> primaryContext; //if set will lookup variables from primaryContext (secondary context is generally target not source)

    bool strictParameterDeclaration = true;
    bool removeDocNamespaces = false;
    bool ownedDoc = false;

    //saved state
    XPathContextStateVector saved;

public:
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IXpathContext>)

    CLibXpathContext(const char * xmldoc, bool _strictParameterDeclaration, bool removeDocNs) : strictParameterDeclaration(_strictParameterDeclaration), removeDocNamespaces(removeDocNs)
    {
        xmlKeepBlanksDefault(0);
        beginScope("/");
        setXmlDoc(xmldoc);
        registerExslt();
    }

    CLibXpathContext(CLibXpathContext *_primary, xmlDocPtr doc, xmlNodePtr node, bool _strictParameterDeclaration) : strictParameterDeclaration(_strictParameterDeclaration)
    {
        xmlKeepBlanksDefault(0);
        primaryContext.set(_primary);
        beginScope("/");
        setContextDocument(doc, node);
        registerExslt();
    }

    ~CLibXpathContext()
    {
        for (XPathInputMap::iterator it=provided.begin(); it!=provided.end(); ++it)
            it->second->Release();
        xmlXPathFreeContext(m_xpathContext);
        if (ownedDoc)
            xmlFreeDoc(m_xmlDoc);
    }

    void registerExslt()
    {
        exsltDateXpathCtxtRegister(m_xpathContext, (xmlChar*)"date");
        exsltMathXpathCtxtRegister(m_xpathContext, (xmlChar*)"math");
        exsltSetsXpathCtxtRegister(m_xpathContext, (xmlChar*)"set");
        exsltStrXpathCtxtRegister(m_xpathContext, (xmlChar*)"str");
    }
    void pushLocation() override
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

    void popLocation() override
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

    bool findVariable(const char *name, StringBuffer &s) override
    {
        xmlXPathObjectPtr obj = findVariable(name, nullptr, nullptr);
        return append(s, obj);
    }

    xmlXPathObjectPtr findVariable(const char *name, const char *ns_uri, CLibXpathScope *scope)
    {
        xmlXPathObjectPtr obj = nullptr;
        if (primaryContext)
        {
            //could always return from here, but may find a use for secondary variables, they currently can't be defined from esdl script, so can't hurt there
            obj = primaryContext->findVariable(name, ns_uri, scope);
            if (obj)
                return obj;
        }
        const char *fullname = name;
        StringBuffer s;
        if (!isEmptyString(ns_uri))
            fullname = s.append(ns_uri).append(':').append(name).str();

        ReadLockBlock wblock(m_rwlock);
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

    void setLocation(xmlNodePtr node)
    {
        WriteLockBlock wblock(m_rwlock);
        m_xpathContext->doc = node->doc;
        m_xpathContext->node = node;
        m_xpathContext->contextSize = 0;
        m_xpathContext->proximityPosition = 0;
    }

    bool setLocation(xmlXPathObjectPtr obj, bool required, const char *xpath)
    {
        if (!obj)
        {
            if (required)
                throw MakeStringException(XPATHERR_InvalidInput,"XpathContext:setLocation: Error: Syntax error XPATH '%s'", xpath);
            return false;
        }
        if (obj->type!=XPATH_NODESET || !obj->nodesetval || obj->nodesetval->nodeNr==0)
        {
            xmlXPathFreeObject(obj);
            if (required)
                throw MakeStringException(XPATHERR_EvaluationFailed,"XpathContext:setLocation: Error: Could not evaluate XPATH '%s'", xpath);
            return false;
        }
        if (obj->nodesetval->nodeNr>1)
        {
            xmlXPathFreeObject(obj);
            if (required)
                throw MakeStringException(XPATHERR_EvaluationFailed,"XpathContext:setLocation: Error: ambiguous XPATH '%s'", xpath);
            return false;
        }
        xmlNodePtr location = obj->nodesetval->nodeTab[0];
        xmlXPathFreeObject(obj);
        setLocation(location);
        return true;
    }

    virtual bool setLocation(ICompiledXpath * compiledXpath, bool required) override
    {
        if (!compiledXpath)
            return false;

        CLibCompiledXpath * ccXpath = static_cast<CLibCompiledXpath *>(compiledXpath);
        if (!ccXpath)
            throw MakeStringException(XPATHERR_InvalidState,"XpathProcessor:setLocation: Error: BAD compiled XPATH");

        xmlXPathObjectPtr obj = evaluate(ccXpath->getCompiledXPathExpression(), compiledXpath->getXpath());
        return setLocation(obj, required, compiledXpath->getXpath());
    }

    virtual bool setLocation(const char *s, bool required) override
    {
        StringBuffer xpath;
        replaceVariables(xpath, s, false, this, "{$", "}");

        xmlXPathObjectPtr obj = evaluate(xpath);
        return setLocation(obj, required, xpath.str());
    }

    virtual void addElementToLocation(const char *name) override
    {
        if (!validateXMLTag(name))
            throw MakeStringException(XPATHERR_InvalidState,"XpathContext:addElementToLocation: invalid name '%s'", name);
        xmlNodePtr location = m_xpathContext->node;
        if (!location)
            return;
        location = xmlNewChild(location, nullptr, (const xmlChar *) name, nullptr);
        if (!location)
            throw MakeStringException(XPATHERR_InvalidState,"XpathContext:addElementToLocation: failed to add element '%s'", name);
        setLocation(location);
    }

    virtual void addXmlContent(const char *xml) override
    {
        xmlNodePtr location = m_xpathContext->node;
        xmlParserCtxtPtr parserCtx = xmlCreateDocParserCtxt((const xmlChar *)xml);
        if (!parserCtx)
            throw MakeStringException(-1, "CLibXpathContext:addXmlContent: Unable to parse xml");
        parserCtx->node = location;
        xmlParseDocument(parserCtx);
        int wellFormed = parserCtx->wellFormed;
        xmlFreeDoc(parserCtx->myDoc); //dummy document
        xmlFreeParserCtxt(parserCtx);
        if (!wellFormed)
            throw MakeStringException(-1, "XpathContext:addXmlContent: Unable to parse %s XML content", xml);

    }
    virtual bool ensureLocation(const char *xpath, bool required) override
    {
        if (isEmptyString(xpath))
            return true;
        if (strstr(xpath, "//")) //maybe in the future, scripting error rather than not found, required==false has no effect
            throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureLocation: '//' not currently allowed '%s'", xpath);

        xmlNodePtr current = m_xpathContext->node;
        xmlNodePtr node = current;
        if (*xpath=='/')
        {
            xpath++;
            if (strncmp(xpath, "esdl_script_context/", 20)!=0)
                throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureLocation: incorect first absolute path node '%s'", xpath);
            xpath+=20;
            node = xmlDocGetRootElement(m_xmlDoc);
        }

        StringArray xpathnodes;
        xpathnodes.appendList(xpath, "/");
        ForEachItemIn(i, xpathnodes)
        {
            const char *finger = xpathnodes.item(i);
            if (isEmptyString(finger)) //shouldn't happen, scripting error rather than not found, required==false has no effect
                throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureLocation: '//' not allowed '%s'", xpath);
            if (*finger=='@')
                throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureLocation: attribute cannot be selected '%s'", xpath);

            xmlXPathObjectPtr obj = xmlXPathNodeEval(node, (const xmlChar*) finger, m_xpathContext);
            xmlXPathSetContextNode(current, m_xpathContext); //restore

            if (!obj)
                throw MakeStringException(XPATHERR_InvalidInput,"XpathContext:ensureLocation: invalid xpath '%s' at node '%s'", xpath, finger);

            if (obj->type!=xmlXPathObjectType::XPATH_NODESET || !obj->nodesetval || obj->nodesetval->nodeNr==0)
            {
                if (*finger=='.' || strpbrk(finger, "[()]*") || strstr(finger, "::")) //complex nodes must already exist
                {
                    if (required)
                        throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureLocation: xpath node not found '%s' in xpath '%s'", finger, xpath);
                    return false;
                }
                node = xmlNewChild(node, nullptr, (const xmlChar *) finger, nullptr);
                if (!node)
                {
                    if (required)
                        throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureLocation: error creating node '%s' in xpath '%s'", finger, xpath);
                    return false;
                }
            }
            else
            {
                xmlNodePtr *nodes = obj->nodesetval->nodeTab;
                if (obj->nodesetval->nodeNr>1)
                {
                    if (required)
                        throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureLocation: ambiguous xpath '%s' at node '%s'", xpath, finger);
                    return false;
                }
                node = *nodes;
            }
            xmlXPathFreeObject(obj);
        }
        setLocation(node);
        return true;
    }

    virtual void setLocationNamespace(const char *prefix, const char * uri, bool current) override
    {
        xmlNodePtr node = m_xpathContext->node;
        if (!node)
            return;
        xmlNsPtr ns = nullptr;
        if (isEmptyString(uri))
            ns = xmlSearchNs(node->doc, node, (const xmlChar *) prefix);
        else
            ns = xmlNewNs(node, (const xmlChar *) uri, (const xmlChar *) prefix);
        if (current && ns)
            xmlSetNs(node, ns);
    }

    enum class ensureValueMode { set, add, appendto };
    virtual void ensureValue(const char *xpath, const char *value, ensureValueMode action, bool required)
    {
        if (isEmptyString(xpath))
            return;
        if (strstr(xpath, "//")) //maybe in the future, scripting error rather than not found, required==false has no effect
            throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureValue: '//' not currently allowed for set operations '%s'", xpath);

        xmlNodePtr current = m_xpathContext->node;
        if (!current || *xpath=='/')
        {
            current = xmlDocGetRootElement(m_xmlDoc); //valuable, but very dangerous
            if (*xpath=='/')
                xpath++;
        }
        xmlNodePtr node = current;

        StringArray xpathnodes;
        xpathnodes.appendList(xpath, "/");
        ForEachItemIn(i, xpathnodes)
        {
            const char *finger = xpathnodes.item(i);
            if (isEmptyString(finger)) //shouldn't happen, scripting error rather than not found, required==false has no effect
                throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureValue: empty xpath node unexpected '%s'", xpath);
            bool last = (i == (xpathnodes.ordinality()-1));
            if (*finger=='@')
            {
                if (!last) //scripting error
                    throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureValue: invalid xpath attribute node '%s' in xpath '%s'", finger, xpath);
                if (action == ensureValueMode::appendto)
                {
                    const char *existing = (const char *) xmlGetProp(node, (const xmlChar *) finger+1);
                    if (!isEmptyString(existing))
                    {
                        StringBuffer appendValue(existing);
                        appendValue.append(value);
                        xmlSetProp(node, (const xmlChar *) finger+1, (const xmlChar *) appendValue.str());
                        return;
                    }
                }
                xmlSetProp(node, (const xmlChar *) finger+1, (const xmlChar *) value);
                return;
            }
            xmlXPathObjectPtr obj = xmlXPathNodeEval(node, (const xmlChar*) finger, m_xpathContext);
            xmlXPathSetContextNode(current, m_xpathContext);

            if (!obj)
                throw MakeStringException(XPATHERR_InvalidInput,"XpathContext:ensureValue: invalid xpath '%s' at node '%s'", xpath, finger);

            if (obj->type!=xmlXPathObjectType::XPATH_NODESET || !obj->nodesetval || obj->nodesetval->nodeNr==0)
            {
                if (*finger=='.' || strpbrk(finger, "[()]*") || strstr(finger, "::")) //complex nodes must already exist
                {
                    if (required)
                        throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureValue: xpath node not found '%s' in xpath '%s'", finger, xpath);
                    return;
                }
                const char *nsx = strchr(finger, ':');
                if (nsx)
                {
                    StringAttr prefix(finger, nsx-finger);
                    finger = nsx+1;
                    //translate the xpath xmlns prefix into the one defined in the content xml (exception if not found)
                    const char *uri = queryNamespace(prefix.str());
                    if (isEmptyString(uri))
                        throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureValue: xmlns prefix '%s' not defined in script used in xpath '%s'", prefix.str(), xpath);
                    xmlNsPtr ns = xmlSearchNsByHref(m_xmlDoc, node, (const xmlChar *) uri);
                    if (!ns)
                        throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureValue: namespace uri '%s' (referenced as xmlns prefix '%s') not declared in xml ", uri, prefix.str());
                    node = xmlNewChild(node, ns, (const xmlChar *) finger, last ? (const xmlChar *) value : nullptr);
                }
                else
                {
                    node = xmlNewChild(node, nullptr, (const xmlChar *) finger, last ? (const xmlChar *) value : nullptr);
                    //default libxml2 behavior here would be very confusing for scripts.  After adding a tag with no prefix you would need a tag on the xpath to read it.
                    //Instead when there is no prefix, create a node with no namespace. To match the parent default namespace, define an xpath prefix for it.
                    //You can always add the namespace you want by declaring it both in the xml (<es:namespace>) and the script (xmlns:prefix on the root node of the script) and then using it on the xpath.
                    xmlSetNs(node, nullptr);
                }

                if (!node)
                {
                    if (required)
                        throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureValue: error creating node '%s' in xpath '%s'", finger, xpath);
                    return;
                }
            }
            else
            {
                xmlNodePtr *nodes = obj->nodesetval->nodeTab;
                if (!last)
                {
                    if (obj->nodesetval->nodeNr>1)
                    {
                        if (required)
                            throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureValue: ambiguous xpath '%s' at node '%s'", xpath, finger);
                        return;
                    }
                    node = *nodes;
                }
                else
                {
                    if (obj->nodesetval->nodeNr>1 && action != ensureValueMode::add)
                    {
                        if (required)
                            throw MakeStringException(XPATHERR_InvalidState,"XpathContext:ensureValue: ambiguous xpath '%s'", xpath);
                        return;
                    }

                    xmlNodePtr parent = node;
                    node = *nodes;
                    switch (action)
                    {
                        case ensureValueMode::add:
                        {
                            StringBuffer name((const char *)node->name);
                            xmlNewChild(parent, node->ns, (const xmlChar *) name.str(), (const xmlChar *) value);
                            break;
                        }
                        case ensureValueMode::appendto:
                        {
                            xmlChar *existing = xmlNodeGetContent(node);
                            StringBuffer appendValue((const char *)existing);
                            xmlNodeSetContent(node, (const xmlChar *) appendValue.append(value).str());
                            xmlFree(existing);
                            break;
                        }
                        case ensureValueMode::set:
                        {
                            xmlNodeSetContent(node, (const xmlChar *) value);
                            break;
                        }
                    }
                }
            }
            xmlXPathFreeObject(obj);
        }
    }

    virtual void ensureSetValue(const char *xpath, const char *value, bool required) override
    {
        ensureValue(xpath, value, ensureValueMode::set, required);
    }
    virtual void ensureAddValue(const char *xpath, const char *value, bool reqired) override
    {
        ensureValue(xpath, value, ensureValueMode::add, reqired);
    }
    virtual void ensureAppendToValue(const char *xpath, const char *value, bool required) override
    {
        ensureValue(xpath, value, ensureValueMode::appendto, required);
    }

    virtual void rename(const char *xpath, const char *name, bool all) override
    {
        if (isEmptyString(xpath) || isEmptyString(name))
            return;
        xmlXPathObjectPtr obj = xmlXPathEval((const xmlChar*) xpath, m_xpathContext);
        if (!obj || obj->type!=xmlXPathObjectType::XPATH_NODESET || obj->nodesetval->nodeNr==0)
            return;
        if (obj->nodesetval->nodeNr>1 && !all)
        {
            xmlXPathFreeObject(obj);
            throw MakeStringException(XPATHERR_InvalidState,"XpathContext:rename: ambiguous xpath '%s' to rename all set 'all'", xpath);
        }
        xmlNodePtr *nodes = obj->nodesetval->nodeTab;
        for (int i = 0; i < obj->nodesetval->nodeNr; i++)
            xmlNodeSetName(nodes[i], (const xmlChar *)name);
        xmlXPathFreeObject(obj);
    }

    virtual void remove(const char *xpath, bool all) override
    {
        if (isEmptyString(xpath))
            return;
        xmlXPathObjectPtr obj = xmlXPathEval((const xmlChar*) xpath, m_xpathContext);
        if (!obj || obj->type!=xmlXPathObjectType::XPATH_NODESET || !obj->nodesetval || obj->nodesetval->nodeNr==0)
            return;
        if (obj->nodesetval->nodeNr>1 && !all)
        {
            xmlXPathFreeObject(obj);
            throw MakeStringException(XPATHERR_InvalidState,"XpathContext:remove: ambiguous xpath '%s' to remove all set 'all'", xpath);
        }
        xmlNodePtr *nodes = obj->nodesetval->nodeTab;
        for (int i = obj->nodesetval->nodeNr - 1; i >= 0; i--)
        {
            xmlUnlinkNode(nodes[i]);
            xmlFreeNode(nodes[i]);
            nodes[i]=nullptr;
        }
        xmlXPathFreeObject(obj);
    }

    virtual void copyFromPrimaryContext(ICompiledXpath *select, const char *newname) override
    {
        if (!primaryContext)
            return;
        xmlNodePtr current = m_xpathContext->node;
        if (!current || !select)
            return;
        CLibCompiledXpath * ccXpath = static_cast<CLibCompiledXpath *>(select);
        xmlXPathObjectPtr obj = primaryContext->evaluate(ccXpath->getCompiledXPathExpression(), ccXpath->getXpath());
        if (!obj)
            throw MakeStringException(XPATHERR_InvalidState, "XpathContext:copyof xpath syntax error '%s'", ccXpath->getXpath());
        if (obj->type!=xmlXPathObjectType::XPATH_NODESET || !obj->nodesetval || obj->nodesetval->nodeNr==0)
        {
            xmlXPathFreeObject(obj);
            return;
        }
        xmlNodePtr *nodes = obj->nodesetval->nodeTab;
        for (int i = 0; i < obj->nodesetval->nodeNr; i++)
        {
            xmlNodePtr copy = xmlCopyNode(nodes[i], 1);
            if (!copy)
                continue;
            if (!isEmptyString(newname))
                xmlNodeSetName(copy, (const xmlChar *)newname);
            xmlAddChild(current, copy);
        }
        xmlXPathFreeObject(obj);
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
            CLibCompiledXpath * ccXpath = static_cast<CLibCompiledXpath *>(compiled);
            xmlXPathObjectPtr obj = evaluate(ccXpath->getCompiledXPathExpression(), ccXpath->getXpath());
            if (!obj)
                throw MakeStringException(-1, "addEvaluateVariable xpath error %s", ccXpath->getXpath());
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

    virtual StringBuffer &toXml(const char *xpath, StringBuffer & xml) override
    {
        xmlXPathObjectPtr obj = evaluate(xpath);
        if (!obj || !obj->type==XPATH_NODESET || !obj->nodesetval || !obj->nodesetval->nodeTab || obj->nodesetval->nodeNr!=1)
            return xml;
        xmlNodePtr node = obj->nodesetval->nodeTab[0];
        if (!node)
            return xml;

        xmlOutputBufferPtr xmlOut = xmlAllocOutputBuffer(nullptr);
        xmlNodeDumpOutput(xmlOut, node->doc, node, 0, 1, nullptr);
        xmlOutputBufferFlush(xmlOut);
        xmlBufPtr buf = (xmlOut->conv != nullptr) ? xmlOut->conv : xmlOut->buffer;
        if (xmlBufUse(buf))
            xml.append(xmlBufUse(buf), (const char *)xmlBufContent(buf));
        xmlOutputBufferClose(xmlOut);
        xmlXPathFreeObject(obj);
        return xml;
    }

    virtual bool evaluateAsString(const char * xpath, StringBuffer & evaluated) override
    {
        if (!xpath || !*xpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsString: Error: Could not evaluate empty XPATH");
        return evaluateAsString(evaluate(xpath), evaluated, xpath);
    }

    virtual bool evaluateAsBoolean(ICompiledXpath * compiledXpath) override
    {
        CLibCompiledXpath * ccXpath = static_cast<CLibCompiledXpath *>(compiledXpath);
        if (!ccXpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsBoolean: Error: Missing compiled XPATH");
        return evaluateAsBoolean(evaluate(ccXpath->getCompiledXPathExpression(), compiledXpath->getXpath()), compiledXpath->getXpath());
    }

    virtual const char * evaluateAsString(ICompiledXpath * compiledXpath, StringBuffer & evaluated) override
    {
        CLibCompiledXpath * ccXpath = static_cast<CLibCompiledXpath *>(compiledXpath);
        if (!ccXpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsString: Error: Missing compiled XPATH");
        return evaluateAsString(evaluate(ccXpath->getCompiledXPathExpression(), compiledXpath->getXpath()), evaluated, compiledXpath->getXpath());
    }

    virtual double evaluateAsNumber(ICompiledXpath * compiledXpath) override
    {
        CLibCompiledXpath * ccXpath = static_cast<CLibCompiledXpath *>(compiledXpath);
        if (!ccXpath)
            throw MakeStringException(XPATHERR_MissingInput,"XpathProcessor:evaluateAsNumber: Error: Missing compiled XPATH");
        return evaluateAsNumber(evaluate(ccXpath->getCompiledXPathExpression(), compiledXpath->getXpath()), compiledXpath->getXpath());
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
        ownedDoc = true;
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

        xmlNodePtr start = (m_xpathContext && m_xpathContext->node) ? m_xpathContext->node : xmlDocGetRootElement(m_xmlDoc);
        xmlNodePtr cur = checkGetSoapNamespace(start, "Envelope", soap);
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

    bool append(StringBuffer &s, xmlXPathObjectPtr obj)
    {
        if (!obj)
            return false;
        xmlChar *value = xmlXPathCastToString(obj);
        if (!value || !*value)
            return false;
        s.append((const char *) value);
        xmlFree(value);
        return true;
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
        if (isEmptyString(xpath))
            return nullptr;
        if (!m_xpathContext)
            throw MakeStringException(XPATHERR_InvalidState,"XpathProcessor:evaluate: Error: Could not evaluate XPATH '%s'; ensure xmldoc has been set", xpath);
        ReadLockBlock rlock(m_rwlock);
        return xmlXPathEval((const xmlChar *)xpath, m_xpathContext);
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
        xmlNodePtr node = xmlXPathNodeSetItem(list, pos);
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

void addChildFromPtree(xmlNodePtr parent, IPropertyTree &tree, const char *name)
{
    if (isEmptyString(name))
        name = tree.queryName();
    if (*name==':') //invalid xml but maybe valid ptree?
        name++;
    StringAttr prefix;
    const char *colon = strchr(name, ':');
    if (colon)
    {
        prefix.set(name, colon-name);
        name = colon+1;
    }
    xmlNodePtr node = xmlNewChild(parent, nullptr, (const xmlChar *)name, (const xmlChar *)tree.queryProp(nullptr));
    if (!node)
        return;
    xmlNsPtr ns = nullptr;
    if (tree.getAttributeCount())
    {
        Owned<IAttributeIterator> atts = tree.getAttributes();
        ForEach(*atts)
        {
            const char *attname = atts->queryName()+1;
            if (strncmp(attname, "xmlns", 5)!=0)
                xmlNewProp(node, (const xmlChar *)attname, (const xmlChar *)atts->queryValue());
            else
            {
                attname+=5;
                if (*attname==':')
                    attname++;
                if (*attname==0)
                    attname=nullptr; //default namespace needs null prefix
                xmlNewNs(node, (const xmlChar *)atts->queryValue(), (const xmlChar *)attname);
            }
        }
    }
    if (prefix.isEmpty())
        node->ns = nullptr;
    else
    {
        xmlNsPtr ns = xmlSearchNs(node->doc, node, (const xmlChar *)prefix.str());
        if (!ns) //invalid but let's repair it
            ns = xmlNewNs(node, (const xmlChar *)"urn:hpcc:unknown", (const xmlChar *)prefix.str());
        node->ns = ns;
    }
    Owned<IPropertyTreeIterator> children = tree.getElements("*");
    ForEach(*children)
        addChildFromPtree(node, children->query(), nullptr);
}

IPropertyTree *createPtreeFromXmlNode(xmlNodePtr node);

void copyXmlNode(IPropertyTree *tree, xmlNodePtr node)
{
    for(xmlAttrPtr att=node->properties;att!=nullptr; att=att->next)
    {
        VStringBuffer name("@%s", (const char *)att->name);
        xmlChar *value = xmlGetProp(node, att->name);
        tree->setProp(name, (const char *)value);
        xmlFree(value);
    }

    for (xmlNodePtr child=xmlFirstElementChild(node); child!=nullptr; child=xmlNextElementSibling(child))
        tree->addPropTree((const char *)child->name, createPtreeFromXmlNode(child));
}

IPropertyTree *createPtreeFromXmlNode(xmlNodePtr node)
{
    if (!node)
        return nullptr;
    Owned<IPropertyTree> tree = createPTree((const char *)node->name);
    copyXmlNode(tree, node);
    return tree.getClear();
}

static const char *queryAttrValue(xmlNodePtr node, const char *name)
{
    if (!node)
        return nullptr;
    xmlAttrPtr att = xmlHasProp(node, (const xmlChar *)name);
    if (!att)
        return nullptr;
    if (att->type != XML_ATTRIBUTE_NODE || att->children==nullptr || (att->children->type != XML_TEXT_NODE && att->children->type != XML_CDATA_SECTION_NODE))
        return nullptr;
    return (const char *) att->children->content;
}

class CEsdlScriptContext : public CInterfaceOf<IEsdlScriptContext>
{
private:
    void *espCtx = nullptr;
    xmlDocPtr doc = nullptr;
    xmlNodePtr root = nullptr;
    xmlXPathContextPtr xpathCtx = nullptr;

public:
    CEsdlScriptContext(void *ctx) : espCtx(ctx)
    {
        doc =   xmlParseDoc((const xmlChar *) "<esdl_script_context/>");
        xpathCtx = xmlXPathNewContext(doc);
        if(xpathCtx == nullptr)
            throw MakeStringException(-1, "CEsdlScriptContext: Unable to create new xPath context");

        root = xmlDocGetRootElement(doc);
        xpathCtx->node = root;
    }

private:
    ~CEsdlScriptContext()
    {
        xmlXPathFreeContext(xpathCtx);
        xmlFreeDoc(doc);
    }
    virtual void *queryEspContext() override
    {
        return espCtx;
    }
    inline void sanityCheckSectionName(const char *name)
    {
        //sanity check the name, not a full validation
        if (strpbrk(name, "/[]()*?"))
            throw MakeStringException(-1, "CEsdlScriptContext:removeSection invalid section name %s", name);
    }
    xmlNodePtr getSectionNode(const char *name, const char *xpath="*[1]")
    {
        sanityCheckSectionName(name);
        StringBuffer fullXpath(name);
        if (!isEmptyString(xpath))
            fullXpath.append('/').append(xpath);
        xmlNodePtr sect = nullptr;
        xmlXPathObjectPtr eval = xmlXPathEval((const xmlChar *) fullXpath.str(), xpathCtx);
        if (eval && XPATH_NODESET == eval->type && eval->nodesetval && eval->nodesetval->nodeNr && eval->nodesetval->nodeTab!=nullptr)
            sect = eval->nodesetval->nodeTab[0];
        xmlXPathFreeObject(eval);
        return sect;
    }

    void addXpathCtxConfigInputs(IXpathContext *tgtXpathCtx)
    {
        xmlXPathObjectPtr eval = xmlXPathEval((const xmlChar *) "config/*/Transform/Param", xpathCtx);
        if (!eval)
            return;
        if (XPATH_NODESET==eval->type && eval->nodesetval!=nullptr && eval->nodesetval->nodeNr!=0 && eval->nodesetval->nodeTab!=nullptr)
        {
            for (int i=0; i<eval->nodesetval->nodeNr; i++)
            {
                xmlNodePtr node = eval->nodesetval->nodeTab[i];
                if (node==nullptr)
                  continue;
                const char *name = queryAttrValue(node, "name");
                if (xmlHasProp(node,(const xmlChar *) "select"))
                  tgtXpathCtx->addInputXpath(name, queryAttrValue(node,"select"));
                else
                  tgtXpathCtx->addInputValue(name, queryAttrValue(node,"value"));
            }
        }
        xmlXPathFreeObject(eval);
    }
    const char *getXPathString(const char *xpath, StringBuffer &s) const override
    {
        xmlXPathObjectPtr eval = xmlXPathEval((const xmlChar *) xpath, xpathCtx);
        if (eval)
        {
            xmlChar *v = xmlXPathCastToString(eval);
            xmlXPathFreeObject(eval);
            if (v)
            {
                if (*v)
                    s.append((const char *)v);
                xmlFree(v);
                return s;
            }
        }
        return nullptr;
    }
    __int64 getXPathInt64(const char *xpath, __int64 dft=0) const override
    {
        __int64 ret = dft;
        xmlXPathObjectPtr eval = xmlXPathEval((const xmlChar *) xpath, xpathCtx);
        if (eval)
        {
            xmlChar *val = xmlXPathCastToString(eval);
            xmlXPathFreeObject(eval);
            if (val)
            {
                if (*val)
                    ret = _atoi64((const char *)val);
                xmlFree(val);
            }
        }
        return ret;
    }
    bool getXPathBool(const char *xpath, bool dft=false) const override
    {
        bool ret = dft;
        xmlXPathObjectPtr eval = xmlXPathEval((const xmlChar *) xpath, xpathCtx);
        if (eval)
        {
            xmlChar *val = xmlXPathCastToString(eval);
            xmlXPathFreeObject(eval);
            if (val)
            {
                if (*val)
                    ret = strToBool((const char *)val);
                xmlFree(val);
            }
        }
        return ret;
    }
    xmlNodePtr getSection(const char *name)
    {
        return getSectionNode(name, nullptr);
    }

    xmlNodePtr ensureCopiedSection(const char *target, const char *source, bool replace)
    {
        if (isEmptyString(source)||isEmptyString(target))
            return nullptr;
        sanityCheckSectionName(target);
        sanityCheckSectionName(source);

        xmlNodePtr targetNode = getSectionNode(target, nullptr);
        if (targetNode && !replace)
            return targetNode;

        xmlNodePtr sourceNode = getSectionNode(source, nullptr);
        if (!sourceNode)
            return nullptr;

        xmlNodePtr copyNode = xmlCopyNode(sourceNode, 1);
        xmlNodeSetName(copyNode, (const xmlChar *) target);

        if (targetNode)
        {
            xmlNodePtr oldNode = xmlReplaceNode(targetNode, copyNode);
            xmlFreeNode(oldNode);
            return copyNode;
        }

        xmlAddChild(root, copyNode);
        return copyNode;
    }

    xmlNodePtr ensureSection(const char *name)
    {
        sanityCheckSectionName(name);
        xmlNodePtr sect = getSection(name);
        if (sect)
            return sect;
        return xmlNewChild(root, nullptr, (const xmlChar *) name, nullptr);
    }
    void removeSection(const char *name)
    {
        sanityCheckSectionName(name);
        xmlXPathObjectPtr eval = xmlXPathEval((const xmlChar *) name, xpathCtx);
        if (!eval)
            return;
        //should be just one node, but if there are more, clean them up anyway
        xmlNodeSetPtr ns = eval->nodesetval;
        if (XPATH_NODESET==eval->type && !xmlXPathNodeSetIsEmpty(ns))
        {
            //shouldn't matter here, but reverse order ensures children are deleted before parents
            for(int i=xmlXPathNodeSetGetLength(ns)-1; i>=0; i--)
            {
                xmlNodePtr node = xmlXPathNodeSetItem(ns, i);
                if (!node)
                    continue;
                xmlUnlinkNode(node);
                xmlFreeNode(node);
                ns->nodeTab[i]=nullptr;
            }
        }
        xmlXPathFreeObject(eval);
    }
    xmlNodePtr replaceSection(const char *name)
    {
        removeSection(name);
        return xmlNewChild(root, nullptr, (const xmlChar *) name, nullptr);
    }
    virtual void setContent(const char *section, const char *xml) override
    {
        xmlNodePtr sect = replaceSection(section);
        if (xml==nullptr) //means delete content
            return;

        xmlParserCtxtPtr parserCtx = xmlCreateDocParserCtxt((const unsigned char *)xml);
        if (!parserCtx)
            throw MakeStringException(-1, "CEsdlScriptContext:setContent: Unable to init parse of %s XML content", section);
        parserCtx->node = sect;
        xmlParseDocument(parserCtx);
        int wellFormed = parserCtx->wellFormed;
        xmlFreeDoc(parserCtx->myDoc); //dummy document
        xmlFreeParserCtxt(parserCtx);
        if (!wellFormed)
           throw MakeStringException(-1, "CEsdlScriptContext:setContent xml string: Unable to parse %s XML content", section);
    }
    virtual void appendContent(const char *section, const char *name, const char *xml) override
    {
        if (xml==nullptr)
            return;

        xmlNodePtr sect = ensureSection(section);
        xmlNodePtr sectNode = getSectionNode(section, name);

        xmlDocPtr doc = xmlParseDoc((const xmlChar *)xml);
        if (!doc)
            return;
        xmlNodePtr content = xmlDocGetRootElement(doc);
        if (!content)
        {
            xmlFreeDoc(doc);
            return;
        }
        if (!sectNode)
        {
            xmlUnlinkNode(content);
            if (!isEmptyString(name))
                xmlNodeSetName(content, (const xmlChar *) name);
            xmlAddChild(sect, content);
            xmlFree(doc);
            return;
        }
        xmlAttrPtr prop = content->properties;
        while (prop != nullptr)
        {
            xmlChar *value = xmlGetProp(content, prop->name);
            xmlSetProp(sectNode, prop->name, value);
            xmlFree(value);
            prop = prop->next;
        }
        for (xmlNodePtr node = xmlFirstElementChild(content); node != nullptr; node = xmlFirstElementChild(content))
        {
            xmlUnlinkNode(node);
            xmlAddChild(sectNode, node);
        }
        xmlFreeDoc(doc);
    }
    virtual void setContent(const char *section, IPropertyTree *tree) override
    {
        xmlNodePtr sect = replaceSection(section);
        if (tree==nullptr) //means delete content
            return;
        addChildFromPtree(sect, *tree, tree->queryName());
    }

    virtual void setAttribute(const char *section, const char *name, const char *value) override
    {
        xmlNodePtr sect = ensureSection(section);
        if (value)
            xmlSetProp(sect, (const xmlChar *)name, (const xmlChar *)value);
        else
            xmlUnsetProp(sect, (const xmlChar *)name);
    }
    virtual const char *queryAttribute(const char *section, const char *name) override
    {
        xmlNodePtr sect = getSection(section);
        if (!sect)
            return nullptr;
        return queryAttrValue(sect, name);
    }
    virtual const char *getAttribute(const char *section, const char *name, StringBuffer &s) override
    {
        xmlNodePtr sect = getSection(section);
        if (!sect)
            return nullptr;
        xmlChar *val = xmlGetProp(sect, (const xmlChar *)name);
        if (!val)
            return nullptr;
        s.append((const char *)val);
        xmlFree(val);
        return s;
    }
    virtual bool tokenize(const char *str, const char *delimeters, StringBuffer &resultPath)
    {
        xmlNodePtr sect = ensureSection("temporaries");
        if (!sect)
            return false;

        StringBuffer unique("T");
        appendGloballyUniqueId(unique);
        xmlNodePtr container = xmlNewChild(sect, nullptr, (const xmlChar *) unique.str(), nullptr);
        if (!container)
            return false;

        resultPath.set("/esdl_script_context/temporaries/").append(unique.str()).append("/token");

        StringArray tkns;
        tkns.appendList(str, delimeters);
        ForEachItemIn(i, tkns)
        {
            const char *tkn = tkns.item(i);
            xmlNewChild(container, nullptr, (const xmlChar *) "token", (const xmlChar *) tkn);
        }
        return true;
    }


    virtual void toXML(StringBuffer &xml, const char *section, bool includeParentNode=false) override
    {
        xmlNodePtr sect = root;
        if (!isEmptyString(section))
        {
            sect = getSectionNode(section, includeParentNode ? nullptr : "*[1]");
            if (!sect)
                return;
        }

        xmlOutputBufferPtr xmlOut = xmlAllocOutputBuffer(nullptr);
        xmlNodeDumpOutput(xmlOut, sect->doc, sect, 0, 1, nullptr);
        xmlOutputBufferFlush(xmlOut);
        xmlBufPtr buf = (xmlOut->conv != nullptr) ? xmlOut->conv : xmlOut->buffer;
        if (xmlBufUse(buf))
            xml.append(xmlBufUse(buf), (const char *)xmlBufContent(buf));
        xmlOutputBufferClose(xmlOut);
    }
    virtual IPropertyTree *createPTreeFromSection(const char *section) override
    {
        if (isEmptyString(section))
            return nullptr;
        xmlNodePtr sect = getSectionNode(section, nullptr);
        if (!sect)
            return nullptr;
        return createPtreeFromXmlNode(sect);
    }

    virtual void toXML(StringBuffer &xml) override
    {
        toXML(xml, nullptr, true);
    }
    IXpathContext* createXpathContext(IXpathContext *primaryContext, const char *section, bool strictParameterDeclaration) override
    {
        xmlNodePtr sect = nullptr;
        if (!isEmptyString(section))
        {
            sect = getSectionNode(section);
            if (!sect)
                throw MakeStringException(-1, "CEsdlScriptContext:createXpathContext: section not found %s", section);
        }
        CLibXpathContext *xpathContext = new CLibXpathContext(static_cast<CLibXpathContext *>(primaryContext), doc, sect, strictParameterDeclaration);
        StringBuffer val;
        xpathContext->addVariable("method", getAttribute("esdl", "method", val));
        xpathContext->addVariable("service", getAttribute("esdl", "service", val.clear()));
        xpathContext->addVariable("request", getAttribute("esdl", "request", val.clear()));

        //external parameters need <es:param> statements to make them accessible (in strict mode)
        addXpathCtxConfigInputs(xpathContext);
        if (!strictParameterDeclaration)
            xpathContext->declareRemainingInputs();

        return xpathContext;
    }
    IXpathContext *getCopiedSectionXpathContext(IXpathContext *primaryContext, const char *tgtSection, const char *srcSection, bool strictParameterDeclaration) override
    {
        ensureCopiedSection(tgtSection, srcSection, false);
        return createXpathContext(primaryContext, tgtSection, strictParameterDeclaration);
    }
    virtual void cleanupBetweenScripts() override
    {
        removeSection("temporaries");
    }
};

IEsdlScriptContext *createEsdlScriptContext(void * espCtx)
{
    return new CEsdlScriptContext(espCtx);
}

extern IXpathContext* getXpathContext(const char * xmldoc, bool strictParameterDeclaration, bool removeDocNamespaces)
{
    return new CLibXpathContext(xmldoc, strictParameterDeclaration, removeDocNamespaces);
}
