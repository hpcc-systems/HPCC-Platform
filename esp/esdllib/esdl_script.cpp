/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "espcontext.hpp"
#include "esdl_script.hpp"
#include "wsexcept.hpp"
#include "httpclient.hpp"
#include "dllserver.hpp"
#include "thorplugin.hpp"
#include "eclrtl.hpp"
#include "rtlformat.hpp"
#include "jsecrets.hpp"
#include "esdl_script.hpp"
#include "txsummary.hpp"

#include <fxpp/FragmentedXmlPullParser.hpp>
using namespace xpp;

class EsdlScriptMaskerScope
{
private:
    IEsdlScriptContext* scriptContext = nullptr;
public:
    EsdlScriptMaskerScope(IEsdlScriptContext* _scriptContext)
        : scriptContext(_scriptContext)
    {
        if (scriptContext)
            scriptContext->pushMaskerScope();
        else
            throw makeStringException(-1, "EsdlScriptMaskerScope failure - missing context");
    }
    ~EsdlScriptMaskerScope()
    {
        scriptContext->popMaskerScope();
    }
};

class EsdlScriptTraceOptionsScope
{
private:
    IEsdlScriptContext* scriptContext = nullptr;
public:
    EsdlScriptTraceOptionsScope(IEsdlScriptContext* _scriptContext)
        : scriptContext(_scriptContext)
    {
        if (scriptContext)
            scriptContext->pushTraceOptionsScope();
        else
            throw makeStringException(-1, "EsdlScriptTraceOptionsScope failure - missing context");
    }
    ~EsdlScriptTraceOptionsScope()
    {
         scriptContext->popTraceOptionsScope();
    }
};

class CEsdlScriptContext : public CInterfaceOf<IEsdlScriptContext>
{
public: // ISectionalXmlDocModel
    virtual IXpathContext* createXpathContext(IXpathContext* parent, const char* section, bool strictParameterDeclaration) override { return docModel->createXpathContext(parent, section, strictParameterDeclaration); }
    virtual IXpathContext* getCopiedSectionXpathContext(IXpathContext* parent, const char* tgtSection, const char* srcSection, bool strictParameterDeclaration) override { return docModel->getCopiedSectionXpathContext(parent, tgtSection, srcSection, strictParameterDeclaration); }
    virtual void setContent(const char* section, const char* xml) override { docModel->setContent(section, xml); }
    virtual void appendContent(const char* section, const char* name, const char* xml) override { docModel->appendContent(section, name, xml); }
    virtual void setContent(const char* section, IPropertyTree* tree) override { docModel->setContent(section, tree); }
    virtual bool tokenize(const char* str, const char* delimiters, StringBuffer& resultPath) override { return docModel->tokenize(str, delimiters, resultPath); }
    virtual void setAttribute(const char* section, const char* name, const char* value) override { docModel->setAttribute(section, name, value); }
    virtual const char* queryAttribute(const char* section, const char* name) override { return docModel->queryAttribute(section, name); }
    virtual const char* getAttribute(const char* section, const char* name, StringBuffer& s) override { return docModel->getAttribute(section, name, s); }
    virtual const char* getXPathString(const char* xpath, StringBuffer& s) const override { return docModel->getXPathString(xpath, s); }
    virtual __int64 getXPathInt64(const char* xpath, __int64 dft = false) const override { return docModel->getXPathInt64(xpath, dft); }
    virtual bool getXPathBool(const char* xpath, bool dft = false) const override { return docModel->getXPathBool(xpath, dft); }
    virtual void toXML(StringBuffer& xml, const char* section, bool includeParentNode = false) override { docModel->toXML(xml, section, includeParentNode); }
    virtual void toXML(StringBuffer& xml) override { docModel->toXML(xml); }
    virtual IPropertyTree* createPTreeFromSection(const char* section) override { return docModel->createPTreeFromSection(section); }
    virtual void cleanupTemporaries() override { docModel->cleanupTemporaries(); }
public: // IEsdlScriptContext
    virtual IEspContext* queryEspContext() const override { return espCtx; }
    virtual IEsdlFunctionRegister* queryFunctionRegister() const override { return functionRegister; }
    virtual void setTraceToStdout(bool val) override
    {
        if (val != traceToStdout)
        {
            traceToStdout = val;
            if (val)
            {
                if (!consoleSink)
                    consoleSink.setown(new CConsoleTraceMsgSink());
                tracer->setSink(consoleSink);
            }
            else
                tracer->setSink(jlogSink);
        }
    }
    virtual bool getTraceToStdout() const override { return traceToStdout; }
    virtual void setTestMode(bool val) override { testMode = val; }
    virtual bool getTestMode() const override { return testMode; }
    virtual ITracer& tracerRef() const override { return *tracer; }
    virtual bool enableMasking(const char* domainId, uint8_t version) override
    {
        if (!maskingEngine)
        {
            tracerRef().uwarnlog("enable masking request not completed - no masking engine");
            return false;
        }
        if (!maskerScopes.empty() && maskerScopes.back())
        {
            domainId = (isEmptyString(domainId) ? maskingEngine->inspector().queryDefaultDomain() : domainId);
            version = (0 == version ? maskingEngine->inspector().queryDefaultVersion() : version);
            IDataMaskingProfileContext* masker = maskerScopes.back();
            if (masker->inspector().acceptsDomain(domainId) && masker->queryVersion() == version)
                return true;
            tracerRef().uwarnlog("enable masking request using %s:%hhu not completed - already enabled using %s:%hhu", domainId, version, masker->queryDomain(), masker->queryVersion());
            return false;
        }
        Owned<IDataMaskingProfileContext> masker(maskingEngine->getContext(domainId, version, tracer));
        if (!masker)
        {
            tracerRef().uwarnlog("enable masking request not completed - no context for '%s:%hhu'", domainId, version);
            return false;
        }
        if (maskerScopes.empty())
            maskerScopes.emplace_back(masker.getLink());
        else
            maskerScopes.back().setown(masker.getClear());
        return true;
    }

    virtual bool maskingEnabled() const override
    {
        return (!maskerScopes.empty() && maskerScopes.back() != nullptr);
    }

    virtual IDataMaskingProfileContext* getMasker() const override
    {
        if (!maskerScopes.empty())
            return maskerScopes.back().getLink();
        return nullptr;
    }

    virtual void setTraceOptions(bool enabled, bool locked) override
    {
        if (traceOptionsScopes.empty())
            traceOptionsScopes.emplace_back(enabled, locked);
        else
        {
            TraceOptionsScope& entry = traceOptionsScopes.back();
            if (!entry.second)
            {
                entry.first = enabled;
                entry.second = locked;
            }
        }
    }

    virtual bool isTraceEnabled() const override
    {
        return (!traceOptionsScopes.empty() && traceOptionsScopes.back().first);
    }

    virtual bool isTraceLocked() const override
    {
        return (!traceOptionsScopes.empty() && traceOptionsScopes.back().second);
    }

protected:
    virtual void pushMaskerScope() override
    {
        if (maskerScopes.empty())
            maskerScopes.emplace_back(nullptr);
        else if (maskerScopes.back())
            maskerScopes.emplace_back(maskerScopes.back()->clone());
        else
            maskerScopes.emplace_back(nullptr);
    }

    virtual void popMaskerScope() override
    {
        if (maskerScopes.empty())
            throw makeStringException(-1, "popMaskerScope failed - unbalanced push");
        maskerScopes.pop_back();
    }

    virtual void pushTraceOptionsScope() override
    {
        if (traceOptionsScopes.empty())
            throw makeStringException(-1, "pushTraceOptionsScope failed - empty stack");
        TraceOptionsScope& parent = traceOptionsScopes.back();
        traceOptionsScopes.emplace_back(parent.first, parent.second);
    }

    virtual void popTraceOptionsScope() override
    {
        if (maskerScopes.empty())
            throw makeStringException(-1, "popTraceOptionsScope failed - unbalanced push");
        traceOptionsScopes.pop_back();
    }

private:
    Owned<IEspContext>            espCtx;
    IEsdlFunctionRegister*        functionRegister = nullptr;
    Owned<ISectionalXmlDocModel>  docModel;
    bool                          traceToStdout = false;
    bool                          testMode = false;
    Owned<CModularTracer>         tracer;
    Owned<IModularTraceMsgSink>   jlogSink;
    Owned<IModularTraceMsgSink>   consoleSink;
    Owned<IDataMaskingEngine>     maskingEngine;
    using MaskerScope = Owned<IDataMaskingProfileContext>;
    using MaskerScopeStack = std::list<MaskerScope>;
    using TraceOptionsScope = std::pair<bool, bool>;
    using TraceOptionsScopeStack = std::list<TraceOptionsScope>;
    MaskerScopeStack              maskerScopes;
    TraceOptionsScopeStack        traceOptionsScopes;
public:
    CEsdlScriptContext(IEspContext* _espCtx, IEsdlFunctionRegister* _functionRegister, IDataMaskingEngine* _engine)
        : functionRegister(_functionRegister)
    {
        if (_espCtx)
            espCtx.set(_espCtx);
        else
            espCtx.setown(createEspContext(nullptr));
        docModel.setown(createSectionalXmlDocModel(this));
        tracer.setown(new CModularTracer());
        jlogSink.setown(tracer->getSink());
        maskingEngine.setown(_engine);
        traceOptionsScopes.emplace_back(true, false);
    }
    ~CEsdlScriptContext()
    {
    }
};

class OptionalCriticalBlock
{
    CriticalSection *crit = nullptr;
public:
    inline OptionalCriticalBlock(CriticalSection *c) : crit(c)
    {
        if (crit)
            crit->enter();
    }
    inline ~OptionalCriticalBlock()
    {
        if (crit)
            crit->leave();
    }
};

/**
 * @brief Abstract interface providing external access to trace messaging in the context of an
 *        ESDL script operation.
 *
 * An implementation encapsulates warning, error, and exception reporting logic for operations.
 * Operations and their helpers can focus on why a message is being generated without having to
 * know how messages are expected to be generated.
 */
interface IEsdlOperationTraceMessenger
{
    /**
     * @brief Record a warning message in trace output.
     *
     * Implementations should not throw an exception.
     *
     * @param code
     * @param msg
     */
    virtual void recordWarning(int code, const char* msg) const = 0;

    /**
     * @brief Record an error in trace output and possibly throw an exception.
     *
     * An implementation may choose to throw exceptions on any or all errors.
     *
     * @param code
     * @param msg
     */
    virtual void recordError(int code, const char* msg) const = 0;

    /**
     * @brief Record an error in trace output and always throw an exception.
     *
     * An implementation is expected to throw exceptions every time.
     *
     * @param code
     * @param msg
     */
    virtual void recordException(int code, const char* msg) const = 0;

};

/**
 * @brief Encapsulation of an operation property that may be given as either an XPath or literal.
 *
 * Properties may be required or optional. Failure setting a required property records an error
 * in a manner consistent with the containing operation's configuration.
 *
 * Required and optional values can be set in one of three ways:
 *
 * 1. Pull parser start tag and literal property name. The XPath property name is derived from the
 *    literal name. The start tag is the source for both the XPath and the literal values.
 * 2. Pull parser start tag, XPath property name, and literal property name. The start tag is the
 *    source for both the XPath and the literal values.
 * 3. An XPath property value and a literal property value.
 *
 * An XPath value, if not empty, takes precedence over any literal value.
 */
class XPathLiteralUnion
{
private:
    Owned<ICompiledXpath> xpath;
    StringAttr            literal;

public:
    inline bool isEmpty() const
    {
        return (!xpath && literal.isEmpty());
    }

    inline bool isXPath() const
    {
        return xpath;
    }

    inline bool isLiteral() const
    {
        return !literal.isEmpty();
    }

    const char* configValue() const
    {
        if (xpath)
            return xpath->getXpath();
        return literal;
    }

    inline bool setOptional(StartTag& stag, const char* literalName)
    {
        VStringBuffer xpathName("xpath_%s", literalName);
        return setOptional(stag, xpathName, literalName);
    }

    inline bool setOptional(StartTag& stag, const char* xpathName, const char* literalName)
    {
        return set(stag.getValue(xpathName), stag.getValue(literalName));
    }

    inline bool setOptional(const char* _xpath, const char* _literal)
    {
        return set(_xpath, _literal);
    }

    inline bool setRequired(StartTag& stag, const char* literalName, IEsdlOperationTraceMessenger& messenger)
    {
        VStringBuffer xpathName("xpath_%s", literalName);
        return setRequired(stag, xpathName, literalName, messenger);
    }

    inline bool setRequired(StartTag& stag, const char* xpathName, const char* literalName, IEsdlOperationTraceMessenger& messenger)
    {
        const char* _xpath = stag.getValue(xpathName);
        if (!set(_xpath, (isEmptyString(_xpath) ? stag.getValue(literalName) : nullptr)))
        {
            VStringBuffer msg("missing both '%s' and '%s' (one is required)", xpathName, literalName);
            messenger.recordError(ESDL_SCRIPT_MissingOperationAttr, msg);
            return false;
        }
        return true;
    }

    inline bool setRequired(const char* _xpath, const char* _literal, IEsdlOperationTraceMessenger& messenger)
    {
        if (!set(_xpath, _literal))
        {
            messenger.recordError(ESDL_SCRIPT_MissingOperationAttr, "missing xpath and literal values (one is required)");
            return false;
        }
        return true;
    }

    const char* get(StringBuffer& buffer, IXpathContext& context) const
    {
        if (xpath)
            return context.evaluateAsString(xpath, buffer);
        buffer.set(literal);
        return buffer;
    }

protected:
    bool set(const char* _xpath, const char* _literal)
    {
        xpath.clear();
        literal.clear();
        if (!isEmptyString(_xpath))
            xpath.setown(compileXpath(_xpath));
        else if (!isEmptyString(_literal))
            literal.set(_literal);
        else
            return false;
        return true;
    }
};

IEsdlTransformOperation *createEsdlTransformOperation(IXmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, IEsdlOperationTraceMessenger& messenger, IEsdlFunctionRegister *functionRegister, bool canCreateFunctions);
void createEsdlTransformOperations(IArrayOf<IEsdlTransformOperation> &operations, IXmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, IEsdlOperationTraceMessenger& messenger, IEsdlFunctionRegister *functionRegister);
typedef void (*esdlOperationsFactory_t)(IArrayOf<IEsdlTransformOperation> &operations, IXmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, IEsdlOperationTraceMessenger& messenger, IEsdlFunctionRegister *functionRegister);

bool getStartTagValueBool(StartTag &stag, const char *name, bool defaultValue)
{
    if (isEmptyString(name))
        return defaultValue;
    const char *value = stag.getValue(name);
    if (isEmptyString(value))
        return defaultValue;
    return strToBool(value);
}


inline void buildEsdlOperationMessage(StringBuffer &s, int code, const char *op, const char *msg, const char *traceName)
{
    s.set("ESDL Script: ");
    if (!isEmptyString(traceName))
        s.append(" '").append(traceName).append("' ");
    if (!isEmptyString(op))
        s.append(" ").append(op).append(" ");
    s.append(msg);
}

inline void esdlOperationWarning(int code, const char *op, const char *msg, const char *traceName)
{
    StringBuffer s;
    buildEsdlOperationMessage(s, code, op, msg, traceName);
    IWARNLOG("%s", s.str());
}

inline void esdlOperationError(int code, const char *op, const char *msg, const char *traceName, bool exception)
{
    StringBuffer s;
    buildEsdlOperationMessage(s, code, op, msg, traceName);
    IERRLOG("%s", s.str());
    if(exception)
        throw MakeStringException(code, "%s", s.str());
}

inline void esdlOperationError(int code, const char *op, const char *msg, bool exception)
{
    esdlOperationError(code, op, msg, "", exception);
}

static inline const char *checkSkipOpPrefix(const char *op, const StringBuffer &prefix)
{
    if (prefix.length())
    {
        if (!hasPrefix(op, prefix, true))
        {
            DBGLOG(1,"Unrecognized script operation: %s", op);
            return nullptr;
        }
        return (op + prefix.length());
    }
    return op;
}

static inline StringBuffer &makeOperationTagName(StringBuffer &s, const StringBuffer &prefix, const char *op)
{
    return s.append(prefix).append(op);
}

class CEsdlTransformOperationBase : public CInterfaceOf<IEsdlTransformOperation>, public IEsdlOperationTraceMessenger
{
protected:
    StringAttr m_tagname;
    StringAttr m_traceName;
    bool m_ignoreCodingErrors = false; //ideally used only for debugging

public:
    CEsdlTransformOperationBase(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix)
    {
        m_tagname.set(stag.getLocalName());
        m_traceName.set(stag.getValue("trace"));
        m_ignoreCodingErrors = getStartTagValueBool(stag, "optional", false);
    }
    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        return exec(nullptr, nullptr, scriptContext, targetContext, sourceContext);
    }
    virtual IInterface *prepareForAsync(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        return nullptr;
    }
    virtual void recordWarning(int code, const char* msg) const override
    {
        esdlOperationWarning(code, m_tagname, msg, m_traceName);
    }
    virtual void recordError(int code, const char* msg) const override
    {
        esdlOperationError(code, m_tagname, msg, m_traceName, !m_ignoreCodingErrors);
    }
    virtual void recordException(int code, const char* msg) const override
    {
        esdlOperationError(code, m_tagname, msg, m_traceName, true);
    }
};

class CEsdlTransformOperationWithChildren : public CEsdlTransformOperationBase
{
protected:
    IArrayOf<IEsdlTransformOperation> m_children;
    bool m_withVariables = false;
    XpathVariableScopeType m_childScopeType = XpathVariableScopeType::simple;

public:
    CEsdlTransformOperationWithChildren(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, bool withVariables, IEsdlFunctionRegister *functionRegister, esdlOperationsFactory_t factory) : CEsdlTransformOperationBase(xpp, stag, prefix), m_withVariables(withVariables)
    {
        //load children
        if (factory)
            factory(m_children, xpp, prefix, withVariables, *this, functionRegister);
        else
            createEsdlTransformOperations(m_children, xpp, prefix, withVariables, *this, functionRegister);
    }

    virtual ~CEsdlTransformOperationWithChildren(){}

    virtual bool processChildren(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext)
    {
        if (!m_children.length())
            return false;

        Owned<CXpathContextScope> scope = m_withVariables ? new CXpathContextScope(sourceContext, m_tagname, m_childScopeType, nullptr) : nullptr;
        bool ret = false;
        ForEachItemIn(i, m_children)
        {
            if (m_children.item(i).process(scriptContext, targetContext, sourceContext))
                ret = true;
        }
        return ret;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        ForEachItemIn(i, m_children)
            m_children.item(i).toDBGLog();
    #endif
    }
};

class CEsdlTransformOperationWithoutChildren : public CEsdlTransformOperationBase
{
public:
    CEsdlTransformOperationWithoutChildren(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationBase(xpp, stag, prefix)
    {
        if (xpp.skipSubTreeEx())
            recordError(ESDL_SCRIPT_Error, "should not have child tags");
    }

    virtual ~CEsdlTransformOperationWithoutChildren(){}
};

class CEsdlTransformOperationVariable : public CEsdlTransformOperationWithChildren
{
protected:
    StringAttr m_name;
    Owned<ICompiledXpath> m_select;

public:
    CEsdlTransformOperationVariable(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
        if (m_traceName.isEmpty())
            m_traceName.set(stag.getValue("name"));
        m_name.set(stag.getValue("name"));
        if (m_name.isEmpty())
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without name");
        const char *select = stag.getValue("select");
        if (!isEmptyString(select))
            m_select.setown(compileXpath(select));
    }

    virtual ~CEsdlTransformOperationVariable()
    {
    }

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        if (m_select)
            return sourceContext->addCompiledVariable(m_name, m_select);
        else if (m_children.length())
        {
            VStringBuffer xpath("/esdl_script_context/temporaries/%s", m_name.str());
            CXpathContextLocation location(targetContext);

            targetContext->ensureLocation(xpath, true);
            processChildren(scriptContext, targetContext, sourceContext); //bulid nodeset
            sourceContext->addXpathVariable(m_name, xpath);
            return false;
        }
        sourceContext->addVariable(m_name, "");
        return false;
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s with select(%s)", m_name.str(), m_tagname.str(), m_select.get() ? m_select->getXpath() : "");
#endif
    }
};

class CEsdlTransformOperationHttpContentXml : public CEsdlTransformOperationWithChildren
{

public:
    CEsdlTransformOperationHttpContentXml(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
    }

    virtual ~CEsdlTransformOperationHttpContentXml(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        CXpathContextLocation location(targetContext);
        targetContext->addElementToLocation("content");
        return processChildren(scriptContext, targetContext, sourceContext);
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

interface IEsdlTransformOperationHttpHeader : public IInterface
{
    virtual bool processHeader(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext, IProperties *headers) = 0;
};

static Owned<ILoadedDllEntry> mysqlPluginDll;
static Owned<IEmbedContext> mysqlplugin;

IEmbedContext &ensureMysqlEmbed()
{
    if (!mysqlplugin)
    {
        mysqlPluginDll.setown(createDllEntry("mysqlembed", false, NULL, false));
        if (!mysqlPluginDll)
            throw makeStringException(0, "Failed to load mysqlembed plugin");
        GetEmbedContextFunction pf = (GetEmbedContextFunction) mysqlPluginDll->getEntry("getEmbedContextDynamic");
        if (!pf)
            throw makeStringException(0, "Failed to load mysqlembed plugin");
        mysqlplugin.setown(pf());
    }
    return *mysqlplugin;
}

class CEsdlTransformOperationMySqlBindParmeter : public CEsdlTransformOperationWithoutChildren
{
protected:
    StringAttr m_name;
    StringAttr m_mysql_type;
    Owned<ICompiledXpath> m_value;
    bool m_bitfield = false;

public:
    IMPLEMENT_IINTERFACE_USING(CEsdlTransformOperationWithoutChildren)

    CEsdlTransformOperationMySqlBindParmeter(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        m_name.set(stag.getValue("name"));
        if (m_name.isEmpty())
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without name or xpath_name");

        const char *value = stag.getValue("value");
        if (isEmptyString(value))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without value");
        m_value.setown(compileXpath(value));

        //optional, conversions normally work well, ONLY WHEN NEEDED we may need to have special handling for mysql types
        m_mysql_type.set(stag.getValue("type"));
        if (m_mysql_type.length() && 0==strnicmp(m_mysql_type.str(), "BIT", 3))
            m_bitfield = true;
    }

    virtual ~CEsdlTransformOperationMySqlBindParmeter(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        return false;
    }

    void bindParameter(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext, IEmbedFunctionContext *functionContext)
    {
        if (!functionContext)
            return;
        StringBuffer value;
        if (m_value)
            sourceContext->evaluateAsString(m_value, value);
        if (value.isEmpty())
            functionContext->bindUTF8Param(m_name, 0, "");
        else
        {
            if (m_bitfield)
                functionContext->bindSignedParam(m_name, atoi64(value.str()));
            else
                functionContext->bindUTF8Param(m_name, rtlUtf8Length(value.length(), value), value);
        }
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG ("> %s (%s, value(%s)) >>>>>>>>>>", m_tagname.str(), m_name.str(), m_value ? m_value->getXpath() : "");
    #endif
    }
};

static inline void buildMissingMySqlParameterMessage(StringBuffer &msg, const char *name)
{
    msg .append(msg.isEmpty() ? "without " : ", ").append(name);
}

static inline void addExceptionsToXpathContext(IXpathContext *targetContext, IMultiException *me)
{
    if (!targetContext || !me)
        return;
    StringBuffer xml;
    me->serialize(xml);
    CXpathContextLocation content_location(targetContext);
    targetContext->ensureSetValue("@status", "error", true);
    targetContext->addXmlContent(xml.str());
}

static inline void addExceptionsToXpathContext(IXpathContext *targetContext, IException *E)
{
    if (!targetContext || !E)
        return;
    Owned<IMultiException> me = makeMultiException("ESDLScript");
    me->append(*LINK(E));
    addExceptionsToXpathContext(targetContext, me);
}

class CEsdlTransformOperationMySqlCall : public CEsdlTransformOperationBase
{
protected:
    StringAttr m_name;

    Owned<ICompiledXpath> m_vaultName;
    Owned<ICompiledXpath> m_secretName;
    Owned<ICompiledXpath> m_section;
    Owned<ICompiledXpath> m_resultsetTag;
    Owned<ICompiledXpath> m_server;
    Owned<ICompiledXpath> m_user;
    Owned<ICompiledXpath> m_password;
    Owned<ICompiledXpath> m_database;

    StringArray m_mysqlOptionNames;
    IArrayOf<ICompiledXpath> m_mysqlOptionXpaths;

    StringBuffer m_sql;

    Owned<ICompiledXpath> m_select;

    IArrayOf<CEsdlTransformOperationMySqlBindParmeter> m_parameters;

public:
    CEsdlTransformOperationMySqlCall(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationBase(xpp, stag, prefix)
    {
        ensureMysqlEmbed();

        m_name.set(stag.getValue("name"));
        if (m_traceName.isEmpty())
            m_traceName.set(m_name.str());

        //select is optional, with select, a mysql call behaves like a for-each, binding and executing each iteration of the selected content
        //without select, it executes once in the current context
        m_select.setown(compileOptionalXpath(stag.getValue("select")));

        m_vaultName.setown(compileOptionalXpath(stag.getValue("vault")));
        m_secretName.setown(compileOptionalXpath(stag.getValue("secret")));
        m_section.setown(compileOptionalXpath(stag.getValue("section")));
        m_resultsetTag.setown(compileOptionalXpath(stag.getValue("resultset-tag")));

        m_server.setown(compileOptionalXpath(stag.getValue("server")));
        m_user.setown(compileOptionalXpath(stag.getValue("user")));
        m_database.setown(compileOptionalXpath(stag.getValue("database")));

        /*
         * Use of secrets for database connection information is the recommended best practice. Use
         * of an inline plaintext password is the worst option, with use of an easily decrypted
         * inline password only slightly less risky.
         *
         * An encrypted password literal takes precedence over a password XPath. A decrypted value
         * is converted into an XPath, allowing its use in place of a password XPath.
         */
        const char* encryptedPassword = stag.getValue("encrypted-password");
        if (!isEmptyString(encryptedPassword))
        {
            try
            {
                StringBuffer tmp;
                decrypt(tmp, encryptedPassword);
                m_password.setown(compileOptionalXpath(VStringBuffer("'%s'", tmp.str())));
            }
            catch (IException* e)
            {
                recordError(ESDL_SCRIPT_InvalidOperationAttr, "invalid encrypted-password");
                e->Release();
            }
            catch (...)
            {
                recordError(ESDL_SCRIPT_InvalidOperationAttr, "invalid encrypted-password");
            }
        }
        else
            m_password.setown(compileOptionalXpath(stag.getValue("password")));

        //script can set any MYSQL options using an attribute with the same name as the option enum, for example
        //    MYSQL_SET_CHARSET_NAME="'latin1'" or MYSQL_SET_CHARSET_NAME="$charset"
        //
        int attCount = stag.getLength();
        for (int i=0; i<attCount; i++)
        {
            const char *attName = stag.getLocalName(i);
            if (attName && hasPrefix(attName, "MYSQL_", false))
            {
                Owned<ICompiledXpath> attXpath = compileOptionalXpath(stag.getValue(i));
                if (attXpath)
                {
                    m_mysqlOptionNames.append(attName);
                    m_mysqlOptionXpaths.append(*attXpath.getClear());
                }
            }
        }

        int type = 0;
        while((type = xpp.next()) != XmlPullParser::END_TAG)
        {
            if (XmlPullParser::START_TAG == type)
            {
                    StartTag stag;
                    xpp.readStartTag(stag);
                    const char *op = stag.getLocalName();
                    if (isEmptyString(op))
                        recordError(ESDL_SCRIPT_Error, "unknown error");
                    if (streq(op, "bind"))
                        m_parameters.append(*new CEsdlTransformOperationMySqlBindParmeter(xpp, stag, prefix));
                    else if (streq(op, "sql"))
                        readFullContent(xpp, m_sql);
                    else
                        xpp.skipSubTreeEx();
            }
        }

        if (!m_section)
            m_section.setown(compileXpath("'temporaries'"));
        StringBuffer errmsg;
        if (m_name.isEmpty())
            buildMissingMySqlParameterMessage(errmsg, "name");
        if (!m_server)
            buildMissingMySqlParameterMessage(errmsg, "server");
        if (!m_user)
            buildMissingMySqlParameterMessage(errmsg, "user");
        if (!m_database)
            buildMissingMySqlParameterMessage(errmsg, "database");
        if (m_sql.isEmpty())
            buildMissingMySqlParameterMessage(errmsg, "sql");
        if (errmsg.length())
            recordError(ESDL_SCRIPT_MissingOperationAttr, errmsg);
    }

    virtual ~CEsdlTransformOperationMySqlCall()
    {
    }

    void bindParameters(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext, IEmbedFunctionContext *functionContext)
    {
        if (!m_parameters.length())
            return;
        CXpathContextLocation location(targetContext);
        ForEachItemIn(i, m_parameters)
            m_parameters.item(i).bindParameter(scriptContext, targetContext, sourceContext, functionContext);
    }

    void missingMySqlOptionError(const char *name, bool required)
    {
        if (required)
        {
            StringBuffer msg("empty or missing ");
            recordException(ESDL_SCRIPT_MissingOperationAttr, msg.append(name));
        }
    }
    IPropertyTree *getSecretInfo(IXpathContext * sourceContext)
    {
        //leaving flexibility for the secret to be configured multiple ways
        //  the most secure option in my opinion is to at least have the server, name, and password all in the secret
        //  with the server included the credentials can't be hijacked and sent somewhere else for capture.
        //
        if (!m_secretName)
            return nullptr;
        StringBuffer name;
        sourceContext->evaluateAsString(m_secretName, name);
        if (name.isEmpty())
        {
            missingMySqlOptionError(name, true);
            return nullptr;
        }
        StringBuffer vault;
        if (m_vaultName)
            sourceContext->evaluateAsString(m_vaultName, vault);
        if (vault.isEmpty())
            return getSecret("esp", name);
        return getVaultSecret("esp", vault, name);
    }
    void appendOption(StringBuffer &options, const char *name, const char *value, bool required)
    {
        if (isEmptyString(value))
        {
            missingMySqlOptionError(name, required);
            return;
        }
        if (options.length())
            options.append(',');
        options.append(name).append('=').append(value);

    }
    void appendOption(StringBuffer &options, const char *name, IXpathContext * sourceContext, ICompiledXpath *cx, IPropertyTree *secret, bool required)
    {
        if (secret && secret->hasProp(name))
        {
            StringBuffer value;
            getSecretKeyValue(value, secret, name);
            appendOption(options, name, value, required);
            return;
        }

        if (!cx)
        {
            missingMySqlOptionError(name, required);
            return;
        }
        StringBuffer value;
        sourceContext->evaluateAsString(cx, value);
        if (!value.length())
        {
            missingMySqlOptionError(name, required);
            return;
        }
        if (options.length())
            options.append(',');
        options.append(name).append('=').append(value);
    }
    IEmbedFunctionContext *createFunctionContext(IXpathContext * sourceContext)
    {
        Owned<IPropertyTree> secret = getSecretInfo(sourceContext);
        StringBuffer options;
        appendOption(options, "server", sourceContext, m_server, secret, true);
        appendOption(options, "user", sourceContext, m_user, secret, true);
        appendOption(options, "database", sourceContext, m_database, secret, true);
        appendOption(options, "password", sourceContext, m_password, secret, true);

        aindex_t count = m_mysqlOptionNames.length();
        for (aindex_t i=0; i<count; i++)
            appendOption(options, m_mysqlOptionNames.item(i), sourceContext, &m_mysqlOptionXpaths.item(i), nullptr, true);

        Owned<IEmbedFunctionContext> fc = ensureMysqlEmbed().createFunctionContext(EFembed, options.str());
        fc->compileEmbeddedScript(m_sql.length(), m_sql);
        return fc.getClear();
    }

    void processCurrent(IEmbedFunctionContext *fc, IXmlWriter *writer, const char *tag, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext)
    {
        bindParameters(scriptContext, targetContext, sourceContext, fc);
        fc->callFunction();
        if (!isEmptyString(tag))
            writer->outputBeginNested(tag, true);
        fc->writeResult(nullptr, nullptr, nullptr, writer);
        if (!isEmptyString(tag))
            writer->outputEndNested(tag);
    }

    IXpathContextIterator *select(IXpathContext * xpathContext)
    {
        IXpathContextIterator *xpathset = nullptr;
        try
        {
            xpathset = xpathContext->evaluateAsNodeSet(m_select);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            recordError(code, msg);
        }
        catch (...)
        {
            VStringBuffer msg("unknown exception evaluating select '%s'", m_select.get() ? m_select->getXpath() : "undefined!");
            recordError(ESDL_SCRIPT_Error, msg);
        }
        return xpathset;
    }

    void getXpathStringValue(StringBuffer &s, IXpathContext * sourceContext, ICompiledXpath *cx, const char *defaultValue)
    {
        if (cx)
            sourceContext->evaluateAsString(cx, s);
        if (defaultValue && s.isEmpty())
            s.set(defaultValue);
    }
    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        //OperationMySqlCall is optimized to write directly into the document object
        //  In future we can create a different version that is optimized to work more asynchronously
        //  If we did, then inside a synchronous tag we might to have it optional which mode is used. For example efficiently streaming in data while
        //  an HTTP call is being made to an external service may still work best in the current mode
        OptionalCriticalBlock block(crit);

        StringBuffer section;
        getXpathStringValue(section, sourceContext, m_section, "temporaries");

        VStringBuffer xpath("/esdl_script_context/%s/%s", section.str(), m_name.str());
        CXpathContextLocation location(targetContext);
        targetContext->ensureLocation(xpath, true);

        Owned<IXpathContextIterator> selected;
        if (m_select)
        {
            selected.setown(select(sourceContext));
            if (!selected || !selected->first())
                return false;
        }

        try
        {
            Owned<IEmbedFunctionContext> fc = createFunctionContext(sourceContext);
            Owned<IXmlWriter> writer = targetContext->createXmlWriter();
            StringBuffer rstag;
            getXpathStringValue(rstag, sourceContext, m_resultsetTag, nullptr);
            if (!selected)
                processCurrent(fc, writer, rstag, scriptContext, targetContext, sourceContext);
            else
            {
                ForEach(*selected)
                    processCurrent(fc, writer, rstag, scriptContext, targetContext, &selected->query());
            }
        }
        catch(IMultiException *me)
        {
            addExceptionsToXpathContext(targetContext, me);
            me->Release();
        }
        catch(IException *E)
        {
            addExceptionsToXpathContext(targetContext, E);
            E->Release();
        }

        sourceContext->addXpathVariable(m_name, xpath);
        return true;
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s with name(%s) server(%s) database(%s)", m_name.str(), m_tagname.str(), m_name.str(), m_server->getXpath(), m_database->getXpath());
#endif
    }
};

class CEsdlTransformOperationHttpHeader : public CEsdlTransformOperationWithoutChildren, implements IEsdlTransformOperationHttpHeader
{
protected:
    XPathLiteralUnion m_name;
    Owned<ICompiledXpath> m_value;

public:
    IMPLEMENT_IINTERFACE_USING(CEsdlTransformOperationWithoutChildren)

    CEsdlTransformOperationHttpHeader(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        m_name.setRequired(stag, "name", *this);
        if (m_name.isEmpty())
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without name or xpath_name");

        const char *value = stag.getValue("value");
        if (isEmptyString(value))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without value");
        m_value.setown(compileXpath(value));
    }

    virtual ~CEsdlTransformOperationHttpHeader(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);
        return processHeader(scriptContext, targetContext, sourceContext, nullptr);
    }

    bool processHeader(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext, IProperties *headers) override
    {
        CXpathContextLocation location(targetContext);
        targetContext->addElementToLocation("header");
        StringBuffer name;
        m_name.get(name, *sourceContext);

        StringBuffer value;
        if (m_value)
            sourceContext->evaluateAsString(m_value, value);
        if (name.length() && value.length())
        {
            if (headers)
                headers->setProp(name, value);
            targetContext->ensureSetValue("@name", name, true);
            targetContext->ensureSetValue("@value", value, true);
        }
        return false;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG ("> %s (%s, value(%s)) >>>>>>>>>>", m_tagname.str(), m_name.configValue(), m_value ? m_value->getXpath() : "");
    #endif
    }
};

class OperationStateHttpPostXml : public CInterfaceOf<IInterface> //plain CInterface doesn't actually give us our opaque IInterface pointer
{
public:
    Owned<IProperties> headers = createProperties();
    StringBuffer url;
    StringBuffer content;
    unsigned testDelay = 0;
};


class CEsdlTransformOperationHttpPostXml : public CEsdlTransformOperationBase
{
protected:
    StringAttr m_name;
    StringAttr m_section;
    Owned<ICompiledXpath> m_url;
    IArrayOf<IEsdlTransformOperationHttpHeader> m_headers;
    Owned<IEsdlTransformOperation> m_content;
    Owned<ICompiledXpath> m_testDelay;

public:
    CEsdlTransformOperationHttpPostXml(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister) : CEsdlTransformOperationBase(xpp, stag, prefix)
    {
        m_name.set(stag.getValue("name"));
        if (m_traceName.isEmpty())
            m_traceName.set(m_name.str());
        m_section.set(stag.getValue("section"));
        if (m_section.isEmpty())
            m_section.set("temporaries");
        if (m_name.isEmpty())
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without name");
        const char *url = stag.getValue("url");
        if (isEmptyString(url))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without url");
        m_url.setown(compileXpath(url));
        const char *msTestDelayStr = stag.getValue("test-delay");
        if (!isEmptyString(msTestDelayStr))
            m_testDelay.setown(compileXpath(msTestDelayStr));

        int type = 0;
        while((type = xpp.next()) != XmlPullParser::END_TAG)
        {
            if (XmlPullParser::START_TAG == type)
            {
                StartTag stag;
                xpp.readStartTag(stag);
                const char *op = stag.getLocalName();
                if (isEmptyString(op))
                    recordError(ESDL_SCRIPT_Error, "unknown error");
                if (streq(op, "http-header"))
                    m_headers.append(*new CEsdlTransformOperationHttpHeader(xpp, stag, prefix));
                else if (streq(op, "content"))
                    m_content.setown(new CEsdlTransformOperationHttpContentXml(xpp, stag, prefix, functionRegister));
                else
                    xpp.skipSubTreeEx();
            }
        }
    }

    virtual ~CEsdlTransformOperationHttpPostXml()
    {
    }

    void buildHeaders(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext, IProperties *headers)
    {
        if (!m_headers.length())
            return;
        CXpathContextLocation location(targetContext);
        targetContext->addElementToLocation("headers");
        ForEachItemIn(i, m_headers)
            m_headers.item(i).processHeader(scriptContext, targetContext, sourceContext, headers);
        if (!headers->hasProp("Accept"))
            headers->setProp("Accept", "text/html, application/xml");
    }

    void buildRequest(OperationStateHttpPostXml &preparedState, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext)
    {
        CXpathContextLocation location(targetContext);
        targetContext->addElementToLocation("request");
        targetContext->ensureSetValue("@url", preparedState.url, true);
        buildHeaders(scriptContext, targetContext, sourceContext, preparedState.headers);
        if (m_content)
            m_content->process(scriptContext, targetContext, sourceContext);
        VStringBuffer xpath("/esdl_script_context/%s/%s/request/content/*[1]", m_section.str(), m_name.str());
        sourceContext->toXml(xpath, preparedState.content);
    }

    virtual IInterface *prepareForAsync(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        Owned<OperationStateHttpPostXml> preparedState = new OperationStateHttpPostXml;
        CXpathContextLocation location(targetContext);
        VStringBuffer xpath("/esdl_script_context/%s/%s", m_section.str(), m_name.str());
        targetContext->ensureLocation(xpath, true);
        if (m_url)
            sourceContext->evaluateAsString(m_url, preparedState->url);

        //don't complain if test-delay is used but test mode is off. Any script can be instrumented for testing but won't run those features outside of testing
        if (scriptContext->getTestMode() && m_testDelay)
            preparedState->testDelay = (unsigned) sourceContext->evaluateAsNumber(m_testDelay);

        buildRequest(*preparedState, scriptContext, targetContext, sourceContext);
        return preparedState.getClear();
    }

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        //if process is called here we are not currently a child of "synchronize", but because we do support synchronize
        //  we keep our state isolated.  Therefor unlike other operations we still need to prepare our "pre async" state object before calling exec
        //  in future additional operations may be optimized for synchronize in this way.
        Owned<IInterface> state = prepareForAsync(scriptContext, targetContext, sourceContext);
        return exec(nullptr, state, scriptContext, targetContext, sourceContext);
    }

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OperationStateHttpPostXml &preparedState = *static_cast<OperationStateHttpPostXml *>(preparedForAsync);
        if (preparedState.content.isEmpty())
            return false;
        HttpClientErrCode code = HttpClientErrCode::OK;
        Owned<IHttpMessage> resp;

        StringBuffer err;
        StringBuffer status;
        StringBuffer contentType;
        StringBuffer response;
        StringBuffer exceptionXml;

        try
        {
            Owned<IHttpClientContext> httpCtx = getHttpClientContext();
            Owned<IHttpClient> httpclient = httpCtx->createHttpClient(NULL, preparedState.url);
            if (!httpclient)
                return false;

            StringBuffer errmsg;
            resp.setown(httpclient->sendRequestEx("POST", "text/xml", preparedState.content, code, errmsg, preparedState.headers));
            err.append((int) code);

            if (code != HttpClientErrCode::OK)
                throw MakeStringException(ESDL_SCRIPT_Error, "ESDL Script error sending request in http-post-xml %s url(%s)", m_traceName.str(), preparedState.url.str());

            resp->getContent(response);
            if (!response.trim().length())
                throw MakeStringException(ESDL_SCRIPT_Error, "ESDL Script empty result calling http-post-xml %s url(%s)", m_traceName.str(), preparedState.url.str());

            resp->getStatus(status);
            resp->getContentType(contentType);
        }
        catch(IMultiException *me)
        {
            me->serialize(exceptionXml);
            me->Release();
        }
        catch(IException *E)
        {
            Owned<IMultiException> me = makeMultiException("ESDLScript");
            me->append(*LINK(E));
            me->serialize(exceptionXml);
            E->Release();
        }

        if (preparedState.testDelay)
            MilliSleep(preparedState.testDelay);

        VStringBuffer xpath("/esdl_script_context/%s/%s", m_section.str(), m_name.str());

        //No need to synchronize until we hit this point and start updating the scriptContext / xml document
        OptionalCriticalBlock block(crit);

        CXpathContextLocation location(targetContext);
        targetContext->ensureLocation(xpath, true);

        if (exceptionXml.length())
        {
            CXpathContextLocation content_location(targetContext);
            targetContext->addElementToLocation("content");
            targetContext->ensureSetValue("@status", "error", true);
            targetContext->addXmlContent(exceptionXml.str());
        }
        else
        {
            CXpathContextLocation response_location(targetContext);
            targetContext->addElementToLocation("response");
            targetContext->ensureSetValue("@status", status.str(), true);
            targetContext->ensureSetValue("@error-code", err.str(), true);
            targetContext->ensureSetValue("@content-type", contentType.str(), true);
            if (strnicmp("text/xml", contentType.str(), 8)==0 || strnicmp("application/xml", contentType.str(), 15) ==0)
            {
                CXpathContextLocation content_location(targetContext);
                targetContext->addElementToLocation("content");
                targetContext->addXmlContent(response.str());
            }
            else
            {
                targetContext->ensureSetValue("text", response.str(), true);
            }
        }

        sourceContext->addXpathVariable(m_name, xpath);
        return false;
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s with name(%s) url(%s)", m_name.str(), m_tagname.str(), m_name.str(), m_url ? m_url->getXpath() : "url error");
#endif
    }
};

class CEsdlTransformOperationParameter : public CEsdlTransformOperationVariable
{
private:
    Owned<ICompiledXpath> m_failureCode;
    Owned<ICompiledXpath> m_failureMsg;
public:
    CEsdlTransformOperationParameter(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationVariable(xpp, stag, prefix, functionRegister)
    {
        if (!m_select)
        {
            m_failureCode.setown(compileOptionalXpath(stag.getValue("failure_code")));
            m_failureMsg.setown(compileOptionalXpath(stag.getValue("failure_message")));
            if (m_failureCode && !m_failureMsg)
                recordError(ESDL_SCRIPT_MissingOperationAttr, "failure_code without failure_message");
        }
    }

    virtual ~CEsdlTransformOperationParameter()
    {
    }

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        if (m_select)
            return sourceContext->declareCompiledParameter(m_name, m_select);
        if (m_failureMsg && !sourceContext->checkParameterName(m_name))
        {
            StringBuffer msg;
            int code = (m_failureCode ? (int)sourceContext->evaluateAsNumber(m_failureCode) : ESDL_SCRIPT_Error);
            sourceContext->evaluateAsString(m_failureMsg, msg);
            throw makeStringExceptionV(code, "%s", msg.str());
        }
        return sourceContext->declareParameter(m_name, "");
    }
};

class CEsdlTransformOperationSetSectionAttributeBase : public CEsdlTransformOperationWithoutChildren
{
protected:
    XPathLiteralUnion m_name;
    Owned<ICompiledXpath> m_select;

public:
    CEsdlTransformOperationSetSectionAttributeBase(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, const char *attrName) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        if (m_traceName.isEmpty())
            m_traceName.set(stag.getValue("name"));
        if (!isEmptyString(attrName))
            m_name.setOptional(nullptr, attrName);
        else
            m_name.setRequired(stag, "name", *this);

        const char *select = stag.getValue("select");
        if (isEmptyString(select))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without select"); //don't mention value, it's deprecated
        m_select.setown(compileXpath(select));
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(name(%s), select('%s'))", m_traceName.str(), m_tagname.str(), m_name.configValue(), m_select->getXpath());
#endif
    }

    virtual ~CEsdlTransformOperationSetSectionAttributeBase(){}

    virtual const char *getSectionName() = 0;

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        if (m_name.isEmpty() || !m_select)
            return false; //only here if "optional" backward compatible support for now (optional syntax errors aren't actually helpful)
        try
        {
            StringBuffer name;
            m_name.get(name, *sourceContext);

            StringBuffer value;
            sourceContext->evaluateAsString(m_select, value);
            scriptContext->setAttribute(getSectionName(), name, value);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            recordError(code, msg);
        }
        catch (...)
        {
            recordError(ESDL_SCRIPT_Error, "unknown exception processing");
        }
        return false;
    }
};

class CEsdlTransformOperationStoreValue : public CEsdlTransformOperationSetSectionAttributeBase
{
public:
    CEsdlTransformOperationStoreValue(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetSectionAttributeBase(xpp, stag, prefix, nullptr)
    {
    }

    virtual ~CEsdlTransformOperationStoreValue(){}
    const char *getSectionName() override {return ESDLScriptCtxSection_Store;}
};

class CEsdlTransformOperationSetLogProfile : public CEsdlTransformOperationSetSectionAttributeBase
{
public:
    CEsdlTransformOperationSetLogProfile(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetSectionAttributeBase(xpp, stag, prefix, "profile")
    {
    }

    virtual ~CEsdlTransformOperationSetLogProfile(){}
    const char *getSectionName() override {return ESDLScriptCtxSection_Logging;}
};

class CEsdlTransformOperationSetLogOption : public CEsdlTransformOperationSetSectionAttributeBase
{
public:
    CEsdlTransformOperationSetLogOption(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetSectionAttributeBase(xpp, stag, prefix, nullptr)
    {
    }

    virtual ~CEsdlTransformOperationSetLogOption(){}
    const char *getSectionName() override {return ESDLScriptCtxSection_Logging;}
};

class CEsdlTransformOperationSetValue : public CEsdlTransformOperationWithoutChildren
{
protected:
    Owned<ICompiledXpath> m_select;
    XPathLiteralUnion m_target;
    bool m_required = true;

public:
    CEsdlTransformOperationSetValue(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        if (m_traceName.isEmpty())
            m_traceName.set(stag.getValue("name"));

        m_target.setRequired(stag, "target", *this);

        const char *select = stag.getValue("select");
        if (isEmptyString(select))
            select = stag.getValue("value");
        if (isEmptyString(select))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without select"); //don't mention value, it's deprecated

        m_select.setown(compileXpath(select));
        m_required = getStartTagValueBool(stag, "required", true);
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(%s, select('%s'))", m_traceName.str(), m_tagname.str(), m_target.configValue(), m_select->getXpath());
#endif
    }

    virtual ~CEsdlTransformOperationSetValue(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        if (m_target.isEmpty() || !m_select)
            return false; //only here if "optional" backward compatible support for now (optional syntax errors aren't actually helpful
        try
        {
            StringBuffer value;
            sourceContext->evaluateAsString(m_select, value);
            return doSet(sourceContext, targetContext, value);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            recordError(code, msg);
        }
        catch (...)
        {
            recordError(ESDL_SCRIPT_Error, "unknown exception processing");
        }
        return false;
    }

    virtual bool doSet(IXpathContext * sourceContext, IXpathContext *targetContext, const char *value)
    {
        StringBuffer target;
        targetContext->ensureSetValue(m_target.get(target, *sourceContext), value, m_required);
        return true;
    }
};

class CEsdlTransformOperationNamespace : public CEsdlTransformOperationWithoutChildren
{
protected:
    StringAttr m_prefix;
    StringAttr m_uri;
    bool m_current = false;

public:
    CEsdlTransformOperationNamespace(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        const char *pfx = stag.getValue("prefix");
        const char *uri = stag.getValue("uri");
        if (m_traceName.isEmpty())
            m_traceName.set(pfx);

        if (!pfx && isEmptyString(uri))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without prefix or uri");
        m_uri.set(uri);
        m_prefix.set(pfx);
        m_current = getStartTagValueBool(stag, "current", m_uri.isEmpty());
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(prefix('%s'), uri('%s'), current(%d))", m_traceName.str(), m_tagname.str(), m_prefix.str(), m_uri.str(), m_current);
#endif
    }

    virtual ~CEsdlTransformOperationNamespace(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        targetContext->setLocationNamespace(m_prefix, m_uri, m_current);
        return false;
    }
};

class CEsdlTransformOperationRenameNode : public CEsdlTransformOperationWithoutChildren
{
protected:
    XPathLiteralUnion m_target;
    XPathLiteralUnion m_new_name;
    bool m_all = false;

public:
    CEsdlTransformOperationRenameNode(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        m_new_name.setRequired(stag, "new_name", *this);
        if (m_traceName.isEmpty())
            m_traceName.set(m_new_name.configValue());

        m_target.setRequired(stag, "target", *this);

        m_all = getStartTagValueBool(stag, "all", false);
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(%s, new_name('%s'))", m_traceName.str(), m_tagname.str(), m_target.configValue(), m_new_name.configValue());
#endif
    }

    virtual ~CEsdlTransformOperationRenameNode(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        if (m_target.isEmpty() || m_new_name.isEmpty())
            return false; //only here if "optional" backward compatible support for now (optional syntax errors aren't actually helpful
        try
        {
            StringBuffer path;
            m_target.get(path, *sourceContext);

            StringBuffer name;
            m_new_name.get(name, *sourceContext);

            targetContext->rename(path, name, m_all);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            recordError(code, msg);
        }
        catch (...)
        {
            recordError(ESDL_SCRIPT_Error, "unknown exception processing");
        }
        return false;
    }
};

class CEsdlTransformOperationCopyOf : public CEsdlTransformOperationWithoutChildren
{
protected:
    Owned<ICompiledXpath> m_select;
    StringAttr m_new_name;

public:
    CEsdlTransformOperationCopyOf(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        const char *select = stag.getValue("select");
        if (isEmptyString(select))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without select");

        m_select.setown(compileXpath(select));
        m_new_name.set(stag.getValue("new_name"));
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(%s, new_name('%s'))", m_traceName.str(), m_tagname.str(), m_select->getXpath(), m_new_name.str());
#endif
    }

    virtual ~CEsdlTransformOperationCopyOf(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        try
        {
            targetContext->copyFromPrimaryContext(m_select, m_new_name);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            recordError(code, msg);
        }
        catch (...)
        {
            recordError(ESDL_SCRIPT_Error, "unknown exception processing");
        }
        return false;
    }
};

class CEsdlTransformOperationTrace : public CEsdlTransformOperationWithoutChildren
{
protected:
    StringAttr m_label;
    Owned<ICompiledXpath> m_test; //optional, if provided trace only if test evaluates to true
    Owned<ICompiledXpath> m_select;
    const LogMsgCategory* m_category = nullptr;

public:
    CEsdlTransformOperationTrace(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        m_label.set(stag.getValue("label"));
        const char *select = stag.getValue("select");
        if (isEmptyString(select))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without select");

        m_select.setown(compileXpath(select));

        const char *test = stag.getValue("test");
        if (!isEmptyString(test))
            m_test.setown(compileXpath(test));

        const char* msgClass = stag.getValue("class");
        if (isEmptyString(msgClass) || strieq(msgClass, "information"))
            m_category = &MCuserInfo;
        else if (strieq(msgClass, "error"))
            m_category = &MCuserError;
        else if (strieq(msgClass, "warning"))
            m_category = &MCuserWarning;
        else if (strieq(msgClass, "progress"))
            m_category = &MCuserProgress;
        else
            recordError(ESDL_SCRIPT_InvalidOperationAttr, "invalid message class");
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(test(%s), label(%s), select(%s))", m_traceName.str(), m_tagname.str(), m_test ? m_test->getXpath() : "true", m_label.str(), m_select->getXpath());
#endif
    }

    virtual ~CEsdlTransformOperationTrace(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        if (!scriptContext->isTraceEnabled())
            return true;

        OptionalCriticalBlock block(crit);

        try
        {
            if (!m_category)
                return false;
            if (m_test && !sourceContext->evaluateAsBoolean(m_test))
                return false;

            StringBuffer                      text;
            Owned<IDataMaskingProfileContext> masker(scriptContext->getMasker());
            if (getText(text, targetContext, sourceContext, masker))
            {
                ITracer& tracer = scriptContext->tracerRef();
                if (m_label.isEmpty())
                    tracer.log(*m_category, "%s", text.str());
                else
                    tracer.log(*m_category, "%s %s", m_label.str(), text.str());
            }
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            recordError(code, msg);
        }
        catch (...)
        {
            recordError(ESDL_SCRIPT_Error, "unknown exception processing");
        }
        return false;
    }

    virtual bool getText(StringBuffer& text, IXpathContext* targetContext, IXpathContext* sourceContext, IDataMaskingProfileContext* masker) = 0;
};

class CEsdlTransformOperationTraceContent : public CEsdlTransformOperationTrace
{
protected:
    StringAttr m_contentType;
    Owned<ICompiledXpath> m_skipMask;

public:
    CEsdlTransformOperationTraceContent(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationTrace(xpp, stag, prefix)
    {
        m_contentType.set(stag.getValue("content_type"));
        const char* skipMask = stag.getValue("skip_mask");
        if (!isEmptyString(skipMask))
            m_skipMask.setown(compileXpath(skipMask));
    }

    virtual bool getText(StringBuffer& text, IXpathContext* targetContext, IXpathContext* sourceContext, IDataMaskingProfileContext* masker) override
    {
        bool isValue = false;
        if (!targetContext->selectText(m_select, text, isValue) || !masker)
            return true;
        if (!m_skipMask || !sourceContext->evaluateAsBoolean(m_skipMask))
        {
            char* buffer = const_cast<char*>(text.str());
            masker->maskContent(m_contentType.get(), buffer, 0, text.length());
        }
        return true;
    }
};

class CEsdlTransformOperationTraceValue : public CEsdlTransformOperationTrace
{
protected:
    XPathLiteralUnion m_valueType;
    XPathLiteralUnion m_maskStyle;

public:
    CEsdlTransformOperationTraceValue(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationTrace(xpp, stag, prefix)
    {
        m_valueType.setRequired(stag, "value_type", *this);
        m_maskStyle.setOptional(stag, "mask_style");
    }

    virtual bool getText(StringBuffer& text, IXpathContext* targetContext, IXpathContext* sourceContext, IDataMaskingProfileContext* masker) override
    {
        bool isValue = true;
        if (!targetContext->selectText(m_select, text, isValue))
        {
            if (!isValue)
            {
                recordWarning(ESDL_SCRIPT_Warning, VStringBuffer("ambiguous value xpath '%s'", m_select->getXpath()));
                return false;
            }
        }
        if (text.isEmpty() || !masker)
            return true;

        StringBuffer vt, ms;
        m_valueType.get(vt, *sourceContext);
        if (vt.isEmpty())
        {
            recordWarning(ESDL_SCRIPT_Warning, VStringBuffer("empty value_type from '%s'", m_valueType.configValue()));
            return false;
        }
        m_maskStyle.get(ms, *sourceContext);
        char* buffer = const_cast<char*>(text.str());
        masker->maskValue(vt, ms, buffer, 0, text.length(), false);
        return true;
    }
};

class CEsdlTransformOperationRemoveNode : public CEsdlTransformOperationWithoutChildren
{
protected:
    XPathLiteralUnion m_target;
    bool m_all = false;

public:
    CEsdlTransformOperationRemoveNode(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        m_target.setRequired(stag, "target", *this);
        if (m_target.isLiteral() && isWildString(m_target.configValue()))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "wildcard in target not yet supported");

        m_all = getStartTagValueBool(stag, "all", false);
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(%s)", m_traceName.str(), m_tagname.str(), m_target.configValue());
#endif
    }

    virtual ~CEsdlTransformOperationRemoveNode(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        if (m_target.isEmpty())
            return false; //only here if "optional" backward compatible support for now (optional syntax errors aren't actually helpful
        try
        {
            StringBuffer path;
            m_target.get(path, *sourceContext);

            targetContext->remove(path, m_all);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            recordError(code, msg);
        }
        catch (...)
        {
            recordError(ESDL_SCRIPT_Error, "unknown exception processing");
        }
        return false;
    }
};

class CEsdlTransformOperationAppendValue : public CEsdlTransformOperationSetValue
{
public:
    CEsdlTransformOperationAppendValue(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetValue(xpp, stag, prefix){}

    virtual ~CEsdlTransformOperationAppendValue(){}

    virtual bool doSet(IXpathContext * sourceContext, IXpathContext *targetContext, const char *value) override
    {
        StringBuffer target;
        targetContext->ensureAppendToValue(m_target.get(target, *sourceContext), value, m_required);
        return true;
    }
};

class CEsdlTransformOperationAddValue : public CEsdlTransformOperationSetValue
{
public:
    CEsdlTransformOperationAddValue(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetValue(xpp, stag, prefix){}

    virtual ~CEsdlTransformOperationAddValue(){}

    virtual bool doSet(IXpathContext * sourceContext, IXpathContext *targetContext, const char *value) override
    {
        StringBuffer target;
        targetContext->ensureAddValue(m_target.get(target, *sourceContext), value, m_required);
        return true;
    }
};

class CEsdlTransformOperationFail : public CEsdlTransformOperationWithoutChildren
{
protected:
    Owned<ICompiledXpath> m_message;
    Owned<ICompiledXpath> m_code;

public:
    CEsdlTransformOperationFail(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        if (m_traceName.isEmpty())
            m_traceName.set(stag.getValue("name"));

        const char *code = stag.getValue("code");
        const char *message = stag.getValue("message");
        if (isEmptyString(code))
            recordException(ESDL_SCRIPT_MissingOperationAttr, "without code");
        if (isEmptyString(message))
            recordException(ESDL_SCRIPT_MissingOperationAttr, "without message");

        m_code.setown(compileXpath(code));
        m_message.setown(compileXpath(message));
    }

    virtual ~CEsdlTransformOperationFail()
    {
    }

    bool doFail(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext)
    {
        int code = m_code.get() ? (int) sourceContext->evaluateAsNumber(m_code) : ESDL_SCRIPT_Error;
        StringBuffer msg;
        if (m_message.get())
            sourceContext->evaluateAsString(m_message, msg);
        throw makeStringException(code, msg.str());
        return true; //avoid compilation error
    }

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);
        return doFail(scriptContext, targetContext, sourceContext);
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s with message(%s)", m_traceName.str(), m_tagname.str(), m_message.get() ? m_message->getXpath() : "");
#endif
    }
};

class CEsdlTransformOperationAssert : public CEsdlTransformOperationFail
{
private:
    Owned<ICompiledXpath> m_test; //assert is like a conditional fail

public:
    CEsdlTransformOperationAssert(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationFail(xpp, stag, prefix)
    {
        const char *test = stag.getValue("test");
        if (isEmptyString(test))
            recordException(ESDL_SCRIPT_MissingOperationAttr, "without test");
        m_test.setown(compileXpath(test));
    }

    virtual ~CEsdlTransformOperationAssert()
    {
    }

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        if (m_test && sourceContext->evaluateAsBoolean(m_test))
            return false;
        return CEsdlTransformOperationFail::doFail(scriptContext, targetContext, sourceContext);
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        const char *testXpath = m_test.get() ? m_test->getXpath() : "SYNTAX ERROR IN test";
        DBGLOG(">%s> %s if '%s' with message(%s)", m_traceName.str(), m_tagname.str(), testXpath, m_message.get() ? m_message->getXpath() : "");
#endif
    }
};

class CEsdlTransformOperationForEach : public CEsdlTransformOperationWithChildren
{
protected:
    Owned<ICompiledXpath> m_select;

public:
    CEsdlTransformOperationForEach(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
        const char *select = stag.getValue("select");
        if (isEmptyString(select))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without select");
        m_select.setown(compileXpath(select));
    }

    virtual ~CEsdlTransformOperationForEach(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        Owned<IXpathContextIterator> contexts = evaluate(sourceContext);
        if (!contexts)
            return false;
        if (!contexts->first())
            return false;
        ForEach(*contexts)
            processChildren(scriptContext, targetContext, &contexts->query());
        return true;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG (">>>>%s %s ", m_tagname.str(), m_select ? m_select->getXpath() : "");
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG ("<<<<%s<<<<<", m_tagname.str());
    #endif
    }

private:
    IXpathContextIterator *evaluate(IXpathContext * xpathContext)
    {
        IXpathContextIterator *xpathset = nullptr;
        try
        {
            xpathset = xpathContext->evaluateAsNodeSet(m_select);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            recordError(code, msg);
        }
        catch (...)
        {
            VStringBuffer msg("unknown exception evaluating select '%s'", m_select.get() ? m_select->getXpath() : "undefined!");
            recordError(ESDL_SCRIPT_Error, msg);
        }
        return xpathset;
    }
};

class CEsdlTransformOperationSynchronize : public CEsdlTransformOperationWithChildren
{
    Owned<ICompiledXpath> m_maxAtOnce;
public:
    CEsdlTransformOperationSynchronize(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, false, functionRegister, nullptr)
    {
        const char *maxAtOnce = stag.getValue("max-at-once");
        if (!isEmptyString(maxAtOnce))
            m_maxAtOnce.setown(compileXpath(maxAtOnce));
    }

    virtual ~CEsdlTransformOperationSynchronize(){}

    virtual bool exec(CriticalSection *externalCrit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        if (!m_children.length())
            return false;

        IPointerArray preps;
        ForEachItemIn(i, m_children)
            preps.append(m_children.item(i).prepareForAsync(scriptContext, targetContext, sourceContext));

        class casyncfor: public CAsyncFor
        {
            CriticalSection synchronizeCrit;
            IPointerArray &preps;
            IArrayOf<IEsdlTransformOperation> &children;
            IEsdlScriptContext *scriptContext = nullptr;
            IXpathContext *targetContext = nullptr;
            IXpathContext *sourceContext = nullptr;

        public:
            casyncfor(IPointerArray &_preps, IArrayOf<IEsdlTransformOperation> &_children, IEsdlScriptContext *_scriptContext, IXpathContext *_targetContext, IXpathContext *_sourceContext)
                : preps(_preps), children(_children), scriptContext(_scriptContext), targetContext(_targetContext), sourceContext(_sourceContext)
            {
            }
            void Do(unsigned i)
            {
                children.item(i).exec(&synchronizeCrit, preps.item(i), scriptContext, targetContext, sourceContext);
            }
        } afor(preps, m_children, scriptContext, targetContext, sourceContext);

        unsigned maxAtOnce = m_maxAtOnce.get() ? (unsigned) sourceContext->evaluateAsNumber(m_maxAtOnce) : 5;
        afor.For(m_children.ordinality(), maxAtOnce, false, false);

        return true;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG (">>>>%s ", m_tagname.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG ("<<<<%s<<<<<", m_tagname.str());
    #endif
    }
};

class CEsdlTransformOperationConditional : public CEsdlTransformOperationWithChildren
{
private:
    Owned<ICompiledXpath> m_test;
    char m_op = 'i'; //'i'=if, 'w'=when, 'o'=otherwise

public:
    CEsdlTransformOperationConditional(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
        const char *op = stag.getLocalName();
        if (isEmptyString(op)) //should never get here, we checked already, but
            recordError(ESDL_SCRIPT_UnknownOperation, "unrecognized conditional missing tag name");
        //m_ignoreCodingErrors means op may still be null
        else if (!op || streq(op, "if"))
            m_op = 'i';
        else if (streq(op, "when"))
            m_op = 'w';
        else if (streq(op, "otherwise"))
            m_op = 'o';
        else //should never get here either, but
            recordError(ESDL_SCRIPT_UnknownOperation, "unrecognized conditional tag name");

        if (m_op!='o')
        {
            const char *test = stag.getValue("test");
            if (isEmptyString(test))
                recordError(ESDL_SCRIPT_MissingOperationAttr, "without test");
            m_test.setown(compileXpath(test));
        }
    }

    virtual ~CEsdlTransformOperationConditional(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        if (!evaluate(sourceContext))
            return false;
        processChildren(scriptContext, targetContext, sourceContext);
        return true; //just means that evaluation succeeded and attempted to process children
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG (">>>>%s %s ", m_tagname.str(), m_test ? m_test->getXpath() : "");
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG ("<<<<%s<<<<<", m_tagname.str());
    #endif
    }

private:
    bool evaluate(IXpathContext * xpathContext)
    {
        if (m_op=='o')  //'o'/"otherwise" is unconditional
            return true;
        bool match = false;
        try
        {
            match = xpathContext->evaluateAsBoolean(m_test);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            recordError(code, msg);
        }
        catch (...)
        {
            VStringBuffer msg("unknown exception evaluating test '%s'", m_test.get() ? m_test->getXpath() : "undefined!");
            recordError(ESDL_SCRIPT_Error, msg);
        }
        return match;
    }
};

void loadChooseChildren(IArrayOf<IEsdlTransformOperation> &operations, IXmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, IEsdlOperationTraceMessenger& messenger, IEsdlFunctionRegister *functionRegister)
{
    Owned<CEsdlTransformOperationConditional> otherwise;

    int type = 0;
    while(true)
    {
        type = xpp.next();
        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                StartTag opTag;
                xpp.readStartTag(opTag);
                const char *op = opTag.getLocalName();
                if (streq(op, "when"))
                    operations.append(*new CEsdlTransformOperationConditional(xpp, opTag, prefix, functionRegister));
                else if (streq(op, "otherwise"))
                {
                    if (otherwise)
                        messenger.recordError(ESDL_SCRIPT_Error, "only 1 otherwise per choose statement allowed");
                    otherwise.setown(new CEsdlTransformOperationConditional(xpp, opTag, prefix, functionRegister));
                }
                break;
            }
            case XmlPullParser::END_TAG:
            {
                if (otherwise)
                    operations.append(*otherwise.getClear());
                return;
            }
        }
    }
}

class CEsdlTransformOperationChoose : public CEsdlTransformOperationWithChildren
{
public:
    CEsdlTransformOperationChoose(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, false, functionRegister, loadChooseChildren)
    {
    }

    virtual ~CEsdlTransformOperationChoose(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        return processChildren(scriptContext, targetContext, sourceContext);
    }

    virtual bool processChildren(IEsdlScriptContext * scriptContext, IXpathContext *targetContext, IXpathContext * sourceContext) override
    {
        if (m_children.length())
        {
            CXpathContextScope scope(sourceContext, "choose", XpathVariableScopeType::simple, nullptr);
            ForEachItemIn(i, m_children)
            {
                if (m_children.item(i).process(scriptContext, targetContext, sourceContext))
                    return true;
            }
        }
        return false;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

void loadCallWithParameters(IArrayOf<IEsdlTransformOperation> &operations, IXmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, IEsdlOperationTraceMessenger& messenger, IEsdlFunctionRegister *functionRegister)
{
    int type = 0;
    while((type = xpp.next()) != XmlPullParser::END_TAG)
    {
        if (XmlPullParser::START_TAG == type)
        {
            StartTag opTag;
            xpp.readStartTag(opTag);
            const char *op = opTag.getLocalName();
            if (streq(op, "with-param"))
                operations.append(*new CEsdlTransformOperationVariable(xpp, opTag, prefix, functionRegister));
            else
                messenger.recordError(ESDL_SCRIPT_Error, VStringBuffer("Unrecognized operation '%s', only 'with-param' allowed within 'call-function'", op));
        }
    }
}

class CEsdlTransformOperationCallFunction : public CEsdlTransformOperationWithChildren
{
private:
    StringAttr m_name;
    //the localFunctionRegister is used at compile time to register this object,
    // and then only for looking up functions defined inside the local script
    IEsdlFunctionRegister *localFunctionRegister = nullptr;
    IEsdlTransformOperation *esdlFunc = nullptr;

public:
    CEsdlTransformOperationCallFunction(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *_functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, false /* we need to handle variable scope below (see comment)*/, _functionRegister, loadCallWithParameters), localFunctionRegister(_functionRegister)
    {
        m_name.set(stag.getValue("name"));
        if (m_name.isEmpty())
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without name parameter");
        localFunctionRegister->registerEsdlFunctionCall(this);
    }
    virtual ~CEsdlTransformOperationCallFunction()
    {
    }
    void bindFunctionCall(const char *scopeDescr, IEsdlFunctionRegister *activeFunctionRegister, bool bindLocalOnly)
    {
        //we always use / cache function pointer defined local to current script
        esdlFunc = localFunctionRegister->findEsdlFunction(m_name, true);
        if (!esdlFunc)
        {
            //the activeFunctionRegister is associated with the ESDL method currently being bound
            IEsdlTransformOperation *foundFunc = activeFunctionRegister->findEsdlFunction(m_name, false);
            if (foundFunc)
            {
                if (!bindLocalOnly)
                    esdlFunc = foundFunc;
            }
            else
            {
                //if bindLocalOnly, it's just a warning if we didn't cache the function pointer
                //  this is intended for function calls in service level scripts which will be looked up at runtime if they aren't local to the script
                VStringBuffer msg("function (%s) not found for %s", m_name.str(), scopeDescr);
                if (bindLocalOnly)
                    recordWarning(ESDL_SCRIPT_Warning, msg.str());
                else
                    recordException(ESDL_SCRIPT_Error, msg.str());
            }
        }
    }

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        //we should only get here with esdlFunc==nullptr for calls made in service level scripts where the function
        //  isn't defined locally to the script
        IEsdlTransformOperation *callFunc = esdlFunc;
        if (!callFunc)
        {
            IEsdlFunctionRegister *activeRegister = scriptContext->queryFunctionRegister();
            if (!activeRegister)
                throw MakeStringException(ESDL_SCRIPT_Error, "Runtime function register not found (looking up %s)", m_name.str());
            callFunc = activeRegister->findEsdlFunction(m_name, false);
            if (!callFunc)
                throw MakeStringException(ESDL_SCRIPT_Error, "Function (%s) not found (runtime)", m_name.str());
        }

        //Can't have CEsdlTransformOperationWithChildren create the scope, it would be destroyed before esdlFunc->process() was called below
        Owned<CXpathContextScope> scope = new CXpathContextScope(sourceContext, m_tagname, XpathVariableScopeType::parameter, nullptr);

        //in this case, processing children is setting up parameters for the function call that follows
        processChildren(scriptContext, targetContext, sourceContext);

        return callFunc->process(scriptContext, targetContext, sourceContext);
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG (">>>>>>>>>>> %s %s >>>>>>>>>>", m_tagname.str(), m_name.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

class CEsdlTransformOperationTarget : public CEsdlTransformOperationWithChildren
{
protected:
    Owned<ICompiledXpath> m_xpath;
    bool m_required = true;
    bool m_ensure = false;

public:
    CEsdlTransformOperationTarget(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
        const char *xpath = stag.getValue("xpath");
        if (isEmptyString(xpath))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without xpath parameter");

        m_xpath.setown(compileXpath(xpath));
        m_required = getStartTagValueBool(stag, "required", m_required);
    }

    virtual ~CEsdlTransformOperationTarget(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        CXpathContextLocation location(targetContext);
        bool success = false;
        if (m_ensure)
            success = targetContext->ensureLocation(m_xpath->getXpath(), m_required);
        else
            success = targetContext->setLocation(m_xpath, m_required);

        if (success)
            return processChildren(scriptContext, targetContext, sourceContext);
        return false;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">>>%s> %s(%s)>>>>", m_traceName.str(), m_tagname.str(), m_xpath.get() ? m_xpath->getXpath() : "");
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

//the script element serves as a simple wrapper around a section of script content
//  the initial use to put a script section child within a synchronize element, providing a section of script to run while other synchronize children are blocked
//but script is supported anywhere and might be useful elsewhere for providing some scope for variables and for structuring code
class CEsdlTransformOperationScript : public CEsdlTransformOperationWithChildren
{
public:
    CEsdlTransformOperationScript(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
    }

    virtual ~CEsdlTransformOperationScript(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);
        return processChildren(scriptContext, targetContext, sourceContext);
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">>>%s> %s>>>>", m_traceName.str(), m_tagname.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

class CEsdlTransformOperationIfTarget : public CEsdlTransformOperationTarget
{
public:
    CEsdlTransformOperationIfTarget(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationTarget(xpp, stag, prefix, functionRegister)
    {
        m_required = false;
    }

    virtual ~CEsdlTransformOperationIfTarget(){}
};

class CEsdlTransformOperationEnsureTarget : public CEsdlTransformOperationTarget
{
public:
    CEsdlTransformOperationEnsureTarget(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationTarget(xpp, stag, prefix, functionRegister)
    {
        m_ensure = true;
    }

    virtual ~CEsdlTransformOperationEnsureTarget(){}
};

class CEsdlTransformOperationSource : public CEsdlTransformOperationWithChildren
{
protected:
    Owned<ICompiledXpath> m_xpath;
    bool m_required = true;

public:
    CEsdlTransformOperationSource(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
        const char *xpath = stag.getValue("xpath");
        if (isEmptyString(xpath))
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without xpath parameter");

        m_xpath.setown(compileXpath(xpath));
        m_required = getStartTagValueBool(stag, "required", m_required);
    }

    virtual ~CEsdlTransformOperationSource(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        CXpathContextLocation location(sourceContext);
        if (sourceContext->setLocation(m_xpath, m_required))
            return processChildren(scriptContext, targetContext, sourceContext);
        return false;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">>>%s> %s(%s)>>>>", m_traceName.str(), m_tagname.str(), m_xpath.get() ? m_xpath->getXpath() : "");
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

class CEsdlTransformOperationIfSource : public CEsdlTransformOperationSource
{
public:
    CEsdlTransformOperationIfSource(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationSource(xpp, stag, prefix, functionRegister)
    {
        m_required = false;
    }

    virtual ~CEsdlTransformOperationIfSource(){}
};


class CEsdlTransformOperationElement : public CEsdlTransformOperationWithChildren
{
protected:
    StringBuffer m_name;
    StringBuffer m_nsuri;

public:
    CEsdlTransformOperationElement(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
        m_name.set(stag.getValue("name"));
        if (m_name.isEmpty())
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without name parameter");
        if (m_traceName.isEmpty())
            m_traceName.set(m_name);

        if (!validateXMLTag(m_name))
        {
            VStringBuffer msg("with invalid element name '%s'", m_name.str());
            recordError(ESDL_SCRIPT_MissingOperationAttr, msg.str());
        }

        m_nsuri.set(stag.getValue("namespace"));
    }

    virtual ~CEsdlTransformOperationElement(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        OptionalCriticalBlock block(crit);

        CXpathContextLocation location(targetContext);
        targetContext->addElementToLocation(m_name);
        return processChildren(scriptContext, targetContext, sourceContext);
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG (">>>>>>>>>>> %s (%s, nsuri(%s)) >>>>>>>>>>", m_tagname.str(), m_name.str(), m_nsuri.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

void createEsdlTransformOperations(IArrayOf<IEsdlTransformOperation> &operations, IXmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, IEsdlOperationTraceMessenger& messenger, IEsdlFunctionRegister *functionRegister)
{
    int type = 0;
    while((type = xpp.next()) != XmlPullParser::END_TAG)
    {
        if (XmlPullParser::START_TAG == type)
        {
            Owned<IEsdlTransformOperation> operation = createEsdlTransformOperation(xpp, prefix, withVariables, messenger, functionRegister, false);
            if (operation)
                operations.append(*operation.getClear());
        }
    }
}

class CEsdlTransformOperationFunction : public CEsdlTransformOperationWithChildren
{
public:
    StringAttr m_name;

public:
    CEsdlTransformOperationFunction(IXmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, IEsdlFunctionRegister *functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
        m_name.set(stag.getValue("name"));
        if (m_name.isEmpty())
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without name parameter");
        m_childScopeType = XpathVariableScopeType::isolated;
    }

    virtual ~CEsdlTransformOperationFunction(){}

    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        return processChildren(scriptContext, targetContext, sourceContext);
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">>>%s> %s %s >>>>", m_traceName.str(), m_tagname.str(), m_name.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

/**
 * @brief Helper structure used by both tx-summary-value and tx-summary-timer.
 *
 * There is common data used by both operations. Because one operation is childless and the other
 * allows children, they cannot have a common base class. Instead each contains an instance of this
 * structure to provide common handling of the common data.
 */
struct TxSummaryCoreInfo
{
    StringAttr m_name;
    unsigned   m_level = LogMin;
    unsigned   m_groups = TXSUMMARY_GRP_ENTERPRISE;

    TxSummaryCoreInfo(StartTag& stag, IEsdlOperationTraceMessenger& messenger)
    {
        m_name.set(stag.getValue("name"));
        if (m_name.isEmpty())
            messenger.recordError(ESDL_SCRIPT_MissingOperationAttr, "without name");
        const char* level = stag.getValue("level");
        if (!isEmptyString(level))
        {
            if (strieq(level, "min"))
                m_level = LogMin;
            else if (strieq(level, "normal"))
                m_level = LogNormal;
            else if (strieq(level, "max"))
                m_level = LogMax;
            else if (TokenDeserializer().deserialize(level, m_level) != Deserialization_SUCCESS || m_level < LogMin || m_level > LogMax)
                messenger.recordError(ESDL_SCRIPT_InvalidOperationAttr, "invalid level");
        }
        const char* coreGroup = stag.getValue("core_group");
        if (!isEmptyString(coreGroup) && strToBool(coreGroup))
            m_groups |= TXSUMMARY_GRP_CORE;
    }
};

static TokenDeserializer s_deserializer;

class CEsdlTransformOperationTxSummaryValue : public CEsdlTransformOperationWithoutChildren
{
protected:
    enum Type { Text, Signed, Unsigned, Decimal };
    enum Mode { Append, Set };
    TxSummaryCoreInfo     m_info;
    Owned<ICompiledXpath> m_value;
    Type                  m_type = Text;
    Mode                  m_mode = Append;

public:
    CEsdlTransformOperationTxSummaryValue(IXmlPullParser& xpp, StartTag& stag, const StringBuffer& prefix)
        : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
        , m_info(stag, *this)
    {
        m_value.setown(compileOptionalXpath(stag.getValue("select")));
        if (!m_value)
            recordError(ESDL_SCRIPT_MissingOperationAttr, "without select");
        const char* type = stag.getValue("type");
        if (!isEmptyString(type))
        {
            if (strieq(type, "signed"))
                m_type = Signed;
            else if (strieq(type, "unsigned"))
                m_type = Unsigned;
            else if (strieq(type, "decimal"))
                m_type = Decimal;
            else if (!strieq(type, "text"))
                recordError(ESDL_SCRIPT_InvalidOperationAttr, "invalid type");
        }
        const char* mode = stag.getValue("mode");
        if (!isEmptyString(mode))
        {
            if (strieq(mode, "set"))
                m_mode = Set;
            else if (!strieq(mode, "append"))
                recordError(ESDL_SCRIPT_InvalidOperationAttr, VStringBuffer("invalid mode '%s'", mode));
        }
    }

    virtual ~CEsdlTransformOperationTxSummaryValue()
    {
    }

    void injectText(const char* value, CTxSummary* txSummary) const
    {
        switch (m_mode)
        {
        case Append:
            if (!txSummary->append(m_info.m_name, value, m_info.m_level, m_info.m_groups))
                UWARNLOG("script operation %s did not append '%s=%s' [text]; name is malformed or is in use", m_tagname.str(), m_info.m_name.str(), value);
            break;
        case Set:
            if (!txSummary->set(m_info.m_name, value, m_info.m_level, m_info.m_groups))
                UWARNLOG("script operation %s did not set '%s=%s' [text]; name is malformed or identifies a cumulative timer", m_tagname.str(), m_info.m_name.str(), value);
            break;
        default:
            break;
        }
    }
    void injectSigned(const char* rawValue, CTxSummary* txSummary) const
    {
        signed long long sll;
        if (s_deserializer.deserialize(rawValue, sll) == Deserialization_SUCCESS)
        {
            switch (m_mode)
            {
            case Append:
                if (!txSummary->append(m_info.m_name, sll, m_info.m_level, m_info.m_groups))
                    UWARNLOG("script operation %s did not append '%s=%s' [signed]; name is malformed or is in use", m_tagname.str(), m_info.m_name.str(), rawValue);
                break;
            case Set:
                if (!txSummary->set(m_info.m_name, sll, m_info.m_level, m_info.m_groups))
                    UWARNLOG("script operation %s did not set '%s=%s' [signed]; name is malformed or identifies a cumulative timer", m_tagname.str(), m_info.m_name.str(), rawValue);
                break;
            default:
                break;
            }
        }
        else
            UWARNLOG("script operation %s did not set '%s=%s'; not a signed integer", m_tagname.str(), m_info.m_name.str(), rawValue);
    }
    void injectUnsigned(const char* rawValue, CTxSummary* txSummary) const
    {
        unsigned long long ull;
        if (s_deserializer.deserialize(rawValue, ull) == Deserialization_SUCCESS)
        {
            switch (m_mode)
            {
            case Append:
                if (!txSummary->append(m_info.m_name, ull, m_info.m_level, m_info.m_groups))
                    UWARNLOG("script operation %s did not append '%s=%s' [unsigned]; name is malformed or is in use", m_tagname.str(), m_info.m_name.str(), rawValue);
                break;
            case Set:
                if (!txSummary->set(m_info.m_name, ull, m_info.m_level, m_info.m_groups))
                    UWARNLOG("script operation %s did not set '%s=%s' [unsigned]; name is malformed or identifies a cumulative timer", m_tagname.str(), m_info.m_name.str(), rawValue);
                break;
            default:
                break;
            }
        }
        else
            UWARNLOG("script operation %s did not set '%s=%s'; not an unsigned integer", m_tagname.str(), m_info.m_name.str(), rawValue);
    }
    void injectDecimal(const char* rawValue, CTxSummary* txSummary) const
    {
        double d;
        if (s_deserializer.deserialize(rawValue, d) == Deserialization_SUCCESS)
        {
            switch (m_mode)
            {
            case Append:
                if (!txSummary->append(m_info.m_name, d, m_info.m_level, m_info.m_groups))
                    UWARNLOG("script operation %s did not append '%s=%s' [decimal]; name is malformed or is in use", m_tagname.str(), m_info.m_name.str(), rawValue);
                break;
            case Set:
                if (!txSummary->set(m_info.m_name, d, m_info.m_level, m_info.m_groups))
                    UWARNLOG("script operation %s did not set '%s=%s' [decimal]; name is malformed or identifies a cumulative timer", m_tagname.str(), m_info.m_name.str(), rawValue);
                break;
            default:
                break;
            }
        }
        else
            UWARNLOG("script operation %s did not set '%s=%s'; not decimal", m_tagname.str(), m_info.m_name.str(), rawValue);
    }
    void injectValue(const char* rawValue, CTxSummary* txSummary)
    {
        switch (m_type)
        {
        case Text:
            injectText(rawValue, txSummary);
            break;
        case Signed:
            injectSigned(rawValue, txSummary);
            break;
        case Unsigned:
            injectUnsigned(rawValue, txSummary);
            break;
        case Decimal:
            injectDecimal(rawValue, txSummary);
            break;
        default:
            break;
        }
    }
    virtual bool exec(CriticalSection* crit, IInterface* preparedForAsync, IEsdlScriptContext* scriptContext, IXpathContext* targetContext, IXpathContext* sourceContext) override
    {
        IEspContext* espCtx = scriptContext->queryEspContext();
        CTxSummary* txSummary = (espCtx ? espCtx->queryTxSummary() : nullptr);
        if (!txSummary)
            return true;
        OptionalCriticalBlock block(crit);

        if (m_value)
        {
            StringBuffer value;
            sourceContext->evaluateAsString(m_value, value);
            injectValue(value, txSummary);
        }
        return true;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">%s> %s(%s)", m_traceName.str(), m_tagname.str(), m_info.m_name.str());
    #endif
    }
};

class CEsdlTransformOperationTxSummaryTimer : public CEsdlTransformOperationWithChildren
{
protected:
    enum Mode { Append, Set, Accumulate };
    TxSummaryCoreInfo m_info;
    Mode              m_mode = Append;

public:
    CEsdlTransformOperationTxSummaryTimer(IXmlPullParser& xpp, StartTag& stag, const StringBuffer& prefix, IEsdlFunctionRegister* functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
        , m_info(stag, *this)
    {
        const char* mode = stag.getValue("mode");
        if (!isEmptyString(mode))
        {
            if (strieq(mode, "set"))
                m_mode = Set;
            else if (strieq(mode, "accumulate"))
                m_mode = Accumulate;
            else if (!strieq(mode, "append"))
                recordError(ESDL_SCRIPT_InvalidOperationAttr, VStringBuffer("invalid mode '%s'", mode));
        }
    }

    virtual ~CEsdlTransformOperationTxSummaryTimer()
    {
    }

    virtual bool exec(CriticalSection* crit, IInterface* preparedForAsync, IEsdlScriptContext* scriptContext, IXpathContext* targetContext, IXpathContext* sourceContext) override
    {
        IEspContext* espCtx = scriptContext->queryEspContext();
        CTxSummary* txSummary = (espCtx ? espCtx->queryTxSummary() : nullptr);
        if (!txSummary)
            return true;
        StringBuffer value;
        OptionalCriticalBlock block(crit);
        CTimeMon timer;
        processChildren(scriptContext, targetContext, sourceContext);
        unsigned delta = timer.elapsed();
        switch (m_mode)
        {
        case Append:
            if (!txSummary->append(m_info.m_name, delta, m_info.m_level, m_info.m_groups))
                UWARNLOG("script operation %s did not append '%s=%u'; name is malformed or is in use", m_tagname.str(), m_info.m_name.str(), delta);
            break;
        case Set:
            if (!txSummary->set(m_info.m_name, delta, m_info.m_level, m_info.m_groups))
                UWARNLOG("script operation %s did not set '%s=%u'; name is malformed or identifies a cumulative timer", m_tagname.str(), m_info.m_name.str(), delta);
            break;
        case Accumulate:
            if (!txSummary->updateTimer(m_info.m_name, delta, m_info.m_level, m_info.m_groups))
                UWARNLOG("script operation %s did not add %ums to cumulative timer '%s'; name is malformed or identifies a scalar value", m_tagname.str(), delta,  m_info.m_name.str());
            break;
        default:
            break;
        }
        return true;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">>>%s> %s %s >>>>", m_traceName.str(), m_tagname.str(), m_info.m_name.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

class CEsdlTransformOperationDelay : public CEsdlTransformOperationWithoutChildren
{
protected:
    unsigned m_millis = 1;
public:
    CEsdlTransformOperationDelay(IXmlPullParser& xpp, StartTag& stag, const StringBuffer& prefix)
        : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        const char* millis = stag.getValue("millis");
        if (!isEmptyString(millis) && s_deserializer.deserialize(millis, m_millis) != Deserialization_SUCCESS)
            recordError(ESDL_SCRIPT_InvalidOperationAttr, "invalid millis");
    }

    virtual ~CEsdlTransformOperationDelay()
    {
    }

    virtual bool exec(CriticalSection* crit, IInterface* preparedForAsync, IEsdlScriptContext* scriptContext, IXpathContext* targetContext, IXpathContext* sourceContext) override
    {
        OptionalCriticalBlock block(crit);
        MilliSleep(m_millis);
        return true;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">%s> %s", m_traceName.str(), m_tagname.str());
    #endif
    }
};

class CEsdlTransformOperationUpdateMaskingContext : public CEsdlTransformOperationBase
{
    class Update : public CEsdlTransformOperationWithoutChildren
    {
    public: // IEsdlTransformOperation
        virtual bool exec(CriticalSection* crit, IInterface* preparedForAsync, IEsdlScriptContext* scriptContext, IXpathContext* targetContext, IXpathContext* sourceContext) override
        {
            return true;
        }
    public:
        virtual bool apply(IDataMaskingProfileContext& masker, IXpathContext& sourceContext) const = 0;
        using CEsdlTransformOperationWithoutChildren::CEsdlTransformOperationWithoutChildren;
    };
    class SetProperty : public Update
    {
    public:
        virtual bool apply(IDataMaskingProfileContext& masker, IXpathContext& sourceContext) const override
        {
            StringBuffer n, v;
            return masker.setProperty(m_name.get(n, sourceContext), m_value.get(v, sourceContext));
        }
        virtual void toDBGLog() override
        {
        #if defined(_DEBUG)
            DBGLOG(">%s> %s %s=%s", m_traceName.str(), m_tagname.str(), m_name.configValue(), m_value.configValue());
        #endif
        }
    private:
        XPathLiteralUnion m_name;
        XPathLiteralUnion m_value;
    public:
        SetProperty(IXmlPullParser& xpp, StartTag& stag, const StringBuffer& prefix)
            : Update(xpp, stag, prefix)
        {
            m_name.setRequired(stag, "name", *this);
            m_value.setRequired(stag, "value", *this);
        }
    };
    class RemoveProperty : public Update
    {
    public:
        virtual bool apply(IDataMaskingProfileContext& target, IXpathContext& sourceContext) const override
        {
            StringBuffer n;
            return target.removeProperty(m_name.get(n, sourceContext));
        }
        virtual void toDBGLog() override
        {
        #if defined(_DEBUG)
            DBGLOG(">%s> %s %s", m_traceName.str(), m_tagname.str(), m_name.configValue());
        #endif
        }
    private:
        XPathLiteralUnion m_name;
    public:
        RemoveProperty(IXmlPullParser& xpp, StartTag& stag, const StringBuffer& prefix)
            : Update(xpp, stag, prefix)
        {
            m_name.setRequired(stag, "name", *this);
        }
    };
private:
    using Updates = std::list<Owned<Update>>;
    Updates m_updates;
public:
    CEsdlTransformOperationUpdateMaskingContext(IXmlPullParser& xpp, StartTag& stag, const StringBuffer& prefix)
        : CEsdlTransformOperationBase(xpp, stag, prefix)
    {
        int type = 0;
        while((type = xpp.next()) != XmlPullParser::END_TAG)
        {
            if (XmlPullParser::START_TAG == type)
            {
                StartTag stag;
                xpp.readStartTag(stag);
                const char *op = stag.getLocalName();
                if (isEmptyString(op))
                    recordError(ESDL_SCRIPT_InvalidOperationAttr, "invalid child operation");
                if (streq(op, "set"))
                    m_updates.emplace_back(new SetProperty(xpp, stag, prefix));
                else if (streq(op, "remove"))
                    m_updates.emplace_back(new RemoveProperty(xpp, stag, prefix));
                else
                {
                    recordError(ESDL_SCRIPT_InvalidOperationAttr, VStringBuffer("invalid child operation '%s'", op));
                    xpp.skipSubTreeEx();
                }
            }
        }
    }

    virtual bool exec(CriticalSection* crit, IInterface* preparedForAsync, IEsdlScriptContext* scriptContext, IXpathContext* targetContext, IXpathContext* sourceContext) override
    {
        Owned<IDataMaskingProfileContext> masker(scriptContext->getMasker());
        if (masker)
        {
            for (const Owned<Update>& u : m_updates)
                u->apply(*masker, *sourceContext);
        }
        return true;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">>>%s> %s  >>>>", m_traceName.str(), m_tagname.str());
        for (const Owned<Update>& u : m_updates)
            u->toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

class CEsdlTransformOperationMaskingContextScope : public CEsdlTransformOperationWithChildren
{
public:
    CEsdlTransformOperationMaskingContextScope(IXmlPullParser& xpp, StartTag& stag, const StringBuffer& prefix, IEsdlFunctionRegister* functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
    }

    virtual ~CEsdlTransformOperationMaskingContextScope()
    {
    }

    virtual bool exec(CriticalSection* crit, IInterface* preparedForAsync, IEsdlScriptContext* scriptContext, IXpathContext* targetContext, IXpathContext* sourceContext) override
    {
        EsdlScriptMaskerScope maskerScope(scriptContext);
        processChildren(scriptContext, targetContext, sourceContext);
        return true;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">>>%s> %s  >>>>", m_traceName.str(), m_tagname.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

class CEsdlTransformOperationSetTraceOptions : public CEsdlTransformOperationWithoutChildren
{
private:
    Owned<ICompiledXpath> m_enabled;
    Owned<ICompiledXpath> m_locked;

public:
    CEsdlTransformOperationSetTraceOptions(IXmlPullParser& xpp, StartTag& stag, const StringBuffer& prefix)
        : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        const char* enabled = stag.getValue("enabled");
        if (!isEmptyString(enabled))
            m_enabled.setown(compileXpath(enabled));

        const char* locked = stag.getValue("locked");
        if (!isEmptyString(locked))
            m_locked.setown(compileXpath(locked));
        
        if (!m_enabled && !m_locked)
            recordError(ESDL_SCRIPT_MissingOperationAttr, "missing all options");
    }

    ~CEsdlTransformOperationSetTraceOptions()
    {
    }

    virtual bool exec(CriticalSection* crit, IInterface* preparedForAsync, IEsdlScriptContext* scriptContext, IXpathContext* targetContext, IXpathContext* sourceContext) override
    {
        bool locked = scriptContext->isTraceLocked();
        if ((m_enabled || m_locked) && !locked)
        {
            bool enabled = (m_enabled ? sourceContext->evaluateAsBoolean(m_enabled) : scriptContext->isTraceEnabled());
            if (m_locked)
                locked = sourceContext->evaluateAsBoolean(m_locked);
            scriptContext->setTraceOptions(enabled, locked);
        }
        return true;
    }

    virtual void toDBGLog() override
    {
        #if defined(_DEBUG)
            DBGLOG(">%s> %s", m_traceName.str(), m_tagname.str());
        #endif
    }
};

class CEsdlTransformOperationTraceOptionsScope : public CEsdlTransformOperationWithChildren
{
private:
    Owned<ICompiledXpath> m_enabled;
    Owned<ICompiledXpath> m_locked;

public:
    CEsdlTransformOperationTraceOptionsScope(IXmlPullParser& xpp, StartTag& stag, const StringBuffer& prefix, IEsdlFunctionRegister* functionRegister)
        : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, functionRegister, nullptr)
    {
        const char* enabled = stag.getValue("enabled");
        if (!isEmptyString(enabled))
            m_enabled.setown(compileXpath(enabled));

        const char* locked = stag.getValue("locked");
        if (!isEmptyString(locked))
            m_locked.setown(compileXpath(locked));
    }

    virtual ~CEsdlTransformOperationTraceOptionsScope()
    {
    }

    virtual bool exec(CriticalSection* crit, IInterface* preparedForAsync, IEsdlScriptContext* scriptContext, IXpathContext* targetContext, IXpathContext* sourceContext) override
    {
        EsdlScriptTraceOptionsScope traceOptionsScope(scriptContext);
        bool locked = scriptContext->isTraceLocked();
        if ((m_enabled || m_locked) && !locked)
        {
            bool enabled = (m_enabled ? sourceContext->evaluateAsBoolean(m_enabled) : scriptContext->isTraceEnabled());
            if (m_locked)
                locked = sourceContext->evaluateAsBoolean(m_locked);
            scriptContext->setTraceOptions(enabled, locked);
        }
        processChildren(scriptContext, targetContext, sourceContext);
        return true;
    }

    virtual void toDBGLog () override
    {
    #if defined(_DEBUG)
        DBGLOG(">>>%s> %s  >>>>", m_traceName.str(), m_tagname.str());
        CEsdlTransformOperationWithChildren::toDBGLog();
        DBGLOG (">>>>>>>>>>> %s >>>>>>>>>>", m_tagname.str());
    #endif
    }
};

IEsdlTransformOperation *createEsdlTransformOperation(IXmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, IEsdlOperationTraceMessenger& messenger, IEsdlFunctionRegister *functionRegister, bool canDeclareFunctions)
{
    StartTag stag;
    xpp.readStartTag(stag);
    const char *op = stag.getLocalName();
    if (isEmptyString(op))
        return nullptr;
    if (functionRegister)
    {
        if (streq(op, "function"))
        {
            if (!canDeclareFunctions)
                messenger.recordError(ESDL_SCRIPT_Error, "can only declare functions at root level");
            Owned<CEsdlTransformOperationFunction> esdlFunc = new CEsdlTransformOperationFunction(xpp, stag, prefix, functionRegister);
            functionRegister->registerEsdlFunction(esdlFunc->m_name.str(), static_cast<IEsdlTransformOperation*>(esdlFunc.get()));
            return nullptr;
        }
    }
    if (withVariables)
    {
        if (streq(op, "variable"))
            return new CEsdlTransformOperationVariable(xpp, stag, prefix, functionRegister);
        if (streq(op, "param"))
            return new CEsdlTransformOperationParameter(xpp, stag, prefix, functionRegister);
    }
    if (streq(op, "choose"))
        return new CEsdlTransformOperationChoose(xpp, stag, prefix, functionRegister);
    if (streq(op, "for-each"))
        return new CEsdlTransformOperationForEach(xpp, stag, prefix, functionRegister);
    if (streq(op, "if"))
        return new CEsdlTransformOperationConditional(xpp, stag, prefix, functionRegister);
    if (streq(op, "set-value") || streq(op, "SetValue"))
        return new CEsdlTransformOperationSetValue(xpp, stag, prefix);
    if (streq(op, "append-to-value") || streq(op, "AppendValue"))
        return new CEsdlTransformOperationAppendValue(xpp, stag, prefix);
    if (streq(op, "add-value"))
        return new CEsdlTransformOperationAddValue(xpp, stag, prefix);
    if (streq(op, "fail"))
        return new CEsdlTransformOperationFail(xpp, stag, prefix);
    if (streq(op, "assert"))
        return new CEsdlTransformOperationAssert(xpp, stag, prefix);
    if (streq(op, "store-value"))
        return new CEsdlTransformOperationStoreValue(xpp, stag, prefix);
    if (streq(op, "set-log-profile"))
        return new CEsdlTransformOperationSetLogProfile(xpp, stag, prefix);
    if (streq(op, "set-log-option"))
        return new CEsdlTransformOperationSetLogOption(xpp, stag, prefix);
    if (streq(op, "rename-node"))
        return new CEsdlTransformOperationRenameNode(xpp, stag, prefix);
    if (streq(op, "remove-node"))
        return new CEsdlTransformOperationRemoveNode(xpp, stag, prefix);
    if (streq(op, "source"))
        return new CEsdlTransformOperationSource(xpp, stag, prefix, functionRegister);
    if (streq(op, "if-source"))
        return new CEsdlTransformOperationIfSource(xpp, stag, prefix, functionRegister);
    if (streq(op, "target"))
        return new CEsdlTransformOperationTarget(xpp, stag, prefix, functionRegister);
    if (streq(op, "if-target"))
        return new CEsdlTransformOperationIfTarget(xpp, stag, prefix, functionRegister);
    if (streq(op, "ensure-target"))
        return new CEsdlTransformOperationEnsureTarget(xpp, stag, prefix, functionRegister);
    if (streq(op, "element"))
        return new CEsdlTransformOperationElement(xpp, stag, prefix, functionRegister);
    if (streq(op, "copy-of"))
        return new CEsdlTransformOperationCopyOf(xpp, stag, prefix);
    if (streq(op, "namespace"))
        return new CEsdlTransformOperationNamespace(xpp, stag, prefix);
    if (streq(op, "http-post-xml"))
        return new CEsdlTransformOperationHttpPostXml(xpp, stag, prefix, functionRegister);
    if (streq(op, "mysql"))
        return new CEsdlTransformOperationMySqlCall(xpp, stag, prefix);
    if (streq(op, "trace-content") || streq(op, "trace"))
        return new CEsdlTransformOperationTraceContent(xpp, stag, prefix);
    if (streq(op, "trace-value"))
        return new CEsdlTransformOperationTraceValue(xpp, stag, prefix);
    if (streq(op, "call-function"))
        return new CEsdlTransformOperationCallFunction(xpp, stag, prefix, functionRegister);
    if (streq(op, "synchronize"))
        return new CEsdlTransformOperationSynchronize(xpp, stag, prefix, functionRegister);
    if (streq(op, "script"))
        return new CEsdlTransformOperationScript(xpp, stag, prefix, functionRegister);
    if (streq(op, "tx-summary-value"))
        return new CEsdlTransformOperationTxSummaryValue(xpp, stag, prefix);
    if (streq(op, "tx-summary-timer"))
        return new CEsdlTransformOperationTxSummaryTimer(xpp, stag, prefix, functionRegister);
    if (streq(op, "delay"))
        return new CEsdlTransformOperationDelay(xpp, stag, prefix);
    if (streq(op, "update-masking-context"))
        return new CEsdlTransformOperationUpdateMaskingContext(xpp, stag, prefix);
    if (streq(op, "masking-context-scope"))
        return new CEsdlTransformOperationMaskingContextScope(xpp, stag, prefix, functionRegister);
    if (streq(op, "set-trace-options"))
        return new CEsdlTransformOperationSetTraceOptions(xpp, stag, prefix);
    if (streq(op, "trace-options-scope"))
        return new CEsdlTransformOperationTraceOptionsScope(xpp, stag, prefix, functionRegister);
    return nullptr;
}

static inline void replaceVariable(StringBuffer &s, IXpathContext *xpathContext, const char *name)
{
    StringBuffer temp;
    const char *val = xpathContext->getVariable(name, temp);
    if (val)
    {
        VStringBuffer match("{$%s}", name);
        s.replaceString(match, val);
    }
}

class CEsdlFunctionRegister : public CInterfaceOf<IEsdlFunctionRegister>
{
private:
    CEsdlFunctionRegister *parent = nullptr; //usual hierarchy of function registries is: service / method / script
    MapStringToMyClass<IEsdlTransformOperation> functions;
    IArrayOf<CEsdlTransformOperationCallFunction> functionCalls;

    //noLocalFunctions
    // - true if we are in an entry point of the type "functions".. adding functions to the service or method scopes, not locally.
    // - false if we are defining a function for local use within an idividual script.
    bool noLocalFunctions = false;

public:
    CEsdlFunctionRegister(CEsdlFunctionRegister *_parent, bool _noLocalFunctions) : parent(_parent), noLocalFunctions(_noLocalFunctions)
    {
        if (noLocalFunctions)
           assertex(parent); //should never happen
    }
    virtual void registerEsdlFunction(const char *name, IEsdlTransformOperation *esdlFunc) override
    {
        if (noLocalFunctions) //register with parent, not locally
            parent->registerEsdlFunction(name, esdlFunc);
        else
            functions.setValue(name, esdlFunc);
    }
    virtual IEsdlTransformOperation *findEsdlFunction(const char *name, bool localOnly) override
    {
        IEsdlTransformOperation *esdlFunc =  functions.getValue(name);
        if (!esdlFunc && !localOnly && parent)
            return parent->findEsdlFunction(name, false);
        return esdlFunc;
    }
    virtual void registerEsdlFunctionCall(IEsdlTransformOperation *esdlFuncCall) override
    {
        //in the case of call-function, if noLocalFunctions is true we are a call-function inside another function that is not local inside a script
        //  so same registration semantics apply
        if (noLocalFunctions) //register with parent, not locally
            parent->registerEsdlFunctionCall(esdlFuncCall);
        else
            functionCalls.append(*LINK(static_cast<CEsdlTransformOperationCallFunction*>(esdlFuncCall)));
    }
    void bindFunctionCalls(const char *scopeDescr, IEsdlFunctionRegister *activeFunctionRegister, bool bindLocalOnly)
    {
        ForEachItemIn(idx, functionCalls)
            functionCalls.item(idx).bindFunctionCall(scopeDescr, activeFunctionRegister, bindLocalOnly);
    }
};

class CEsdlOperationDefaultTraceMessenger : public IEsdlOperationTraceMessenger
{
public:
    virtual void recordWarning(int code, const char* msg) const override
    {
        esdlOperationWarning(code, m_tagname, msg, m_traceName);
    }
    virtual void recordError(int code, const char* msg) const override
    {
        esdlOperationError(code, m_tagname, msg, m_traceName, true);
    }
    virtual void recordException(int code, const char* msg) const override
    {
        esdlOperationError(code, m_tagname, msg, m_traceName, true);
    }
protected:
    const char* m_tagname;
    const StringAttr& m_traceName;
public:
    CEsdlOperationDefaultTraceMessenger(const char* tagname, const StringAttr& traceName)
        : m_tagname(tagname)
        , m_traceName(traceName)
    {
    }
};

class CEsdlCustomTransform : public CInterfaceOf<IEsdlCustomTransform>
{
private:
    IArrayOf<IEsdlTransformOperation> m_operations;
    CEsdlFunctionRegister functionRegister;

    Owned<IProperties> namespaces = createProperties(false);
    StringAttr m_name;
    StringAttr m_target;
    StringAttr m_source;
    StringBuffer m_prefix;

public:
    CEsdlCustomTransform(CEsdlFunctionRegister *parentRegister, bool loadingCommonFunctions)
        : functionRegister(parentRegister, loadingCommonFunctions) {}

    CEsdlCustomTransform(IXmlPullParser &xpp, StartTag &stag, const char *ns_prefix, CEsdlFunctionRegister *parentRegister, bool loadingCommonFunctions)
        : functionRegister(parentRegister, loadingCommonFunctions), m_prefix(ns_prefix)
    {
        const char *tag = stag.getLocalName();

        m_name.set(stag.getValue("name"));
        m_target.set(stag.getValue("target"));
        m_source.set(stag.getValue("source"));

        DBGLOG("Compiling ESDL Transform: '%s'", m_name.str());

        std::map< std::string, const SXT_CHAR* >::const_iterator it = xpp.getNsBegin();
        while (it != xpp.getNsEnd())
        {
            if (it->first.compare("xml")!=0)
                namespaces->setProp(it->first.c_str(), it->second);
            it++;
        }

        CEsdlOperationDefaultTraceMessenger defaultMessenger(tag, m_name);
        int type = 0;
        while((type = xpp.next()) != XmlPullParser::END_TAG)
        {
            if (XmlPullParser::START_TAG == type)
            {
                Owned<IEsdlTransformOperation> operation = createEsdlTransformOperation(xpp, m_prefix, true, defaultMessenger, &functionRegister, true);
                if (operation)
                    m_operations.append(*operation.getClear());
            }
        }
    }

    virtual void appendPrefixes(StringArray &prefixes) override
    {
        if (m_prefix.length())
        {
            StringAttr copy(m_prefix.str(), m_prefix.length()-1); //remove the colon
            prefixes.appendUniq(copy.str());
        }
        else
            prefixes.appendUniq("");
    }
    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">>>>>>>>>>>>>>>>transform: '%s'>>>>>>>>>>", m_name.str());
        ForEachItemIn(i, m_operations)
            m_operations.item(i).toDBGLog();
        DBGLOG("<<<<<<<<<<<<<<<<transform<<<<<<<<<<<<");
#endif
     }

    virtual ~CEsdlCustomTransform(){}

    void processTransformImpl(IEsdlScriptContext * scriptContext, const char *srcSection, const char *tgtSection, IXpathContext *sourceContext, const char *target) override
    {
        if (m_target.length())
            target = m_target.str();
        Owned<IXpathContext> targetXpath = nullptr;
        if (isEmptyString(tgtSection))
            targetXpath.setown(scriptContext->createXpathContext(sourceContext, srcSection, true));
        else
            targetXpath.setown(scriptContext->getCopiedSectionXpathContext(sourceContext, tgtSection, srcSection, true));

        Owned<IProperties> savedNamespaces = createProperties(false);
        Owned<IPropertyIterator> ns = namespaces->getIterator();
        ForEach(*ns)
        {
            const char *prefix = ns->getPropKey();
            const char *existing = sourceContext->queryNamespace(prefix);
            savedNamespaces->setProp(prefix, isEmptyString(existing) ? "" : existing);
            sourceContext->registerNamespace(prefix, namespaces->queryProp(prefix));
            targetXpath->registerNamespace(prefix, namespaces->queryProp(prefix));
        }
        CXpathContextScope scope(sourceContext, "transform", XpathVariableScopeType::simple, savedNamespaces);
        if (!isEmptyString(target) && !streq(target, "."))
            targetXpath->setLocation(target, true);
        if (!m_source.isEmpty() && !streq(m_source, "."))
            sourceContext->setLocation(m_source, true);
        ForEachItemIn(i, m_operations)
            m_operations.item(i).process(scriptContext, targetXpath, sourceContext);
        scriptContext->cleanupTemporaries();
    }
    void bindFunctionCalls(const char *scopeDescr, IEsdlFunctionRegister *activeFunctionRegister, bool bindLocalOnly)
    {
        functionRegister.bindFunctionCalls(scopeDescr, activeFunctionRegister, bindLocalOnly);
    }

    void processTransform(IEsdlScriptContext * scriptCtx, const char *srcSection, const char *tgtSection) override;
};

class CEsdlCustomTransformWrapper : public CInterfaceOf<IEsdlTransformSet>
{
    Linked<CEsdlCustomTransform> crt;
public:
    CEsdlCustomTransformWrapper(CEsdlCustomTransform *t) : crt(t) {}
    void processTransformImpl(IEsdlScriptContext * context, const char *srcSection, const char *tgtSection, IXpathContext *sourceContext, const char *target) override
    {
        crt->processTransformImpl(context, srcSection, tgtSection, sourceContext, target);
    }
    void appendPrefixes(StringArray &prefixes) override
    {
        crt->appendPrefixes(prefixes);
    }
    aindex_t length() override
    {
        return crt ? 1 : 0;
    }
};

void CEsdlCustomTransform::processTransform(IEsdlScriptContext * scriptCtx, const char *srcSection, const char *tgtSection)
{
    CEsdlCustomTransformWrapper tfw(this);
    processServiceAndMethodTransforms(scriptCtx, {static_cast<IEsdlTransformSet*>(&tfw)}, srcSection, tgtSection);
}

void processServiceAndMethodTransforms(IEsdlScriptContext * scriptCtx, std::initializer_list<IEsdlTransformSet *> const &transforms, const char *srcSection, const char *tgtSection)
{
    LogLevel level = LogMin;
    if (!scriptCtx)
        return;
    if (!transforms.size())
        return;
    if (isEmptyString(srcSection))
    {
      if (!isEmptyString(tgtSection))
        return;
    }
    level = (LogLevel) scriptCtx->getXPathInt64("target/*/@traceLevel", level);

    const char *method = scriptCtx->queryAttribute(ESDLScriptCtxSection_ESDLInfo, "method");
    if (isEmptyString(method))
        throw MakeStringException(ESDL_SCRIPT_Error, "ESDL script method name not set");
    const char *service = scriptCtx->queryAttribute(ESDLScriptCtxSection_ESDLInfo, "service");
    if (isEmptyString(service))
        throw MakeStringException(ESDL_SCRIPT_Error, "ESDL script service name not set");
    const char *reqtype = scriptCtx->queryAttribute(ESDLScriptCtxSection_ESDLInfo, "request_type");
    if (isEmptyString(reqtype))
        throw MakeStringException(ESDL_SCRIPT_Error, "ESDL script request name not set");

    IEspContext *context = scriptCtx->queryEspContext();

    if (level >= LogMax)
    {
        StringBuffer logtxt;
        scriptCtx->toXML(logtxt, srcSection, false);
        DBGLOG("ORIGINAL content: %s", logtxt.str());
        scriptCtx->toXML(logtxt.clear(), ESDLScriptCtxSection_BindingConfig);
        DBGLOG("BINDING CONFIG: %s", logtxt.str());
        scriptCtx->toXML(logtxt.clear(), ESDLScriptCtxSection_TargetConfig);
        DBGLOG("TARGET CONFIG: %s", logtxt.str());
    }

    bool strictParams = scriptCtx->getXPathBool("config/*/@strictParams", false);
    Owned<IXpathContext> sourceContext = scriptCtx->createXpathContext(nullptr, srcSection, strictParams);

    StringArray prefixes;
    for ( IEsdlTransformSet * const & item : transforms)
    {
        if (item)
            item->appendPrefixes(prefixes);
    }

    registerEsdlXPathExtensions(sourceContext, scriptCtx, prefixes);

    VStringBuffer ver("%g", context->getClientVersion());
    if(!sourceContext->addVariable("clientversion", ver.str()))
        OERRLOG("Could not set ESDL Script variable: clientversion:'%s'", ver.str());

    //in case transform wants to make use of these values:
    //make them few well known values variables rather than inputs so they are automatically available
    StringBuffer temp;
    sourceContext->addVariable("query", scriptCtx->getXPathString("target/*/@queryname", temp));

    ISecUser *user = context->queryUser();
    if (user)
    {
        static const std::map<SecUserStatus, const char*> statusLabels =
        {
#define STATUS_LABEL_NODE(s) { s, #s }
            STATUS_LABEL_NODE(SecUserStatus_Inhouse),
            STATUS_LABEL_NODE(SecUserStatus_Active),
            STATUS_LABEL_NODE(SecUserStatus_Exempt),
            STATUS_LABEL_NODE(SecUserStatus_FreeTrial),
            STATUS_LABEL_NODE(SecUserStatus_csdemo),
            STATUS_LABEL_NODE(SecUserStatus_Rollover),
            STATUS_LABEL_NODE(SecUserStatus_Suspended),
            STATUS_LABEL_NODE(SecUserStatus_Terminated),
            STATUS_LABEL_NODE(SecUserStatus_TrialExpired),
            STATUS_LABEL_NODE(SecUserStatus_Status_Hold),
            STATUS_LABEL_NODE(SecUserStatus_Unknown),
#undef STATUS_LABEL_NODE
        };

        Owned<IPropertyIterator> userPropIt = user->getPropertyIterator();
        ForEach(*userPropIt)
        {
            const char *name = userPropIt->getPropKey();
            if (name && *name)
                sourceContext->addInputValue(name, user->getProperty(name));
        }

        auto it = statusLabels.find(user->getStatus());

        sourceContext->addInputValue("espTransactionID", context->queryTransactionID());
        sourceContext->addInputValue("espUserName", user->getName());
        sourceContext->addInputValue("espUserRealm", user->getRealm() ? user->getRealm() : "");
        sourceContext->addInputValue("espUserPeer", user->getPeer() ? user->getPeer() : "");
        sourceContext->addInputValue("espUserStatus", VStringBuffer("%d", int(user->getStatus())));
        if (it != statusLabels.end())
            sourceContext->addInputValue("espUserStatusString", it->second);
        else
            throw MakeStringException(ESDL_SCRIPT_Error, "encountered unexpected secure user status (%d) while processing transform", int(user->getStatus()));
    }
    else
    {
        // Input values do not default to empty when added. They default to empty when
        // imported as parameters. Setting empty here serves no purpose.
    }

    StringBuffer defaultTarget; //This default gives us backward compatibility with only being able to write to the actual request
    StringBuffer queryName;
    const char *tgtQueryName = scriptCtx->getXPathString("target/*/@queryname", queryName);
    if (!isEmptyString(srcSection) && streq(srcSection, ESDLScriptCtxSection_ESDLRequest))
        defaultTarget.setf("soap:Body/%s/%s", tgtQueryName ? tgtQueryName : method, reqtype);

    for ( auto&& item : transforms)
    {
        if (item)
        {
            item->processTransformImpl(scriptCtx, srcSection, tgtSection, sourceContext, defaultTarget);
        }
    }

    if (level >= LogMax)
    {
        StringBuffer content;
        scriptCtx->toXML(content);
        DBGLOG(1,"Entire script context after transforms: %s", content.str());
    }
}

IEsdlCustomTransform *createEsdlCustomTransform(const char *scriptXml, const char *ns_prefix)
{
    if (isEmptyString(scriptXml))
        return nullptr;
    std::unique_ptr<fxpp::IFragmentedXmlPullParser> xpp(fxpp::createParser());
    int bufSize = strlen(scriptXml);
    xpp->setSupportNamespaces(true);
    xpp->setInput(scriptXml, bufSize);

    int type;
    StartTag stag;
    EndTag etag;


    while((type = xpp->next()) != XmlPullParser::END_DOCUMENT)
    {
        if(type == XmlPullParser::START_TAG)
        {
            StartTag stag;
            xpp->readStartTag(stag);
            if (strieq(stag.getLocalName(), "Transforms")) //allow common mistake,.. starting with the outer tag, not the script
                continue;
            return new CEsdlCustomTransform(*xpp, stag, ns_prefix, nullptr, false);
        }
    }
    return nullptr;
}

class CEsdlTransformSet : public CInterfaceOf<IEsdlTransformSet>
{
    IArrayOf<CEsdlCustomTransform> transforms;
    CEsdlFunctionRegister *functions = nullptr;

public:
    CEsdlTransformSet(CEsdlFunctionRegister *_functions) : functions(_functions)
    {
    }
    virtual void appendPrefixes(StringArray &prefixes) override
    {
        ForEachItemIn(i, transforms)
            transforms.item(i).appendPrefixes(prefixes);
    }

    virtual void processTransformImpl(IEsdlScriptContext * scriptContext, const char *srcSection, const char *tgtSection, IXpathContext *sourceContext, const char *target) override
    {
        ForEachItemIn(i, transforms)
            transforms.item(i).processTransformImpl(scriptContext, srcSection, tgtSection, sourceContext, target);
    }
    virtual void add(IXmlPullParser &xpp, StartTag &stag)
    {
        transforms.append(*new CEsdlCustomTransform(xpp, stag, nullptr, functions, false));
    }
    virtual aindex_t length() override
    {
        return transforms.length();
    }
    void bindFunctionCalls(const char *scopeDescr, IEsdlFunctionRegister *activeFunctionRegister, bool bindLocalOnly)
    {
        ForEachItemIn(idx, transforms)
            transforms.item(idx).bindFunctionCalls(scopeDescr, activeFunctionRegister, bindLocalOnly);
    }

};

class CEsdlTransformEntryPointMap : public CInterfaceOf<IEsdlTransformEntryPointMap>
{
    MapStringToMyClass<CEsdlTransformSet> map;
    CEsdlFunctionRegister functionRegister;

public:
    CEsdlTransformEntryPointMap(CEsdlFunctionRegister *parentRegister) : functionRegister(parentRegister, false)
    {
    }

    ~CEsdlTransformEntryPointMap(){}

    virtual IEsdlFunctionRegister *queryFunctionRegister() override
    {
        return &functionRegister;
    }

    void addFunctions(IXmlPullParser &xpp, StartTag &esdlFuncTag)
    {
        //child functions will be loaded directly into the common register, container class is then no longer needed
        CEsdlCustomTransform factory(xpp, esdlFuncTag, nullptr, &functionRegister, true);
    }

    virtual void addChild(IXmlPullParser &xpp, StartTag &childTag, bool &foundNonLegacyTransforms)
    {
        const char *tagname = childTag.getLocalName();
        if (streq("Scripts", tagname) || streq("Transforms", tagname)) //allow nesting of root structure
            add(xpp, childTag, foundNonLegacyTransforms);
        else if (streq(tagname, ESDLScriptEntryPoint_Functions))
            addFunctions(xpp, childTag);
        else
        {
            if (streq(tagname, ESDLScriptEntryPoint_Legacy))
                tagname = ESDLScriptEntryPoint_BackendRequest;
            else
                foundNonLegacyTransforms = true;
            CEsdlTransformSet *set = map.getValue(tagname);
            if (set)
                set->add(xpp, childTag);
            else
            {
                Owned<CEsdlTransformSet> set = new CEsdlTransformSet(&functionRegister);
                map.setValue(tagname, set);
                set->add(xpp, childTag);
            }
        }
    }

    virtual void add(IXmlPullParser &xpp, StartTag &stag, bool &foundNonLegacyTransforms)
    {
        int type;
        StartTag childTag;
        while((type = xpp.next()) != XmlPullParser::END_TAG)
        {
            if (XmlPullParser::START_TAG == type)
            {
                xpp.readStartTag(childTag);
                const char *tagname = childTag.getLocalName();
                if (streq("Scripts", tagname) || streq("Transforms", tagname)) //allow nesting of container structures for maximum compatability
                    add(xpp, childTag, foundNonLegacyTransforms);
                else
                    addChild(xpp, childTag,foundNonLegacyTransforms);
            }
        }
    }
    void add(const char *scriptXml, bool &foundNonLegacyTransforms)
    {
        if (isEmptyString(scriptXml))
            return;
        std::unique_ptr<XmlPullParser> xpp(new XmlPullParser());
        int bufSize = strlen(scriptXml);
        xpp->setSupportNamespaces(true);
        xpp->setInput(scriptXml, bufSize);

        int type;
        StartTag stag;
        while((type = xpp->next()) != XmlPullParser::END_DOCUMENT)
        {
            switch (type)
            {
                case XmlPullParser::START_TAG:
                {
                    xpp->readStartTag(stag);
                    addChild(*xpp, stag, foundNonLegacyTransforms);
                    break;
                }
            }
        }
    }

    virtual IEsdlTransformSet *queryEntryPoint(const char *name) override
    {
        return map.getValue(name);
    }
    virtual void removeEntryPoint(const char *name) override
    {
        map.remove(name);
    }
    void bindFunctionCalls(const char *scopeDescr, IEsdlFunctionRegister *activeFunctionRegister, bool bindLocalOnly)
    {
        functionRegister.bindFunctionCalls(scopeDescr, activeFunctionRegister, bindLocalOnly);
        HashIterator it(map);
        ForEach (it)
        {
            CEsdlTransformSet *item = map.getValue((const char *)it.query().getKey());
            if (item)
                item->bindFunctionCalls(scopeDescr, activeFunctionRegister, bindLocalOnly);
        }
    }
};

class CEsdlTransformMethodMap : public CInterfaceOf<IEsdlTransformMethodMap>
{
    MapStringToMyClass<CEsdlTransformEntryPointMap> map;
    Owned<CEsdlTransformEntryPointMap> service;
    IEsdlFunctionRegister *serviceFunctionRegister = nullptr;

public:
    CEsdlTransformMethodMap()
    {
        //ensure the service entry (name = "") exists right away, the function hierarchy depends on it
        service.setown(new CEsdlTransformEntryPointMap(nullptr));
        serviceFunctionRegister = service->queryFunctionRegister();
        map.setValue("", service.get());
    }
    virtual IEsdlTransformEntryPointMap *queryMethod(const char *name) override
    {
        return map.getValue(name);
    }
    virtual IEsdlFunctionRegister *queryFunctionRegister(const char *method) override
    {
        IEsdlTransformEntryPointMap *methodEntry = queryMethod(method);
        return (methodEntry) ? methodEntry->queryFunctionRegister() : nullptr;
    }
    virtual IEsdlTransformSet *queryMethodEntryPoint(const char *method, const char *name) override
    {
        IEsdlTransformEntryPointMap *epm = queryMethod(method);
        if (epm)
            return epm->queryEntryPoint(name);
        return nullptr;
    }

    virtual void removeMethod(const char *name) override
    {
        map.remove(name);
        if (isEmptyString(name))
        {
            service.setown(new CEsdlTransformEntryPointMap(nullptr));
            serviceFunctionRegister = service->queryFunctionRegister();
        }
    }
    virtual void addMethodTransforms(const char *method, const char *scriptXml, bool &foundNonLegacyTransforms) override
    {
        try
        {
            CEsdlTransformEntryPointMap *entry = map.getValue(method ? method : "");
            if (entry)
                entry->add(scriptXml, foundNonLegacyTransforms);
            else
            {
                //casting up from interface to known implementation, do it explicitly
                Owned<CEsdlTransformEntryPointMap> epm = new CEsdlTransformEntryPointMap(static_cast<CEsdlFunctionRegister*>(serviceFunctionRegister));
                epm->add(scriptXml, foundNonLegacyTransforms);
                map.setValue(method, epm.get());
            }
        }
        catch (XmlPullParserException& xppe)
        {
            VStringBuffer msg("Error parsing ESDL transform script (method '%s', line %d, col %d) %s", method ? method : "", xppe.getLineNumber(), xppe.getColumnNumber(), xppe.what());
            IERRLOG("%s", msg.str());
            throw MakeStringException(ESDL_SCRIPT_Error, "%s", msg.str());
        }
    }
    virtual void bindFunctionCalls() override
    {
        HashIterator it(map);
        ForEach (it)
        {
            const char *method = (const char *)it.query().getKey();
            if (isEmptyString(method)) //we validate the service level function calls against each method, not separately, (they are quasi virtual)
                continue;
            CEsdlTransformEntryPointMap *item = map.getValue(method);
            if (item)
            {
                item->bindFunctionCalls(method, item->queryFunctionRegister(), false);

                //service level function calls are resolved at runtime unless defined locally and called from the same script
                //  but we can issue a warning if they can't be resolved while handling the current method
                VStringBuffer serviceScopeDescr("service level at method %s", method);
                service->bindFunctionCalls(serviceScopeDescr, item->queryFunctionRegister(), true);
            }
        }
    }
};

esdl_decl IEsdlScriptContext* createEsdlScriptContext(IEspContext* espCtx, IEsdlFunctionRegister* functionRegister, IDataMaskingEngine* engine)
{
    return new CEsdlScriptContext(espCtx, functionRegister, engine);
}
esdl_decl IEsdlTransformMethodMap *createEsdlTransformMethodMap()
{
    return new CEsdlTransformMethodMap();
}
