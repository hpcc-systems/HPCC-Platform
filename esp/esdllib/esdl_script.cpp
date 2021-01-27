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

#include <xpp/XmlPullParser.h>
using namespace xpp;

interface IEsdlTransformOperation : public IInterface
{
    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) = 0;
    virtual void toDBGLog() = 0;
};

IEsdlTransformOperation *createEsdlTransformOperation(XmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, bool ignoreCodingErrors);
void createEsdlTransformOperations(IArrayOf<IEsdlTransformOperation> &operations, XmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, bool ignoreCodingErrors);
void createEsdlTransformChooseOperations(IArrayOf<IEsdlTransformOperation> &operations, XmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, bool ignoreCodingErrors);
typedef void (*esdlOperationsFactory_t)(IArrayOf<IEsdlTransformOperation> &operations, XmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, bool ignoreCodingErrors);

bool getStartTagValueBool(StartTag &stag, const char *name, bool defaultValue)
{
    if (isEmptyString(name))
        return defaultValue;
    const char *value = stag.getValue(name);
    if (isEmptyString(value))
        return defaultValue;
    return strToBool(value);
}

inline void esdlOperationError(int code, const char *op, const char *msg, const char *traceName, bool exception)
{
    StringBuffer s("ESDL Script: ");
    if (!isEmptyString(traceName))
        s.append(" '").append(traceName).append("' ");
    if (!isEmptyString(op))
        s.append(" ").append(op).append(" ");
    s.append(msg);
    if(exception)
        throw MakeStringException(code, "%s", s.str());

    IERRLOG("%s", s.str());
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

class CEsdlTransformOperationBase : public CInterfaceOf<IEsdlTransformOperation>
{
protected:
    StringAttr m_tagname;
    StringAttr m_traceName;
    bool m_ignoreCodingErrors = false; //ideally used only for debugging

public:
    CEsdlTransformOperationBase(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix)
    {
        m_tagname.set(stag.getLocalName());
        m_traceName.set(stag.getValue("trace"));
        m_ignoreCodingErrors = getStartTagValueBool(stag, "optional", false);
    }
};

class CEsdlTransformOperationWithChildren : public CEsdlTransformOperationBase
{
protected:
    IArrayOf<IEsdlTransformOperation> m_children;
    bool m_withVariables = false;

public:
    CEsdlTransformOperationWithChildren(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, bool withVariables, esdlOperationsFactory_t factory) : CEsdlTransformOperationBase(xpp, stag, prefix), m_withVariables(withVariables)
    {
        //load children
        if (factory)
            factory(m_children, xpp, prefix, withVariables, m_ignoreCodingErrors);
        else
            createEsdlTransformOperations(m_children, xpp, prefix, withVariables, m_ignoreCodingErrors);
    }

    virtual ~CEsdlTransformOperationWithChildren(){}

    virtual bool processChildren(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext)
    {
        if (!m_children.length())
            return false;

        Owned<CXpathContextScope> scope = m_withVariables ? new CXpathContextScope(sourceContext, m_tagname) : nullptr;
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
    CEsdlTransformOperationWithoutChildren(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationBase(xpp, stag, prefix)
    {
        if (xpp.skipSubTreeEx())
            esdlOperationError(ESDL_SCRIPT_Error, m_tagname, "should not have child tags", m_traceName, !m_ignoreCodingErrors);
    }

    virtual ~CEsdlTransformOperationWithoutChildren(){}

};

class CEsdlTransformOperationVariable : public CEsdlTransformOperationWithChildren
{
protected:
    StringAttr m_name;
    Owned<ICompiledXpath> m_select;

public:
    CEsdlTransformOperationVariable(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, nullptr)
    {
        if (m_traceName.isEmpty())
            m_traceName.set(stag.getValue("name"));
        m_name.set(stag.getValue("name"));
        if (m_name.isEmpty())
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without name", m_traceName, !m_ignoreCodingErrors);
        const char *select = stag.getValue("select");
        if (!isEmptyString(select))
            m_select.setown(compileXpath(select));
    }

    virtual ~CEsdlTransformOperationVariable()
    {
    }

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
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
    CEsdlTransformOperationHttpContentXml(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, nullptr)
    {
    }

    virtual ~CEsdlTransformOperationHttpContentXml(){}

    bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
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


class CEsdlTransformOperationHttpHeader : public CEsdlTransformOperationWithoutChildren, implements IEsdlTransformOperationHttpHeader
{
protected:
    StringAttr m_name;
    Owned<ICompiledXpath> m_xpath_name;
    Owned<ICompiledXpath> m_value;

public:
    IMPLEMENT_IINTERFACE_USING(CEsdlTransformOperationWithoutChildren)

    CEsdlTransformOperationHttpHeader(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        m_name.set(stag.getValue("name"));
        const char *xpath_name = stag.getValue("xpath_name");
        if (m_name.isEmpty() && isEmptyString(xpath_name))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without name or xpath_name", m_traceName, !m_ignoreCodingErrors);
        if (!isEmptyString(xpath_name))
            m_xpath_name.setown(compileXpath(xpath_name));

        const char *value = stag.getValue("value");
        if (isEmptyString(value))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without value", m_traceName, !m_ignoreCodingErrors);
        m_value.setown(compileXpath(value));
    }

    virtual ~CEsdlTransformOperationHttpHeader(){}

    bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        return processHeader(scriptContext, targetContext, sourceContext, nullptr);
    }

    bool processHeader(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext, IProperties *headers) override
    {
        CXpathContextLocation location(targetContext);
        targetContext->addElementToLocation("header");
        StringBuffer name;
        if (m_xpath_name)
            sourceContext->evaluateAsString(m_xpath_name, name);
        else
            name.set(m_name);

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
        DBGLOG ("> %s (%s, value(%s)) >>>>>>>>>>", m_tagname.str(), m_xpath_name ? m_xpath_name->getXpath() : m_name.str(), m_value ? m_value->getXpath() : "");
    #endif
    }
};



class CEsdlTransformOperationHttpPostXml : public CEsdlTransformOperationBase
{
protected:
    StringAttr m_name;
    StringAttr m_section;
    Owned<ICompiledXpath> m_url;
    IArrayOf<IEsdlTransformOperationHttpHeader> m_headers;
    Owned<IEsdlTransformOperation> m_content;

public:
    CEsdlTransformOperationHttpPostXml(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationBase(xpp, stag, prefix)
    {
        m_name.set(stag.getValue("name"));
        if (m_traceName.isEmpty())
            m_traceName.set(m_name.str());
        m_section.set(stag.getValue("section"));
        if (m_section.isEmpty())
            m_section.set("temporaries");
        if (m_name.isEmpty())
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without name", m_traceName, !m_ignoreCodingErrors);
        const char *url = stag.getValue("url");
        if (isEmptyString(url))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without url", m_traceName, !m_ignoreCodingErrors);
        m_url.setown(compileXpath(url));

        int type = 0;
        while((type = xpp.next()) != XmlPullParser::END_DOCUMENT)
        {
            switch(type)
            {
                case XmlPullParser::START_TAG:
                {
                    StartTag stag;
                    xpp.readStartTag(stag);
                    const char *op = stag.getLocalName();
                    if (isEmptyString(op))
                        esdlOperationError(ESDL_SCRIPT_Error, m_tagname, "unknown error", m_traceName, !m_ignoreCodingErrors);
                    if (streq(op, "http-header"))
                        m_headers.append(*new CEsdlTransformOperationHttpHeader(xpp, stag, prefix));
                    else if (streq(op, "content"))
                        m_content.setown(new CEsdlTransformOperationHttpContentXml(xpp, stag, prefix));
                    else
                        xpp.skipSubTreeEx();
                    break;
                }
                case XmlPullParser::END_TAG:
                case XmlPullParser::END_DOCUMENT:
                    return;
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
    }

    void buildRequest(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext, const char *url, IProperties *headers)
    {
        CXpathContextLocation location(targetContext);
        targetContext->addElementToLocation("request");
        targetContext->ensureSetValue("@url", url, true);
        buildHeaders(scriptContext, targetContext, sourceContext, headers);
        if (m_content)
            m_content->process(scriptContext, targetContext, sourceContext);
    }

    void postRequest(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext, const char *url, IProperties *headers)
    {
        VStringBuffer xpath("/esdl_script_context/%s/%s/request/content/*[1]", m_section.str(), m_name.str());

        StringBuffer content;
        sourceContext->toXml(xpath, content);
        if (!content)
            return;
        CXpathContextLocation location(targetContext);
        targetContext->addElementToLocation("response");

        Owned<IHttpClientContext> httpCtx = getHttpClientContext();
        Owned<IHttpClient> httpclient = httpCtx->createHttpClient(NULL, url);
        if (!httpclient)
            return;

        try
        {
            StringBuffer status;
            StringBuffer response;
            StringBuffer errmsg;

            if (headers && !headers->hasProp("Accept"))
                headers->setProp("Accept", "text/html, application/xml");

            HttpClientErrCode code = HttpClientErrCode::OK;
            Owned<IHttpMessage> resp = httpclient->sendRequestEx("POST", "text/xml", content, code, errmsg, headers);
            targetContext->ensureSetValue("@status", status.str(), true);

            StringBuffer err;
            err.append((int) code);
            targetContext->ensureSetValue("@error-code", err.str(), true);
            if (code != HttpClientErrCode::OK)
                throw MakeStringException(ESDL_SCRIPT_Error, "ESDL Script error sending request in http-post-xml %s url(%s)", m_traceName.str(), url);

            resp->getStatus(status);
            targetContext->ensureSetValue("@status", status.str(), true);

            resp->getContent(response);
            if (!response.trim().length())
                throw MakeStringException(ESDL_SCRIPT_Error, "ESDL Script empty result calling http-post-xml %s url(%s)", m_traceName.str(), url);

            StringBuffer contentType;
            resp->getContentType(contentType);
            targetContext->ensureSetValue("@content-type", contentType.str(), true);
            if (strnicmp("text/xml", contentType.str(), 8)==0 || strnicmp("application/xml", contentType.str(), 15) ==0)
            {
                CXpathContextLocation content_location(targetContext);
                targetContext->addElementToLocation("content");
                targetContext->addXmlContent(response.str());
            }
            else
            {
                targetContext->ensureSetValue("@status", "error", true);
                targetContext->ensureSetValue("text", response.str(), true);
            }
        }
        catch(IMultiException *me)
        {
            StringBuffer xml;
            me->serialize(xml);
            CXpathContextLocation content_location(targetContext);
            targetContext->ensureSetValue("@status", "error", true);
            targetContext->addElementToLocation("content");
            targetContext->addXmlContent(xml.str());
            me->Release();
        }
        catch(IException *E)
        {
            StringBuffer xml;
            Owned<IMultiException> me = makeMultiException("ESDLScript");
            me->append(*LINK(E));
            me->serialize(xml);
            CXpathContextLocation content_location(targetContext);
            targetContext->ensureSetValue("@status", "error", true);
            targetContext->addElementToLocation("content");
            targetContext->addXmlContent(xml.str());
            E->Release();
        }
    }

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        VStringBuffer xpath("/esdl_script_context/%s/%s", m_section.str(), m_name.str());
        CXpathContextLocation location(targetContext);
        targetContext->ensureLocation(xpath, true);
        StringBuffer url;
        if (m_url)
            sourceContext->evaluateAsString(m_url, url);

        Owned<IProperties> headers = createProperties();
        buildRequest(scriptContext, targetContext, sourceContext, url, headers);
        postRequest(scriptContext, targetContext, sourceContext, url, headers);

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
public:
    CEsdlTransformOperationParameter(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationVariable(xpp, stag, prefix)
    {
    }

    virtual ~CEsdlTransformOperationParameter()
    {
    }

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        if (m_select)
            return sourceContext->declareCompiledParameter(m_name, m_select);
        return sourceContext->declareParameter(m_name, "");
    }
};

class CEsdlTransformOperationSetSectionAttributeBase : public CEsdlTransformOperationWithoutChildren
{
protected:
    StringAttr m_name;
    Owned<ICompiledXpath> m_xpath_name;
    Owned<ICompiledXpath> m_select;

public:
    CEsdlTransformOperationSetSectionAttributeBase(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix, const char *attrName) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        if (m_traceName.isEmpty())
            m_traceName.set(stag.getValue("name"));
        if (!isEmptyString(attrName))
            m_name.set(attrName);
        else
        {
            m_name.set(stag.getValue("name"));

            const char *xpath_name = stag.getValue("xpath_name");
            if (!isEmptyString(xpath_name))
                m_xpath_name.setown(compileXpath(xpath_name));
            else if (m_name.isEmpty())
                esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without name", m_traceName, !m_ignoreCodingErrors); //don't mention value, it's deprecated
        }

        const char *select = stag.getValue("select");
        if (isEmptyString(select))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without select", m_traceName, !m_ignoreCodingErrors); //don't mention value, it's deprecated
        m_select.setown(compileXpath(select));
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(name(%s), select('%s'))", m_traceName.str(), m_tagname.str(), (m_xpath_name) ? m_xpath_name->getXpath() : m_name.str(), m_select->getXpath());
#endif
    }

    virtual ~CEsdlTransformOperationSetSectionAttributeBase(){}

    virtual const char *getSectionName() = 0;

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        if ((!m_name && !m_xpath_name) || !m_select)
            return false; //only here if "optional" backward compatible support for now (optional syntax errors aren't actually helpful)
        try
        {
            StringBuffer name;
            if (m_xpath_name)
                sourceContext->evaluateAsString(m_xpath_name, name);
            else
                name.set(m_name);

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
            esdlOperationError(code, m_tagname, msg, m_traceName, !m_ignoreCodingErrors);
        }
        catch (...)
        {
            esdlOperationError(ESDL_SCRIPT_Error, m_tagname, "unknown exception processing", m_traceName, !m_ignoreCodingErrors);
        }
        return false;
    }
};

class CEsdlTransformOperationStoreValue : public CEsdlTransformOperationSetSectionAttributeBase
{
public:
    CEsdlTransformOperationStoreValue(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetSectionAttributeBase(xpp, stag, prefix, nullptr)
    {
    }

    virtual ~CEsdlTransformOperationStoreValue(){}
    const char *getSectionName() override {return ESDLScriptCtxSection_Store;}
};

class CEsdlTransformOperationSetLogProfile : public CEsdlTransformOperationSetSectionAttributeBase
{
public:
    CEsdlTransformOperationSetLogProfile(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetSectionAttributeBase(xpp, stag, prefix, "profile")
    {
    }

    virtual ~CEsdlTransformOperationSetLogProfile(){}
    const char *getSectionName() override {return ESDLScriptCtxSection_Logging;}
};

class CEsdlTransformOperationSetLogOption : public CEsdlTransformOperationSetSectionAttributeBase
{
public:
    CEsdlTransformOperationSetLogOption(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetSectionAttributeBase(xpp, stag, prefix, nullptr)
    {
    }

    virtual ~CEsdlTransformOperationSetLogOption(){}
    const char *getSectionName() override {return ESDLScriptCtxSection_Logging;}
};

class CEsdlTransformOperationSetValue : public CEsdlTransformOperationWithoutChildren
{
protected:
    Owned<ICompiledXpath> m_select;
    Owned<ICompiledXpath> m_xpath_target;
    StringAttr m_target;
    bool m_required = true;

public:
    CEsdlTransformOperationSetValue(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        if (m_traceName.isEmpty())
            m_traceName.set(stag.getValue("name"));

        const char *xpath_target = stag.getValue("xpath_target");
        const char *target = stag.getValue("target");

        if (isEmptyString(target) && isEmptyString(xpath_target))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname.str(), "without target", m_traceName.str(), !m_ignoreCodingErrors);

        const char *select = stag.getValue("select");
        if (isEmptyString(select))
            select = stag.getValue("value");
        if (isEmptyString(select))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without select", m_traceName, !m_ignoreCodingErrors); //don't mention value, it's deprecated

        m_select.setown(compileXpath(select));
        if (!isEmptyString(xpath_target))
            m_xpath_target.setown(compileXpath(xpath_target));
        else if (!isEmptyString(target))
            m_target.set(target);
        m_required = getStartTagValueBool(stag, "required", true);
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(%s, select('%s'))", m_traceName.str(), m_tagname.str(), m_target.str(), m_select->getXpath());
#endif
    }

    virtual ~CEsdlTransformOperationSetValue(){}

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        if ((!m_xpath_target && m_target.isEmpty()) || !m_select)
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
            esdlOperationError(code, m_tagname, msg, m_traceName, !m_ignoreCodingErrors);
        }
        catch (...)
        {
            esdlOperationError(ESDL_SCRIPT_Error, m_tagname, "unknown exception processing", m_traceName, !m_ignoreCodingErrors);
        }
        return false;
    }

    const char *getTargetPath(IXpathContext * xpathContext, StringBuffer &s)
    {
        if (m_xpath_target)
        {
            xpathContext->evaluateAsString(m_xpath_target, s);
            return s;
        }
        return m_target.str();
    }
    virtual bool doSet(IXpathContext * sourceContext, IXpathContext *targetContext, const char *value)
    {
        StringBuffer xpath;
        const char *target = getTargetPath(sourceContext, xpath);
        targetContext->ensureSetValue(target, value, m_required);
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
    CEsdlTransformOperationNamespace(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        const char *pfx = stag.getValue("prefix");
        const char *uri = stag.getValue("uri");
        if (m_traceName.isEmpty())
            m_traceName.set(pfx);

        if (!pfx && isEmptyString(uri))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname.str(), "without prefix or uri", m_traceName.str(), !m_ignoreCodingErrors);
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

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        targetContext->setLocationNamespace(m_prefix, m_uri, m_current);
        return false;
    }
};

class CEsdlTransformOperationRenameNode : public CEsdlTransformOperationWithoutChildren
{
protected:
    StringAttr m_target;
    StringAttr m_new_name;
    Owned<ICompiledXpath> m_xpath_target;
    Owned<ICompiledXpath> m_xpath_new_name;
    bool m_all = false;

public:
    CEsdlTransformOperationRenameNode(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        const char *new_name = stag.getValue("new_name");
        const char *xpath_new_name = stag.getValue("xpath_new_name");
        if (isEmptyString(new_name) && isEmptyString(xpath_new_name))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname.str(), "without new name", m_traceName.str(), !m_ignoreCodingErrors);
        if (m_traceName.isEmpty())
            m_traceName.set(new_name ? new_name : xpath_new_name);

        const char *target = stag.getValue("target");
        const char *xpath_target = stag.getValue("xpath_target");
        if (isEmptyString(target) && isEmptyString(xpath_target))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname.str(), "without target", m_traceName.str(), !m_ignoreCodingErrors);

        if (!isEmptyString(xpath_target))
            m_xpath_target.setown(compileXpath(xpath_target));
        else if (!isEmptyString(target))
            m_target.set(target);

        if (!isEmptyString(xpath_new_name))
            m_xpath_new_name.setown(compileXpath(xpath_new_name));
        else if (!isEmptyString(new_name))
            m_new_name.set(new_name);
        m_all = getStartTagValueBool(stag, "all", false);
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        const char *target = (m_xpath_target) ? m_xpath_target->getXpath() : m_target.str();
        const char *new_name = (m_xpath_new_name) ? m_xpath_new_name->getXpath() : m_new_name.str();
        DBGLOG(">%s> %s(%s, new_name('%s'))", m_traceName.str(), m_tagname.str(), target, new_name);
#endif
    }

    virtual ~CEsdlTransformOperationRenameNode(){}

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        if ((!m_xpath_target && m_target.isEmpty()) || (!m_xpath_new_name && m_new_name.isEmpty()))
            return false; //only here if "optional" backward compatible support for now (optional syntax errors aren't actually helpful
        try
        {
            StringBuffer path;
            if (m_xpath_target)
                sourceContext->evaluateAsString(m_xpath_target, path);
            else
                path.set(m_target);

            StringBuffer name;
            if (m_xpath_new_name)
                sourceContext->evaluateAsString(m_xpath_new_name, name);
            else
                name.set(m_new_name);

            targetContext->rename(path, name, m_all);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            esdlOperationError(code, m_tagname, msg, m_traceName, !m_ignoreCodingErrors);
        }
        catch (...)
        {
            esdlOperationError(ESDL_SCRIPT_Error, m_tagname, "unknown exception processing", m_traceName, !m_ignoreCodingErrors);
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
    CEsdlTransformOperationCopyOf(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        const char *select = stag.getValue("select");
        if (isEmptyString(select))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname.str(), "without select", m_traceName.str(), !m_ignoreCodingErrors);

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

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
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
            esdlOperationError(code, m_tagname, msg, m_traceName, !m_ignoreCodingErrors);
        }
        catch (...)
        {
            esdlOperationError(ESDL_SCRIPT_Error, m_tagname, "unknown exception processing", m_traceName, !m_ignoreCodingErrors);
        }
        return false;
    }
};

class CEsdlTransformOperationRemoveNode : public CEsdlTransformOperationWithoutChildren
{
protected:
    StringAttr m_target;
    Owned<ICompiledXpath> m_xpath_target;
    bool m_all = false;

public:
    CEsdlTransformOperationRemoveNode(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        const char *target = stag.getValue("target");
        const char *xpath_target = stag.getValue("xpath_target");
        if (isEmptyString(target) && isEmptyString(xpath_target))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname.str(), "without target", m_traceName.str(), !m_ignoreCodingErrors);
        if (target && isWildString(target))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname.str(), "wildcard in target not yet supported", m_traceName.str(), !m_ignoreCodingErrors);

        if (!isEmptyString(xpath_target))
            m_xpath_target.setown(compileXpath(xpath_target));
        else if (!isEmptyString(target))
            m_target.set(target);
        m_all = getStartTagValueBool(stag, "all", false);
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        const char *target = (m_xpath_target) ? m_xpath_target->getXpath() : m_target.str();
        DBGLOG(">%s> %s(%s)", m_traceName.str(), m_tagname.str(), target);
#endif
    }

    virtual ~CEsdlTransformOperationRemoveNode(){}

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        if ((!m_xpath_target && m_target.isEmpty()))
            return false; //only here if "optional" backward compatible support for now (optional syntax errors aren't actually helpful
        try
        {
            StringBuffer path;
            if (m_xpath_target)
                sourceContext->evaluateAsString(m_xpath_target, path);
            else
                path.set(m_target);

            targetContext->remove(path, m_all);
        }
        catch (IException* e)
        {
            int code = e->errorCode();
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            esdlOperationError(code, m_tagname, msg, m_traceName, !m_ignoreCodingErrors);
        }
        catch (...)
        {
            esdlOperationError(ESDL_SCRIPT_Error, m_tagname, "unknown exception processing", m_traceName, !m_ignoreCodingErrors);
        }
        return false;
    }
};

class CEsdlTransformOperationAppendValue : public CEsdlTransformOperationSetValue
{
public:
    CEsdlTransformOperationAppendValue(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetValue(xpp, stag, prefix){}

    virtual ~CEsdlTransformOperationAppendValue(){}

    virtual bool doSet(IXpathContext * sourceContext, IXpathContext *targetContext, const char *value) override
    {
        StringBuffer xpath;
        const char *target = getTargetPath(sourceContext, xpath);
        targetContext->ensureAppendToValue(target, value, m_required);
        return true;
    }
};

class CEsdlTransformOperationAddValue : public CEsdlTransformOperationSetValue
{
public:
    CEsdlTransformOperationAddValue(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSetValue(xpp, stag, prefix){}

    virtual ~CEsdlTransformOperationAddValue(){}

    virtual bool doSet(IXpathContext * sourceContext, IXpathContext *targetContext, const char *value) override
    {
        StringBuffer xpath;
        const char *target = getTargetPath(sourceContext, xpath);
        targetContext->ensureAddValue(target, value, m_required);
        return true;
    }
};

class CEsdlTransformOperationFail : public CEsdlTransformOperationWithoutChildren
{
protected:
    Owned<ICompiledXpath> m_message;
    Owned<ICompiledXpath> m_code;

public:
    CEsdlTransformOperationFail(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithoutChildren(xpp, stag, prefix)
    {
        if (m_traceName.isEmpty())
            m_traceName.set(stag.getValue("name"));

        const char *code = stag.getValue("code");
        const char *message = stag.getValue("message");
        if (isEmptyString(code))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without code", m_traceName.str(), true);
        if (isEmptyString(message))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without message", m_traceName.str(), true);

        m_code.setown(compileXpath(code));
        m_message.setown(compileXpath(message));
    }

    virtual ~CEsdlTransformOperationFail()
    {
    }

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        int code = m_code.get() ? (int) sourceContext->evaluateAsNumber(m_code) : ESDL_SCRIPT_Error;
        StringBuffer msg;
        if (m_message.get())
            sourceContext->evaluateAsString(m_message, msg);
        throw makeStringException(code, msg.str());
        return true; //avoid compilation error
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
    CEsdlTransformOperationAssert(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationFail(xpp, stag, prefix)
    {
        const char *test = stag.getValue("test");
        if (isEmptyString(test))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without test", m_traceName.str(), true);
        m_test.setown(compileXpath(test));
    }

    virtual ~CEsdlTransformOperationAssert()
    {
    }

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        if (m_test && sourceContext->evaluateAsBoolean(m_test))
            return false;
        return CEsdlTransformOperationFail::process(scriptContext, targetContext, sourceContext);
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
    CEsdlTransformOperationForEach(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, nullptr)
    {
        const char *select = stag.getValue("select");
        if (isEmptyString(select))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without select", !m_ignoreCodingErrors);
        m_select.setown(compileXpath(select));
    }

    virtual ~CEsdlTransformOperationForEach(){}

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
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
            esdlOperationError(code, m_tagname, msg, !m_ignoreCodingErrors);
        }
        catch (...)
        {
            VStringBuffer msg("unknown exception evaluating select '%s'", m_select.get() ? m_select->getXpath() : "undefined!");
            esdlOperationError(ESDL_SCRIPT_Error, m_tagname, msg, !m_ignoreCodingErrors);
        }
        return xpathset;
    }
};

class CEsdlTransformOperationConditional : public CEsdlTransformOperationWithChildren
{
private:
    Owned<ICompiledXpath> m_test;
    char m_op = 'i'; //'i'=if, 'w'=when, 'o'=otherwise

public:
    CEsdlTransformOperationConditional(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, nullptr)
    {
        const char *op = stag.getLocalName();
        if (isEmptyString(op)) //should never get here, we checked already, but
            esdlOperationError(ESDL_SCRIPT_UnknownOperation, m_tagname, "unrecognized conditional missing tag name", !m_ignoreCodingErrors);
        //m_ignoreCodingErrors means op may still be null
        else if (!op || streq(op, "if"))
            m_op = 'i';
        else if (streq(op, "when"))
            m_op = 'w';
        else if (streq(op, "otherwise"))
            m_op = 'o';
        else //should never get here either, but
            esdlOperationError(ESDL_SCRIPT_UnknownOperation, m_tagname, "unrecognized conditional tag name", !m_ignoreCodingErrors);

        if (m_op!='o')
        {
            const char *test = stag.getValue("test");
            if (isEmptyString(test))
                esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without test", !m_ignoreCodingErrors);
            m_test.setown(compileXpath(test));
        }
    }

    virtual ~CEsdlTransformOperationConditional(){}

    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
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
            esdlOperationError(code, m_tagname, msg, !m_ignoreCodingErrors);
        }
        catch (...)
        {
            VStringBuffer msg("unknown exception evaluating test '%s'", m_test.get() ? m_test->getXpath() : "undefined!");
            esdlOperationError(ESDL_SCRIPT_Error, m_tagname, msg, !m_ignoreCodingErrors);
        }
        return match;
    }
};

void loadChooseChildren(IArrayOf<IEsdlTransformOperation> &operations, XmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, bool ignoreCodingErrors)
{
    Owned<CEsdlTransformOperationConditional> otherwise;

    int type = 0;
    while((type = xpp.next()) != XmlPullParser::END_DOCUMENT)
    {
        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                StartTag opTag;
                xpp.readStartTag(opTag);
                const char *op = opTag.getLocalName();
                if (streq(op, "when"))
                    operations.append(*new CEsdlTransformOperationConditional(xpp, opTag, prefix));
                else if (streq(op, "otherwise"))
                {
                    if (otherwise)
                        esdlOperationError(ESDL_SCRIPT_Error, op, "only 1 otherwise per choose statement allowed", ignoreCodingErrors);
                    otherwise.setown(new CEsdlTransformOperationConditional(xpp, opTag, prefix));
                }
                break;
            }
            case XmlPullParser::END_TAG:
            case XmlPullParser::END_DOCUMENT:
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
    CEsdlTransformOperationChoose(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(xpp, stag, prefix, false, loadChooseChildren)
    {
    }

    virtual ~CEsdlTransformOperationChoose(){}

    bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
        return processChildren(scriptContext, targetContext, sourceContext);
    }

    virtual bool processChildren(IEsdlScriptContext * scriptContext, IXpathContext *targetContext, IXpathContext * sourceContext) override
    {
        if (m_children.length())
        {
            CXpathContextScope scope(sourceContext, "choose");
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

class CEsdlTransformOperationTarget : public CEsdlTransformOperationWithChildren
{
protected:
    Owned<ICompiledXpath> m_xpath;
    bool m_required = true;
    bool m_ensure = false;

public:
    CEsdlTransformOperationTarget(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, nullptr)
    {
        const char *xpath = stag.getValue("xpath");
        if (isEmptyString(xpath))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, "target", "without xpath parameter", m_traceName.str(), !m_ignoreCodingErrors);

        m_xpath.setown(compileXpath(xpath));
        m_required = getStartTagValueBool(stag, "required", m_required);
    }

    virtual ~CEsdlTransformOperationTarget(){}

    bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
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

class CEsdlTransformOperationIfTarget : public CEsdlTransformOperationTarget
{
public:
    CEsdlTransformOperationIfTarget(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationTarget(xpp, stag, prefix)
    {
        m_required = false;
    }

    virtual ~CEsdlTransformOperationIfTarget(){}
};

class CEsdlTransformOperationEnsureTarget : public CEsdlTransformOperationTarget
{
public:
    CEsdlTransformOperationEnsureTarget(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationTarget(xpp, stag, prefix)
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
    CEsdlTransformOperationSource(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(xpp, stag, prefix, false, nullptr)
    {
        const char *xpath = stag.getValue("xpath");
        if (isEmptyString(xpath))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, "target", "without xpath parameter", m_traceName.str(), !m_ignoreCodingErrors);

        m_xpath.setown(compileXpath(xpath));
        m_required = getStartTagValueBool(stag, "required", m_required);
    }

    virtual ~CEsdlTransformOperationSource(){}

    bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
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
    CEsdlTransformOperationIfSource(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationSource(xpp, stag, prefix)
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
    CEsdlTransformOperationElement(XmlPullParser &xpp, StartTag &stag, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(xpp, stag, prefix, true, nullptr)
    {
        m_name.set(stag.getValue("name"));
        if (m_name.isEmpty())
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, "element", "without name parameter", m_traceName.str(), !m_ignoreCodingErrors);
        if (m_traceName.isEmpty())
            m_traceName.set(m_name);

        if (!validateXMLTag(m_name))
        {
            VStringBuffer msg("with invalid element name '%s'", m_name.str());
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, "element", msg.str(), m_traceName.str(), !m_ignoreCodingErrors);
        }

        m_nsuri.set(stag.getValue("namespace"));
    }

    virtual ~CEsdlTransformOperationElement(){}

    bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) override
    {
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

void createEsdlTransformOperations(IArrayOf<IEsdlTransformOperation> &operations, XmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, bool ignoreCodingErrors)
{
    int type = 0;
    while((type = xpp.next()) != XmlPullParser::END_DOCUMENT)
    {
        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                Owned<IEsdlTransformOperation> operation = createEsdlTransformOperation(xpp, prefix, withVariables, ignoreCodingErrors);
                if (operation)
                    operations.append(*operation.getClear());
                break;
            }
            case XmlPullParser::END_TAG:
                return;
            case XmlPullParser::END_DOCUMENT:
                return;
        }
    }
}

IEsdlTransformOperation *createEsdlTransformOperation(XmlPullParser &xpp, const StringBuffer &prefix, bool withVariables, bool ignoreCodingErrors)
{
    StartTag stag;
    xpp.readStartTag(stag);
    const char *op = stag.getLocalName();
    if (isEmptyString(op))
        return nullptr;
    if (withVariables)
    {
        if (streq(op, "variable"))
            return new CEsdlTransformOperationVariable(xpp, stag, prefix);
        if (streq(op, "param"))
            return new CEsdlTransformOperationParameter(xpp, stag, prefix);
    }
    if (streq(op, "choose"))
        return new CEsdlTransformOperationChoose(xpp, stag, prefix);
    if (streq(op, "for-each"))
        return new CEsdlTransformOperationForEach(xpp, stag, prefix);
    if (streq(op, "if"))
        return new CEsdlTransformOperationConditional(xpp, stag, prefix);
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
        return new CEsdlTransformOperationSource(xpp, stag, prefix);
    if (streq(op, "if-source"))
        return new CEsdlTransformOperationIfSource(xpp, stag, prefix);
    if (streq(op, "target"))
        return new CEsdlTransformOperationTarget(xpp, stag, prefix);
    if (streq(op, "if-target"))
        return new CEsdlTransformOperationIfTarget(xpp, stag, prefix);
    if (streq(op, "ensure-target"))
        return new CEsdlTransformOperationEnsureTarget(xpp, stag, prefix);
    if (streq(op, "element"))
        return new CEsdlTransformOperationElement(xpp, stag, prefix);
    if (streq(op, "copy-of"))
        return new CEsdlTransformOperationCopyOf(xpp, stag, prefix);
    if (streq(op, "namespace"))
        return new CEsdlTransformOperationNamespace(xpp, stag, prefix);
    if (streq(op, "http-post-xml"))
        return new CEsdlTransformOperationHttpPostXml(xpp, stag, prefix);
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

class CEsdlCustomTransform : public CInterfaceOf<IEsdlCustomTransform>
{
private:
    IArrayOf<IEsdlTransformOperation> m_operations;
    Owned<IProperties> namespaces = createProperties(false);
    StringAttr m_name;
    StringAttr m_target;
    StringAttr m_source;
    StringBuffer m_prefix;

public:
    CEsdlCustomTransform(){}

    CEsdlCustomTransform(XmlPullParser &xpp, StartTag &stag, const char *ns_prefix) : m_prefix(ns_prefix)
    {
        const char *tag = stag.getLocalName();

        m_name.set(stag.getValue("name"));
        m_target.set(stag.getValue("target"));
        m_source.set(stag.getValue("source"));

        DBGLOG("Compiling ESDL Transform: '%s'", m_name.str());

        map< string, const SXT_CHAR* >::iterator it = xpp.getNsBegin();
        while (it != xpp.getNsEnd())
        {
            if (it->first.compare("xml")!=0)
                namespaces->setProp(it->first.c_str(), it->second);
            it++;
        }

        int type = 0;
        while((type = xpp.next()) != XmlPullParser::END_DOCUMENT)
        {
            switch(type)
            {
                case XmlPullParser::START_TAG:
                {
                    Owned<IEsdlTransformOperation> operation = createEsdlTransformOperation(xpp, m_prefix, true, false);
                    if (operation)
                        m_operations.append(*operation.getClear());
                    break;
                }
                case XmlPullParser::END_TAG:
                case XmlPullParser::END_DOCUMENT:
                    return;
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
        CXpathContextScope scope(sourceContext, "transform", savedNamespaces);
        if (!isEmptyString(target) && !streq(target, "."))
            targetXpath->setLocation(target, true);
        if (!m_source.isEmpty() && !streq(m_source, "."))
            sourceContext->setLocation(m_source, true);
        ForEachItemIn(i, m_operations)
            m_operations.item(i).process(scriptContext, targetXpath, sourceContext);
        scriptContext->cleanupBetweenScripts();
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

    IEspContext *context = reinterpret_cast<IEspContext*>(scriptCtx->queryEspContext());

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
        // enable transforms to distinguish secure versus insecure requests
        sourceContext->addInputValue("espUserName", "");
        sourceContext->addInputValue("espUserRealm", "");
        sourceContext->addInputValue("espUserPeer", "");
        sourceContext->addInputValue("espUserStatus", "");
        sourceContext->addInputValue("espUserStatusString", "");
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
    std::unique_ptr<XmlPullParser> xpp(new XmlPullParser());
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
            return new CEsdlCustomTransform(*xpp, stag, ns_prefix);
        }
    }
    return nullptr;
}

class CEsdlTransformSet : public CInterfaceOf<IEsdlTransformSet>
{
        IArrayOf<CEsdlCustomTransform> transforms;

public:
    CEsdlTransformSet()
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
    virtual void add(XmlPullParser &xpp, StartTag &stag)
    {
        transforms.append(*new CEsdlCustomTransform(xpp, stag, nullptr));
    }
    virtual aindex_t length() override
    {
        return transforms.length();
    }
};

class CEsdlTransformEntryPointMap : public CInterfaceOf<IEsdlTransformEntryPointMap>
{
    MapStringToMyClass<CEsdlTransformSet> map;

public:
    CEsdlTransformEntryPointMap()
    {
    }

    virtual void addChild(XmlPullParser &xpp, StartTag &childTag, bool &foundNonLegacyTransforms)
    {
        const char *tagname = childTag.getLocalName();
        if (streq("Scripts", tagname) || streq("Transforms", tagname)) //allow nesting of root structure
            add(xpp, childTag, foundNonLegacyTransforms);
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
                Owned<CEsdlTransformSet> set = new CEsdlTransformSet();
                map.setValue(tagname, set);
                set->add(xpp, childTag);
            }
        }
    }

    virtual void add(XmlPullParser &xpp, StartTag &stag, bool &foundNonLegacyTransforms)
    {
        int type;
        StartTag childTag;
        while((type = xpp.next()) != XmlPullParser::END_DOCUMENT)
        {
            switch (type)
            {
                case XmlPullParser::START_TAG:
                {
                    xpp.readStartTag(childTag);
                    const char *tagname = childTag.getLocalName();
                    if (streq("Scripts", tagname) || streq("Transforms", tagname)) //allow nesting of container structures for maximum compatability
                        add(xpp, childTag, foundNonLegacyTransforms);
                    else
                        addChild(xpp, childTag,foundNonLegacyTransforms);
                    break;
                }
                case XmlPullParser::END_TAG:
                    return;
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
};


class CEsdlTransformMethodMap : public CInterfaceOf<IEsdlTransformMethodMap>
{
    MapStringToMyClass<CEsdlTransformEntryPointMap> map;

public:
    CEsdlTransformMethodMap()
    {
    }

    virtual IEsdlTransformEntryPointMap *queryMethod(const char *name) override
    {
        return map.getValue(name);
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
                Owned<CEsdlTransformEntryPointMap> epm = new CEsdlTransformEntryPointMap();
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
};

esdl_decl IEsdlTransformMethodMap *createEsdlTransformMethodMap()
{
    return new CEsdlTransformMethodMap();
}
