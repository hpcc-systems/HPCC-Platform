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

interface IEsdlTransformOperation : public IInterface
{
    virtual const char *queryMergedTarget() = 0;
    virtual bool process(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) = 0;
    virtual void toDBGLog() = 0;
};

IEsdlTransformOperation *createEsdlTransformOperation(IPropertyTree *element, const StringBuffer &prefix);

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
    StringAttr m_mergedTarget;
    StringAttr m_tagname;
    bool m_ignoreCodingErrors = false; //ideally used only for debugging

public:
    CEsdlTransformOperationBase(IPropertyTree *tree, const StringBuffer &prefix)
    {
        m_tagname.set(tree->queryName());
        if (tree->hasProp("@_crtTarget"))
            m_mergedTarget.set(tree->queryProp("@_crtTarget"));
        m_ignoreCodingErrors = tree->getPropBool("@optional", false);
    }

    virtual const char *queryMergedTarget() override
    {
        return m_mergedTarget;
    }
};


class CEsdlTransformOperationVariable : public CEsdlTransformOperationBase
{
protected:
    StringAttr m_name;
    Owned<ICompiledXpath> m_select;

public:
    CEsdlTransformOperationVariable(IPropertyTree *tree, const StringBuffer &prefix) : CEsdlTransformOperationBase(tree, prefix)
    {
        m_name.set(tree->queryProp("@name"));
        if (m_name.isEmpty())
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without name", m_name, !m_ignoreCodingErrors);
        const char *select = tree->queryProp("@select");
        if (!isEmptyString(select))
            m_select.setown(compileXpath(select));
    }

    virtual ~CEsdlTransformOperationVariable()
    {
    }

    virtual bool process(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) override
    {
        if (m_select)
            return xpathContext->addCompiledVariable(m_name, m_select);
        return xpathContext->addVariable(m_name, "");
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s with select(%s)", m_name.str(), m_tagname.str(), m_select.get() ? m_select->getXpath() : "");
#endif
    }
};

class CEsdlTransformOperationParameter : public CEsdlTransformOperationVariable
{
public:
    CEsdlTransformOperationParameter(IPropertyTree *tree, const StringBuffer &prefix) : CEsdlTransformOperationVariable(tree, prefix)
    {
    }

    virtual ~CEsdlTransformOperationParameter()
    {
    }

    virtual bool process(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) override
    {
        if (m_select)
            return xpathContext->declareCompiledParameter(m_name, m_select);
        return xpathContext->declareParameter(m_name, "");
    }
};

class CEsdlTransformOperationSetValue : public CEsdlTransformOperationBase
{
protected:
    Owned<ICompiledXpath> m_select;
    Owned<ICompiledXpath> m_xpath_target;
    StringAttr m_target;
    StringAttr m_traceName;

public:
    CEsdlTransformOperationSetValue(IPropertyTree *tree, const StringBuffer &prefix) : CEsdlTransformOperationBase(tree, prefix)
    {
        m_traceName.set(tree->queryProp("@name"));

        if (isEmptyString(tree->queryProp("@target")) && isEmptyString(tree->queryProp("@xpath_target")))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname.str(), "without target", m_traceName.str(), !m_ignoreCodingErrors);

        const char *select = tree->queryProp("@select");
        if (isEmptyString(select))
            select = tree->queryProp("@value");
        if (isEmptyString(select))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without select", m_traceName, !m_ignoreCodingErrors); //don't mention value, it's deprecated

        m_select.setown(compileXpath(select));
        if (tree->hasProp("@xpath_target"))
            m_xpath_target.setown(compileXpath(tree->queryProp("@xpath_target")));
        else if (tree->hasProp("@target"))
            m_target.set(tree->queryProp("@target"));
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        DBGLOG(">%s> %s(%s, select('%s'))", m_traceName.str(), m_tagname.str(), m_target.str(), m_select->getXpath());
#endif
    }

    virtual ~CEsdlTransformOperationSetValue(){}

    virtual bool process(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) override
    {
        if ((!m_xpath_target && m_target.isEmpty()) || !m_select)
            return false; //only here if "optional" backward compatible support for now (optional syntax errors aren't actually helpful
        try
        {
            StringBuffer value;
            xpathContext->evaluateAsString(m_select, value);
            return doSet(xpathContext, content, value);
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
    virtual bool doSet(IXpathContext * xpathContext, IPropertyTree *tree, const char *value)
    {
        StringBuffer xpath;
        const char *target = getTargetPath(xpathContext, xpath);
        ensurePTree(tree, target);
        tree->setProp(target, value);
        return true;
    }
};

class CEsdlTransformOperationAppendValue : public CEsdlTransformOperationSetValue
{
public:
    CEsdlTransformOperationAppendValue(IPropertyTree *tree, const StringBuffer &prefix) : CEsdlTransformOperationSetValue(tree, prefix){}

    virtual ~CEsdlTransformOperationAppendValue(){}

    virtual bool doSet(IXpathContext * xpathContext, IPropertyTree *tree, const char *value)
    {
        StringBuffer xpath;
        const char *target = getTargetPath(xpathContext, xpath);
        ensurePTree(tree, target);
        tree->appendProp(target, value);
        return true;
    }
};

class CEsdlTransformOperationAddValue : public CEsdlTransformOperationSetValue
{
public:
    CEsdlTransformOperationAddValue(IPropertyTree *tree, const StringBuffer &prefix) : CEsdlTransformOperationSetValue(tree, prefix){}

    virtual ~CEsdlTransformOperationAddValue(){}

    virtual bool doSet(IXpathContext * xpathContext, IPropertyTree *tree, const char *value)
    {
        StringBuffer xpath;
        const char *target = getTargetPath(xpathContext, xpath);
        if (tree->getCount(target)==0)
        {
            ensurePTree(tree, target);
            tree->setProp(target, value);
        }
        else
            tree->addProp(target, value);
        return true;
    }
};

class CEsdlTransformOperationFail : public CEsdlTransformOperationBase
{
protected:
    StringAttr m_traceName;
    Owned<ICompiledXpath> m_message;
    Owned<ICompiledXpath> m_code;

public:
    CEsdlTransformOperationFail(IPropertyTree *tree, const StringBuffer &prefix) : CEsdlTransformOperationBase(tree, prefix)
    {
        m_traceName.set(tree->queryProp("@name"));

        if (isEmptyString(tree->queryProp("@code")))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without code", m_traceName.str(), true);
        if (isEmptyString(tree->queryProp("@message")))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without message", m_traceName.str(), true);

        m_code.setown(compileXpath(tree->queryProp("@code")));
        m_message.setown(compileXpath(tree->queryProp("@message")));
    }

    virtual ~CEsdlTransformOperationFail()
    {
    }

    virtual bool process(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) override
    {
        int code = m_code.get() ? (int) xpathContext->evaluateAsNumber(m_code) : ESDL_SCRIPT_Error;
        StringBuffer msg;
        if (m_message.get())
            xpathContext->evaluateAsString(m_message, msg);
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
    CEsdlTransformOperationAssert(IPropertyTree *tree, const StringBuffer &prefix) : CEsdlTransformOperationFail(tree, prefix)
    {
        if (isEmptyString(tree->queryProp("@test")))
            esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without test", m_traceName.str(), true);
        m_test.setown(compileXpath(tree->queryProp("@test")));
    }

    virtual ~CEsdlTransformOperationAssert()
    {
    }

    virtual bool process(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) override
    {
        if (m_test && xpathContext->evaluateAsBoolean(m_test))
            return false;
        return CEsdlTransformOperationFail::process(context, content, xpathContext);
    }

    virtual void toDBGLog() override
    {
#if defined(_DEBUG)
        const char *testXpath = m_test.get() ? m_test->getXpath() : "SYNTAX ERROR IN test";
        DBGLOG(">%s> %s if '%s' with message(%s)", m_traceName.str(), m_tagname.str(), testXpath, m_message.get() ? m_message->getXpath() : "");
#endif
    }
};

class CEsdlTransformOperationWithChildren : public CEsdlTransformOperationBase
{
protected:
    IArrayOf<IEsdlTransformOperation> m_children;

public:
    CEsdlTransformOperationWithChildren(IPropertyTree *tree, const StringBuffer &prefix, bool withVariables) : CEsdlTransformOperationBase(tree, prefix)
    {
        if (tree)
            loadChildren(tree, prefix, withVariables);
    }

    virtual ~CEsdlTransformOperationWithChildren(){}

    virtual bool processChildren(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext)
    {
        bool ret = false;
        ForEachItemIn(i, m_children)
        {
            if (m_children.item(i).process(context, content, xpathContext))
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

protected:
    virtual void loadChildren(IPropertyTree * tree, const StringBuffer &prefix, bool withVariables)
    {
        if (withVariables)
        {
            StringBuffer xpath;
            Owned<IPropertyTreeIterator> parameters = tree->getElements(makeOperationTagName(xpath, prefix, "param"));
            ForEach(*parameters)
                m_children.append(*new CEsdlTransformOperationParameter(&parameters->query(), prefix));

            Owned<IPropertyTreeIterator> variables = tree->getElements(makeOperationTagName(xpath.clear(), prefix, "variable"));
            ForEach(*variables)
                m_children.append(*new CEsdlTransformOperationVariable(&variables->query(), prefix));
        }
        Owned<IPropertyTreeIterator> children = tree->getElements("*");
        ForEach(*children)
        {
            Owned<IEsdlTransformOperation> operation = createEsdlTransformOperation(&children->query(), prefix);
            if (operation)
                m_children.append(*operation.getClear());
        }
    }
};

class CEsdlTransformOperationForEach : public CEsdlTransformOperationWithChildren
{
protected:
    Owned<ICompiledXpath> m_select;

public:
    CEsdlTransformOperationForEach(IPropertyTree *tree, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(tree, prefix, true)
    {
        if (tree)
        {
            if (isEmptyString(tree->queryProp("@select")))
                esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without select", !m_ignoreCodingErrors);
            m_select.setown(compileXpath(tree->queryProp("@select")));
        }
    }

    virtual ~CEsdlTransformOperationForEach(){}

    bool process(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) override
    {
        Owned<IXpathContextIterator> contexts = evaluate(xpathContext);
        if (!contexts)
            return false;
        if (!contexts->first())
            return false;
        CXpathContextScope scope(xpathContext, "for-each"); //new variables are scoped
        ForEach(*contexts)
            processChildren(context, content, &contexts->query());
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
    CEsdlTransformOperationConditional(IPropertyTree * tree, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(tree, prefix, true)
    {
        if (tree)
        {
            const char *op = checkSkipOpPrefix(tree->queryName(), prefix);
            if (isEmptyString(op))
                esdlOperationError(ESDL_SCRIPT_UnknownOperation, m_tagname, "unrecognized conditional", !m_ignoreCodingErrors);
            //m_ignoreCodingErrors means op may still be null
            if (!op || streq(op, "if"))
                m_op = 'i';
            else if (streq(op, "when"))
                m_op = 'w';
            else if (streq(op, "otherwise"))
                m_op = 'o';
            if (m_op!='o')
            {
                if (isEmptyString(tree->queryProp("@test")))
                    esdlOperationError(ESDL_SCRIPT_MissingOperationAttr, m_tagname, "without test", !m_ignoreCodingErrors);
                m_test.setown(compileXpath(tree->queryProp("@test")));
            }
        }
    }

    virtual ~CEsdlTransformOperationConditional(){}

    bool process(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) override
    {
        if (!evaluate(xpathContext))
            return false;
        CXpathContextScope scope(xpathContext, m_tagname); //child variables are scoped
        processChildren(context, content, xpathContext);
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

class CEsdlTransformOperationChoose : public CEsdlTransformOperationWithChildren
{
public:
    CEsdlTransformOperationChoose(IPropertyTree * tree, const StringBuffer &prefix) : CEsdlTransformOperationWithChildren(tree, prefix, false)
    {
        if (tree)
        {
            loadWhens(tree, prefix);
            loadOtherwise(tree, prefix);
        }
    }

    virtual ~CEsdlTransformOperationChoose(){}

    bool process(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) override
    {
        return processChildren(context, content, xpathContext);
    }

    virtual bool processChildren(IEspContext * context, IPropertyTree *content, IXpathContext * xpathContext) override
    {
        ForEachItemIn(i, m_children)
        {
            if (m_children.item(i).process(context, content, xpathContext))
                return true;
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

protected:
    void loadWhens(IPropertyTree * tree, const StringBuffer &prefix)
    {
        StringBuffer xpath;
        Owned<IPropertyTreeIterator> children = tree->getElements(makeOperationTagName(xpath, prefix, "when"));
        ForEach(*children)
            m_children.append(*new CEsdlTransformOperationConditional(&children->query(), prefix));
    }

    void loadOtherwise(IPropertyTree * tree, const StringBuffer &prefix)
    {
        StringBuffer xpath;
        IPropertyTree * otherwise = tree->queryPropTree(makeOperationTagName(xpath, prefix, "otherwise"));
        if (!otherwise)
            return;
        m_children.append(*new CEsdlTransformOperationConditional(otherwise, prefix));
    }
    virtual void loadChildren(IPropertyTree * tree, const StringBuffer &prefix, bool withVariables) override
    {
        loadWhens(tree, prefix);
        loadOtherwise(tree, prefix);
    }
};

IEsdlTransformOperation *createEsdlTransformOperation(IPropertyTree *element, const StringBuffer &prefix)
{
    const char *op = checkSkipOpPrefix(element->queryName(), prefix);
    if (isEmptyString(op))
        return nullptr;
    if (streq(op, "choose"))
        return new CEsdlTransformOperationChoose(element, prefix);
    if (streq(op, "for-each"))
        return new CEsdlTransformOperationForEach(element, prefix);
    if (streq(op, "if"))
        return new CEsdlTransformOperationConditional(element, prefix);
    if (streq(op, "set-value") || streq(op, "SetValue"))
        return new CEsdlTransformOperationSetValue(element, prefix);
    if (streq(op, "append-to-value") || streq(op, "AppendValue"))
        return new CEsdlTransformOperationAppendValue(element, prefix);
    if (streq(op, "add-value"))
        return new CEsdlTransformOperationAddValue(element, prefix);
    if (streq(op, "fail"))
        return new CEsdlTransformOperationFail(element, prefix);
    if (streq(op, "assert"))
        return new CEsdlTransformOperationAssert(element, prefix);
    return nullptr;
}

static IPropertyTree *getTargetPTree(IPropertyTree *tree, IXpathContext *xpathContext, const char *target)
{
    StringBuffer xpath(target);
    if (xpath.length())
    {
        //we can use real xpath processing in the future, for now simple substitution is fine
        StringBuffer variable;
        xpath.replaceString("{$query}", xpathContext->getVariable("query", variable));
        xpath.replaceString("{$method}", xpathContext->getVariable("method", variable.clear()));
        xpath.replaceString("{$service}", xpathContext->getVariable("service", variable.clear()));
        xpath.replaceString("{$request}", xpathContext->getVariable("request", variable.clear()));

        IPropertyTree *child = tree->queryPropTree(xpath.str());  //get pointer to the write-able area
        if (!child)
            throw MakeStringException(ESDL_SCRIPT_Error, "EsdlCustomTransform error getting target xpath %s", xpath.str());
        return child;
    }
    return tree;
}
static IPropertyTree *getOperationTargetPTree(MapStringToMyClass<IPropertyTree> &treeMap, IPropertyTree *currentTree, IEsdlTransformOperation &operation, IPropertyTree *tree, IXpathContext *xpathContext, const char *target)
{
    const char *mergedTarget = operation.queryMergedTarget();
    if (isEmptyString(mergedTarget) || streq(mergedTarget, target))
        return currentTree;
    IPropertyTree *opTree = treeMap.getValue(mergedTarget);
    if (opTree)
        return opTree;
    opTree = getTargetPTree(tree, xpathContext, mergedTarget);
    if (opTree)
        treeMap.setValue(mergedTarget, opTree);
    return opTree;
}

class CEsdlCustomTransform : public CInterfaceOf<IEsdlCustomTransform>
{
private:
    IArrayOf<IEsdlTransformOperation> m_variables; //keep separate and only at top level for now
    IArrayOf<IEsdlTransformOperation> m_operations;
    Owned<IProperties> namespaces = createProperties(false);
    StringAttr m_name;
    StringAttr m_target;
    StringBuffer m_prefix;

public:
    CEsdlCustomTransform(){}
    void verifyPrefixDeclared(IPropertyTree &tree, const char *prefix)
    {
        StringBuffer attpath("@xmlns");
        if (!isEmptyString(prefix))
            attpath.append(':').append(prefix);
        const char *uri = tree.queryProp(attpath.str());
        if (!uri || !streq(uri, "urn:hpcc:esdl:script"))
            throw MakeStringException(ESDL_SCRIPT_Error, "Undeclared script xmlns prefix %s", isEmptyString(prefix) ? "<default>" : prefix);
    }
    CEsdlCustomTransform(IPropertyTree &tree, const char *ns_prefix) : m_prefix(ns_prefix)
    {
        if (m_prefix.length())
            m_prefix.append(':');
        else
        {
            const char *tag = tree.queryName();
            if (!tag)
                m_prefix.set("xsdl:");
            else
            {
                const char *colon = strchr(tag, ':');
                if (!colon)
                    verifyPrefixDeclared(tree, nullptr);
                else
                {
                    if (colon == tag)
                        throw MakeStringException(ESDL_SCRIPT_Error, "Tag shouldn't start with colon %s", tag);
                    m_prefix.append(colon-tag, tag);
                    if (!streq(m_prefix, "xsdl"))
                        verifyPrefixDeclared(tree, m_prefix);
                    //add back the colon for easy comparison
                    m_prefix.append(':');
                }
            }
        }

        m_name.set(tree.queryProp("@name"));
        m_target.set(tree.queryProp("@target"));

        DBGLOG("Compiling custom ESDL Transform: '%s'", m_name.str());

        Owned<IAttributeIterator> attributes = tree.getAttributes();
        ForEach(*attributes)
        {
            const char *name = attributes->queryName();
            if (strncmp(name, "@xmlns:", 7)==0)
                namespaces->setProp(name+7, attributes->queryValue());
        }

        StringBuffer xpath;
        Owned<IPropertyTreeIterator> parameters = tree.getElements(makeOperationTagName(xpath, m_prefix, "param"));
        ForEach(*parameters)
            m_variables.append(*new CEsdlTransformOperationParameter(&parameters->query(), m_prefix));

        Owned<IPropertyTreeIterator> variables = tree.getElements(makeOperationTagName(xpath.clear(), m_prefix, "variable"));
        ForEach(*variables)
            m_variables.append(*new CEsdlTransformOperationVariable(&variables->query(), m_prefix));

        Owned<IPropertyTreeIterator> children = tree.getElements("*");
        ForEach(*children)
        {
            Owned<IEsdlTransformOperation> operation = createEsdlTransformOperation(&children->query(), m_prefix);
            if (operation)
                m_operations.append(*operation.getClear());
        }
    }

    virtual void appendEsdlURIPrefixes(StringArray &prefixes) override
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

    void processTransformImpl(IEspContext * context, IPropertyTree *theroot, IXpathContext *xpathContext, const char *target)
    {
        Owned<IProperties> savedNamespaces = createProperties(false);
        Owned<IPropertyIterator> ns = namespaces->getIterator();
        ForEach(*ns)
        {
            const char *prefix = ns->getPropKey();
            const char *existing = xpathContext->queryNamespace(prefix);
            savedNamespaces->setProp(prefix, isEmptyString(existing) ? "" : existing);
            xpathContext->registerNamespace(prefix, namespaces->queryProp(prefix));
        }
        CXpathContextScope scope(xpathContext, "transform", savedNamespaces);
        if (m_target.length())
            target = m_target.str();
        MapStringToMyClass<IPropertyTree> treeMap; //cache trees because when there are merged targets they are likely to repeat
        IPropertyTree *txTree = getTargetPTree(theroot, xpathContext, target);
        treeMap.setValue(target, txTree);
        ForEachItemIn(v, m_variables)
            m_variables.item(v).process(context, txTree, xpathContext);
        ForEachItemIn(i, m_operations)
        {
            IPropertyTree *opTree = getOperationTargetPTree(treeMap, txTree, m_operations.item(i), theroot, xpathContext, target);
            m_operations.item(i).process(context, opTree, xpathContext);
        }
    }

    void processTransform(IEspContext * context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, StringBuffer & content, IPropertyTree * bindingCfg) override
    {
        processServiceAndMethodTransforms({static_cast<IEsdlCustomTransform*>(this)}, context, tgtcfg, srvdef, mthdef, content, bindingCfg);
    }
    void processTransform(IEspContext * context, IPropertyTree *tgtcfg, const char *service, const char *method, const char* reqtype, StringBuffer & content, IPropertyTree * bindingCfg) override
    {
        processServiceAndMethodTransforms({static_cast<IEsdlCustomTransform*>(this)}, context, tgtcfg, service, method, reqtype, content, bindingCfg);
    }
};

void processServiceAndMethodTransforms(std::initializer_list<IEsdlCustomTransform *> const &transforms, IEspContext * context, IPropertyTree *tgtcfg, const char *service, const char *method, const char* reqtype, StringBuffer & content, IPropertyTree * bindingCfg)
{
    LogLevel level = LogMin;
    if (!transforms.size())
        return;
    if (tgtcfg)
        level = (unsigned) tgtcfg->getPropInt("@traceLevel", level);

    if (content.length()!=0)
    {
        if (level >= LogMax)
        {
            DBGLOG("ORIGINAL content: %s", content.str());
            StringBuffer marshalled;
            if (bindingCfg)
                toXML(bindingCfg, marshalled.clear());
            DBGLOG("BINDING CONFIG: %s", marshalled.str());
            if (tgtcfg)
                toXML(tgtcfg, marshalled.clear());
            DBGLOG("TARGET CONFIG: %s", marshalled.str());
        }

        bool strictParams = bindingCfg ? bindingCfg->getPropBool("@strictParams", false) : false;
        Owned<IXpathContext> xpathContext = getXpathContext(content.str(), strictParams, false);

        StringArray prefixes;
        for ( IEsdlCustomTransform * const & item : transforms)
        {
            if (item)
                item->appendEsdlURIPrefixes(prefixes);
        }

        registerEsdlXPathExtensions(xpathContext, context, prefixes);

        VStringBuffer ver("%g", context->getClientVersion());
        if(!xpathContext->addVariable("clientversion", ver.str()))
            OERRLOG("Could not set ESDL Script variable: clientversion:'%s'", ver.str());

        //in case transform wants to make use of these values:
        //make them few well known values variables rather than inputs so they are automatically available
        xpathContext->addVariable("query", tgtcfg->queryProp("@queryname"));
        xpathContext->addVariable("method", method);
        xpathContext->addVariable("service", service);
        xpathContext->addVariable("request", reqtype);

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
                    xpathContext->addInputValue(name, user->getProperty(name));
            }

            auto it = statusLabels.find(user->getStatus());

            xpathContext->addInputValue("espUserName", user->getName());
            xpathContext->addInputValue("espUserRealm", user->getRealm() ? user->getRealm() : "");
            xpathContext->addInputValue("espUserPeer", user->getPeer() ? user->getPeer() : "");
            xpathContext->addInputValue("espUserStatus", VStringBuffer("%d", int(user->getStatus())));
            if (it != statusLabels.end())
                xpathContext->addInputValue("espUserStatusString", it->second);
            else
                throw MakeStringException(ESDL_SCRIPT_Error, "encountered unexpected secure user status (%d) while processing transform", int(user->getStatus()));
        }
        else
        {
            // enable transforms to distinguish secure versus insecure requests
            xpathContext->addInputValue("espUserName", "");
            xpathContext->addInputValue("espUserRealm", "");
            xpathContext->addInputValue("espUserPeer", "");
            xpathContext->addInputValue("espUserStatus", "");
            xpathContext->addInputValue("espUserStatusString", "");
        }

        //external parameters need <es:param> statements to make them accessible (in strict mode)
        Owned<IPropertyTreeIterator> configParams;
        if (bindingCfg)
            configParams.setown(bindingCfg->getElements("Transform/Param"));
        if (configParams)
        {
            ForEach(*configParams)
            {
                IPropertyTree & currentParam = configParams->query();
                if (currentParam.hasProp("@select"))
                    xpathContext->addInputXpath(currentParam.queryProp("@name"), currentParam.queryProp("@select"));
                else
                    xpathContext->addInputValue(currentParam.queryProp("@name"), currentParam.queryProp("@value"));
            }
        }
        if (!strictParams)
            xpathContext->declareRemainingInputs();

        Owned<IPropertyTree> theroot = createPTreeFromXMLString(content.str());
        StringBuffer defaultTarget;
            //This default gives us backward compatibility with only being able to write to the actual request
        const char *tgtQueryName = tgtcfg->queryProp("@queryname");
        defaultTarget.setf("soap:Body/%s/%s", tgtQueryName ? tgtQueryName : method, reqtype);

        for ( auto&& item : transforms)
        {
            if (item)
            {
                CEsdlCustomTransform *transform = static_cast<CEsdlCustomTransform*>(item);
                transform->processTransformImpl(context, theroot, xpathContext, defaultTarget);
            }
        }

        toXML(theroot, content.clear());

        if (level >= LogMax)
            DBGLOG(1,"MODIFIED content: %s", content.str());
    }
}

void processServiceAndMethodTransforms(std::initializer_list<IEsdlCustomTransform *> const &transforms, IEspContext * context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, StringBuffer & content, IPropertyTree * bindingCfg)
{
    processServiceAndMethodTransforms(transforms, context, tgtcfg, srvdef.queryName(), mthdef.queryMethodName(), mthdef.queryRequestType(), content, bindingCfg);
}

IEsdlCustomTransform *createEsdlCustomTransform(IPropertyTree &tree, const char *ns_prefix)
{
    return new CEsdlCustomTransform(tree, ns_prefix);
}
