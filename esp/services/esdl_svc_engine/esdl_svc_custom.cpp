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

#include "esdl_svc_custom.hpp"

//
// CEsdlCustomTransformChoose methods
//

CEsdlCustomTransformChoose::CEsdlCustomTransformChoose(IPropertyTree * choosewhen)
{
    if (choosewhen)
    {
        IPropertyTree * whentree = choosewhen->queryPropTree("xsdl:when");
        if (whentree)
        {
            StringBuffer testatt;
            testatt.set(whentree->queryProp("@test"));
            m_compiledConditionalXpath.set(getCompiledXpath(testatt.str()));

            compileClauses(whentree, false);
            compileChildChoose(whentree, false);

            IPropertyTree * otherwise = choosewhen->queryPropTree("xsdl:otherwise");
            if (otherwise)
            {
                compileClauses(otherwise, true);
                compileChildChoose(otherwise, true);
            }
        }
        else
            ERRLOG("CEsdlCustomTransformChoose: Found xsdl:choose clause without required xsdl:when");
    }
}

void CEsdlCustomTransformChoose::processClauses(IPropertyTree *request, IXpathContext * xpathContext, CIArrayOf<CEsdlCustomTransformRule> & transforms)
{
    if (request)
    {
        StringBuffer evaluatedValue;
        ForEachItemIn(i, transforms)
        {
            CEsdlCustomTransformRule & cur = transforms.item(i);
            const char * targetField = cur.queryTargetField();
            bool optional = cur.isOptional();

            if (xpathContext)
            {
                if (targetField && *targetField)
                {
                    try
                    {
                        xpathContext->evaluateAsString(cur.queryCompiledValuePath(), evaluatedValue.clear());
                        if (cur.isPerformingASet())
                        {
                            request->setProp(targetField, evaluatedValue.str());
                        }
                        else
                        {
                            ensurePTree(request, targetField);
                            request->appendProp(targetField, evaluatedValue.str());
                        }
                    }
                    catch (...)
                    {
                        VStringBuffer msg("Could not process Custom Transform: '%s' ", cur.queryName());
                        if (!optional)
                            throw MakeStringException(-1, "%s", msg.str());
                        else
                            ERRLOG("%s", msg.str());
                    }
                }
                else
                    throw MakeStringException(-1, "Encountered field transform rule without target field declaration.");
            }
            else
                throw MakeStringException(-1, "Could not process custom transform (xpathcontext == null)");

        }
    }
}

void CEsdlCustomTransformChoose::processClauses(IEspContext * context, IPropertyTree *request, IXpathContext * xpathContext, bool othwerwise)
{
    if (request)
    {
        if (!othwerwise)
        {
            processClauses(request, xpathContext, m_chooseClauses);
        }
        else
        {
            processClauses(request, xpathContext, m_otherwiseClauses);
        }
    }
}

void CEsdlCustomTransformChoose::processChildClauses(IEspContext * context, IPropertyTree *request, IXpathContext * xpathContext,  bool otherwise)
{
    if (!otherwise)
    {
        ForEachItemIn(currNestedConditionalIndex, m_childChooseClauses)
        {
            m_childChooseClauses.item(currNestedConditionalIndex).process(context, request, xpathContext);
        }
    }
    else
    {
        ForEachItemIn(currNestedConditionalIndex, m_childOtherwiseClauses)
        {
            m_childOtherwiseClauses.item(currNestedConditionalIndex).process(context, request, xpathContext);
        }
    }
}

void CEsdlCustomTransformChoose::compileChildChoose(IPropertyTree * nested, bool otherwise)
{
    Owned<IPropertyTreeIterator> conditionalIterator = nested->getElements("xsdl:choose");
    ForEach(*conditionalIterator)
    {
        auto xslchooseelement = &conditionalIterator->query();
        addChildTransformClause(new CEsdlCustomTransformChoose(xslchooseelement), otherwise);
    }
}

void CEsdlCustomTransformChoose::compileClauses(IPropertyTreeIterator * rulesiter, bool otherwise, bool typeIsSet)
{
    ForEach(*rulesiter)
    {
        IPropertyTree & cur = rulesiter->query();

        bool optional = cur.getPropBool("@optional", false);
        const char * ruleName = cur.queryProp("@name");
        const char * targetFieldPath = cur.queryProp("@target");
        if (!targetFieldPath || !*targetFieldPath)
        {
            VStringBuffer msg("Encountered custom transform rule '%s' without TargetField path", ruleName ? ruleName : "No Name Provided");
            if(!optional)
                throw MakeStringException(-1, "%s", msg.str());
            else
                ERRLOG("%s", msg.str());
                continue;
        }

        const char * pathToValue = cur.queryProp("@value");
        if (!pathToValue || !*pathToValue)
        {
            VStringBuffer msg("Encountered custom transform rule '%s' without Value path", ruleName ? ruleName : "No Name Provided");
            if(!optional)
                throw MakeStringException(-1, "%s", msg.str());
            else
                ERRLOG("%s", msg.str());
                continue;
        }

        addTransformClause(new CEsdlCustomTransformRule(ruleName, targetFieldPath, pathToValue, optional, typeIsSet ), otherwise);
    }
}

void CEsdlCustomTransformChoose::compileClauses(IPropertyTree * rules, bool otherwise)
{
    compileClauses(rules->getElements("xsdl:SetValue"), otherwise, true);
    compileClauses(rules->getElements("xsdl:AppendValue"), otherwise, false);
}

void CEsdlCustomTransformChoose::addTransformClause(CEsdlCustomTransformRule * fieldmapping, bool otherwise)
{
    if (!otherwise)
        m_chooseClauses.append(*LINK(fieldmapping));
    else
        m_otherwiseClauses.append(*LINK(fieldmapping));
}

void CEsdlCustomTransformChoose::addChildTransformClause(CEsdlCustomTransformChoose * nestedConditional, bool otherwise)
{
    if(!otherwise)
        m_childChooseClauses.append(*LINK(nestedConditional));
    else
        m_childOtherwiseClauses.append(*LINK(nestedConditional));
}

bool CEsdlCustomTransformChoose::evaluate(IXpathContext * xpathContext)
{
    bool evalresp = false;
    try
    {
        evalresp = xpathContext->evaluateAsBoolean(m_compiledConditionalXpath.getLink());
    }
    catch (...)
    {
        DBGLOG("CEsdlCustomTransformChoose:evaluate: Could not evaluate xpath '%s'", xpathContext->getXpath());
    }
    return evalresp;
}

void CEsdlCustomTransformChoose::process(IEspContext * context, IPropertyTree *request, IXpathContext * xpathContext)
{
    bool result = false;
    try
    {
        bool result = evaluate(xpathContext);
        processClauses(context, request, xpathContext, !result);
        processChildClauses(context, request, xpathContext, !result);
    }
    catch (...)
    {
        ERRLOG("EsdlCustomTransformClause::process internal error");
    }
}

#if defined(_DEBUG)
void CEsdlCustomTransformChoose::toDBGLog ()
{
    DBGLOG (">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>WHEN>>>>>>>>>>");
    StringBuffer str;

    str.set (m_compiledConditionalXpath->getXpath ());
    DBGLOG ("WHEN %s ", str.str ());

    ForEachItemIn(i, m_chooseClauses)
    {
        CEsdlCustomTransformRule & cur = m_chooseClauses.item (i);
        cur.toDBGLog ();
    }

    ForEachItemIn(nci, m_childChooseClauses)
    {
        CEsdlCustomTransformChoose & cur = m_childChooseClauses.item (nci);
        cur.toDBGLog ();
    }

    if (m_otherwiseClauses.ordinality () != 0 || m_childOtherwiseClauses.ordinality () != 0)
    {
        DBGLOG ("OTHERWISE ");
        ForEachItemIn(i, m_otherwiseClauses)
        {
            CEsdlCustomTransformRule & cur = m_otherwiseClauses.item (i);
            cur.toDBGLog ();
        }

        ForEachItemIn(nci, m_childOtherwiseClauses)
        {
            CEsdlCustomTransformChoose & cur =
            m_childOtherwiseClauses.item (nci);
            cur.toDBGLog ();
         }
    }
    DBGLOG (">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>WHEN>>>>>>>>>>");
}
#endif

//
// CEsdlCustomTransform methods
//
CEsdlCustomTransform::CEsdlCustomTransform(IPropertyTree &currentTransform)
{
    m_name.set(currentTransform.queryProp("@name"));
    DBGLOG("Compiling custom ESDL Transform: '%s'", m_name.str());

    Owned<IPropertyTreeIterator> conditionalIterator = currentTransform.getElements("xsdl:choose");
    ForEach(*conditionalIterator)
    {
        auto xslchooseelement = &conditionalIterator->query();
        Owned<CEsdlCustomTransformChoose> currconditional = new CEsdlCustomTransformChoose(xslchooseelement);
        m_customTransformClauses.append(*LINK(currconditional));
    }

#if defined(_DEBUG)
    toDBGLog();
#endif
}

void CEsdlCustomTransform::processTransform(IEspContext * context, StringBuffer & request, IPropertyTree * bindingCfg)
{
    if (request.length()!=0)
    {
#if defined(_DEBUG)
        DBGLOG("ORIGINAL REQUEST: %s", request.str());

        StringBuffer marshalled;
        toXML(bindingCfg, marshalled.clear());
        DBGLOG("INCOMING CONFIG: %s", marshalled.str());
#endif
        Owned<IXpathContext> xpathContext = getXpathContext(request.str());

        VStringBuffer ver("%g", context->getClientVersion());
        if(!xpathContext->addVariable("clientversion", ver.str()))
            ERRLOG("Could not set custom transform variable: clientversion:'%s'", ver.str());

        auto user = context->queryUser();
        if (user)
        {
            Owned<IPropertyIterator> userPropIt = user->getPropertyIterator();
            ForEach(*userPropIt)
            {
                const char *name = userPropIt->getPropKey();
                if (name && *name)
                    xpathContext->addVariable(name, user->getProperty(name));
            }
        }

        Owned<IPropertyTreeIterator> configParams = bindingCfg->getElements("Transform/Param");
        {
            ForEach(*configParams)
            {
                IPropertyTree & currentParam = configParams->query();
                xpathContext->addVariable(currentParam.queryProp("@name"), currentParam.queryProp("@value"));
            }
        }

        Owned<IPropertyTree> thereq = createPTreeFromXMLString(request.str());
        ForEachItemIn(currConditionalIndex, m_customTransformClauses)
        {
            m_customTransformClauses.item(currConditionalIndex).process(context, thereq, xpathContext);
        }
        toXML(thereq, request.clear());

#if defined(_DEBUG)
        DBGLOG("MODIFIED REQUEST: %s", request.str());
#endif

    }
}
