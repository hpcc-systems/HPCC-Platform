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

#ifndef ESDL_SVC_CUSTOM_HPP_
#define ESDL_SVC_CUSTOM_HPP_

#ifdef ESDL_TRANSFORM_EXPORTS
 #define esdl_svc_cust_decl DECL_EXPORT
#else
 #define esdl_svc_cust_decl
#endif

#include "jlib.hpp"
#include "jstring.hpp"
#include "jptree.hpp"
#include "jlog.hpp"
#include "esp.hpp"

#include <map>
#include <mutex>
#include <thread>

#include "tokenserialization.hpp"
#include "xpathprocessor.hpp"

class CEsdlCustomTransformRule : public CInterface
{
private:
    StringAttr m_targetField;
    StringAttr m_xpathToValue;
    StringAttr m_ruleName;
    Owned<ICompiledXpath> m_compiledValueXpath;
    bool m_optional = false;
    bool m_attemptToSet = false;

public:

    IMPLEMENT_IINTERFACE;
    CEsdlCustomTransformRule(const char * transferName, const char * targetfield, const char * xpathToValue, bool optional, bool attemptToSet)
    :  m_ruleName(transferName), m_targetField(targetfield), m_xpathToValue(xpathToValue), m_optional(optional), m_attemptToSet(attemptToSet)
    {
        m_compiledValueXpath.set(getCompiledXpath(xpathToValue));
    }

#if defined(_DEBUG)
    void toDBGLog()
    {
        DBGLOG(">%s> %s field '%s' to '%s'", m_ruleName.str(), (m_attemptToSet) ? "SetValue" : "AppendValue", m_targetField.str() , m_xpathToValue.str());
    }
#endif

    virtual ~CEsdlCustomTransformRule(){}

    const char * queryValueXpath() const
    {
        return m_xpathToValue.str();
    }
    const char * queryTargetField() const
    {
        return m_targetField.str();
    }

    const char * queryName() const
    {
        return m_ruleName.str();
    }

    ICompiledXpath * queryCompiledValuePath()
    {
        return m_compiledValueXpath.getLink();
    }

    bool isOptional () const { return m_optional; }
    bool isPerformingASet() const { return m_attemptToSet; }

};

class CEsdlCustomTransformChoose : public CInterface
{
private:
    Owned<ICompiledXpath> m_compiledConditionalXpath;
    CIArrayOf<CEsdlCustomTransformRule> m_chooseClauses;
    CIArrayOf<CEsdlCustomTransformRule> m_otherwiseClauses;
    CIArrayOf<CEsdlCustomTransformChoose> m_childChooseClauses;
    CIArrayOf<CEsdlCustomTransformChoose> m_childOtherwiseClauses;

public:
    IMPLEMENT_IINTERFACE;

    CEsdlCustomTransformChoose(IPropertyTree * choosewhen);
    ~CEsdlCustomTransformChoose()
    {
#if defined(_DEBUG)
        DBGLOG("CEsdlCustomTransformClause released!");
#endif
    }
    void process(IEspContext * context, IPropertyTree *request, IXpathContext * xpathContext);
#if defined(_DEBUG)
    void toDBGLog();
#endif

private:
    void compileChildChoose(IPropertyTree * nested, bool otherwise);
    void compileClauses(IPropertyTreeIterator * clauseIter, bool otherwise, bool typeIsSet);
    void compileClauses(IPropertyTree * clauses, bool otherwise);
    void addTransformClause(CEsdlCustomTransformRule * fieldmapping, bool otherwise);
    void addChildTransformClause(CEsdlCustomTransformChoose * childConditional, bool otherwise);
    bool evaluate(IXpathContext * xpathContext);
    void processClauses(IPropertyTree *request, IXpathContext * xpathContext, CIArrayOf<CEsdlCustomTransformRule> & m_fieldTransforms);
    void processClauses(IEspContext * context, IPropertyTree *request, IXpathContext * xpathContext, bool otherwise);
    void processChildClauses(IEspContext * context, IPropertyTree *request, IXpathContext * xpathContext,  bool otherwise);
};

interface IEsdlCustomTransform : extends IInterface
{
       virtual void processTransform(IEspContext * context,  StringBuffer & request, IPropertyTree * bindingCfg)=0;
};

class CEsdlCustomTransform : implements IEsdlCustomTransform, public CInterface
{
private:
    CIArrayOf<CEsdlCustomTransformChoose> m_customTransformClauses;
    StringAttr m_name;

public:
    IMPLEMENT_IINTERFACE;
    CEsdlCustomTransform(){}
    CEsdlCustomTransform(IPropertyTree &cfg);

#if defined(_DEBUG)
    void toDBGLog()
    {
        DBGLOG(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>transform: '%s'>>>>>>>>>>", m_name.str());
        ForEachItemIn(i, m_customTransformClauses)
        {
            CEsdlCustomTransformChoose & cur = m_customTransformClauses.item(i);
            cur.toDBGLog();
        }
        DBGLOG("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<transform<<<<<<<<<<<<");
      }
#endif

    virtual ~CEsdlCustomTransform()
    {
#if defined(_DEBUG)
        DBGLOG("CEsdltransformProcessor '%s' released!", m_name.str());
#endif
    }

    void processTransform(IEspContext * context,  StringBuffer & request, IPropertyTree * bindingCfg);
};

#endif /* ESDL_SVC_CUSTOM_HPP_ */
