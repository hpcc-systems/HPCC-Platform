 /*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef XPATH_MANAGER_HPP_
#define XPATH_MANAGER_HPP_

#include "xmllib.hpp"
#include "jliball.hpp"

interface XMLLIB_API ICompiledXpath : public IInterface
{
    virtual const char * getXpath() = 0;
    virtual void extractReferences(StringArray &functions, StringArray &variables) = 0;
};

interface IXpathContextIterator;

interface XMLLIB_API IXpathContext : public IInterface
{
    virtual bool addVariable(const char * name, const char * val) = 0;
    virtual bool addXpathVariable(const char * name, const char * xpath) = 0;
    virtual bool addCompiledVariable(const char * name, ICompiledXpath * compiled) = 0;

    virtual const char * getVariable(const char * name, StringBuffer & variable) = 0;

    virtual bool evaluateAsBoolean(const char * xpath) = 0;
    virtual bool evaluateAsString(const char * xpath, StringBuffer & evaluated) = 0;
    virtual bool evaluateAsBoolean(ICompiledXpath * compiledXpath) = 0;
    virtual const char * evaluateAsString(ICompiledXpath * compiledXpath, StringBuffer & evaluated) = 0;
    virtual double evaluateAsNumber(ICompiledXpath * compiledXpath) = 0;
    virtual  IXpathContextIterator *evaluateAsNodeSet(ICompiledXpath * compiledXpath) = 0;

    virtual bool setXmlDoc(const char * xmldoc) = 0;
    virtual void setUserData(void *) = 0;
    virtual void *getUserData() = 0;

    virtual void registerFunction(const char *xmlns, const char * name, void *f) = 0;
    virtual void registerNamespace(const char *prefix, const char *uri) = 0;
    virtual const char *queryNamespace(const char *prefix) = 0;
    virtual void beginScope(const char *name) = 0;
    virtual void endScope() = 0;

    virtual bool addInputXpath(const char * name, const char * xpath) = 0; //values should be declared as parameters before use, "strict parameter mode" requires it
    virtual bool addInputValue(const char * name, const char * value) = 0; //values should be declared as parameters before use, "strict parameter mode" requires it
    virtual bool declareParameter(const char * name, const char *value) = 0;
    virtual bool declareCompiledParameter(const char * name, ICompiledXpath * compiled) = 0;
    virtual void declareRemainingInputs() = 0;
};

interface IXpathContextIterator : extends IIteratorOf<IXpathContext> { };

class CXpathContextScope : CInterface
{
private:
    Linked<IXpathContext> context;
    Linked<IProperties> namespaces;
public:
    IMPLEMENT_IINTERFACE;
    CXpathContextScope(IXpathContext *ctx, const char *name, IProperties *ns=nullptr) : context(ctx), namespaces(ns)
    {
        context->beginScope(name);
    }
    virtual ~CXpathContextScope()
    {
        if (namespaces)
        {
            Owned<IPropertyIterator> ns = namespaces->getIterator();
            ForEach(*ns)
            {
                const char *prefix = ns->getPropKey();
                const char *uri = namespaces->queryProp(prefix);
                context->registerNamespace(prefix, isEmptyString(uri) ? nullptr : uri);
            }
        }
        context->endScope();
    }
};

extern "C" XMLLIB_API ICompiledXpath* compileXpath(const char * xpath);
extern "C" XMLLIB_API IXpathContext*  getXpathContext(const char * xmldoc, bool strictParameterDeclaration, bool removeDocNamespaces);

#define ESDLScriptCtxSection_Store "store"
#define ESDLScriptCtxSection_Logging "logging"
#define ESDLScriptCtxSection_TargetConfig "target"
#define ESDLScriptCtxSection_BindingConfig "config"
#define ESDLScriptCtxSection_ESDLInfo "esdl"
#define ESDLScriptCtxSection_ESDLRequest "ESDLRequest"
#define ESDLScriptCtxSection_FinalRequest "FinalRequest"

interface IEsdlScriptContext : extends IInterface
{
    virtual IXpathContext* createXpathContext(const char *section, bool strictParameterDeclaration) = 0;
    virtual void *queryEspContext() = 0;
    virtual void setContent(const char *section, const char *xml) = 0;
    virtual void setContent(const char *section, IPropertyTree *tree) = 0;
    virtual void setAttribute(const char *section, const char *name, const char *value) = 0;
    virtual const char *queryAttribute(const char *section, const char *name) = 0;
    virtual const char *getAttribute(const char *section, const char *name, StringBuffer &s) = 0;
    virtual const char *getXPathString(const char *xpath, StringBuffer &s) const = 0;
    virtual __int64 getXPathInt64(const char *xpath, __int64 dft=0) const = 0;
    virtual bool getXPathBool(const char *xpath, bool dft=false) const = 0;
    virtual void toXML(StringBuffer &xml, const char *section, bool includeParentNode=false) = 0;
    virtual void toXML(StringBuffer &xml) = 0;
    virtual IPropertyTree *createPTreeFromSection(const char *section) = 0;
};

extern "C" XMLLIB_API IEsdlScriptContext *createEsdlScriptContext(void * espContext);

#endif /* XPATH_MANAGER_HPP_ */
