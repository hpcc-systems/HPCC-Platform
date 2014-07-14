/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#ifndef XMLDB_INCL
#define XMLDB_INCL

#include "jiface.hpp"
#include "jiter.hpp"
#include "hql.hpp"
#include "hqlerror.hpp"

//-----------------------------------------------------------------------------
// Class Definitions
//-----------------------------------------------------------------------------
/**
 * Iterator interface
 */
class StringBuffer; 

interface IXmlScope : public IInterface
{
    virtual IIterator* getScopes(const char *, bool doAll) = 0;
    virtual bool getValue(const char*, StringBuffer &) = 0;
    virtual bool setValue(const char*, const char*) = 0;
    virtual bool appendValue(const char*, const char*) = 0;
    virtual bool declareValue(const char *) = 0;

    virtual int getInt(const char*) = 0;
    virtual int getInt(const char*, int defValue) = 0;
    virtual void loadXML(const char * text, const char * element) = 0;
};


interface IEclRepository;

interface ITemplateContext : public IInterface
{
    // context variables
    virtual IXmlScope* queryXmlScope() = 0;

    // convenient functions
    virtual bool isInModule(const char* module_name, const char* attr_name) = 0;
    virtual StringBuffer& getDataType(const char* field, StringBuffer& tgt) = 0;
    
    virtual StringBuffer& mangle(const char* src, StringBuffer& mangled) = 0;
    virtual StringBuffer& demangle(const char* mangled, StringBuffer& demangled) = 0;

    virtual void reportError(int errNo,const char* format,...) __attribute__((format(printf, 3, 4))) = 0;
    virtual void reportWarning(int warnNo,const char* format,...) __attribute__((format(printf, 3, 4))) = 0;

    // Ideally, the user has no need to use this.
    virtual IEclRepository* queryDataServer() = 0;
};

class EclTemplateBase
{
protected:
    StringBuffer& m_result;
    ITemplateContext* m_templateContext;

public:
    // constructor
    EclTemplateBase(ITemplateContext* context, StringBuffer& result) 
        : m_result(result), m_templateContext(context) { }

    // convenient methods
    IXmlScope* queryRootScope() { return m_templateContext->queryXmlScope(); }
    ITemplateContext* queryContext() { return m_templateContext; }

    bool isInModule(const char* moduleName, const char* attrName) { return m_templateContext->isInModule(moduleName,attrName); }
    StringBuffer& mangle(const char* src, StringBuffer& mangled) { return m_templateContext->mangle(src,mangled); }
    StringBuffer& demangle(const char* src, StringBuffer& mangled) { return m_templateContext->demangle(src,mangled); }
    StringBuffer& getDataType(const char* field,StringBuffer& tgt) { return m_templateContext->getDataType(field,tgt); }

    // action
    virtual void doExpand() = 0;
};

#define HashFor(sub,scope)  \
    IIterator*  sub##Itr = scope->getScopes(#sub, false); \
    ForEach(*sub##Itr) \
    { \
        IXmlScope* sub = (IXmlScope*)&sub##Itr->query(); 

#define EndHashFor(sub) \
    } \
    sub##Itr->Release(); 

/* variable name can not be defaulted to the subscope name */
#define HashForEx(sub,name,scope)  \
    IIterator*  sub##Itr = scope->getScopes(name, false); \
    ForEach(*sub##Itr) \
    { \
        IXmlScope* sub = (IXmlScope*)&sub##Itr->query(); 

#define EndHashForEx(sub) \
    } \
    sub##Itr->Release(); 

//-----------------------------------------------------------------------------
// Prototypes
//-----------------------------------------------------------------------------

HQL_API IXmlScope* loadXML(const char* filename);
HQL_API IXmlScope* createXMLScope();

#endif
