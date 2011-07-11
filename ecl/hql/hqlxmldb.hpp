/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
#ifndef XMLDB_INCL
#define XMLDB_INCL

#include "jiface.hpp"
#include "jiter.hpp"
#include "hql.hpp"

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

    virtual void reportError(int errNo,const char* format,...) = 0;
    virtual void reportWarning(int warnNo,const char* format,...) = 0;      

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
