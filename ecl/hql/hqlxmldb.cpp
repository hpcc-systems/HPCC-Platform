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
#include "hqlxmldb.hpp"
#include "jptree.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jprop.hpp"

class CXmlScope : public IXmlScope, public CInterface
{
    friend class CXmlScopeIterator;

protected:
    IPropertyTree* root;
    CXmlScope *parent;
    IProperties *locals;
    bool _getValue(const char* x, StringBuffer &);
    CXmlScope *_findValue(const char *name);
public:
    IMPLEMENT_IINTERFACE;

    CXmlScope(IPropertyTree *_root, CXmlScope *_parent);

    ~CXmlScope();
    virtual IIterator* getScopes(const char *id, bool doAll);
    virtual bool getValue(const char* x, StringBuffer &val);
    virtual bool setValue(const char*, const char*);
    virtual bool appendValue(const char*, const char*);
    virtual bool declareValue(const char *);
    virtual int getInt(const char* tag)
    {
        StringBuffer tmp;
        getValue(tag,tmp);
        return atoi(tmp.str());
    }
    virtual int getInt(const char* tag,int defValue)
    {
        StringBuffer tmp;
        getValue(tag,tmp);
        if (tmp.length()==0)
            return defValue;
        else
            return atoi(tmp.str());
    }
    virtual void loadXML(const char * text, const char * element);
};

class CXmlScopeIterator : public IIterator, public CInterface
{
protected:
    Array subscopes;
    unsigned index;
public:
    IMPLEMENT_IINTERFACE;
    CXmlScopeIterator(CXmlScope *parent, const char *name, bool doAll)
    {
        index = 0;
        StringBuffer x;
        if (doAll)
            x.append(".//");
        x.append(name);
        IPropertyTreeIterator* pitr = parent->root->getElements(x.str());
        for(pitr->first(); pitr->isValid(); pitr->next())
        {
            subscopes.append(*new CXmlScope(&pitr->query(),parent));
        }
        pitr->Release();
    }

    ~CXmlScopeIterator()
    {
    }
    virtual bool first()
    {
        index = 0;
        return isValid();
    };
    virtual bool next()
    {
        index++;
        return isValid();
    }
    virtual bool isValid()
    {
        return subscopes.isItem(index);
    }
    virtual IInterface & query()
    {
        return subscopes.item(index);
    }
    virtual IInterface & get()
    {
        IInterface &ret = query(); ret.Link(); return ret;
    }
};


CXmlScope::CXmlScope(IPropertyTree* _root, CXmlScope *_parent)
{
    root = _root;
    parent = _parent;
    ::Link(parent);
    locals = NULL;
}

CXmlScope::~CXmlScope()
{
    if (!parent)
    {
        root->Release();
    }

    ::Release(parent);
    ::Release(locals);
}

IIterator* CXmlScope::getScopes(const char *id, bool doAll)
{
    return new CXmlScopeIterator(this, id, doAll);
};

bool CXmlScope::_getValue(const char* x, StringBuffer &ret)
{
    if (locals && locals->hasProp(x))
    {
        locals->getProp(x, ret);
        return true;
    }
    return root->getProp(x,ret);
};

bool CXmlScope::getValue(const char* x, StringBuffer &ret)
{
    CXmlScope *scope = this;
    do
    {
        if (scope->_getValue(x, ret))
            return true;
        scope = scope->parent;
    } while (scope);
    return false;
};

/* return false if the name is already defined. */
bool CXmlScope::declareValue(const char *name)
{
    if (locals && locals->hasProp(name))
        return false;
    
    if (!locals)
        locals = createProperties(true);
    locals->setProp(name, "");

    return true;
};

CXmlScope *CXmlScope::_findValue(const char *name)
{
    CXmlScope *scope = this;
    do
    {
        if (scope->locals && scope->locals->hasProp(name))
            return scope;
        scope = scope->parent;
    } while (scope);
    return NULL;
}

/* return false if the name is not defined. */
bool CXmlScope::setValue(const char *name, const char *value)
{
    CXmlScope *scope = _findValue(name);
    if (scope)
    {
        scope->locals->setProp(name, value);
        return true;
    }
    else
    {
        // recover
        declareValue(name);
        return false;
    }
};

/* return false if the name is not defined. */
bool CXmlScope::appendValue(const char *name, const char *value)
{
    CXmlScope *scope = _findValue(name);
    if (scope)
    {
        scope->locals->appendProp(name, value);
        return true;
    }
    else
    {
        // recovery:
        declareValue(name);
        return false;
    }
};

void CXmlScope::loadXML(const char * text, const char * element)
{
    IPropertyTree * ptree = createPTreeFromXMLString(text, ipt_caseInsensitive);
    root->setPropTree(element, ptree);
}

HQL_API IXmlScope* loadXML(const char* xml)
{
    assertex(xml);
    IPropertyTree * ptree = createPTreeFromXMLString(xml, ipt_caseInsensitive);
    return ptree ? new CXmlScope(ptree, NULL) : NULL;
}

HQL_API IXmlScope* createXMLScope()
{
    return loadXML("<?xml version=\"1.0\" encoding=\"UTF-8\"?><root/>");
}
