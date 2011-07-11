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
#include "hqlxmldb.hpp"
#include "jptree.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jprop.hpp"


static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/hql/hqlxmldb.cpp $ $Id: hqlxmldb.cpp 65973 2011-07-04 15:37:51Z ghalliday $");

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
        StringBuffer s;
        scope->locals->getProp(name, s);
        scope->locals->setProp(name, s.append(value).str());
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
    IPropertyTree * ptree = loadPropertyTree(text, true);
    root->setPropTree(element, ptree);
}

HQL_API IXmlScope* loadXML(const char* filename)
{
    IPropertyTree * ptree = loadPropertyTree(filename, true);
    return ptree ? new CXmlScope(ptree, NULL) : NULL;
}

HQL_API IXmlScope* createXMLScope()
{
    return loadXML("<?xml version=\"1.0\" encoding=\"UTF-8\"?><root/>");
}
