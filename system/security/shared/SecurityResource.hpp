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

#ifndef SECURITYRESOURCE_INCL
#define SECURITYRESOURCE_INCL

#include "seclib.hpp"
class CSecurityResource : public CInterface,
    implements ISecResource
{
private:
    StringAttr m_name;
    StringBuffer m_value;
    int        m_access;
    int        m_required_access;
    Owned<IProperties> m_parameters;

    StringBuffer m_description;

    SecResourceType m_resourcetype;

public: 
    IMPLEMENT_IINTERFACE

    CSecurityResource(const char *name) : m_name(name), m_access(SecAccess_Unknown), m_required_access(SecAccess_Unknown), m_resourcetype(RT_DEFAULT)
    {
    }

    void addAccess(int flags)
    {
        m_access |= flags;
    }
    
//interface ISecResource : extends IInterface
    const char * getName()
    {
        return m_name.get();
    }

    void setAccessFlags(int flags)
    {
        m_access = flags;
    }
        
    int getAccessFlags()
    {
        return m_access;
    }

    virtual int addParameter(const char* name, const char* value)
    {
        if (!m_parameters)
            m_parameters.setown(createProperties(false));
        m_parameters->setProp(name, value);
        return 0;
    }

    virtual const char * getParameter(const char * name)
    {
        if (m_parameters)
        {
            const char *value = m_parameters->queryProp(name);
            return value;
        }

        return NULL;

    }
    virtual void setRequiredAccessFlags(int flags)
    {
        m_required_access = flags;
    }

    virtual int getRequiredAccessFlags()
    {
        return m_required_access;
    }

    virtual void setDescription(const char* description)
    {
        m_description.clear().append(description);
    }

    virtual const char* getDescription()
    {
        return m_description.str();
    }

    virtual void setValue(const char* Value)
    {
        m_value.clear().append(Value);
    }

    virtual const char* getValue()
    {
        return m_value.str();
    }

    virtual ISecResource* clone()
    {
        CSecurityResource* _res = new CSecurityResource(m_name.get());
        if(!_res)
            return NULL;

        _res->setResourceType(m_resourcetype);
        _res->setDescription(m_description.str());
        _res->setValue(m_value.str());

        _res->setRequiredAccessFlags(getRequiredAccessFlags());
        _res->setAccessFlags(getAccessFlags());

        if(!m_parameters)
            return _res;

        Owned<IPropertyIterator> Itr = m_parameters->getIterator();
        Itr->first();
        while(Itr->isValid())
        {
            _res->addParameter(Itr->getPropKey(),m_parameters->queryProp(Itr->getPropKey()));
            Itr->next();
        }
        return _res;
    }

    virtual void copy(ISecResource* from)
    {
        if(!from)
            return;
        CSecurityResource* _res = (CSecurityResource*)(from);
        if(!_res)
            return;

        setDescription(_res->m_description.str());
        setValue(_res->m_value.str());
        setAccessFlags(_res->getAccessFlags());

        if(!_res->m_parameters)
            return;

        Owned<IPropertyIterator> Itr = _res->m_parameters->getIterator();
        Itr->first();
        while(Itr->isValid())
        {
            addParameter(Itr->getPropKey(), _res->m_parameters->queryProp(Itr->getPropKey()));
            Itr->next();
        }
        return;
    }

    virtual SecResourceType getResourceType()
    {
        return m_resourcetype;
    }

    virtual void setResourceType(SecResourceType resourcetype)
    {
        m_resourcetype = resourcetype;
    }

    StringBuffer& toString(StringBuffer& s)
    {
        s.appendf("%s: %s (value: %s, rqr'ed access: %d, type: %s)", m_name.get(), m_description.str(), 
            m_value.str(), m_required_access, resTypeDesc(m_resourcetype));
        return s;
    }
};




#endif // SECURITYRESOURCE_INCL
//end
