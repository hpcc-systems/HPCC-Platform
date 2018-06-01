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

#ifndef SECURITYRESOURCE_INCL
#define SECURITYRESOURCE_INCL

#include "seclib.hpp"
class CSecurityResource : implements ISecResource, public CInterface
{
private:
    StringAttr m_name;
    StringBuffer m_value;
    SecAccessFlags    m_access;
    SecAccessFlags    m_required_access;
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
        m_access =  SecAccessFlags((int)m_access | flags);
    }
    
//interface ISecResource : extends IInterface
    const char * getName()
    {
        return m_name.get();
    }

    void setAccessFlags(SecAccessFlags flags)
    {
        m_access = flags;
    }
        
    SecAccessFlags getAccessFlags()
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

    virtual IPropertyIterator * getParameterIterator() const override
    {
        return (m_parameters.get() ? m_parameters->getIterator() : nullptr);
    }

    virtual void setRequiredAccessFlags(SecAccessFlags flags)
    {
        m_required_access = flags;
    }

    virtual SecAccessFlags getRequiredAccessFlags()
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

        setDescription(from->getDescription());
        setValue(from->getValue());
        setAccessFlags(from->getAccessFlags());

        Owned<IPropertyIterator> Itr = from->getParameterIterator();
        if(!Itr.get())
            return;
        Itr->first();
        while(Itr->isValid())
        {
            addParameter(Itr->getPropKey(), from->getParameter(Itr->getPropKey()));
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
