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

#ifndef SECRESOURCELIST_INCL
#define SECRESOURCELIST_INCL


#include "seclib.hpp"
#include <map>
#include <string>


class CSecurityResourceList : implements ISecResourceList, public CInterface
{
private:
    bool m_complete;
    StringAttr m_name;
    IArrayOf<ISecResource> m_rlist;
    std::map<std::string, Linked<ISecResource> > m_rmap;

public:
    IMPLEMENT_IINTERFACE

    CSecurityResourceList(const char *name) : m_complete(0)
    {
        m_name.set(name);
    }

    void setAuthorizationComplete(bool value)
    {
        m_complete=value;
    }

    IArrayOf<ISecResource>& getResourceList()
    {
        return m_rlist;
    }

//interface ISecResourceList : extends IInterface
    bool isAuthorizationComplete()
    {
        return m_complete;
    }

    ISecResourceList * clone()
    {
        ISecResourceList* _newList = new CSecurityResourceList(m_name.get());
        if(!_newList)
            return NULL;
        copyTo(*_newList);
        return _newList;
    }

    void clear()
    {
        m_rmap.clear();
        m_rlist.kill();
    }


    bool copyTo(ISecResourceList& destination)
    {
        ForEachItemIn(x, m_rlist)
        {
            CSecurityResource* res = (CSecurityResource*)(&(m_rlist.item(x)));
            if(res)
                destination.addResource(res->clone());
        }
        return true;
    }

    
    ISecResource* addResource(const char * name)
    {
        if(!name || !*name)
            return NULL;

        ISecResource* resource = m_rmap[name];
        if(resource == NULL)
        {   
            resource = new CSecurityResource(name);
            m_rlist.append(*resource);
            m_rmap[name].set(resource);
        }
        return resource;
    }

    void addResource(ISecResource* resource)
    {
        if(resource == NULL)
            return;

        const char* name = resource->getName();
        if(!name || !*name)
            return;

        ISecResource* r = m_rmap[name];
        if(r == NULL)
        {
            m_rlist.append(*resource);
            m_rmap[name].set(resource);
        }
        else
            resource->Release();
                    
    }

    bool addCustomResource(const char * name, const char * config)
    {
        return false;
    }

    ISecResource * getResource(const char * Resource)
    {
        if(!Resource || !*Resource)
            return NULL;

        ISecResource* r = m_rmap[Resource];
        if(r)
            return LINK(r);
        else
            return NULL;
    }

    virtual unsigned count()
    {
        return m_rlist.length();
    }
    virtual const char* getName()
    {
        return m_name.get();
    }
    virtual ISecResource * queryResource(unsigned seq)
    {
        if(seq < m_rlist.length())
            return &(m_rlist.item(seq));
        else
            return NULL;
    }

    ISecPropertyIterator * getPropertyItr()
    {
        return new ArrayIIteratorOf<IArrayOf<struct ISecResource>, ISecProperty, ISecPropertyIterator>(m_rlist);

    }
    virtual ISecProperty* findProperty(const char* name)
    {
        if(!name || !*name)
            return NULL;
        return m_rmap[name];
    }

    StringBuffer& toString(StringBuffer& s)
    {
        s.appendf("name=%s, count=%u.", m_name.get(), count());
        for (unsigned i=0; i<count(); i++)
        {
            s.appendf("\nItem %d: ",i+1);
            queryResource(i)->toString(s);
        }
        return s;
    }
};



#endif // SECRESOURCELIST_INCL
//end
