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

#ifndef SECUREUSER_INCL
#define SECUREUSER_INCL


#include "seclib.hpp"
//#include "MD5.hpp"

class CSecureUser : public CInterface,
    implements ISecUser,
    implements ISecCredentials
{
private:
    StringBuffer    m_realm;
    StringBuffer    m_name;
    StringBuffer    m_pw;
    StringBuffer    m_encodedPw;
    bool            m_isAuthenticated;
    StringBuffer    m_fullname;
    StringBuffer    m_firstname;
    StringBuffer    m_lastname;
    unsigned        m_userID;
    StringBuffer    m_Fqdn;
    StringBuffer    m_Peer;
    SecUserStatus   m_status;
    Owned<IProperties> m_parameters;

    CriticalSection crit;
public:
    IMPLEMENT_IINTERFACE

    CSecureUser(const char *name, const char *pw) : 
        m_name(name), m_pw(pw), m_isAuthenticated(false),m_userID(0), m_status(SecUserStatus_Unknown)
    {
    }

    virtual ~CSecureUser()
    {
    }

    virtual void setAuthenticated(bool authenticated)
    {
        m_isAuthenticated = authenticated;
    }

//interface ISecUser
    const char * getName()
    {
        return m_name.str();
    }
    
    bool setName(const char * name)
    {
        m_name.clear().append(name);
        return true;
    }
    
    const char * getFullName()
    {
        return m_fullname.str();
    }
    
    bool setFullName(const char * name)
    {
        m_fullname.clear().append(name);
        return TRUE;
    }

    virtual const char * getFirstName()
    {
        return m_firstname.str();
    }

    virtual bool setFirstName(const char * fname)
    {
        if(fname != NULL)
        {
            m_firstname.clear().append(fname);
        }
        return true;
    }

    virtual const char * getLastName()
    {
        return m_lastname.str();
    }
    virtual bool setLastName(const char * lname)
    {
        if(lname != NULL)
        {
            m_lastname.clear().append(lname);
        }
        return true;
    }
    const char * getRealm()
    {
        return m_realm.str();
    }
    bool setRealm(const char * name)
    {
        m_realm.clear().append(name);
        return true;
    }
    const char * getFqdn()
    {
        return m_Fqdn.str();
    }
    bool setFqdn(const char * Fqdn)
    {
        m_Fqdn.clear().append(Fqdn);
        return true;
    }
   const char *getPeer()
   {
        return m_Peer.str();
   }
   bool setPeer(const char *Peer)
   {
       m_Peer.clear().append(Peer);
       return true;
   }

    virtual SecUserStatus getStatus()
    {
        return m_status;
    }

    virtual bool setStatus(SecUserStatus Status)
    {
        m_status = Status;
        return true;
    }

    bool isAuthenticated()
    {
        return m_isAuthenticated;
    }

    ISecCredentials & credentials()
    {
        return *this;
    }

    void setProperty(const char* name, const char* value)
    {
        if (!m_parameters)
            m_parameters.setown(createProperties(false));
        m_parameters->setProp(name, value);
    }

    const char* getProperty(const char* name)
    {
        if (m_parameters)
            return m_parameters->queryProp(name);
        return NULL;
    }

    void setPropertyInt(const char* name, int value)
    {
        if (!m_parameters)
            m_parameters.setown(createProperties(false));
        m_parameters->setProp(name, value);
    }

    int getPropertyInt(const char* name)
    {
        if (m_parameters)
            return m_parameters->getPropInt(name);
        return 0;
    }

    

//interface ISecCredentials
    bool setPassword(const char * pw)
    {
        m_pw.clear();
        m_pw.append(pw);
        return true;
    }
    const char* getPassword()
    {
        return m_pw.str();
    }

    bool addToken(unsigned type, void * data, unsigned length)
    {
        return false;  //not supported yet
    }
    virtual unsigned getUserID()
    {
        return m_userID;
    }

    virtual CDateTime & getPasswordExpiration(CDateTime& expirationDate){ assertex(false); return expirationDate; }
    virtual bool setPasswordExpiration(CDateTime& expirationDate) { assertex(false);return true; }
    virtual int getPasswordDaysRemaining() {assertex(false);return -1;}

    virtual void copyTo(ISecUser& destination)
    {
        destination.setAuthenticated(isAuthenticated());
        destination.setName(getName());
        destination.setFullName(getFullName());
        destination.setFirstName(getFirstName());
        destination.setLastName(getLastName());
        destination.setRealm(getRealm());
        destination.setFqdn(getFqdn());
        destination.setPeer(getPeer());
        destination.credentials().setPassword(credentials().getPassword());
        CDateTime tmpTime;
        destination.setPasswordExpiration(getPasswordExpiration(tmpTime));
        destination.setStatus(getStatus());

        if(m_parameters.get()==NULL)
            return;
        CriticalBlock b(crit);
        Owned<IPropertyIterator> Itr = m_parameters->getIterator();
        Itr->first();
        while(Itr->isValid())
        {
            destination.setProperty(Itr->getPropKey(),m_parameters->queryProp(Itr->getPropKey()));
            Itr->next();
        }


        //addToken is not currently implemented....
//      DBGLOG("Copied name %s to %s",getName(),destination.getName());
    }

    ISecUser * clone()
    {
        //DBGLOG("Beginning of clone()");
        CSecureUser* newuser = new CSecureUser(m_name.str(), m_pw.str());
        //DBGLOG("Before copy to");
        if(newuser)
            copyTo(*newuser);
    //DBGLOG("After copy to");
        return newuser;
    }

};

#endif // SECUREUSER_INCL
//end
