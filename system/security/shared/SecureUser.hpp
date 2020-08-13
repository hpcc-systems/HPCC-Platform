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

#ifndef SECUREUSER_INCL
#define SECUREUSER_INCL


#include "seclib.hpp"
//#include "MD5.hpp"

class CSecureUser : implements ISecUser, implements ISecCredentials, public CInterface
{
private:
    StringBuffer    m_realm;
    StringBuffer    m_name;
    StringBuffer    m_pw;
    StringBuffer    m_encodedPw;
    authStatus      m_authenticateStatus;
    StringBuffer    m_fullname;
    StringBuffer    m_firstname;
    StringBuffer    m_lastname;
    StringBuffer    m_employeeID;
    StringBuffer    m_employeeNumber;
    StringBuffer    m_distinguishedName;
    unsigned        m_userID;
    StringBuffer    m_Fqdn;
    StringBuffer    m_Peer;
    SecUserStatus   m_status;
    Owned<IProperties> m_parameters;
    unsigned        m_sessionToken;
    StringBuffer    m_signature;
    static const SecFeatureSet s_safeFeatures = SUF_ALL_FEATURES;
    static const SecFeatureSet s_implementedFeatures = (s_safeFeatures & ~(SUF_GetDataElement | SUF_GetDataElements | SUF_SetData));

    CriticalSection crit;
public:
    IMPLEMENT_IINTERFACE

    CSecureUser(const char *name, const char *pw) :
        m_name(name), m_pw(pw), m_authenticateStatus(AS_UNKNOWN), m_userID(0), m_status(SecUserStatus_Unknown), m_sessionToken(0), m_parameters(createProperties(false))
    {
    }

    virtual ~CSecureUser()
    {
    }

//interface ISecUser
    SecFeatureSet queryFeatures(SecFeatureSupportLevel level) const override
    {
        switch (level)
        {
        case SFSL_Safe:
            return s_safeFeatures;
        case SFSL_Implemented:
            return s_implementedFeatures;
        case SFSL_Unsafe:
            return (SUF_ALL_FEATURES & ~s_safeFeatures);
        default:
            return SUF_NO_FEATURES;
        }
    }

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

    const char * getEmployeeID()
    {
        return m_employeeID.str();
    }

    bool setEmployeeID(const char * emplID)
    {
        m_employeeID.set(emplID);
        return true;
    }

    const char * getEmployeeNumber()
    {
        return m_employeeNumber.str();
    }

    bool setEmployeeNumber(const char * emplNumber)
    {
        m_employeeNumber.set(emplNumber);
        return true;
    }

    const char * getDistinguishedName()
    {
        return m_distinguishedName.str();
    }

    bool setDistinguishedName(const char * dn)
    {
        m_distinguishedName.set(dn);
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

    ISecCredentials & credentials()
    {
        return *this;
    }

    void setProperty(const char* name, const char* value)
    {
        m_parameters->setProp(name, value);
    }

    const char* getProperty(const char* name)
    {
        return m_parameters->queryProp(name);
    }

    void setPropertyInt(const char* name, int value)
    {
        m_parameters->setProp(name, value);
    }

    int getPropertyInt(const char* name)
    {
        return m_parameters->getPropInt(name);
    }

    IPropertyIterator * getPropertyIterator() const override
    {
        return m_parameters->getIterator();
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

    void setSessionToken(unsigned token)
    {
        m_sessionToken = token;
    }

    unsigned getSessionToken()
    {
        return m_sessionToken;
    }

    void setSignature(const char * signature)
    {
        m_signature.clear().append(signature);
    }

    const char * getSignature()
    {
        return m_signature.str();
    }

    virtual unsigned getUserID()
    {
        return m_userID;
    }

    virtual CDateTime & getPasswordExpiration(CDateTime& expirationDate){ return expirationDate; }
    virtual bool setPasswordExpiration(CDateTime& expirationDate) { return true; }
    virtual int getPasswordDaysRemaining() {return scPasswordNeverExpires;}//never expires
    virtual authStatus getAuthenticateStatus() {return m_authenticateStatus;}
    virtual void setAuthenticateStatus(authStatus status){m_authenticateStatus = status;}

    virtual void copyTo(ISecUser& destination)
    {
        destination.setAuthenticateStatus(getAuthenticateStatus());
        destination.setName(getName());
        destination.setFullName(getFullName());
        destination.setFirstName(getFirstName());
        destination.setLastName(getLastName());
        destination.setEmployeeID(getEmployeeID());
        destination.setEmployeeNumber(getEmployeeNumber());
        destination.setRealm(getRealm());
        destination.setFqdn(getFqdn());
        destination.setPeer(getPeer());
        destination.credentials().setPassword(credentials().getPassword());
        destination.credentials().setSessionToken(credentials().getSessionToken());
        destination.credentials().setSignature(credentials().getSignature());
        CDateTime exp;
        credentials().getPasswordExpiration(exp);
        destination.credentials().setPasswordExpiration(exp);
        CDateTime tmpTime;
        destination.setPasswordExpiration(getPasswordExpiration(tmpTime));
        destination.setStatus(getStatus());
        CriticalBlock b(crit);
        Owned<IPropertyIterator> Itr = m_parameters->getIterator();
        ForEach(*Itr)
        {
            destination.setProperty(Itr->getPropKey(),m_parameters->queryProp(Itr->getPropKey()));
        }


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

    IPropertyTree* getDataElement(const char* xpath = ".") const override
    {
        return nullptr;
    }

    IPropertyTreeIterator* getDataElements(const char* xpath = ".") const override
    {
        return nullptr;
    }

    bool setData(IPropertyTree* data) override
    {
        return false;
    }
};

#endif // SECUREUSER_INCL
//end
