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

#pragma warning(disable : 4786)
#ifndef _ESP_BIND_NG_HPP__
#define _ESP_BIND_NG_HPP__

#include "jliball.hpp"
#include "esp.hpp"
#include "soapesp.hpp"

class CEspNgContext  : public CInterface, implements IEspContext
{
protected:
    StringBuffer xuserid;
    StringBuffer xpw;
    
    Owned<ISecUser> secuser;
    Owned<ISecManager> secmgr;
    Owned<ISecResourceList> reslist;
    Owned<IAuthMap> authMap;
    Owned<ISecPropertyList> secprops;
    Owned<IProperties> props;
    Owned<IMapInfo> mapInfo;
    Owned<ISecResourceList> m_resources;

    StringBuffer contextPath;
    void *bindvalue;
    void *servvalue;

    StringBuffer xpeer;
    StringBuffer servname;

public:
    IMPLEMENT_IINTERFACE;

    CEspNgContext(){}
    virtual ~CEspNgContext(){}

//IEspContext
    void setUserID(const char * userid){xuserid.clear().append(userid);}
    StringBuffer & getUserID(StringBuffer & userid){return userid.clear().append(xuserid);}
    const char * queryUserId(){return xuserid.str();}
    
    void setPassword(const char * password){xpw.clear().append(password);}
    virtual StringBuffer & getPassword(StringBuffer & password){return password.clear().append(xpw);}
    const char * queryPassword(){return xpw.str();}
    
    void setRealm(const char * realm){}
    StringBuffer & getRealm(StringBuffer & realm){return realm.clear();}
    const char * queryRealm(){return "";}
    
    void setUser(ISecUser * user){secuser.setown(user);}
    ISecUser * queryUser(){return secuser.get();}
    
    virtual void setResources(ISecResourceList* rlist)
    {
        m_resources.setown(rlist);
    }

    virtual ISecResourceList* queryResources()
    {
        return m_resources.get();
    }


    void setSecManger(ISecManager * mgr){secmgr.setown(mgr);}
    ISecManager * querySecManager(){return secmgr.get();}

    void setContextPath(const char * path){contextPath.clear().append(path);}
    const char * getContextPath(){return contextPath.str();}
    
    void setBindingValue(void * value){bindvalue=value;}
    void * getBindingValue(){return bindvalue;}
    
    void setServiceValue(void * value){servvalue=value;}
    void * getServiceValue(){return servvalue;}
    
    void setToBeAuthenticated(bool val){}
    bool toBeAuthenticated(){return false;}
    
    void setPeer(const char * peer){xpeer.clear().append(peer);}
    StringBuffer & getPeer(StringBuffer & peer){return peer.clear().append(xpeer);}
    
    void setFeatureAuthMap(IAuthMap * map){authMap.setown(map);}
    IAuthMap * queryAuthMap(){return authMap.get();}

    void setSecuritySettings(ISecPropertyList * slist){secprops.setown(slist);}
    ISecPropertyList * querySecuritySettings(){return secprops.get();}

    bool authorizeFeature(const char * pszFeatureUrl, SecAccessFlags & access){return true;}
    bool authorizeFeatures(StringArray & features, IEspStringIntMap & pmap){return true;}
    bool authorizeFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & access){return true;}
    bool authorizeFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & access,bool bCheckTrial , SecUserStatus& user_status){return true;}
    bool authorizeFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & access,bool bCheckTrial ,int DebitUnits,  SecUserStatus& user_status) {return true;}

    bool validateFeatureAccess(const char * pszFeatureUrl, unsigned required, bool throwExcpt){return true;}
    void setServAddress(const char * host, short port){}
    void getServAddress(StringBuffer & host, short & port){}
    void AuditMessage(AuditType type, const char * filterType, const char * title, const char * parms, ...) {}

    void setServiceName(const char *name){servname.clear().append(servname);}
    const char * queryServiceName(const char *name){return servname.str();}

    IProperties *   queryRequestParameters(){return props.get();}
    void            setRequestParameters(IProperties * Parameters){props.set(Parameters);}

    IProperties * queryXslParameters(){return NULL;}
    void addOptions(unsigned opts){}
    void removeOptions(unsigned opts){}
    unsigned queryOptions(){return 0;}

    // versioning
    double getClientVersion(){return 0.0;}
    void setClientVersion(double ver){}

    bool checkMinVer(double minver){return true;}
    bool checkMaxVer(double maxver){return true;}
    bool checkMinMaxVer(double minver, double maxver){return true;}
    bool checkOptional(const char*){return false;}
    IMapInfo& queryMapInfo(){return *mapInfo.get();}
    bool suppressed(const char* structName, const char* fieldName){return false;}

    virtual void setUseragent(const char * useragent){}
    virtual StringBuffer & getUseragent(StringBuffer & useragent){return useragent;}

    virtual bool isMethodAllowed(double version, const char* optional, const char* security, double minver, double maxver)
    {
        //TODO: to support method versioning?
        return true;
    }

    virtual void addOptGroup(const char* optGroup) { assertex(false); }
    virtual BoolHash& queryOptGroups()  { static BoolHash emptyHash; assertex(false); return emptyHash; }
};


class EspNgStringParameter : public CInterface, implements IEspNgParameter
{
private:
    StringBuffer name;
    StringBuffer value;
    unsigned maxlen;
    bool nil;
public:
    IMPLEMENT_IINTERFACE;

    EspNgStringParameter() : maxlen((unsigned)-1), nil(true) {} // TO Bob: shouldn't it be 0, instead -1 
    EspNgStringParameter(const char *tagname, unsigned maxlen) : name(tagname), maxlen(maxlen), nil(true) {}

    const char * queryName(){return name.str();};
    const char * queryValue(){return (nil) ? NULL : value.str();};
    void setValue(const char *val){nil=(val==NULL); value.clear().append(val);};
    unsigned getMaxLength(){return maxlen;}
    bool isNull(){return nil;}
    void setNull(){value.clear(); nil=true;}
};


class EspNgParamConverter
{
private:
    IEspNgParameter *param;
public:
    EspNgParamConverter(IEspNgParameter *p) : param(p){}
    operator const char *(){return param->queryValue();}
};

class EspNgParameterIterator : public CInterface, implements IEspNgParameterIterator
{
private:
    IEspNgParameter **p_array;
    unsigned acount;
    unsigned pos;
public:
    IMPLEMENT_IINTERFACE;

    EspNgParameterIterator(IEspNgParameter **arr, unsigned count) : p_array(arr), acount(count), pos(0) {}

    bool first(){pos=0; return isValid();}
    bool next(){pos++; return isValid();}
    bool isValid(){return (pos<acount);}

    IEspNgParameter *query(){return (isValid()) ? p_array[pos] : NULL;}
};

class CEspNgComplexType : public CInterface, implements IEspStruct, implements IEspNgComplexType
{
public:
    IMPLEMENT_IINTERFACE;
    virtual const char * queryName(){return NULL;}
};

class CEspNgRequest : public CInterface, implements IEspRequest, implements IEspNgRequest, implements IRpcSerializable
{
public:
    IMPLEMENT_IINTERFACE;
    virtual const char * queryName(){return NULL;}

    virtual void serialize(IEspContext* ctx, StringBuffer & buffer, const char * rootname)
    {
        if(rootname!=NULL && *rootname!='\0')
            buffer.appendf("<%s>",rootname);

        StringBuffer encodedStr;
        Owned<IEspNgParameterIterator> reqparms = getParameterIterator();
        for(reqparms->first(); reqparms->isValid(); reqparms->next())
        {
            
            IEspNgParameter *parm=reqparms->query();
            encodedStr.clear();
            if(parm->queryValue())
                encodeXML(parm->queryValue(),encodedStr);
            buffer.appendf("<%s>%s</%s>",parm->queryName(),encodedStr.str(),parm->queryName());
        }
        if(rootname!=NULL && *rootname!='\0')
            buffer.appendf("</%s>",rootname);
    }

    virtual bool unserialize(IRpcMessage & rpc, const char * tagname, const char * basepath) { return false;  }

    virtual IEspNgParameterIterator * getParameterIterator()
    {
        return NULL;
    }
};

class CEspNgResponse : public CInterface, implements IEspResponse, implements IEspNgResponse
{
    Owned<IMultiException> me;
public:
    CEspNgResponse(){me.setown(MakeMultiException());}
    IMPLEMENT_IINTERFACE;

    virtual void setRedirectUrl(const char * url){}
    virtual const IMultiException & getExceptions(){return *me.get();}
    virtual void noteException(IException & e){me->append(e);}
    virtual const char * queryName(){return NULL;}
};

#endif
