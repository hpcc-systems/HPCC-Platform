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

#ifndef _SOAPSERVICE_HPP__
#define _SOAPSERVICE_HPP__

//Jlib
#include "jiface.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "soapesp.hpp"

//ESP Bindings
#include "SOAP/Platform/soapmessage.hpp"
#include "http/platform/httpservice.hpp"



class CSoapService : public CInterface, implements ISoapService
{
private:
    Owned<IEspSoapBinding> m_soapbinding;
    IEspContainer* m_container;

public:
    IMPLEMENT_IINTERFACE;

    CSoapService(IEspSoapBinding* soapbinding)
    {
        m_soapbinding.set(soapbinding);
        m_container = NULL;
    };
    //CSoapService(CHttpSoapBinding* httpsoapbinding){m_httpsoapbinding.set(httpsoapbinding);};
    virtual ~CSoapService() {};
    virtual int processRequest(ISoapMessage &req, ISoapMessage& resp);
    virtual int processHeader(CHeader* header, IEspContext* ctx);
    virtual int onPost() {return 0;};

    virtual void setContainer(IEspContainer *container){m_container = container;}
    IEspContainer * queryContainer(){return m_container;}
    
    virtual const char * getServiceType() {return "SeiSoap";};

    bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process)
   {
      return true;
   }
   
};


#endif
