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

#ifndef _ESPWIZ_WS_CONFIG_HPP__
#define _ESPWIZ_WS_CONFIG_HPP__

#include "ws_config_esp.ipp"
#include "environment.hpp"

//==========================================================================================



class Cws_configEx : public Cws_config
{
public:
    IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual ~Cws_configEx();

    virtual bool onGetConfigAccess(IEspContext &context, IEspConfigAccessRequest& req, IEspConfigAccessResponse& resp);

private:
};



class Cws_configSoapBindingEx : public Cws_configSoapBinding
{
public:
    Cws_configSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) 
      : Cws_configSoapBinding(cfg, name, process, llevel){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
    }
};

#endif //_ESPWIZ_WS_CONFIG_HPP__

