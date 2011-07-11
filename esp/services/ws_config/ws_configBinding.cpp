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

#include "ws_configService.hpp"
#include "jwrapper.hpp"



class Cws_configBindingEx : public Cws_configSoapBinding
{
public:
    Cws_configBindingEx(http_soap_log_level level=hsl_none) : Cws_configSoapBinding(level)
    {
    }
    
    Cws_configBindingEx(IPropertyTree* cfg, const char *bindname, const char *procname, http_soap_log_level level=hsl_none) : Cws_configSoapBinding(cfg, bindname, procname, level)
    {
    }

    virtual void getNavSettings(int &width, bool &resizable, bool &scroll)
    {
        width=200;
        resizable=true; 
        scroll=true;
    }

    void getNavigationData(IEspContext &context, IPropertyTree& data)
    {
        Owned<Cws_configEx> pSvc = dynamic_cast<Cws_configEx*>(getService());
        if (pSvc)
            pSvc->getNavigationData(&data);
    }

    void getDynNavData(IEspContext &context, IProperties *params, IPropertyTree & data)
    {
        if (!params)
            return;

    }

    int onGetRoot(IEspContext &context, CHttpRequest* request,  CHttpResponse* response)
    {
        return onGetInstantQuery(context, request, response, "ws_config", "Init");
    }
private:
};



Cws_configSoapBinding* createws_configSoapBinding(IPropertyTree *cfg, const char *name, const char *process)
{
   return new Cws_configBindingEx(cfg, name, process);
}
