/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "ws_configmgrService.hpp"


class Cws_configMgrSoapBindingEx : public Cws_configmgrSoapBinding
{
public:
    Cws_configMgrSoapBindingEx(http_soap_log_level level=hsl_none) : Cws_configmgrSoapBinding(level)  { }
    Cws_configMgrSoapBindingEx(IPropertyTree* cfg, const char *bindname, const char *procname, http_soap_log_level level=hsl_none) : Cws_configmgrSoapBinding(cfg, bindname, procname, level)   {  }
    //virtual const char* getRootPage(IEspContext* ctx)  {  return "config2html";  }


private:
};
