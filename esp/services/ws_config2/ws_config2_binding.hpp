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

#include "ws_config2Service.hpp"


class Cws_config2SoapBindingEx : public Cws_config2SoapBinding
{
public:
    Cws_config2SoapBindingEx(http_soap_log_level level=hsl_none) : Cws_config2SoapBinding(level)  { }
    Cws_config2SoapBindingEx(IPropertyTree* cfg, const char *bindname, const char *procname, http_soap_log_level level=hsl_none) : Cws_config2SoapBinding(cfg, bindname, procname, level)   {  }
    //virtual const char* getRootPage(IEspContext* ctx)  {  return "config2html";  }


private:
};
