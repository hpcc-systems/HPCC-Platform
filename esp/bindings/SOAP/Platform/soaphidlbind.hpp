/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

#ifndef _SOAPHIDLBIND_HPP__
#define _SOAPHIDLBIND_HPP__

#include "soapbind.hpp"
#include <algorithm>

class esp_http_decl CHttpSoapHidlBinding : public CHttpSoapBinding
{
public:
    CHttpSoapHidlBinding(IPropertyTree* cfg, const char *bindname=NULL, const char *procname=NULL, http_soap_log_level level=hsl_none) :
        CHttpSoapBinding(cfg, bindname, procname, level) {}
    virtual bool qualifyMethodName(IEspContext &context, const char *methname, StringBuffer *methQName);

protected:
    virtual void registerMethodNames(std::initializer_list<const char *> names);

private:
    std::map<std::string, std::string> m_qualifiedMethodNames;
};

#endif
