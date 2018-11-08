/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#ifndef HTTPSECURECONTEXT_HPP
#define HTTPSECURECONTEXT_HPP

#include "espbasesecurecontext.hpp"
#include "http/platform/httptransport.ipp"
#include "esphttp.hpp"

enum HttpPropertyType
{
    HTTP_PROPERTY_TYPE_COOKIE,
    HTTP_PROPERTY_TYPE_HEADER,
    HTTP_PROPERTY_TYPE_REMOTE_ADDRESS,
};

esp_http_decl IEspSecureContext* createHttpSecureContext(CHttpRequest* request);

#endif // HTTPSECURECONTEXT_HPP
