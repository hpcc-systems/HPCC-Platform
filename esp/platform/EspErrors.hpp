/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef __ESP_ERRORS_HPP__
#define __ESP_ERRORS_HPP__

#include "errorlist.h"

#define ECLWATCH_HTTPBINDING_OBJ_CREATE_ERROR   ESP_ERROR_START+0
#define ECLWATCH_HTTPBINDING_LOAD_FAILED        ESP_ERROR_START+1
#define ECLWATCH_HTTPBINDING_INV_CONFIG_DATA    ESP_ERROR_START+2
#define ECLWATCH_HTTPBINDING_INV_REQUEST        ESP_ERROR_START+3
#define ECLWATCH_HTTPBINDING_INV_RESPONSE       ESP_ERROR_START+4
#define ECLWATCH_HTTPBINDING_PERMISSION_DENIED  ESP_ERROR_START+5
#define ECLWATCH_HTTPBINDING_INV_DATA           ESP_ERROR_START+6
//ensure errors do not exceep ESP_ERROR_END

#endif //__ESP_ERRORS_HPP__
