/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#ifndef _OPENSSLCOMMON_HPP__
#define _OPENSSLCOMMON_HPP__

#if defined(_USE_OPENSSL)
#if defined(_USE_OPENSSLV3)
    //If OPENSSL Version 3.x or newer, ensure deprecated calls to RSA_free (and others) are still allowed
    #define OPENSSL_API_COMPAT 0x10100000L
#endif
#endif

#endif