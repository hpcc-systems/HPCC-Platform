/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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
#pragma once

#include "jtime.hpp"

// Parses an LDAP GeneralizedTime string (RFC 4517 3.3.13), in the form "YYYYMMDDHHMMSSZ"
// (always UTC/'Z', no fractional seconds) as used by 389ds's "passwordExpirationTime"
// attribute, into a local-time CDateTime. Returns false (and leaves dt untouched) if
// val does not match the expected format.
bool parseLdapGeneralizedTime(CDateTime &dt, unsigned len, const char * val);
