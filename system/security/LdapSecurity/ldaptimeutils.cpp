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

#include "ldaptimeutils.hpp"

bool parseLdapGeneralizedTime(CDateTime &dt, unsigned len, const char * val)
{
    if (!val || len < 15 || val[14] != 'Z')
        return false;

    for (unsigned x = 0; x < 14; x++)
    {
        if (val[x] < '0' || val[x] > '9')
            return false;
    }

    unsigned year   = (val[0]-'0')*1000 + (val[1]-'0')*100 + (val[2]-'0')*10 + (val[3]-'0');
    unsigned month  = (val[4]-'0')*10 + (val[5]-'0');
    unsigned day    = (val[6]-'0')*10 + (val[7]-'0');
    unsigned hour   = (val[8]-'0')*10 + (val[9]-'0');
    unsigned minute = (val[10]-'0')*10 + (val[11]-'0');
    unsigned second = (val[12]-'0')*10 + (val[13]-'0');

    dt.setDate(year, month, day); //midnight UTC on the date given
    dt.setTime(hour, minute, second, 0, false); //time given is UTC, not local
    dt.adjustTime(dt.queryUtcToLocalDelta()); //convert to local, matching calcPWExpiry() convention
    return true;
}
