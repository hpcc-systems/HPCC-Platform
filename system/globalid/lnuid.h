/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef __LNUID_INCL__
#define __LNUID_INCL__
#include <ctime>
#include <string>

using namespace std;

/**
* This is an implementation of Globally Unique Transaction ID’s.
* Note: This class currently generates unique id on OS's that supports device /dev/urandom.
* @author - Amol Patwardhan
* Version: 1.0
*/

namespace ln_uid {

    //Random byte count
    const unsigned int uid_size = 16;

    typedef unsigned char ln_uid_t[uid_size];

    ln_uid_t &createUniqueId(ln_uid_t &out);
    string createUniqueIdString();

    std::string uniqueIdToString(const ln_uid_t &uid);
    ln_uid_t &uniqueIdFromString(const char* uid, ln_uid_t &out);

    time_t timeFromUniqueId(const char* uid);
    time_t timeFromUniqueId(const ln_uid_t &uid);

    void getUniqueIdRange(time_t start, time_t end, ln_uid_t &uid_start, ln_uid_t &uid_end);
    void getUniqueIdDateRange(const char *start, const char *end, ln_uid_t &uid_start, ln_uid_t &uid_end);

    bool sameUniqueId(const ln_uid_t &uid1, const ln_uid_t &uid2);
    void copyUniqueId(ln_uid_t &to, const ln_uid_t &from);

    int get_utc_offset();
};

#endif
