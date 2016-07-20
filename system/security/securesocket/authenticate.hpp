/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef _AUTHENTICATE_HPP__
#define _AUTHENTICATE_HPP__

class Authenticator
{
public:
    static bool authenticate(const char* realm, const char* userid, const char* password)
    {
        if((strcmp(realm, "EspHttpDemo") == 0) && (strcmp(userid, "yma") == 0) && (strcmp(password, "password")) == 0)
            return true;
        else if(strcmp(realm, "SOAP") == 0)
            return true;
        else
            return false;
    }
};

#endif
