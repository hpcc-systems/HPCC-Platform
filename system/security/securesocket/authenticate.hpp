/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef _AUTHENTICATE_HPP__
#define _AUTHENTICATE_HPP__

class Authenticator : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

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
