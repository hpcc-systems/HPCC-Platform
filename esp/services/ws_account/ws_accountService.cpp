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

#include "ws_accountService.hpp"
#include "exception_util.hpp"

const int CUTOFF_MAJOR = 533;
const int CUTOFF_MINOR = 6;

bool Cws_accountEx::onVerifyUser(IEspContext &context, IEspVerifyUserRequest &req, IEspVerifyUserResponse &resp)
{
    try
    {
        ISecUser* usr = context.queryUser();
        if(!usr || !usr->isAuthenticated())
        {
            resp.setRetcode(-1);
            return false;
        }

        const char* ver = req.getVersion();
        if (!ver || !*ver)
        {
            throw MakeStringException(ECLWATCH_OLD_CLIENT_VERSION, "Client version not found");
        }

        int minor = 0;
        int major = 0;
        const char* dot1 = strrchr(ver, '.');
        if (!dot1)
            minor = atoi(ver);
        else if (strlen(dot1) > 1)
        {
            minor = atoi(dot1 + 1);
            if(dot1 > ver)
            {
                const char* dot2 = dot1 - 1;

                while(dot2 > ver && *dot2 != '.')
                    dot2--;
                if(*dot2 == '.')
                    dot2++;
                if(dot2 < dot1)
                {
                    StringBuffer majorstr;
                    majorstr.append(dot1 - dot2, dot2);
                    major = atoi(majorstr.str());
                }
            }
        }

        if(major > CUTOFF_MAJOR || (major == CUTOFF_MAJOR && minor >= CUTOFF_MINOR))
        {
            resp.setRetcode(0);
            return true;
        }

        const char* build_ver = getBuildVersion();
        if (build_ver && *build_ver)
            throw MakeStringException(ECLWATCH_OLD_CLIENT_VERSION, "Client version %s (server %s) is out of date.", ver, build_ver);
        else
            throw MakeStringException(ECLWATCH_OLD_CLIENT_VERSION, "Client version %s is out of date.", ver);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

