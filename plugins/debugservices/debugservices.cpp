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

#include "platform.h"
#include "debugservices.hpp"

static char buildVersion[] = "$HeadURL: https://svn.br.seisint.com/ecl/trunk/plugins/debugservices/debugservices.cpp $ $Id: debugservices.cpp 62376 2011-02-04 21:59:58Z sort $";

#define DEBUGSERVICES_VERSION "DEBUGSERVICES 1.0.1"

const char * EclDefinition = 
"export DebugServices := SERVICE\n"
"  Sleep(integer millis) : c,pure,entrypoint='dsSleep',initFunction='dsInitDebugServices'; \n"
"  varstring GetBuildInfo() : c,pure,entrypoint='dsGetBuildInfo',initFunction='dsInitDebugServices';\n"
"END;";

static const char * compatibleVersions[] = {
    "DEBUGSERVICES 1.0  [7294888b4271178e0cfda307826d4823]", 
    "DEBUGSERVICES 1.0.1",
    NULL };

DEBUGSERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb) 
{
    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = DEBUGSERVICES_VERSION;
    pb->moduleName = "lib_debugservices";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "DEBUGSERVICES library";
    return true;
}

DEBUGSERVICES_API char * DEBUGSERVICES_CALL dsGetBuildInfo(void)
{ 
    return strdup(buildVersion);
}

//-------------------------------------------------------------------------------------------------------------------------------------------

DEBUGSERVICES_API void DEBUGSERVICES_CALL dsSleep(unsigned milli)
{
#ifdef _WIN32
    Sleep(milli);
#else
    timespec sleepTime;
    
    if (milli>=1000)
    {
        sleepTime.tv_sec = milli/1000;
        milli %= 1000;
    }
    else
        sleepTime.tv_sec = 0;
    sleepTime.tv_nsec = milli * 1000000;
    nanosleep(&sleepTime, NULL);
#endif
}

