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

#include "platform.h"
#include "debugservices.hpp"

#define DEBUGSERVICES_VERSION "DEBUGSERVICES 1.0.1"

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
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "DEBUGSERVICES library";
    return true;
}

DEBUGSERVICES_API char * DEBUGSERVICES_CALL dsGetBuildInfo(void)
{ 
    return strdup(DEBUGSERVICES_VERSION);
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

