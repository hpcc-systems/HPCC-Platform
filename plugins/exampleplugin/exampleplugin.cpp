/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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
#include "eclrtl.hpp"
#include "jstring.hpp"
#include "exampleplugin.hpp"

#define EXAMPLE_PLUGIN_VERSION "example-plugin plugin 1.0.0"
ECL_EXAMPLE_PLUGIN_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    /*  Warning:    This function may be called without the plugin being loaded fully.
     *              It should not make any library calls or assume that dependent modules
     *              have been loaded or that it has been initialised.
     *
     *              Specifically:  "The system does not call DllMain for process and thread
     *              initialization and termination.  Also, the system does not load
     *              additional executable modules that are referenced by the specified module."
     */

    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = EXAMPLE_PLUGIN_VERSION;
    pb->moduleName = "lib_redis";
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ECL plugin library for BLAH BLAH BLAH";
    return true;
}

namespace ExamplePlugin {


//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------
ECL_EXAMPLE_PLUGIN_API unsigned ECL_EXAMPLE_PLUGIN_CALL func1(ICodeContext * ctx, const char * param1, const char * param2, unsigned param3)
{
    return param3 + 1;
}

ECL_EXAMPLE_PLUGIN_API void ECL_EXAMPLE_PLUGIN_CALL func2 (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * param1, const char * param2, size32_t param3ValueLength, const char * param3Value)
{
    StringBuffer buffer(param3Value);
    buffer.toLowerCase();
    returnLength = buffer.length();
    returnValue = buffer.detach();
    return;
}

}//close namespace
