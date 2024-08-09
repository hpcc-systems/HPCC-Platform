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
    pb->moduleName = "lib_exampleplugin";
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

ECL_EXAMPLE_PLUGIN_API void ECL_EXAMPLE_PLUGIN_CALL test1(size32_t & returnLength, char * & returnValue,
    uint8_t p1, uint16_t p2, uint32_t p3, __uint64 p4, 
    char p5, int16_t p6, int32_t p7, __int64 p8, __uint64 p9, __uint64 p10)
{
    VStringBuffer buffer("%u %u %u %llu %d %d %d %lld %llu %llu", p1, p2, p3, p4, p5, p6, p7, p8, p9, p10);
    returnLength = buffer.length();
    returnValue = buffer.detach();
    return;
}

ECL_EXAMPLE_PLUGIN_API void ECL_EXAMPLE_PLUGIN_CALL test2(size32_t & returnLength, char * & returnValue,
    float p1, float p2, float p3, float p4, 
    double p5, double p6, double p7, double p8)
{
    VStringBuffer buffer("%.3f %.3f %.3f %.3f %.3f %.3f %.3f %.3f", p1, p2, p3, p4, p5, p6, p7, p8);
    returnLength = buffer.length();
    returnValue = buffer.detach();
    return;
}

ECL_EXAMPLE_PLUGIN_API void ECL_EXAMPLE_PLUGIN_CALL test3(size32_t & returnLength, char * & returnValue,
    uint8_t p1, uint16_t p2, uint32_t p3, __uint64 p4, 
    char p5, int16_t p6, int32_t p7, __int64 p8, __uint64 p9, __uint64 p10,
    float r1, float r2, float r3, float r4, 
    double r5, double r6, double r7, double r8)
{
    VStringBuffer buffer("%u %u %u %llu %d %d %d %lld %llu %llu", p1, p2, p3, p4, p5, p6, p7, p8, p9, p10);
    buffer.appendf(" %.3f %.3f %.3f %.3f %.3f %.3f %.3f %.3f", r1, r2, r3, r4, r5, r6, r7, r8);
    returnLength = buffer.length();
    returnValue = buffer.detach();
    return;
}

ECL_EXAMPLE_PLUGIN_API void ECL_EXAMPLE_PLUGIN_CALL test4(size32_t & returnLength, char * & returnValue,
    size32_t slen, const char *s,
    const char *s10,
    const char *v,
    const char *v10)
{
    VStringBuffer buffer("%.*s,%.10s,%s,%s", slen, s, s10, v, v10);
    returnLength = buffer.length();
    returnValue = buffer.detach();
    return;
}


}//close namespace
