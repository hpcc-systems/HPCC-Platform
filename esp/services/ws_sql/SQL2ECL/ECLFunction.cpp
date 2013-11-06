/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#include "ECLFunction.hpp"

bool ECLFunctions::funcsinited = false;
std::map<std::string,ECLFunctionDefCfg> ECLFunctions::eclfuncstable;

void ECLFunctions::init()
{
    if (!funcsinited)
    {
        eclfuncstable.insert(std::pair<std::string,ECLFunctionDefCfg>(COUNT_FUNCTION, ECLFunctionDefCfg( COUNT_FUNCTION, true,  "INTEGER", true,  true,  AGGREGATE_FUNCTION_TYPE,        COUNT_FUNCTION, false)));
        eclfuncstable.insert(std::pair<std::string,ECLFunctionDefCfg>(MAX_FUNCTION,   ECLFunctionDefCfg(MAX_FUNCTION, true,  "NUMERIC",   false, false, AGGREGATE_FUNCTION_TYPE,                                            MAX_FUNCTION, true)));
        eclfuncstable.insert(std::pair<std::string,ECLFunctionDefCfg>( MIN_FUNCTION,  ECLFunctionDefCfg(MIN_FUNCTION, true,  "NUMERIC",   false, false, AGGREGATE_FUNCTION_TYPE,        MIN_FUNCTION, true)));
        eclfuncstable.insert(std::pair<std::string,ECLFunctionDefCfg>(SUM_FUNCTION,   ECLFunctionDefCfg(SUM_FUNCTION, true,  "NUMERIC",   false, false, AGGREGATE_FUNCTION_TYPE,        SUM_FUNCTION, true)));
        eclfuncstable.insert(std::pair<std::string,ECLFunctionDefCfg>(AVG_FUNCTION,   ECLFunctionDefCfg(AVG_FUNCTION, true,  "DECIMAL",   false, false, AGGREGATE_FUNCTION_TYPE,        "AVE", false)));
        eclfuncstable.insert(std::pair<std::string,ECLFunctionDefCfg>(UPPER_FUNCTION, ECLFunctionDefCfg(UPPER_FUNCTION, false, "STRING",            false, false, CONTENT_MODIFIER_FUNCTION_TYPE, "Std.Str.ToUpperCase", true)));
        eclfuncstable.insert(std::pair<std::string,ECLFunctionDefCfg>(LOWER_FUNCTION, ECLFunctionDefCfg(LOWER_FUNCTION, false, "STRING",            false, false, CONTENT_MODIFIER_FUNCTION_TYPE, "Std.Str.ToLowerCase", true)));

        funcsinited = true;
    }
}
