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

#ifndef ECLFUNCTION_HPP_
#define ECLFUNCTION_HPP_

#include "ws_sql.hpp"
#include "ws_sql_esp.ipp"

#define AGGREGATE_FUNCTION_TYPE 1
#define CONTENT_MODIFIER_FUNCTION_TYPE 2

#define COUNT_FUNCTION "COUNT"
#define MAX_FUNCTION "MAX"
#define MIN_FUNCTION "MIN"
#define SUM_FUNCTION "SUM"
#define AVG_FUNCTION "AVG"
#define UPPER_FUNCTION "UPPER"
#define LOWER_FUNCTION "LOWER"

struct ECLFunctionDefCfg
{
    const char * name;
    bool acceptsWildCard;
    const char * returnType;
    bool acceptsMultipleInputs;
    bool acceptsEmptyInput;
    int functionType;
    const char * eclFunctionName;
    bool returnsSameAsArgumentType;

    ECLFunctionDefCfg()
    {
        name = "";
        acceptsWildCard = false;
        returnType = "";
        acceptsMultipleInputs = false;
        acceptsEmptyInput = false;
        functionType = -1;
        eclFunctionName = "";
        returnsSameAsArgumentType = false;
    }
    ECLFunctionDefCfg( const char * n, bool awc, const char * rt, bool ami, bool aei, int ft, const char * efn, bool rsaat)
    {
        name = n;
        acceptsWildCard = awc;
        returnType = rt;
        acceptsMultipleInputs = ami;
        acceptsEmptyInput = aei;
        functionType = ft;
        eclFunctionName = efn;
        returnsSameAsArgumentType = rsaat;
    }

};

/*
typedef enum ECLFunctionDefs_
{
    UnknownECLFunc  = -1,
    CountECLFunc    = 0,
    MaxECLFunc      = 1,
    MinECLFunc      = 2,
    SumECLFunc      = 3,
    AvgECLFunc      = 4,
    UpperECLFunc    = 5,
    LowerECLFunc    = 6,
    ECLFuncMax      = 7
} EclFunctionDef;

*/

class ECLFunctions
{

public:
    ECLFunctions();
    virtual ~ECLFunctions();

    static void init();

    static ECLFunctionDefCfg getEclFuntionDef(const char * funcname)
    {
        if (!funcsinited)
        {
            init();
        }

        if (funcname && strlen(funcname)>0)
        {
            StringBuffer fnnameupper = funcname;

            if (eclfuncstable.count(fnnameupper.toUpperCase().str()))
                return  eclfuncstable.find(fnnameupper.toUpperCase().str())->second;
        }

        throw MakeStringException(-1, "Invalid ECL function: %s", funcname);
    }

    static bool funcsinited;
    static std::map<std::string,ECLFunctionDefCfg> eclfuncstable;
};

#endif /* ECLFUNCTION_HPP_ */
