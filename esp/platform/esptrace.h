/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

#pragma once

#include "jtrace.hpp"
#include <initializer_list>

// Refer to helm/examples/esp/README.md for more information on the trace options.

constexpr const char* propTraceFlags = "traceFlags";
constexpr const char* propTraceLevel = "traceLevel";

// ESP process names for use with options mapping
constexpr const char* eclwatchApplication = "eclwatch";
constexpr const char* eclservicesApplication = "eclservices";
constexpr const char* eclqueriesApplication = "eclqueires";
constexpr const char* esdlsandboxApplication = "esdl-sandbox";
constexpr const char* esdlApplication = "esdl";
constexpr const char* sql2eclApplication = "sql2ecl";
constexpr const char* dfsApplication = "dfs";
constexpr const char* ldapenvironmentApplication = "ldapenvironment";
constexpr const char* loggingserviceApplication = "loggingservice";

// Trace options for ESP
constexpr TraceFlags traceLevel = TraceFlags::LevelMask;

// Trace option list fragment for jtrace-defined options used by ESPs
#define PLATFORM_OPTIONS_FRAGMENT \
    TRACEOPT(traceSecMgr),

// Trace option list fragment for options used by most ESPs
#define ESP_OPTIONS_FRAGMENT \
    PLATFORM_OPTIONS_FRAGMENT \
    TRACEOPT(traceLevel),

// Trace option initializer list for ESPs that do not define their own options.
constexpr std::initializer_list<TraceOption> espTraceOptions
{
    ESP_OPTIONS_FRAGMENT
};

/**
 * @brief Returns the trace options appropriate for the ESP process being initialized.
 *
 * Most ESPs will simply return espTraceOptions. Any ESP that defines options not applicable to
 * other ESPs would return a different list. The determination of which list to return is
 * expected to be based on the configuration's `application` property.
 *
 * If options for all ESPs are defined with no value collisions, there may be no need to define
 * separate option lists for individual ESPs. However, if value collisions cannot be avoided,
 * separate lists will be needed.
 *
 * Consider ESDL Script, and the applications that use it. The potential for a significant number
 * of options is high, increasing the likelihood of collisions with applications that don't use it.
 */
inline const std::initializer_list<TraceOption>& mapTraceOptions(const IPTree* config)
{
    return espTraceOptions;
}
