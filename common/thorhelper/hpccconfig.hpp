/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#ifndef _HPCCCONFIG_HPP_
#define _HPCCCONFIG_HPP_

#ifdef THORHELPER_EXPORTS
#define THORHELPER_API DECL_EXPORT
#else
#define THORHELPER_API DECL_IMPORT
#endif

extern THORHELPER_API bool getService(StringBuffer &serviceAddress, const char *serviceName, bool failIfNotFound);

typedef std::function<bool(const char *prop, StringBuffer &result)> GetJobValueFunction;
extern THORHELPER_API void getMemorySpecifications(std::unordered_map<std::string, __uint64> &memorySpecifications, const IPropertyTree *config, const char *context, unsigned maxMB, GetJobValueFunction getJobValueFunction);

#endif // _HPCCCONFIG_HPP_

