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

#ifndef _SYSTEM_ERROR_LIST_HPP__
#define _SYSTEM_ERROR_LIST_HPP__

// Define start and end ranges for error codes that may be thrown, written to a log, or returned in any manner
// create a range that will be big enough

//Error ranges used throughout the system...

#define ERR_EXE_START           1     // All .exes can safely use the same range.
#define ERR_EXE_END             999   // Codes 1000+ should be used in .dlls.

// actual error codes do not belong in this file

#define ERRORID_UNKNOWN         999      // Right now, this value is used to filter out error messages which have 
                                                    // not been verified for well-defined error code and message content. For
                                                    // example, if an error code is not greater than this value, ECLWatch displays 
                                                    // the error as "internal system error".

//Some legacy ecl warnings appear in the following range, most new ones are in the range below
#define ECL_WARN_START          1000
#define ECL_WARN_END            1099

#define ROXIEMM_ERROR_START     1300
#define ROXIEMM_ERROR_END       1349

#define ROXIE_ERROR_START       1400   // roxie is already using this start value
#define ROXIE_ERROR_END         1799

#define ROXIE_MGR_START         1800   // used in esp service to communicate / gather info for roxie
#define ROXIE_MGR_END           1999

#define HQL_ERROR_START         2000    // 2000..3999 in hql, 4000..4999 in hqlcpp
#define HQL_ERROR_END           4999

#define WORKUNIT_ERROR_START    5000
#define WORKUNIT_ERROR_END      5099

#define WORKFLOW_ERROR_START    5100
#define WORKFLOW_ERROR_END      5199

#define PACKAGE_ERROR_START     5200
#define PACKAGE_ERROR_END       5299

#define THORHELPER_ERROR_START  5300
#define THORHELPER_ERROR_END    5399

#define ECLAGENT_ERROR_START    5400
#define ECLAGENT_ERROR_END      5499

#define WUWEB_ERROR_START       5500
#define WUWEB_ERROR_END         5599

#define XSLT_ERROR_START        5600
#define XSLT_ERROR_END          5699

#define THOR_ERROR_START        5700
#define THOR_ERROR_END          5799

#define DALI_ERROR_START        5800
#define DALI_ERROR_END          5899

//Jlib
#define JLIB_ERROR_START        6000
#define JLIB_ERROR_END          6499

//Ecl runtime library
#define ECLRTL_ERROR_START      6500
#define ECLRTL_ERROR_END        6699

//File view
#define FILEVIEW_ERROR_START    6700
#define FILEVIEW_ERROR_END      6749

#define REMOTE_ERROR_START      8000    // dafilesrv etc - see common/remote/remoteerr.hpp
#define REMOTE_ERROR_END        8099

#define JHTREE_ERROR_START      8500
#define JHTREE_ERROR_END        8699

#define DISPATCH_ERROR_START    9000
#define DISPATCH_ERROR_END      9399

#define QUERYREGISTRY_ERROR_START   9600
#define QUERYREGISTRY_ERROR_END     9799

#define JVM_API_ERROR_START     10000
#define JVM_API_ERROR_END       10499

#define PKG_PROCESS_ERROR_START 11000
#define PKG_PROCESS_ERROR_END   11100

#define ECLWATCH_ERROR_START    20000
#define ECLWATCH_ERROR_END      29999

#define CONFIG_MGR_ERROR_START  30000
#define CONFIG_MGR_ERROR_END    30099

#define WORKUNIT_ANALYZER_START 31000
#define WORKUNIT_ANALYZER_END   31999

#define CODESIGNER_ERROR_START  32000
#define CODESIGNER_ERROR_END    32999
#endif

