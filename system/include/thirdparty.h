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

#ifndef _THIRDPARTY_H_
#define _THIRDPARTY_H_

#ifdef _DEBUG
// When creating debug builds for developer use, you can disable the use of varions third party libraries using the flags below
// The main purpose of this would be for use on a platform or machine where the appropriate third-party support has not been installed
// DO NOT release any version of this file where any of the followinf are uncommented without careful consideration
// DO NOT define any of these outside of the _DEBUG section without even more careful consideration

//#define _NO_MYSQL     // Allow system to build without mysql client library support
//#define _NO_SYBASE        // Allow system to build without sybaseclient library support
//#define _NO_SAMI      // Allow system to build without SAMI/Agentxx etc

#endif

#define NO_LINUX_SSL

#endif //_THIRDPARTY_H_

