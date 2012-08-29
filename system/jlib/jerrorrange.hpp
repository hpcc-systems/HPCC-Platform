/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


#ifndef JERRORRANGE_HPP
#define JERRORRANGE_HPP

//Error ranges used throughout the system...

#define ERR_EXE_FIRST       1     // All .exes can safely use the same range.
#define ERR_EXE_LAST        999   // Codes 1000+ should be used in .dlls.

#define ERR_JLIB_FIRST      1000
#define ERR_JLIB_LAST       1999
#define ERR_FILEVIEW_FIRST  2000
#define ERR_FILEVIEW_LAST   2049
#endif
