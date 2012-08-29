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

#ifndef FVWUGEN_HPP
#define FVWUGEN_HPP

#ifdef _WIN32
    #ifdef FILEVIEW2_EXPORTS
        #define FILEVIEW_API __declspec(dllexport)
    #else
        #define FILEVIEW_API __declspec(dllimport)
    #endif
#else
    #define FILEVIEW_API
#endif

#define LOWER_LIMIT_ID      "__startPos__"
#define RECORD_LIMIT_ID     "__numRecords__"

extern FILEVIEW_API IHqlExpression * buildWorkUnitViewerEcl(IHqlExpression * record, const char * wuid, unsigned sequence, const char * name);
extern FILEVIEW_API IHqlExpression * buildDiskOutputEcl(const char * logicalName, IHqlExpression * record);
extern FILEVIEW_API IHqlExpression * buildDiskFileViewerEcl(const char * logicalName, IHqlExpression * record);
extern FILEVIEW_API IHqlExpression * buildQueryViewerEcl(IHqlExpression * selectFields);

#endif
