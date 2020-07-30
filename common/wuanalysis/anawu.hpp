/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef ANAWU_HPP
#define ANAWU_HPP

#ifdef WUANALYSIS_EXPORTS
    #define WUANALYSIS_API DECL_EXPORT
#else
    #define WUANALYSIS_API DECL_IMPORT
#endif

void WUANALYSIS_API analyseWorkunit(IWorkUnit * wu, IPropertyTree *options);
void WUANALYSIS_API analyseAndPrintIssues(IConstWorkUnit * wu, bool updatewu);
#endif
