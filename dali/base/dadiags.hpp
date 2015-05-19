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

#ifndef DADIAGS_HPP
#define DADIAGS_HPP

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

extern da_decl StringBuffer & getDaliDiagnosticValue(const char *name,StringBuffer &ret);
extern da_decl MemoryBuffer & getDaliDiagnosticValue(MemoryBuffer &m);

// for server use
interface IDaliServer;
extern da_decl IDaliServer *createDaliDiagnosticsServer(); // called for coven members

#endif
