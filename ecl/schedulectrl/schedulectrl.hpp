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
#ifndef __SCHEDULECTRL_HPP_
#define __SCHEDULECTRL_HPP_

#ifdef SCHEDULECTRL_EXPORTS
#define SCHEDULECTRL_API DECL_EXPORT
#else
#define SCHEDULECTRL_API DECL_IMPORT
#endif

#include "workunit.hpp"

extern SCHEDULECTRL_API void scheduleWorkUnit(char const * wuid);
extern SCHEDULECTRL_API void scheduleWorkUnit(char const * wuid, ISecManager & secmgr, ISecUser & secuser);
extern SCHEDULECTRL_API void descheduleWorkUnit(char const * wuid);
extern SCHEDULECTRL_API void descheduleWorkUnit(char const * wuid, ISecManager & secmgr, ISecUser & secuser);
extern SCHEDULECTRL_API void descheduleAllWorkUnits();
extern SCHEDULECTRL_API void descheduleAllWorkUnits(ISecManager & secmgr, ISecUser & secuser);
extern SCHEDULECTRL_API void descheduleNonexistentWorkUnit(char const * wuid); // to be used if scheduler triggers wu which has been manually removed --- do not use for existing wu as will not change state etc.
extern SCHEDULECTRL_API bool isScheduledWorkUnit(char const * wuid);
extern SCHEDULECTRL_API void cleanupWorkUnitSchedule();

#endif
