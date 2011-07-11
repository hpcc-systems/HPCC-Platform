/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
#ifndef __SCHEDULECTRL_HPP_
#define __SCHEDULECTRL_HPP_

#ifdef SCHEDULECTRL_EXPORTS
#define SCHEDULECTRL_API __declspec(dllexport)
#else
#define SCHEDULECTRL_API __declspec(dllimport)
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
