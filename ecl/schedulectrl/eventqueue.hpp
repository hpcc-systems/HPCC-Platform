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
#ifndef __EVENTQUEUE_HPP_
#define __EVENTQUEUE_HPP_

#ifdef SCHEDULECTRL_EXPORTS
#define SCHEDULECTRL_API __declspec(dllexport)
#else
#define SCHEDULECTRL_API __declspec(dllimport)
#endif

#include "jexcept.hpp"

//PG MORE: forms for pushing with an XML payload, and passing it through the system

interface IScheduleEventPusher : public IInterface
{
    virtual unsigned push(char const * name, char const * text, const char * target) = 0;
};

interface IScheduleEventProcessor : public IInterface
{
    virtual void start() = 0;
    virtual void stop() = 0;
};

interface IScheduleEventExecutor : public IInterface
{
    virtual void execute(char const * wuid, char const * name, char const * text) = 0;
};

extern SCHEDULECTRL_API IScheduleEventPusher * getScheduleEventPusher();
extern SCHEDULECTRL_API IScheduleEventProcessor * getScheduleEventProcessor(char const * serverName, IScheduleEventExecutor * executor, IExceptionHandler * handler); //owns executor

#endif
