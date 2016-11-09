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
#ifndef __EVENTQUEUE_HPP_
#define __EVENTQUEUE_HPP_

#ifdef SCHEDULECTRL_EXPORTS
#define SCHEDULECTRL_API DECL_EXPORT
#else
#define SCHEDULECTRL_API DECL_IMPORT
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
