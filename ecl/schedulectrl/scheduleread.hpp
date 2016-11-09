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
#ifndef __SCHEDULEREAD_HPP_
#define __SCHEDULEREAD_HPP_

#ifdef SCHEDULECTRL_EXPORTS
#define SCHEDULECTRL_API DECL_EXPORT
#else
#define SCHEDULECTRL_API DECL_IMPORT
#endif

#include "jstring.hpp"
#include "jtime.hpp"

interface IScheduleReaderIterator : public IInterface
{
    virtual bool isValidEventName() const = 0;
    virtual bool isValidEventText() const = 0;
    virtual bool isValidWuid() const = 0;
    virtual bool nextEventName() = 0;
    virtual bool nextEventText() = 0;
    virtual bool nextWuid() = 0;
    virtual StringBuffer & getEventName(StringBuffer & out) const = 0;
    virtual StringBuffer & getEventText(StringBuffer & out) const = 0;
    virtual StringBuffer & getWuid(StringBuffer & out) const = 0;
    virtual bool queryUpdateLatest(CDateTime const & dt) const = 0;
};

interface IScheduleReader : public IInterface
{
    virtual IScheduleReaderIterator * getIterator(char const * eventName = NULL, char const * eventText = NULL) = 0; // use NULL args to iterate over all event names/texts
};

interface IScheduleSubscriber : public IInterface
{
    virtual void notify() = 0;
};

interface ISchedulerListIterator : public IInterface
{
    virtual void first() = 0;
    virtual bool isValid() const = 0;
    virtual bool next() = 0;
    virtual char const * query() const = 0;
};

extern SCHEDULECTRL_API IScheduleReader * getScheduleReader(char const * serverName, char const * eventName = NULL); // use NULL arg to get reader for whole schedule
extern SCHEDULECTRL_API IScheduleReader * getSubscribingScheduleReader(char const * serverName, IScheduleSubscriber * subscriber, char const * eventName = NULL); // as above, but subscribes, calls subscriber is non-NULL its notify method is called
extern SCHEDULECTRL_API ISchedulerListIterator * getSchedulerList();

/* If you get a subscribing reader, it will keep itself up to date
 * with the schedule in SDS. However, if you have any unreleased
 * iterators on the reader, it won't update itself and will schedule
 * an update to be done when you release the last iterator. And
 * conversely, getIterator will be blocked while an update is taking
 * place. */

#endif
