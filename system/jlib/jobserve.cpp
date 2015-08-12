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


#include "jobserve.hpp"

#include "jlib.hpp"
#include "jobserve.ipp"

//-----------------------------------------------------------------------
//-- Classes for helping with notification
//-----------------------------------------------------------------------


NotifyAction Notification::getAction(void)
{
    return action;
}

IObservable * Notification::querySource(void)
{
    return source;
}


//-----------------------------------------------------------------------

bool IObserver::onNotify(INotification &)
{
    return true;
}

//-----------------------------------------------------------------------

void IObservable::addObserver(IObserver & observer)
{
    assertex(!"cannot add observer");
}

void IObservable::removeObserver(IObserver & observer)
{
    assertex(!"cannot remove observer");
}

//-----------------------------------------------------------------------

void ManyObservers::addObserver(IObserver & observer)
{
    observers.append(observer);
}

void ManyObservers::removeObserver(IObserver & observer)
{
    observers.zap(observer);
}

bool ManyObservers::sendBroadcast(NotifyAction action, IObservable & self)
{
    Notification notify(action, &self);
    return broadcast(notify);
}

bool ManyObservers::broadcast(INotification & notify)
{
    ForEachItemInRev(idx,observers)
    {
        if (!((IObserver &)observers.item(idx)).onNotify(notify))
            return false;
    }
    return true;
}

//-----------------------------------------------------------------------

bool SingleObserver::sendBroadcast(NotifyAction action, IObservable & self)
{
    Notification notify(action, &self);
    return broadcast(notify);
}

bool SingleObserver::broadcast(INotification & notify)
{
    if (observer)
        return observer->onNotify(notify);
    return true;
}

