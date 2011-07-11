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


#include "jobserve.hpp"

#include "jlib.hpp"
#include "jobserve.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/system/jlib/jobserve.cpp $ $Id: jobserve.cpp 62376 2011-02-04 21:59:58Z sort $");

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

