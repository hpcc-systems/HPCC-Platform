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


#ifndef JOBSERVE_IPP
#define JOBSERVE_IPP

#include "jlib.hpp"

class jlib_decl Notification : public INotification
{
  public:
    Notification(NotifyAction _action, IObservable * _source)
       { action = _action; source = _source; }

    virtual NotifyAction        getAction(void);
    virtual IObservable *       querySource(void);

  private:
    NotifyAction        action;
    IObservable *       source;
};


//mixin class
class jlib_decl ManyObservers
{
public:
    void addObserver(IObserver &);
    void removeObserver(IObserver &);

    bool sendBroadcast(NotifyAction, IObservable & self);
    bool broadcast(INotification & notify);

protected:
    CopyArray                   observers;
};


class jlib_decl SingleObserver
{
public:
    SingleObserver() { observer = NULL; }

    void addObserver(IObserver & _observer) { assertex(!observer); observer = &_observer; }
    void removeObserver(IObserver & _observer) { assertex(observer == &_observer); observer = NULL; }

    bool sendBroadcast(NotifyAction, IObservable & self);
    bool broadcast(INotification & notify);

protected:
    IObserver * observer;
};


#define IMPLEMENT_IOBSERVABLE(PARENT, CHILD)                            \
    virtual void beforeDispose()                                          \
    {                                                                     \
        CHILD.sendBroadcast(NotifyOnDispose, *this);                        \
        PARENT::beforeDispose();                                            \
    }                                                                   \
    virtual void addObserver(IObserver & _observer) { CHILD.addObserver(_observer); }               \
    virtual void removeObserver(IObserver & _observer) { CHILD.removeObserver(_observer); }

#endif
