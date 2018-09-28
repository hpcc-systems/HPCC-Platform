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
    ICopyArray                   observers;
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
    virtual bool beforeDispose() override                               \
    {                                                                   \
        CHILD.sendBroadcast(NotifyOnDispose, *this);                    \
        return PARENT::beforeDispose();                                 \
    }                                                                   \
    virtual void addObserver(IObserver & _observer) { CHILD.addObserver(_observer); }               \
    virtual void removeObserver(IObserver & _observer) { CHILD.removeObserver(_observer); }

#endif
