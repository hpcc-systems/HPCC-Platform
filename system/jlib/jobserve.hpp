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



#ifndef JOBSERVE_HPP
#define JOBSERVE_HPP

#include "jexpdef.hpp"
#include "jiface.hpp"

interface IObservable;

typedef unsigned NotifyAction;
enum _NotifyAction
{
   NotifyNone,
   NotifyOnDispose,                     // object disposed
   NotifyEnd
};


interface INotification //: extends IInterface
{
  public:
    virtual NotifyAction        getAction(void) = 0;
    virtual IObservable *       querySource(void) = 0;
};


interface jlib_decl IObserver : extends IInterface
{
  public:
    virtual bool                onNotify(INotification & notify);
};


interface jlib_decl IObservable : extends IInterface
{
  public:
    virtual void                addObserver(IObserver & observer);
    virtual void                removeObserver(IObserver & observer);
};

#endif
