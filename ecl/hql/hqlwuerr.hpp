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
#ifndef __HQLWUERR_HPP__
#define __HQLWUERR_HPP__

#include "hqlexpr.hpp"
#include "workunit.hpp"

class HQL_API WorkUnitErrorReceiver : public CInterface, implements IErrorReceiver
{
public:
    WorkUnitErrorReceiver(IWorkUnit * _wu, const char * _component) { wu.set(_wu); component.set(_component); }
    IMPLEMENT_IINTERFACE;

    virtual void reportError(int errNo, const char *msg, const char * filename, int lineno, int column, int pos);
    virtual void report(IECLError*);
    virtual void reportWarning(int warnNo, const char *msg, const char * filename, int lineno, int column, int pos);
    virtual size32_t errCount();
    virtual size32_t warnCount();

protected:
    void initializeError(IWUException * exception, int errNo, const char *msg, const char * filename, int lineno, int column, int pos);

private:
    Owned<IWorkUnit> wu;
    StringAttr component;
};


extern HQL_API IErrorReceiver * createCompoundErrorReceiver(IErrorReceiver * primary, IErrorReceiver * secondary);

#endif //__HQLWUERR_HPP__
