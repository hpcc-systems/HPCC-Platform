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
#include "jliball.hpp"
#include "hqlwuerr.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/hql/hqlwuerr.cpp $ $Id: hqlwuerr.cpp 62376 2011-02-04 21:59:58Z sort $");


static void formatError(StringBuffer & out, int errNo, const char *msg, _ATOM modulename, _ATOM attributename, int lineno, int column)
{
    out.append(modulename);
    if (attributename)
        out.append('.').append(attributename);
    
    if(lineno && column)
        out.append('(').append(lineno).append(',').append(column).append(')');
    out.append(" : ");

    out.append(errNo).append(": ").append(msg);
}


void WorkUnitErrorReceiver::initializeError(IWUException * exception, int errNo, const char *msg, const char *filename, int lineno, int column, int pos)
{
    exception->setExceptionCode(errNo);
    exception->setExceptionMessage(msg);
    exception->setExceptionSource(component);

    exception->setExceptionFileName(filename);
    exception->setExceptionLineNo(lineno);
    exception->setExceptionColumn(column);
    exception->setTimeStamp(NULL);
}

void WorkUnitErrorReceiver::reportError(int errNo, const char *msg, const char * filename, int lineno, int column, int pos)
{
    Owned<IWUException> exception = wu->createException();
    exception->setSeverity(ExceptionSeverityError);
    initializeError(exception, errNo, msg, filename, lineno, column, pos);
}

void WorkUnitErrorReceiver::report(IECLError* eclError)
{
    Owned<IWUException> exception = wu->createException();
    if (!eclError->isError())
        exception->setSeverity(ExceptionSeverityWarning);

    StringBuffer msg;
    initializeError(exception, eclError->errorCode(), eclError->errorMessage(msg).str(), 
                    eclError->getFilename(), eclError->getLine(), eclError->getColumn(), eclError->getPosition());
}

void WorkUnitErrorReceiver::reportWarning(int warnNo, const char *msg, const char * filename, int lineno, int column, int pos)
{
    Owned<IWUException> exception = wu->createException();
    exception->setSeverity(ExceptionSeverityWarning);
    initializeError(exception, warnNo, msg, filename, lineno, column, pos);
}

size32_t WorkUnitErrorReceiver::errCount()
{
    unsigned count = 0;
    Owned<IConstWUExceptionIterator> exceptions = &wu->getExceptions();
    ForEach(*exceptions)
        if (exceptions->query().getSeverity() == ExceptionSeverityError)
            count++;
    return count;
}

size32_t WorkUnitErrorReceiver::warnCount()
{
    unsigned count = 0;
    Owned<IConstWUExceptionIterator> exceptions = &wu->getExceptions();
    ForEach(*exceptions)
        if (exceptions->query().getSeverity() == ExceptionSeverityWarning)
            count++;
    return count;
}


class CompoundErrorReceiver : public CInterface, implements IErrorReceiver
{
public:
    CompoundErrorReceiver(IErrorReceiver * _primary, IErrorReceiver * _secondary) { primary.set(_primary); secondary.set(_secondary); }
    IMPLEMENT_IINTERFACE;

    virtual void reportError(int errNo, const char *msg, const char * filename, int lineno, int column, int pos)
    {
        primary->reportError(errNo, msg, filename, lineno, column, pos);
        secondary->reportError(errNo, msg, filename, lineno, column, pos);
    }
    virtual void report(IECLError* err)
    {
        primary->report(err);
        secondary->report(err);
    }
    virtual void reportWarning(int warnNo, const char *msg, const char * filename, int lineno, int column, int pos)
    {
        primary->reportWarning(warnNo, msg, filename, lineno, column, pos);
        secondary->reportWarning(warnNo, msg, filename, lineno, column, pos);
    }
    virtual size32_t errCount()    { return primary->errCount(); }
    virtual size32_t warnCount()       { return primary->warnCount(); }

private:
    Owned<IErrorReceiver> primary;
    Owned<IErrorReceiver> secondary;
};

extern HQL_API IErrorReceiver * createCompoundErrorReceiver(IErrorReceiver * primary, IErrorReceiver * secondary)
{
    return new CompoundErrorReceiver(primary, secondary);
}
