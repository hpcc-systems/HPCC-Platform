/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jliball.hpp"
#include "hqlwuerr.hpp"

static void formatError(StringBuffer & out, int errNo, const char *msg, IIdAtom * modulename, IIdAtom * attributename, int lineno, int column)
{
    out.append(modulename->str());
    if (attributename)
        out.append('.').append(attributename->str());
    
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
