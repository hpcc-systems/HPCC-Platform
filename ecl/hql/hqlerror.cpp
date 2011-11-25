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

#include "jlog.hpp"
#include "hqlerror.hpp"
#include "hqlerrors.hpp"

void MultiErrorReceiver::reportError(int errNo, const char* msg, const char * filename, int lineno, int column, int position)
{
    Owned<IECLError> err = createECLError(errNo,msg,filename,lineno,column,position);
    report(err);
}

void MultiErrorReceiver::reportWarning(int warnNo, const char* msg, const char * filename, int lineno, int column, int position)
{
    Owned<IECLError> warn = createECLWarning(warnNo,msg,filename,lineno,column,position);
    report(warn);
}

void MultiErrorReceiver::report(IECLError* error) 
{
    msgs.append(*LINK(error)); 
    if (error->isError())
        errs++;
    else
        warns++;
    StringBuffer msg;
    DBGLOG("reportError(%d:%d) : %s", error->getLine(), error->getColumn(), error->errorMessage(msg).str());
}

StringBuffer& MultiErrorReceiver::toString(StringBuffer& buf)
{
    ForEachItemIn(i, msgs)
    {
        IECLError* error=item(i);
        error->toString(buf);
        buf.append('\n');
    }
    return buf;
}

IECLError *MultiErrorReceiver::firstError()
{
    ForEachItemIn(i, msgs)
    {
        IECLError* error = item(i);
        if (error->isError())
            return error;
    }
    return NULL;
}

CECLError::CECLError(bool _isError, int _no, const char* _msg, const char* _filename, int _lineno, int _column, int _position):
  msg(_msg), filename(_filename)
{
    iserror = _isError;
    no = _no;
    lineno = _lineno;
    column = _column;
    position = _position;
}


StringBuffer& CECLError::toString(StringBuffer& buf)
{
    buf.append(filename);
    
    if(lineno && column)
        buf.append('(').append(lineno).append(',').append(column).append(')');
    buf.append(" : ");

    buf.append(no).append(": ").append(msg);
    return buf;
}

extern HQL_API IECLError *createECLError(bool isError, int errNo, const char *msg, const char * filename, int lineno, int column, int pos)
{
    return new CECLError(isError,errNo,msg,filename,lineno,column,pos);
}

extern HQL_API IECLError * changeErrorType(bool isError, IECLError * error)
{
    StringBuffer msg;
    return new CECLError(isError,
                         error->errorCode(), error->errorMessage(msg).str(), error->getFilename(), 
                         error->getLine(), error->getColumn(), error->getPosition());
}

extern HQL_API void reportErrors(IErrorReceiver & receiver, IECLErrorArray & errors)
{
    ForEachItemIn(i, errors)
        receiver.report(&errors.item(i));
}

//---------------------------------------------------------------------------------------------------------------------

class HQL_API FileErrorReceiver : public CInterface, implements IErrorReceiver
{
public:
    IMPLEMENT_IINTERFACE;

    int errcount;
    int warncount;
    FILE *f;

    FileErrorReceiver(FILE *_f)
    {
        errcount = 0;
        warncount = 0;
        f = _f;
    }

    virtual void reportError(int errNo, const char *msg, const char * filename, int _lineno, int _column, int _pos)
    {
        errcount++;
        if (!filename) filename = "";
        fprintf(f, "%s(%d,%d): error C%04d: %s\n", filename, _lineno, _column, errNo, msg);
    }

    virtual void reportWarning(int warnNo, const char *msg, const char * filename, int _lineno, int _column, int _pos)
    {
        warncount++;
        if (!filename) filename = *unknownAtom;
        fprintf(f, "%s(%d,%d): warning C%04d: %s\n", filename, _lineno, _column, warnNo, msg);
    }

    virtual void report(IECLError* e)
    {
        expandReportError(this, e);
    }

    virtual size32_t errCount() { return errcount; };
    virtual size32_t warnCount() { return warncount; };
};

extern HQL_API IErrorReceiver *createFileErrorReceiver(FILE *f)
{
    return new FileErrorReceiver(f);
}

//---------------------------------------------------------------------------------------------------------------------

void ThrowingErrorReceiver::reportError(int errNo, const char *msg, const char *filename, int lineno, int column, int pos)
{
    throw createECLError(errNo, msg, filename, lineno, column, pos);
}

void ThrowingErrorReceiver::report(IECLError* error)
{
    expandReportError(this, error);
}

void ThrowingErrorReceiver::reportWarning(int warnNo, const char *msg, const char *filename, int lineno, int column, int pos)
{
}

//---------------------------------------------------------------------------------------------------------------------

void reportErrorVa(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char* format, va_list args)
{
    StringBuffer msg;
    msg.valist_appendf(format, args);
    if (errors)
        errors->reportError(errNo, msg.str(), loc.sourcePath->str(), loc.lineno, loc.column, loc.position);
    else
        throw createECLError(errNo, msg.str(), loc.sourcePath->str(), loc.lineno, loc.column, loc.position);
}

void reportError(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    reportErrorVa(errors, errNo, loc, format, args);
    va_end(args);
}


void expandReportError(IErrorReceiver * errors, IECLError* error)
{
    StringBuffer msg;
    error->errorMessage(msg);
    if (error->isError())
        errors->reportError(error->errorCode(), msg.str(), error->getFilename(), error->getLine(), error->getColumn(), error->getPosition());
    else
        errors->reportWarning(error->errorCode(), msg.str(), error->getFilename(), error->getLine(), error->getColumn(), error->getPosition());
}


//---------------------------------------------------------------------------------------------------------------------

class IndirectErrorReceiver : public CInterface, implements IErrorReceiver
{
public:
    IndirectErrorReceiver(IErrorReceiver * _prev) : prev(_prev) {}
    IMPLEMENT_IINTERFACE

    virtual void reportError(int errNo, const char *msg, const char *filename, int lineno, int column, int pos)
    {
        prev->reportError(errNo, msg, filename, lineno, column, pos);
    }
    virtual void report(IECLError* err)
    {
        prev->report(err);
    }
    virtual void reportWarning(int warnNo, const char *msg, const char *filename, int lineno, int column, int pos)
    {
        prev->reportWarning(warnNo, msg, filename, lineno, column, pos);
    }
    virtual size32_t errCount()
    {
        return prev->errCount();
    }
    virtual size32_t warnCount()
    {
        return prev->warnCount();
    }

protected:
    Linked<IErrorReceiver> prev;
};

class ErrorInserter : public IndirectErrorReceiver
{
public:
    ErrorInserter(IErrorReceiver * _prev, IECLError * _error) : IndirectErrorReceiver(_prev), error(_error) {}

    virtual void reportError(int errNo, const char *msg, const char *filename, int lineno, int column, int pos)
    {
        flush();
        IndirectErrorReceiver::reportError(errNo, msg, filename, lineno, column, pos);
    }
    virtual void report(IECLError* err)
    {
        flush();
        IndirectErrorReceiver::report(err);
    }
    virtual void reportWarning(int warnNo, const char *msg, const char *filename, int lineno, int column, int pos)
    {
        flush();
        IndirectErrorReceiver::reportWarning(warnNo, msg, filename, lineno, column, pos);
    }

protected:
    void flush()
    {
        if (error)
        {
            IndirectErrorReceiver::report(error);
            error.clear();
        }
    }

protected:
    Linked<IECLError> error;
};

//---------------------------------------------------------------------------------------------------------------------

void checkEclVersionCompatible(Shared<IErrorReceiver> & errors, const char * eclVersion)
{
    if (eclVersion)
    {
        unsigned major, minor, subminor;
        if (extractVersion(major, minor, subminor, eclVersion))
        {
            if (major != LANGUAGE_VERSION_MAJOR)
                throwError2(HQLERR_VersionMismatch, eclVersion, LANGUAGE_VERSION);
            if (minor != LANGUAGE_VERSION_MINOR)
            {
                StringBuffer msg;
                msg.appendf("Mismatch in minor version number (%s v %s)", eclVersion, LANGUAGE_VERSION);
                errors->reportWarning(HQLERR_VersionMismatch, msg.str(), NULL, 0, 0, 0);
            }
            else if (subminor != LANGUAGE_VERSION_MINOR)
            {
                //This adds the warning if any other warnings occur.
                StringBuffer msg;
                msg.appendf("Mismatch in subminor version number (%s v %s)", eclVersion, LANGUAGE_VERSION);
                Owned<IECLError> warning = createECLWarning(HQLERR_VersionMismatch, msg.str(), NULL, 0, 0);
                errors.setown(new ErrorInserter(errors, warning));
            }
        }
    }
}
