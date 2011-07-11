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

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/hql/hqlerror.cpp $ $Id: hqlerror.cpp 66009 2011-07-06 12:28:32Z ghalliday $");

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
