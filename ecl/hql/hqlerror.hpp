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
#ifndef _HQLERROR_HPP_
#define _HQLERROR_HPP_

#include "jhash.hpp"
#include "jexcept.hpp"
#include "hqlexpr.hpp"

class HQL_API CECLError : public CInterface, implements IECLError
{
public:
    IMPLEMENT_IINTERFACE;

    CECLError(bool _isError, int _no, const char* _msg, const char* _filename, int _lineno, int _column, int _position);

    virtual int             errorCode() const { return no; }
    virtual StringBuffer &  errorMessage(StringBuffer & ret) const { return ret.append(msg); }
    virtual MessageAudience errorAudience() const { return MSGAUD_user; }
    virtual const char* getFilename() { return filename; }
    virtual int getLine() { return lineno; }
    virtual int getColumn() { return column; }
    virtual int getPosition() { return position; }
    virtual StringBuffer& toString(StringBuffer&);
    virtual bool isError() { return iserror; }

protected:
    bool iserror;
    int no;
    StringAttr msg;
    StringAttr filename;
    int lineno;
    int column;
    int position;
};

class HQL_API MultiErrorReceiver : public CInterface, implements IErrorReceiver
{
public:
    MultiErrorReceiver() { errs = warns = 0; }

    virtual void reportError(int errNo, const char* msg, const char * filename=0, int lineno=0, int column=0, int position=1);
    virtual void reportWarning(int warnNo, const char* msg, const char * filename, int lineno, int column, int position);
    virtual void report(IECLError* error);

    size32_t length() { return errCount() + warnCount();}
    size32_t errCount() { return errs; }
    size32_t warnCount() { return warns; }
    IECLError* item(size32_t index) { return &msgs.item(index); }
    IECLError* firstError();
    StringBuffer& toString(StringBuffer&);
    void clear() { msgs.kill(); }
    IMPLEMENT_IINTERFACE;

private:
    IECLErrorArray msgs;
    unsigned errs;
    unsigned warns;
};

class HQL_API ThrowingErrorReceiver : public CInterface, implements IErrorReceiver
{
    IMPLEMENT_IINTERFACE

    virtual void reportError(int errNo, const char *msg, const char *filename=NULL, int lineno=0, int column=0, int pos=0);
    virtual void report(IECLError* error);
    virtual void reportWarning(int warnNo, const char *msg, const char *filename=NULL, int lineno=0, int column=0, int pos=0);
    virtual size32_t errCount() { return 0; }
    virtual size32_t warnCount() { return 0; }
};

class HQL_API NullErrorReceiver : public CInterface, implements IErrorReceiver
{
public:
    IMPLEMENT_IINTERFACE;

    void reportError(int errNo, const char *msg,  const char * filename, int _lineno, int _column, int _pos) {}
    void reportWarning(int warnNo, const char *msg,  const char * filename, int _lineno, int _column, int _pos) {}
    void report(IECLError*) { }
    virtual size32_t errCount() { return 0; };
    virtual size32_t warnCount() { return 0; };
};


extern HQL_API IECLError *createECLError(bool isError, int errNo, const char *msg, const char *filename, int lineno=0, int column=0, int pos=0);
inline IECLError * createECLError(int errNo, const char *msg, const char *filename, int lineno=0, int column=0, int pos=0)
{
    return createECLError(true, errNo, msg, filename, lineno, column, pos);
}
inline IECLError *createECLWarning(int errNo, const char *msg, const char *filename, int lineno=0, int column=0, int pos=0)
{
    return createECLError(false, errNo, msg, filename, lineno, column, pos);
}
extern HQL_API IECLError *changeErrorType(bool isError, IECLError * error);
extern HQL_API void reportErrors(IErrorReceiver & receiver, IECLErrorArray & errors);
void HQL_API reportErrorVa(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char* format, va_list args);
void HQL_API reportError(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char * format, ...) __attribute__((format(printf, 4, 5)));
void HQL_API expandReportError(IErrorReceiver * errors, IECLError* error);
extern HQL_API IErrorReceiver *createFileErrorReceiver(FILE *f);

extern HQL_API void checkEclVersionCompatible(Shared<IErrorReceiver> & errors, const char * eclVersion);


#endif // _HQLERROR_HPP_
