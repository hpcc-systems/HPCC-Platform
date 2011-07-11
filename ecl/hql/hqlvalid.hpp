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
#ifndef __HQLVALID_HPP_
#define __HQLVALID_HPP_

#include "hqlexpr.hpp"

//Error reporting helpers.
void HQL_API reportErrorVa(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char* format, va_list args);
void HQL_API reportError(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char * format, ...);
void HQL_API expandReportError(IErrorReceiver * errors, IECLError* error);

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


//Checking functions
IHqlExpression * checkCreateConcreteModule(IErrorReceiver * errors, IHqlExpression * expr, const ECLlocation & errpos);
extern HQL_API IHqlExpression * checkCreateConcreteModule(IErrorReceiver * errors, IHqlExpression * expr, const IHqlExpression * locationExpr);
extern HQL_API IHqlExpression * createLocationAttr(ISourcePath * filename, int lineno, int column, int position);

#endif
