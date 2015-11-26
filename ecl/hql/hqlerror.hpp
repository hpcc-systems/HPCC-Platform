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
#ifndef _HQLERROR_HPP_
#define _HQLERROR_HPP_

#include "jhash.hpp"
#include "jexcept.hpp"
#include "hql.hpp"

#define HQLERR_ErrorAlreadyReported             4799            // special case...

interface IWorkUnit;
interface HQL_API IErrorReceiver : public IInterface
{
    virtual void report(IError* error) = 0;
    virtual IError * mapError(IError * error) = 0;
    virtual size32_t errCount() = 0;
    virtual size32_t warnCount() = 0;
    virtual void exportMappings(IWorkUnit * wu) const = 0;

    //global helper functions
    void reportError(int errNo, const char *msg, const char *filename, int lineno, int column, int pos);
    void reportWarning(WarnErrorCategory category, int warnNo, const char *msg, const char *filename, int lineno, int column, int pos);
};

typedef IArrayOf<IError> IErrorArray;


//---------------------------------------------------------------------------------------------------------------------

class HQL_API ErrorReceiverSink : public CInterfaceOf<IErrorReceiver>
{
public:
    ErrorReceiverSink() { errs = warns = 0; }

    virtual IError * mapError(IError * error) { return LINK(error); }
    virtual void exportMappings(IWorkUnit * wu) const { }
    virtual void report(IError* err);
    virtual size32_t errCount() { return errs; }
    virtual size32_t warnCount() { return warns; }

private:
    unsigned errs;
    unsigned warns;
};

//---------------------------------------------------------------------------------------------------------------------

class IndirectErrorReceiver : public CInterfaceOf<IErrorReceiver>
{
public:
    IndirectErrorReceiver(IErrorReceiver & _prev) : prevErrorProcessor(&_prev) {}

    virtual void report(IError* error)
    {
        prevErrorProcessor->report(error);
    }
    virtual IError * mapError(IError * error)
    {
        return prevErrorProcessor->mapError(error);
    }
    virtual size32_t errCount()
    {
        return prevErrorProcessor->errCount();
    }
    virtual size32_t warnCount()
    {
        return prevErrorProcessor->warnCount();
    }
    virtual void exportMappings(IWorkUnit * wu) const
    {
        prevErrorProcessor->exportMappings(wu);
    }

protected:
    Linked<IErrorReceiver> prevErrorProcessor;
};

//---------------------------------------------------------------------------------------------------------------------

class HQL_API MultiErrorReceiver : public ErrorReceiverSink
{
public:
    MultiErrorReceiver() {}

    virtual void report(IError* err);

    size32_t length() { return errCount() + warnCount();}
    IError* item(size32_t index) { return &msgs.item(index); }
    IError* firstError();
    StringBuffer& toString(StringBuffer& out);
    void clear() { msgs.kill(); }

private:
    IErrorArray msgs;
};

//---------------------------------------------------------------------------------------------------------------------

extern HQL_API ErrorSeverity queryDefaultSeverity(WarnErrorCategory category);
extern HQL_API WarnErrorCategory getCategory(const char * category);
extern HQL_API ErrorSeverity getSeverity(IAtom * name);
extern HQL_API ErrorSeverity getCheckSeverity(IAtom * name);

//---------------------------------------------------------------------------------------------------------------------

inline IError * createError(int errNo, const char *msg, const char *filename, int lineno=0, int column=0, int pos=0)
{
    return createError(CategoryError, SeverityFatal, errNo, msg, filename, lineno, column, pos);
}
extern HQL_API void reportErrors(IErrorReceiver & receiver, IErrorArray & errors);
void HQL_API reportErrorVa(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char* format, va_list args) __attribute__((format(printf, 4, 0)));
void HQL_API reportError(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char * format, ...) __attribute__((format(printf, 4, 5)));

extern HQL_API IErrorReceiver * createFileErrorReceiver(FILE *f);
extern HQL_API IErrorReceiver * createNullErrorReceiver();
extern HQL_API IErrorReceiver * createThrowingErrorReceiver();
extern HQL_API IErrorReceiver * createAbortingErrorReceiver(IErrorReceiver & prev);
extern HQL_API IErrorReceiver * createDedupingErrorReceiver(IErrorReceiver & prev);

extern HQL_API void checkEclVersionCompatible(Shared<IErrorReceiver> & errors, const char * eclVersion);


#endif // _HQLERROR_HPP_
