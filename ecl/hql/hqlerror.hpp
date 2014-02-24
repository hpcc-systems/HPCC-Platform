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
#ifndef _HQLERROR_HPP_
#define _HQLERROR_HPP_

#include "jhash.hpp"
#include "jexcept.hpp"
#include "hql.hpp"

#define HQLERR_ErrorAlreadyReported             4799            // special case...

enum ErrorSeverity
{
    SeverityIgnore,
    SeverityInfo,
    SeverityWarning,
    SeverityError,    // a warning treated as an error
    SeverityFatal,      // a fatal error - can't be mapped to anything else
    SeverityUnknown,
};

inline bool isError(ErrorSeverity severity) { return severity >= SeverityError; }
inline bool isFatal(ErrorSeverity severity) { return severity == SeverityFatal; }

//TBD in a separate commit - add support for warnings to be associated with different categories
enum WarnErrorCategory
{
    WECunknown,
};

interface HQL_API IECLError: public IException
{
public:
    virtual const char* getFilename() const = 0;
    virtual WarnErrorCategory getCategory() const = 0;
    virtual int getLine() const = 0;
    virtual int getColumn() const = 0;
    virtual int getPosition() const = 0;
    virtual StringBuffer& toString(StringBuffer&) const = 0;
    virtual ErrorSeverity getSeverity() const = 0;
    virtual IECLError * cloneSetSeverity(ErrorSeverity _severity) const = 0;
};
inline bool isFatal(IECLError * error) { return isFatal(error->getSeverity()); }

interface HQL_API IErrorReceiver : public IInterface
{
    virtual void report(IECLError* error) = 0;
    virtual IECLError * mapError(IECLError * error) = 0;
    virtual size32_t errCount() = 0;
    virtual size32_t warnCount() = 0;

    //global helper functions
    void reportError(int errNo, const char *msg, const char *filename, int lineno, int column, int pos);
    void reportWarning(int warnNo, const char *msg, const char *filename, int lineno, int column, int pos);
};

typedef IArrayOf<IECLError> IECLErrorArray;


//---------------------------------------------------------------------------------------------------------------------

class HQL_API ErrorReceiverSink : public CInterfaceOf<IErrorReceiver>
{
public:
    ErrorReceiverSink() { errs = warns = 0; }

    virtual IECLError * mapError(IECLError * error) { return LINK(error); }
    virtual void report(IECLError* err);
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

    virtual void report(IECLError* error)
    {
        prevErrorProcessor->report(error);
    }
    virtual IECLError * mapError(IECLError * error)
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

protected:
    Linked<IErrorReceiver> prevErrorProcessor;
};

//---------------------------------------------------------------------------------------------------------------------

class HQL_API MultiErrorReceiver : public ErrorReceiverSink
{
public:
    MultiErrorReceiver() {}

    virtual void report(IECLError* err);

    size32_t length() { return errCount() + warnCount();}
    IECLError* item(size32_t index) { return &msgs.item(index); }
    IECLError* firstError();
    StringBuffer& toString(StringBuffer& out);
    void clear() { msgs.kill(); }

private:
    IECLErrorArray msgs;
};

//---------------------------------------------------------------------------------------------------------------------

extern HQL_API IECLError *createECLError(ErrorSeverity severity, int errNo, const char *msg, const char *filename, int lineno=0, int column=0, int pos=0);
inline IECLError * createECLError(int errNo, const char *msg, const char *filename, int lineno=0, int column=0, int pos=0)
{
    return createECLError(SeverityFatal, errNo, msg, filename, lineno, column, pos);
}
extern HQL_API void reportErrors(IErrorReceiver & receiver, IECLErrorArray & errors);
void HQL_API reportErrorVa(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char* format, va_list args);
void HQL_API reportError(IErrorReceiver * errors, int errNo, const ECLlocation & loc, const char * format, ...) __attribute__((format(printf, 4, 5)));

extern HQL_API IErrorReceiver * createFileErrorReceiver(FILE *f);
extern HQL_API IErrorReceiver * createNullErrorReceiver();
extern HQL_API IErrorReceiver * createThrowingErrorReceiver();
extern HQL_API IErrorReceiver * createAbortingErrorReceiver(IErrorReceiver & prev);
extern HQL_API IErrorReceiver * createDedupingErrorReceiver(IErrorReceiver & prev);

extern HQL_API void checkEclVersionCompatible(Shared<IErrorReceiver> & errors, const char * eclVersion);


#endif // _HQLERROR_HPP_
