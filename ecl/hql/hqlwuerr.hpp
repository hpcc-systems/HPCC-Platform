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
#ifndef __HQLWUERR_HPP__
#define __HQLWUERR_HPP__

#include "hqlexpr.hpp"
#include "workunit.hpp"

class HQL_API WorkUnitErrorReceiver : public CInterface, implements IErrorReceiver
{
public:
    WorkUnitErrorReceiver(IWorkUnit * _wu, const char * _component) { wu.set(_wu); component.set(_component); }
    IMPLEMENT_IINTERFACE;

    virtual IECLError * mapError(IECLError * error);
    virtual void report(IECLError*);
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
