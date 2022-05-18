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

#ifndef DFURUN_HPP
#define DFURUN_HPP

class CSDSServerStatus;

#include "dfuwu.hpp"


interface IDFUengine: extends IInterface
{
    virtual void startListener(const char *queuename,CSDSServerStatus *status)=0;
    virtual void startMonitor(const char *queuename,CSDSServerStatus *status,unsigned timeout)=0;
    virtual void joinListeners()=0;
    virtual void abortListeners()=0;
    virtual DFUstate runWU(const char *dfuwuid)=0;
    virtual void setDefaultTransferBufferSize(size32_t size) = 0;
    virtual void setDFUServerName(const char* name) = 0;
};

IDFUengine *createDFUengine(const IPropertyTree *config);
void stopDFUserver(const char *qname);

#endif

