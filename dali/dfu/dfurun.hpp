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
};

IDFUengine *createDFUengine();
void stopDFUserver(const char *qname);

extern ILogMsgHandler * fileMsgHandler;

#endif

