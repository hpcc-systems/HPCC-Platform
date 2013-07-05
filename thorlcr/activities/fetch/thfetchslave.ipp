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

#ifndef _THFETCHSLAVE_IPP
#define _THFETCHSLAVE_IPP

#include "platform.h"

#include "slave.ipp"


interface IFetchHandler
{
    virtual offset_t extractFpos(const void *key) = 0;
    virtual size32_t fetch(ARowBuilder & rowBuilder, const void *keyRow, unsigned filePartIndex, unsigned __int64 localFpos, unsigned __int64 fpos) = 0;
    virtual void onLimitExceeded() = 0;
};

interface IFetchStream : extends IInterface
{
    virtual void start(IRowStream *input) = 0;
    virtual IRowStream *queryOutput() = 0;
    virtual IFileIO *queryPartIO(unsigned part) = 0;
    virtual StringBuffer &getPartName(unsigned part, StringBuffer &out) = 0;
    virtual void abort() = 0;
};

IFetchStream *createFetchStream(CSlaveActivity &owner, IRowInterfaces *keyRowIf, IRowInterfaces *fetchRowIf, bool &abortSoon, CPartDescriptorArray &parts, unsigned offsetCount, size32_t offsetMapSz, const void *offsetMap, IFetchHandler *iFetchHandler, mptag_t tag, IExpander *eexp=NULL);

activityslaves_decl CActivityBase *createFetchSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createCsvFetchSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createXmlFetchSlave(CGraphElementBase *container);

#endif
