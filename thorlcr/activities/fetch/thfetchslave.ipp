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
};

IFetchStream *createFetchStream(CSlaveActivity &owner, IRowInterfaces *keyRowIf, IRowInterfaces *fetchRowIf, bool &abortSoon, CPartDescriptorArray &parts, unsigned offsetCount, size32_t offsetMapSz, const void *offsetMap, IFetchHandler *iFetchHandler, mptag_t tag, IExpander *eexp=NULL);

activityslaves_decl CActivityBase *createFetchSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createCsvFetchSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createXmlFetchSlave(CGraphElementBase *container);

#endif
