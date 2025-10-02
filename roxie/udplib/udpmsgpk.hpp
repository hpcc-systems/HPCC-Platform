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

#ifndef __UDPMSGPK__
#define __UDPMSGPK__

#include "roxiemem.hpp"
#include <queue>

class PackageSequencer;

typedef unsigned __int64 PUID;
typedef MapXToMyClass<PUID, PUID, PackageSequencer> msg_map;

class CMessageCollator : public CInterfaceOf<IMessageCollator>
{
private:
    Linked<roxiemem::IRowManager> rowMgr;  // Must be placed first to ensure it is destroyed last
    std::deque<PackageSequencer*> queue;
    msg_map             mapping;  // Note - only accessed from collator thread
    RelaxedAtomic<bool> activity;
    bool                memLimitExceeded;
    bool                encrypted;
    CriticalSection     queueCrit;
    InterruptableSemaphore sem;
    ruid_t ruid;
    RelaxedAtomic<unsigned> totalBytesReceived = {0};
    RelaxedAtomic<unsigned> totalDuplicates = {0};
    RelaxedAtomic<unsigned> totalResends = {0};

    void collate(roxiemem::DataBuffer *dataBuff);
public:
    CMessageCollator(roxiemem::IRowManager *_rowMgr, unsigned _ruid, bool encrypted);
    virtual ~CMessageCollator();

    virtual ruid_t queryRUID() const override
    {
        return ruid;
    }

    virtual unsigned queryBytesReceived() const override;
    virtual unsigned queryDuplicates() const override;
    virtual unsigned queryResends() const override;
    virtual IMessageResult *getNextResult(unsigned time_out, bool &anyActivity) override;
    virtual void interrupt(IException *E) override;

    bool attach_databuffer(roxiemem::DataBuffer *dataBuff);
    bool attach_data(const void *data, unsigned len);
    void noteDuplicate(bool isResend);
};
#endif // __UDPMSGPK__
