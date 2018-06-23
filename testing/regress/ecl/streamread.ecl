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

//class=embedded

copies := IF(__PLATFORM__='roxie',1,CLUSTERSIZE);

outRecord := RECORD
    STRING10 name;
    unsigned1  id;
END;

streamed dataset(outRecord) doRead(const varstring name) := EMBED(C++ : distributed,time)

    const char * rows[] = {
        "Gavin     \x01",
        "Simon     \x02",
        "Charlotte \x09",
        "TheEnd    \x00" };

    class StreamCreator : public IRowStream, public RtlCInterface
    {
    public:
        StreamCreator(ICodeContext * _ctx, IEngineRowAllocator * _resultAllocator) : resultAllocator(_resultAllocator)
        {
            idx = 0;
        }
        RTLIMPLEMENT_IINTERFACE

        virtual const void * nextRow() override
        {
            if (idx >= sizeof(rows)/sizeof(*rows))
                return NULL;

            RtlDynamicRowBuilder builder(resultAllocator);
            memcpy(builder.getSelf(), rows[idx++], 11);
            return builder.finalizeRowClear(11);
        }

        virtual void stop() override
        {
        }

    private:
        Linked<IEngineRowAllocator> resultAllocator;
        unsigned idx;
    };

    #body
    return new StreamCreator(ctx, _resultAllocator);
ENDEMBED;

ds := doRead('C:\\temp\\simple');

count(ds) = copies * 4;


linkcounted dataset(outRecord) doReadRows(const varstring name) := EMBED(C++ : distributed,time)

    static const char * rows2[] = {
        "Gavin     \x01",
        "Simon     \x02",
        "Charlotte \x09",
        "TheEnd    \x00" };

    #body
    //Can return constant allocations as roxie rows
    __countResult = 4;
    __result = (const byte * *)rows2;
ENDEMBED;


dsRows := doReadRows('C:\\temp\\simple');

count(dsRows) = copies * 4;

dataset(outRecord) doReadBlock(const varstring name) := EMBED(C++ : distributed,time)

    static const char * rows3 = "Gavin     \x01Simon     \002Charlotte \x09TheEnd    \x00";

    #body
    __lenResult = 44;
    __result = rtlMalloc(44);
    memcpy(__result, rows3, 44);
ENDEMBED;


dsBlock := doReadBlock('C:\\temp\\simple');

count(dsBlock) = copies * 4;
