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

inRec := { unsigned id };
doneRec := { unsigned4 execid };
out1rec := { unsigned id; };
out2rec := { real id; };

dataset(doneRec) doSomethingNasty(DATASET(inRec) input) := BEGINC++
  __lenResult = 4;
  __result = rtlMalloc(8);
  *(unsigned *)__result = 91823;
ENDC++;

dataset(out1Rec) extractResult1(doneRec done) := BEGINC++
   const unsigned id = *(unsigned *)done;
   const unsigned cnt = 10;
   __lenResult = cnt * sizeof(unsigned __int64);
   __result = rtlMalloc(__lenResult);
   for (unsigned i=0; i < cnt; i++)
       ((unsigned __int64 *)__result)[i] = id + i + 1;
ENDC++;

_LINKCOUNTED_ dataset(out2Rec) extractResult2(doneRec done) := BEGINC++
   const unsigned id = *(unsigned *)done;
   const unsigned cnt = 10;
   __countResult = cnt;
   __result = _resultAllocator->createRowset(cnt);
   for (unsigned i=0; i < cnt; i++)
   {
       size32_t allocSize;
        void * row = _resultAllocator->createRow(allocSize);
        *(double *)row = id + i + 1;
        __result[i] =  (byte *)_resultAllocator->finalizeRow(allocSize, row, allocSize);
   }
ENDC++;

streamed dataset(out1Rec) extractResult3(doneRec done) := BEGINC++
   class myStream : public IRowStream, public RtlCInterface
   {
    public:
        myStream(IEngineRowAllocator * _allocator, unsigned _id) : allocator(_allocator), id(_id), idx(0) {}
        RTLIMPLEMENT_IINTERFACE

        virtual const void *nextRow()
        {
            if (idx >= 10)
               return NULL;
            size32_t allocSize;
            void * row = allocator->createRow(allocSize);
            *(unsigned __int64 *)row = id + ++idx;
            return allocator->finalizeRow(allocSize, row, allocSize);
        }
        virtual void stop() {}
    private:
        Linked<IEngineRowAllocator> allocator;
        unsigned id;
        unsigned idx;
    };
    #body
    const unsigned id = *(unsigned *)done;
    return new myStream(_resultAllocator, id);
ENDC++;

ds := dataset([1,2,3,4], inRec);

processed := doSomethingNasty(ds);

out1 := NORMALIZE(processed, extractResult1(LEFT), transform(RIGHT));
out2 := NORMALIZE(processed, extractResult2(LEFT), transform(RIGHT));
out3 := NORMALIZE(processed, extractResult3(LEFT), transform(RIGHT));

SEQUENTIAL(
output(out1);
output(out2);
output(out3);
);

