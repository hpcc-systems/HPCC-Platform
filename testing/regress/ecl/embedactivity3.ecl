r := RECORD
    UNSIGNED value;
END;

//This function takes two streamed inputs, and outputs the result of two values from the left multiply together and added to a row from the right

streamed dataset(r) myDataset(streamed dataset(r) ds1, streamed dataset(r) ds2) := EMBED(C++ : activity)
#include <stdio.h>
#body
    class MyStreamInlineDataset : public RtlCInterface, implements IRowStream
    {
    public:
        MyStreamInlineDataset(IEngineRowAllocator * _resultAllocator, IRowStream * _ds1, IRowStream * _ds2)
        : resultAllocator(_resultAllocator), ds1(_ds1), ds2(_ds2)
        {
        }
        RTLIMPLEMENT_IINTERFACE

        virtual const void *nextRow() override
        {
            const byte * next1a = (const byte *)ds1->nextRow();
            if (!next1a)
                return nullptr;
            const byte * next1b = (const byte *)ds1->nextRow();
            const byte * next2 = (const byte *)ds2->nextRow();
            if (!next1b || !next2)
                rtlFailUnexpected();

            unsigned __int64 value1a = *(const unsigned __int64 *)next1a;
            unsigned __int64 value1b = *(const unsigned __int64 *)next1b;
            unsigned __int64 value2 = *(const unsigned __int64 *)next2;
            rtlReleaseRow(next1a);
            rtlReleaseRow(next1b);
            rtlReleaseRow(next2);
            
            unsigned __int64 result = value1a * value1b + value2;
            RtlDynamicRowBuilder rowBuilder(resultAllocator);
            byte * row = rowBuilder.getSelf();
            *(__uint64 *)(row) = result;
            return rowBuilder.finalizeRowClear(sizeof(unsigned __int64));
        }
        virtual void stop() override
        {
            ds1->stop();
            ds2->stop();
        }


    protected:
        Linked<IEngineRowAllocator> resultAllocator;
        IRowStream * ds1;
        IRowStream * ds2;
    };

    return new MyStreamInlineDataset(_resultAllocator, ds1, ds2);
ENDEMBED;


ds1 := DATASET([1,3,4,5,9,10,1,1], r);
ds2 := DATASET([0,3,9,-1], r);

output(myDataset(ds1, ds2));
