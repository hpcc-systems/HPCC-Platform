r := RECORD
    UNSIGNED value;
END;

//This function takes four streamed inputs, and outputs the result of ds1*ds2+ds3*ds4

streamed dataset(r) myDataset(streamed dataset(r) ds1, streamed dataset(r) ds2, streamed dataset(r) ds3, streamed dataset(r) ds4) := EMBED(C++ : activity)
#include <stdio.h>
#body
    class MyStreamInlineDataset : public RtlCInterface, implements IRowStream
    {
    public:
        MyStreamInlineDataset(IEngineRowAllocator * _resultAllocator, IRowStream * _ds1, IRowStream * _ds2, IRowStream * _ds3, IRowStream * _ds4)
        : resultAllocator(_resultAllocator), ds1(_ds1), ds2(_ds2), ds3(_ds3), ds4(_ds4)
        {
        }
        RTLIMPLEMENT_IINTERFACE

        virtual const void *nextRow() override
        {
            const byte * next1 = (const byte *)ds1->nextRow();
            if (!next1)
                return nullptr;
            const byte * next2 = (const byte *)ds2->nextRow();
            const byte * next3 = (const byte *)ds3->nextRow();
            const byte * next4 = (const byte *)ds4->nextRow();
            if (!next2 || !next3 || !next4)
                rtlFailUnexpected();

            unsigned __int64 value1 = *(const unsigned __int64 *)next1;
            unsigned __int64 value2 = *(const unsigned __int64 *)next2;
            unsigned __int64 value3 = *(const unsigned __int64 *)next3;
            unsigned __int64 value4 = *(const unsigned __int64 *)next4;
            rtlReleaseRow(next1);
            rtlReleaseRow(next2);
            rtlReleaseRow(next3);
            rtlReleaseRow(next4);
            
            unsigned __int64 result = value1 * value2 + value3 * value4;
            RtlDynamicRowBuilder rowBuilder(resultAllocator);
            byte * row = rowBuilder.getSelf();
            *(__uint64 *)(row) = result;
            return rowBuilder.finalizeRowClear(sizeof(unsigned __int64));
        }
        virtual void stop() override
        {
            ds1->stop();
            ds2->stop();
            ds3->stop();
            ds4->stop();
        }

    protected:
        Linked<IEngineRowAllocator> resultAllocator;
        IRowStream * ds1;
        IRowStream * ds2;
        IRowStream * ds3;
        IRowStream * ds4;
    };

    return new MyStreamInlineDataset(_resultAllocator, ds1, ds2, ds3, ds4);
ENDEMBED;


ds1 := DATASET([1,3,4,5], r);
ds2 := DATASET([1,2,3,4], r);
ds3 := DATASET([0,1,1,2], r);
ds4 := DATASET([9,8,7,6], r);

//Global operation
output(myDataset(ds1, ds2, ds3, ds4));

//Execute within a child query
mkDs(unsigned num, unsigned base) := DATASET(num, TRANSFORM(r, SELF.value := COUNTER+base));
dsx := mkDs(10, 0);
dsy := PROJECT(dsx, TRANSFORM(r, SELF.value := SUM(myDataset(mkDs(LEFT.value, 0), mkDs(LEFT.value, 1), mkDs(LEFT.value, 2), mkDs(LEFT.value, 3)), value)));
output(dsy);
