r := RECORD
    UNSIGNED id;
    STRING name;
END;

streamed dataset(r) myDataset(unsigned numRows, boolean isLocal = false, unsigned numParallel = 0) := EMBED(C++ : activity, local(isLocal), parallel(numParallel))
static const char * const names[] = {"Gavin","John","Bart"};
static const unsigned numNames = (unsigned)(sizeof(names) / sizeof(names[0]));
#body
    class MyStreamInlineDataset : public RtlCInterface, implements IRowStream
    {
    public:
        MyStreamInlineDataset(IEngineRowAllocator * _resultAllocator, unsigned _first, unsigned _last)
        : resultAllocator(_resultAllocator), first(_first), last(_last)
        {
            current = first;
        }
        RTLIMPLEMENT_IINTERFACE

        virtual const void *nextRow() override
        {
            if (current >= last)
                return nullptr;

            unsigned id = current++;
            unsigned curName = id % numNames;
            const char * name = names[curName];
            size32_t lenName = strlen(name);

            RtlDynamicRowBuilder rowBuilder(resultAllocator);
            unsigned len = sizeof(__int64) + sizeof(size32_t) + lenName;
            byte * row = rowBuilder.ensureCapacity(len, NULL);
            *(__uint64 *)(row) = id;
            *(size32_t *)(row + sizeof(__uint64)) = lenName;
            memcpy(row+sizeof(__uint64)+sizeof(size32_t), name, lenName);
            return rowBuilder.finalizeRowClear(len);
        }
        virtual void stop() override
        {
            current = (unsigned)-1;
        }


    protected:
        Linked<IEngineRowAllocator> resultAllocator;
        unsigned current;
        unsigned first;
        unsigned last;
    };

    unsigned numRows = numrows;
    unsigned numSlaves = activity->numSlaves();
    unsigned numParallel = numSlaves * activity->numStrands();
    unsigned rowsPerPart = (numRows + numParallel - 1) / numParallel;
    unsigned thisSlave = activity->querySlave();
    unsigned thisIndex = thisSlave * activity->numStrands() + activity->queryStrand();
    unsigned first = thisIndex * rowsPerPart;
    unsigned last = first + rowsPerPart;
    if (first > numRows)
        first = numRows;
    if (last > numRows)
        last = numRows;

    return new MyStreamInlineDataset(_resultAllocator, first, last);
ENDEMBED;


//Global activity - fixed number of rows
output(myDataset(10));
//Local version of the activity 
output(count(myDataset(10, isLocal := true)) = CLUSTERSIZE * 10);

//Check that stranding (if implemented) still generates unique records
output(COUNT(DEDUP(myDataset(1000, numParallel := 5), id, ALL)));

r2 := RECORD
    UNSIGNED id;
    DATASET(r) child;
END;

//Check that the activity can also be executed in a child query
output(DATASET(10, TRANSFORM(r2, SELF.id := COUNTER; SELF.child := myDataset(COUNTER))));

//Test stranding inside a child query
output(DATASET(10, TRANSFORM(r2, SELF.id := COUNTER; SELF.child := myDataset(COUNTER, NumParallel := 3))));
