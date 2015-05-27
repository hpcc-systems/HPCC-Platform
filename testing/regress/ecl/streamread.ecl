outRecord := RECORD
    STRING10 name;
    unsigned1  id;
END;

streamed dataset(outRecord) doRead(const varstring name) := EMBED(C++ : distributed)

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

        virtual const void * nextRow()
        {
            if (idx >= sizeof(rows)/sizeof(*rows))
                return NULL;

            RtlDynamicRowBuilder builder(resultAllocator);
            memcpy(builder.getSelf(), rows[idx++], 11);
            return builder.finalizeRowClear(11);
        }

        virtual void stop()
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

count(ds) = CLUSTERSIZE * 4;


linkcounted dataset(outRecord) doReadRows(const varstring name) := EMBED(C++ : distributed)

    static const char * rows2[] = {
        "Gavin     \x01",
        "Simon     \x02",
        "Charlotte \x09",
        "TheEnd    \x00" };

    #body
    //Can return constant allocations as roxie rows
    __countResult = 4;
    __result = (byte * *)rows2;
ENDEMBED;


dsRows := doReadRows('C:\\temp\\simple');

count(dsRows) = CLUSTERSIZE * 4;

dataset(outRecord) doReadBlock(const varstring name) := EMBED(C++ : distributed)

    static const char * rows3 = "Gavin     \x01Simon     \002Charlotte \x09TheEnd    \x00";

    #body
    __lenResult = 44;
    __result = rtlMalloc(44);
    memcpy(__result, rows3, 44);
ENDEMBED;


dsBlock := doReadBlock('C:\\temp\\simple');

count(dsBlock) = CLUSTERSIZE * 4;
