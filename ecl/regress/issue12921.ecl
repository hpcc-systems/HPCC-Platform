#option ('spanMultipleCpp', false);

outRecord := RECORD
    STRING2  x;
    STRING10 name;
    STRING1  term;
    STRING2  nl;
END;

myService := SERVICE
    streamed dataset(outRecord) testRead(const varstring name) : distributed;
    testWrite(streamed dataset(outRecord) out);
    testWrite3(streamed dataset(outRecord) out1, streamed dataset(outRecord) out2, streamed dataset(outRecord) out3);
END;

streamed dataset(outRecord) doRead(const varstring name) := EMBED(C++ : distributed)
    #include "platform.h"
    #include "jiface.hpp"
    #include "jfile.hpp"
    #include "jstring.hpp"

    class StreamReader : public CInterfaceOf<IRowStream>
    {
    public:
        FileReader(ICodeContext * _ctx, IEngineRowAllocator * _resultAllocator) : resultAllocator(_resultAllocator)
        {
            deserializer.setown(resultAllocator->createDiskDeserializer(_ctx));
        }

        virtual const void * nextRow() override
        {
            if (!source || source->isEof())
                return NULL;

            RtlDynamicRowBuilder builder(resultAllocator);
            size32_t size = deserializer->deserialize(builder, *source);
            return builder.finalizeRowClear(size);
        }

        virtual void stop() override
        {
        }

    private:
        Linked<IOutputRowDeserializer> deserializer;
        Linked<IEngineRowAllocator> resultAllocator;
        Linked<IRowDeserializerSource> source;
    };

    class FileReader : public CInterfaceOf<IRowStream>
    {
    public:
        FileReader(ICodeContext * _ctx, IEngineRowAllocator * _resultAllocator, IFileIO * _in) : StreamReader(_ctx, _resultAllocator)
        {
            source.setown(createSeralizerSource(in));
        }

    private:
        Linked<IFileIO> in;
    };

    #body
    unsigned numParts = ctx->getNodes();
    unsigned whichPart = ctx->getNodeNum();
    StringBuffer filename;
    filename.append(name).append(".").append(whichPart).append("_of_").append(numParts);
    Owned<IFile> out = createIFile(filename);
    Owned<IFileIO> io = out->open(IFOread);
    return new FileReader(ctx, _resultAllocator, io);
ENDEMBED;



doWrite(streamed dataset ds, const varstring name) := BEGINC++
    #include "platform.h"
    #include "jiface.hpp"
    #include "jfile.hpp"
    #include "jstring.hpp"

    #body
    unsigned numParts = ctx->getNodes();
    unsigned whichPart = ctx->getNodeNum();
    StringBuffer filename;
    filename.append(name).append(".").append(whichPart).append("_of_").append(numParts);
    Owned<IFile> out = createIFile(filename);
    Owned<IFileIO> io = out->open(IFOcreate);
    //create a buffered io stream
    //create a serializer
    for(;;)
    {
        const void * next = ds->nextRow();
        if (!next)
        {
            next = ds->nextRow();
            if (!next)
                break;
        }
        //serialize row... to buffered stream
        rtlReleaseRow(next);
    }
ENDC++;

doRead('C:\\temp\\simple');

ds := DATASET(20, TRANSFORM(outRecord, SELF.name := (string)HASH32(counter); SELF.x := (string2)COUNTER; SELF.nl := '\r\n'; SELF.term := '!'));

sds := SORT(NOFOLD(ds), name);

doWrite(sds, 'C:\\temp\\simple2');


allNodesDs := DATASET(1, TRANSFORM({ unsigned id }, SELF.id := 0), LOCAL);
streamedDs := NORMALIZE(allNodesDs, doRead('C:\\temp\\simple2'), TRANSFORM(RIGHT));
output(streamedDs);

/*
Problems
- no way to specify a function that returns a dataset with a user supplied format
  * We could probably use macros to solve the problems for external services.
- code for streaming output is poor.
  * Need to introduce a new user-output activity
- no way to cordinate between instances on different nodes.
  * Needs more thought.  Might be required if input dataset required partitioning
- no way that a dataset can be specified as local/executed on all
  * Probably want a new syntax.  DATASET(function, LOCAL/DISTRIBUTED??).
- no way to represent a filtered join against an external dataset
  * A prefetch project almost provides what you need.  We should introduce a new syntax that allows
    joins against datasources where the filter is pushed to the source.  There are other situtions where
    this might help - e.g., remote filtering when reading from other thors, filtering on the disk controller etc.
*/

myService.testWrite(sds);
myService.testWrite3(sds, sds, sds(name != 'gavin'));
output(myService.testRead('x'));
