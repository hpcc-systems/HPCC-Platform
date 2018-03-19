r := RECORD
    UNSIGNED id;
    STRING name;
END;

traceDataset(streamed dataset(r) ds, boolean isLocal = false) := EMBED(C++ : activity, local(isLocal))
#include <stdio.h>
#body
    for(;;)
    {
        const byte * next = (const byte *)ds->nextRow();
        if (!next)
        {
            next = (const byte *)ds->nextRow();
            if (!next)
                return;
        }

        unsigned __int64 id = *(__uint64 *)(next);
        size32_t lenName = *(size32_t *)(next + sizeof(__uint64));
        const char * name = (char *)(next + sizeof(__uint64) + sizeof(size32_t));

        printf("id(%u) name(%.*s)\n", (unsigned)id, lenName, name);
        rtlReleaseRow(next);
   }

ENDEMBED;

ds := DATASET([
    {1,'GCH'},{2,'RKC'},{3,'Count Dracular'}, {4, 'Boris'}
    ], r);

traceDataset(ds);
