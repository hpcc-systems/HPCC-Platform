r := RECORD
    UNSIGNED value;
END;

//This function returns the sum of the squares of the inputs from the dataset as a single non streamed row

dataset(r) myDataset(streamed dataset(r) ds) := EMBED(C++ : activity)
#include <stdio.h>
#body
    unsigned __int64 sum = 0;
    for (;;)
    {
        const byte * next = (const byte *)ds->nextRow();
        if (!next)
            break;
        unsigned __int64 value = *(const unsigned __int64 *)next;
        rtlReleaseRow(next);
        sum += value * value;
    }

    __lenResult = sizeof(unsigned __int64);
    __result = rtlMalloc(__lenResult);
    *(unsigned __int64 *)__result = sum;
ENDEMBED;


ds1 := DATASET([1,3,4,5,9,10,1,1], r, distributed);

output(TABLE(myDataset(ds1), { unsigned value := SUM(GROUP, value); }));
