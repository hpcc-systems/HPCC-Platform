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

//Legacy thor doesn't support link counted rows/streamed rows, so disable the test for that engine
//nothor

// Three different varieties of c++ functions that return a dataset
namesRecord := RECORD
    STRING name;
END;

// A dataset returned as a contiguous block of data
dataset(namesRecord) blockedNames(string prefix) := BEGINC++
#define numElements(x) (sizeof(x)/sizeof(x[0]))
    const char * const names[] = {"Gavin","John","Bart"};
    unsigned len=0;
    for (unsigned i = 0; i < numElements(names); i++)
        len += sizeof(size32_t) + lenPrefix + strlen(names[i]);

    byte * p = (byte *)rtlMalloc(len);
    unsigned offset = 0;
    for (unsigned j = 0; j < numElements(names); j++)
    {
        *(size32_t *)(p + offset) = lenPrefix + strlen(names[j]);
        memcpy(p+offset+sizeof(size32_t), prefix, lenPrefix);
        memcpy(p+offset+sizeof(size32_t)+lenPrefix, names[j], strlen(names[j]));
        offset += sizeof(size32_t) + lenPrefix + strlen(names[j]);
    }

    __lenResult = len;
    __result = p;
ENDC++;


// A dataset returned as a rowset of linked rows

_linkcounted_ dataset(namesRecord) linkedNames(string prefix) := BEGINC++

#define numElements(x) (sizeof(x)/sizeof(x[0]))
    const char * const names[] = {"Gavin","John","Bart"};
    __countResult = numElements(names);
    __result = _resultAllocator->createRowset(numElements(names));
    for (unsigned i = 0; i < numElements(names); i++)
    {
        const char * name = names[i];
        size32_t lenName = strlen(name);

        RtlDynamicRowBuilder rowBuilder(_resultAllocator);
        unsigned len = sizeof(size32_t) + lenPrefix + lenName;
        byte * row = rowBuilder.ensureCapacity(len, NULL);
        *(size32_t *)(row) = lenPrefix + lenName;
        memcpy(row+sizeof(size32_t), prefix, lenPrefix);
        memcpy(row+sizeof(size32_t)+lenPrefix, name, lenName);
        __result[i] = (byte *)rowBuilder.finalizeRowClear(len);
    }

ENDC++;


// A dataset returned as a stream of linked rows
streamed dataset(namesRecord) streamedNames(string prefix) := BEGINC++

#define numElements(x) (sizeof(x)/sizeof(x[0]))

class StreamDataset : public RtlCInterface, implements IRowStream
{
public:
    StreamDataset(IEngineRowAllocator * _resultAllocator, unsigned _lenPrefix, const char * _prefix)
    : resultAllocator(_resultAllocator),lenPrefix(_lenPrefix), prefix(_prefix)
    {
        count = 0;
    }
    RTLIMPLEMENT_IINTERFACE

    virtual const void *nextRow()
    {
        const char * const names[] = {"Gavin","John","Bart"};
        if (count >= numElements(names))
            return NULL;

        const char * name = names[count++];
        size32_t lenName = strlen(name);

        RtlDynamicRowBuilder rowBuilder(resultAllocator);
        unsigned len = sizeof(size32_t) + lenPrefix + lenName;
        byte * row = rowBuilder.ensureCapacity(len, NULL);
        *(size32_t *)(row) = lenPrefix + lenName;
        memcpy(row+sizeof(size32_t), prefix, lenPrefix);
        memcpy(row+sizeof(size32_t)+lenPrefix, name, lenName);
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop()
    {
        count = (unsigned)-1;
    }


protected:
    Linked<IEngineRowAllocator> resultAllocator;
    unsigned count;
    unsigned lenPrefix;
    const char * prefix;
};

#body
    return new StreamDataset(_resultAllocator, lenPrefix, prefix);
ENDC++;




// Simple output

output(blockedNames('Mr '));
output(blockedNames('Rev. '));
output(blockedNames('Rev. '));
output(linkedNames('Mr. '));
output(linkedNames('Rev. '));
output(linkedNames('Rev. '));
output(streamedNames('Mr. '));
output(streamedNames('Rev. '));
output(streamedNames('Rev. '));

// Slightly unusual case involving a denormalize.

titles := dataset(['', 'Mr. ', 'Rev. '], { string title });

output(normalize(titles, blockedNames(left.title), transform(right)));
output(normalize(titles, linkedNames(left.title), transform(right)));
output(normalize(titles, streamedNames(left.title), transform(right)));


// Project

output(table(titles, { title, ': ', dataset blockedNames := blockedNames(title), '\n' } ));
output(table(titles, { title, ': ', dataset linkedNames := linkedNames(title) , '\n' } ));
output(table(titles, { title, ': ', dataset streamedNames := streamedNames(title), '\n' } ));
