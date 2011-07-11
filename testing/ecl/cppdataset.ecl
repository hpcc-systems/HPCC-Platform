/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

output(table(titles, { title, ': ', dataset blockedNames(title), '\n' } ));
output(table(titles, { title, ': ', dataset linkedNames(title) , '\n' } ));
output(table(titles, { title, ': ', dataset streamedNames(title), '\n' } ));
