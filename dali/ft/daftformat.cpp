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

#include "jliball.hpp"

#include "platform.h"

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"

#include "fterror.hpp"
#include "dadfs.hpp"
#include "rmtspawn.hpp"
#include "jptree.hpp"
#include "daft.hpp"
#include "daftcfg.hpp"
#include "daftmc.hpp"
#include "daftformat.ipp"
#include "junicode.hpp"

//----------------------------------------------------------------------------

/*
  This needs to partition with split points from (thisOffset/numParts) to (thisOffset+thisSize-1/numParts).  
  It returns the point just before the expected point, rather than the closest.
  NB: It assumes records cannot be split between parts!
*/

const static unsigned maxNumberOfBufferOverrun = 100;
const offset_t noSizeLimit = I64C(0x7fffffffffffffff);

CPartitioner::CPartitioner()
{
    whichInput = (unsigned) -1;
    numParts = 0;
    thisSize = noSizeLimit;
    thisOffset = 0;
    thisHeaderSize = 0;
    totalSize = 0;
    partitioning = false;
}

void CPartitioner::calcPartitions(Semaphore * sem)
{
    commonCalcPartitions();
    if (sem)
        sem->signal();
}


void CPartitioner::commonCalcPartitions()
{
    if (thisSize == 0)
    {
        LOG(MCwarning, unknownJob, "Unexpected: Part %s has zero length!", fullPath.get());
        return;
    }

    const offset_t partSize = totalSize / numParts;
    const offset_t endOffset = thisOffset + thisSize;
    unsigned firstSplit;
    unsigned lastSplit;
    bool appendingContent=false;
    if (partSize)
    {
        firstSplit = (unsigned)((thisOffset + partSize-1)/partSize);
        lastSplit = (unsigned)((endOffset-1)/partSize);
        appendingContent=(thisOffset % partSize)!=0;
    }
    else
    {
        firstSplit = (unsigned)((thisOffset*numParts)/totalSize);
        lastSplit = (unsigned)(((endOffset-1)*numParts)/totalSize);
        appendingContent=((thisOffset*numParts) % totalSize)!=0;
    }
    if (endOffset == totalSize) lastSplit = numParts-1;
    if (lastSplit >= numParts) lastSplit = numParts-1;                                      // very rare with variable length records, last file is very small or copying a couple of records 50 ways.

    if (!partSeparator.isEmpty() && appendingContent) //appending to existing content, add a separator if necessary
    {
        Owned<PartitionPoint> separator = new PartitionPoint;
        separator->inputOffset = 0;
        separator->inputLength = partSeparator.length();
        separator->outputLength = partSeparator.length();
        separator->fixedText.set(partSeparator.length(), partSeparator.get());
        separator->whichInput = whichInput;
        separator->whichOutput = firstSplit-1;
        results.append(*separator.getClear());
    }

    offset_t startInputOffset = thisOffset;
    offset_t startOutputOffset = 0;

    PartitionCursor cursor(thisOffset);
    if (target)
        target->setOutput(0);

    for (unsigned split=firstSplit; split <= lastSplit; split++)
    {
        offset_t splitPoint;
        if (split == numParts) 
            splitPoint = endOffset;
        else if (partSize==0)
            splitPoint =  (split * totalSize) / numParts;
        else
            splitPoint =  split * partSize;
        findSplitPoint(splitPoint, cursor);
        const offset_t inputOffset = cursor.inputOffset;
        assertex(inputOffset >= thisOffset && inputOffset <= thisOffset + thisSize);

        //Don't add an empty block on the start of the this chunk to transfer.
        if ((split != firstSplit) || (inputOffset != startInputOffset))
        {
            results.append(*new PartitionPoint(whichInput, split-1, startInputOffset-thisOffset+thisHeaderSize, inputOffset - startInputOffset - cursor.trimLength, cursor.outputOffset-startOutputOffset));
            startInputOffset = inputOffset;
            startOutputOffset = cursor.outputOffset;
        }
    }

    assertex(startInputOffset != endOffset || splitAfterPoint());
    findSplitPoint(endOffset, cursor);

    killBuffer(); // don't keep buffer longer than needed
    
    results.append(*new PartitionPoint(whichInput, lastSplit, startInputOffset-thisOffset+thisHeaderSize, endOffset - startInputOffset, cursor.outputOffset-startOutputOffset));
}

void CPartitioner::getResults(PartitionPointArray & partition)
{
    ForEachItemIn(idx, results)
    {
        PartitionPoint & cur = results.item(idx);
        partition.append(OLINK(cur));
    }
}

void CPartitioner::setPartitionRange(offset_t _totalSize, offset_t _thisOffset, offset_t _thisSize, unsigned _thisHeaderSize, unsigned _numParts)
{
    partitioning = true;
    assertex(whichInput == (unsigned)-1);   // must be called before setSource
    totalSize = _totalSize;
    thisOffset = _thisOffset;
    thisSize = _thisSize;
    thisHeaderSize = _thisHeaderSize;
    numParts = _numParts;
}

void CPartitioner::setSource(unsigned _whichInput, const RemoteFilename & _inputName, bool /*_compressedInput*/, const char * /*_decryptKey*/)
{
    whichInput = _whichInput;
    inputName.set(_inputName);

    StringBuffer fullPathText;
    inputName.getRemotePath(fullPathText);
    fullPath.set(fullPathText.str());
}

void CPartitioner::setTarget(IOutputProcessor * _target)
{
    target.set(_target);
}

void CPartitioner::setRecordStructurePresent(bool _recordStructurePresent)
{

}

void CPartitioner::getRecordStructure(StringBuffer & _recordStructure)
{
    _recordStructure.clear();
}


//----------------------------------------------------------------------------


//----------------------------------------------------------------------------

CSimpleFixedPartitioner::CSimpleFixedPartitioner(unsigned _recordSize, bool _noTranslation)
{
    LOG(MCdebugProgressDetail, unknownJob, "CSimpleFixedPartitioner::CSimpleFixedPartitioner( _recordSize:%d, _noTranslation:%d)", _recordSize, _noTranslation);
    recordSize = _recordSize;
    noTranslation = _noTranslation;
}

void CSimpleFixedPartitioner::findSplitPoint(offset_t splitOffset, PartitionCursor & cursor)
{
    offset_t prevOffset = cursor.inputOffset;
    cursor.inputOffset = (splitOffset / recordSize) * recordSize;
    if (noTranslation)
        cursor.outputOffset += (cursor.inputOffset - prevOffset);
}

void CSimpleFixedPartitioner::setPartitionRange(offset_t _totalSize, offset_t _thisOffset, offset_t _thisSize, unsigned _thisHeaderSize, unsigned _numParts)
{
    if (_thisSize == (offset_t)-1)
        throwError1(DFTERR_CouldNotOpenFile, fullPath.get());
    if ((_thisSize % recordSize) != 0)
        throwError1(DFTERR_FixedWidthInconsistent, fullPath.get());
    CSimplePartitioner::setPartitionRange(_totalSize, _thisOffset, _thisSize, _thisHeaderSize, _numParts);
}

//----------------------------------------------------------------------------

CSimpleBlockedPartitioner::CSimpleBlockedPartitioner(bool _noTranslation) : CSimpleFixedPartitioner(EFX_BLOCK_SIZE, _noTranslation)
{
    LOG(MCdebugProgressDetail, unknownJob, "CSimpleBlockedPartitioner::CSimpleBlockedPartitioner( _noTranslation:%d)", _noTranslation);
}

//----------------------------------------------------------------------------



bool CInputBasePartitioner::ensureBuffered(unsigned required)
{
    // returns false if eof hit
    byte *buffer = bufferBase();
    assertex(required <= bufferSize);
    while (bufferOffset + required > numInBuffer)
    {
        if ((bufferOffset + required > bufferSize) || (numInBuffer + blockSize > bufferSize))
        {
            memmove(buffer, buffer+bufferOffset, numInBuffer-bufferOffset);
            numInBuffer -= bufferOffset;
            bufferOffset = 0;
        }

        size32_t numRead = inStream->read(blockSize, buffer+numInBuffer);
        if (numRead == 0)
            return false;
        numInBuffer += numRead;
    }
    return true;
}

offset_t CInputBasePartitioner::tellInput()
{
    return inStream->tell() - numInBuffer + bufferOffset;
}

void CInputBasePartitioner::findSplitPoint(offset_t splitOffset, PartitionCursor & cursor)
{
    offset_t inputOffset = cursor.inputOffset;
    offset_t nextInputOffset = cursor.nextInputOffset;
    const byte *buffer = bufferBase();

    offset_t logStepOffset;
    if( (splitOffset-inputOffset) < 1000000000 )
    {
        logStepOffset = splitOffset / 4;  // 25% step to display progress for files < ~1G split
    }
    else
    {
        logStepOffset = splitOffset / 100;  // 1% step to display progress for files > ~1G split
    }
    offset_t oldInputOffset = nextInputOffset;

    while (nextInputOffset < splitOffset)
    {
        if( nextInputOffset > oldInputOffset + logStepOffset)
        {
            // Display progress
            oldInputOffset = nextInputOffset;
            LOG(MCdebugProgressDetail, unknownJob, "findSplitPoint(splitOffset:%" I64F "d) progress: %3.0f%% done.", splitOffset, (double)100.0*(double)nextInputOffset/(double)splitOffset);
        }

        inputOffset = nextInputOffset;

        ensureBuffered(headerSize);
        assertex((headerSize ==0) || (numInBuffer != bufferOffset));

        bool processFullBuffer =  (nextInputOffset + blockSize) < splitOffset;

        unsigned size = getSplitRecordSize(buffer+bufferOffset, numInBuffer-bufferOffset, processFullBuffer);

        if (size==0)
            throwError1(DFTERR_PartitioningZeroSizedRowLink,((offset_t)(buffer+bufferOffset)));

        if (size > bufferSize)
        {
            LOG(MCdebugProgressDetail, unknownJob, "Split record size %d (0x%08x) is larger than the buffer size: %d", size, size, bufferSize);
            throwError2(DFTERR_WrongSplitRecordSize, size, size);
        }

        ensureBuffered(size);

        nextInputOffset += size;
        if (target)
        {
            //Need to be very careful which offsets get updated (e.g. blocked format on
            //boundary needs to close block correctly...
            //NB: If equal, then will update outside the loop
            if (nextInputOffset > splitOffset)
                target->finishOutputOffset();
            cursor.outputOffset = target->getOutputOffset();
            target->updateOutputOffset(size, buffer+bufferOffset);
        }

        bufferOffset += size;
    }
    if (nextInputOffset == splitOffset)
    {
        inputOffset = nextInputOffset;
        if (target)
        {
            target->finishOutputOffset();
            cursor.outputOffset = target->getOutputOffset();
        }
    }

    cursor.inputOffset = inputOffset;
    cursor.nextInputOffset = nextInputOffset;
    LOG(MCdebugProgressDetail, unknownJob, "findSplitPoint(splitOffset:%" I64F "d) progress: %3.0f%% done.", splitOffset, 100.0);
}



CInputBasePartitioner::CInputBasePartitioner(unsigned _headerSize, unsigned expectedRecordSize)
{
    headerSize = _headerSize;
    blockSize = 0x40000;
    bufferSize = 4 * blockSize + expectedRecordSize;
    doInputCRC = false;
    CriticalBlock block(openfilecachesect);
    if (!openfilecache) 
        openfilecache = createFileIOCache(16);
    else
        openfilecache->Link();

    clearBufferOverrun();
}

IFileIOCache *CInputBasePartitioner::openfilecache = NULL;
CriticalSection CInputBasePartitioner::openfilecachesect;

CInputBasePartitioner::~CInputBasePartitioner()
{
    inStream.clear();
    if (openfilecache) {
        CriticalBlock block(openfilecachesect);
        if (openfilecache->Release()) 
            openfilecache = NULL;
    }
}

void CInputBasePartitioner::setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool _compressedInput, const char *_decryptKey)
{
    CPartitioner::setSource(_whichInput, _fullPath, _compressedInput,_decryptKey);
    Owned<IFileIO> inIO;
    Owned<IFile> inFile = createIFile(inputName);
    if (!inFile->exists()) {
        StringBuffer tmp;
        inputName.getRemotePath(tmp);
        throwError1(DFTERR_CouldNotOpenFilePart, tmp.str());
    }
    inIO.setown(openfilecache->addFile(inputName,IFOread));

    if (_compressedInput) {
        Owned<IExpander> expander;
        if (_decryptKey&&*_decryptKey) {
            StringBuffer key;
            decrypt(key,_decryptKey);
            expander.setown(createAESExpander256(key.length(),key.str()));
        }
        inIO.setown(createCompressedFileReader(inIO,expander)); 
    }

    if (thisSize != noSizeLimit)
        inIO.setown(createIORange(inIO, 0, thisHeaderSize+thisSize));
    inStream.setown(createIOStream(inIO));
    seekInput(thisHeaderSize);
}


void CInputBasePartitioner::seekInput(offset_t offset)
{
    inStream->seek(offset, IFSbegin);
    numInBuffer = 0;
    bufferOffset = 0;
}

void CInputBasePartitioner::beginTransform(offset_t thisOffset, offset_t thisLength, TransformCursor & cursor)
{
    cursor.inputOffset = thisOffset;
    inStream->seek(thisOffset, IFSbegin);
}

void CInputBasePartitioner::endTransform(TransformCursor & cursor)
{
    target->finishOutputRecords();
}

unsigned CInputBasePartitioner::transformBlock(offset_t endOffset, TransformCursor & cursor)
{
    const byte *buffer = bufferBase();
    offset_t startOffset = cursor.inputOffset;
    offset_t inputOffset = startOffset;

    while (inputOffset - startOffset < 32768)
    {
        if (inputOffset == endOffset)
            break;

        ensureBuffered(headerSize);
        assertex((headerSize ==0) || (numInBuffer != bufferOffset));
        unsigned readSize = numInBuffer - bufferOffset;
        if (readSize + inputOffset > endOffset)
            readSize = (unsigned)(endOffset - inputOffset);
        unsigned size = getTransformRecordSize(buffer+bufferOffset, readSize);
        ensureBuffered(size);

        if (doInputCRC)
            inputCRC = crc32((const char *)buffer+bufferOffset, size, inputCRC);
        target->outputRecord(size, buffer+bufferOffset);

        inputOffset += size;
        bufferOffset += size;
    }

    cursor.inputOffset = inputOffset;
    return (size32_t)(inputOffset - startOffset);
}

//----------------------------------------------------------------------------

CFixedPartitioner::CFixedPartitioner(size32_t _recordSize) : CInputBasePartitioner(0, _recordSize)
{
    LOG(MCdebugProgressDetail, unknownJob, "CFixedPartitioner::CFixedPartitioner( recordSize:%d)", recordSize);
    recordSize = _recordSize;
}

size32_t CFixedPartitioner::getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer)
{
    return recordSize;
}

size32_t CFixedPartitioner::getTransformRecordSize(const byte * record, unsigned maxToRead)
{
    return recordSize;
}


//----------------------------------------------------------------------------

CBlockedPartitioner::CBlockedPartitioner() : CFixedPartitioner(EFX_BLOCK_SIZE)
{
    LOG(MCdebugProgressDetail, unknownJob, "CBlockedPartitioner::CBlockedPartitioner()");
}


void CBlockedPartitioner::setTarget(IOutputProcessor * _target)
{
    Owned<IOutputProcessor> hook = new CBlockedProcessorHook(_target);
    CInputBasePartitioner::setTarget(hook);
}

//----------------------------------------------------------------------------

CVariablePartitioner::CVariablePartitioner(bool _bigendian) : CInputBasePartitioner(sizeof(varLenType), EXPECTED_VARIABLE_LENGTH)
{
    LOG(MCdebugProgressDetail, unknownJob, "CVariablePartitioner::CVariablePartitioner(_bigendian:%d)", _bigendian);
    assertex(sizeof(varLenType) == 4);
    bigendian = _bigendian;
}

size32_t CVariablePartitioner::getRecordSize(const byte * record, unsigned maxToRead)
{
    varLenType nextSize;
    if (bigendian)
        _WINCPYREV4(&nextSize, record);
    else
        memcpy(&nextSize, record, sizeof(varLenType));
    return nextSize + sizeof(varLenType);
}


size32_t CVariablePartitioner::getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer)
{
    return getRecordSize(record, maxToRead);
}


size32_t CVariablePartitioner::getTransformRecordSize(const byte * record, unsigned maxToRead)
{
    return getRecordSize(record, maxToRead);
}


void CVariablePartitioner::setTarget(IOutputProcessor * _target)
{
    Owned<IOutputProcessor> hook = new CVariableProcessorHook(_target);
    CInputBasePartitioner::setTarget(hook);
}

//----------------------------------------------------------------------------

#define BDW_SIZE (4)  // 4 bytes of the block size (including BDW) and should be >= 4
#define RDW_SIZE (4)  // first 2 bytes size (including RDW), size >= 4 and next 2 byte should be 0.

CRECFMvbPartitioner::CRECFMvbPartitioner(bool blocked) : CInputBasePartitioner(blocked?BDW_SIZE:RDW_SIZE, EXPECTED_VARIABLE_LENGTH)
{
    LOG(MCdebugProgressDetail, unknownJob, "CRECFMvbPartitioner::CRECFMvbPartitioner(blocked:%d)", blocked);
    isBlocked = blocked;
}

size32_t CRECFMvbPartitioner::getRecordSize(const byte * record, unsigned maxToRead)
{
    unsigned short recordsize;
    _WINCPYREV2(&recordsize, record);

    unsigned short rest;
    _WINCPYREV2(&rest, record+2);

    if (rest)
    {
        LOG(MCdebugProgressDetail, unknownJob, "Wrong RECFMv RDW info: size:%d (0x%04x) rest:%d (0x%04x) at pos :%d", recordsize ,recordsize, rest, rest, unsigned (record - bufferBase()) );
        throwError1(DFTERR_WrongRECFMvRecordDescriptorWord, rest);
    }

    return recordsize;
}

size32_t CRECFMvbPartitioner::getBlockSize(const byte * record, unsigned maxToRead)
{
    size32_t bdw;           //RECFMVB Block Description Word
    _WINCPYREV4(&bdw, record);
    size32_t blockSize = 0;
    if ( bdw & 0x80000000 )
    {
        // Extended BDW
        // Bit 0 -> 1       MSB0
        // Bit 1..31 -> block size
        blockSize = bdw & 0x7fffffff;
    }
    else
    {
        // Standard BDW
        // Bit 0 -> 0;      MSB0
        // Bit 1..15 -> block size
        // Bit 16..31 -> must be 0
        if ( bdw & 0x0000ffff )
        {
            throwError1(DFTERR_WrongRECFMvbBlockDescriptorWord, bdw);
        }

        blockSize = bdw >> 16;
    }
    if ( blockSize < 8 )
        throwError1(DFTERR_WrongRECFMvbBlockDescriptorWord, bdw);

    return blockSize;
}


size32_t CRECFMvbPartitioner::getSplitRecordSize(const byte * record, unsigned maxToRead, bool processFullBuffer)
{
    return (isBlocked ? getBlockSize(record, maxToRead) : getRecordSize(record, maxToRead));
}


size32_t CRECFMvbPartitioner::getTransformRecordSize(const byte * record, unsigned maxToRead)
{
    return (isBlocked ? getBlockSize(record, maxToRead) : getRecordSize(record, maxToRead));
}


//void CRECFMvbPartitioner::setTarget(IOutputProcessor * _target)  // don't need hook
//{
//  Owned<IOutputProcessor> hook = new CVariableProcessorHook(_target);
//  CInputBasePartitioner::setTarget(hook);
//}


unsigned CRECFMvbPartitioner::transformBlock(offset_t endOffset, TransformCursor & cursor)
{
    LOG(MCdebugProgressDetail, unknownJob, "CRECFMvbPartitioner::transformBlock(offset_t endOffset: %" I64F "d (0x%016" I64F "x), TransformCursor & cursor)", endOffset ,endOffset);
    const byte *buffer = bufferBase();
    offset_t startOffset = cursor.inputOffset;
    offset_t inputOffset = startOffset;
    while (inputOffset - startOffset < 0x10000)
    {
        if (inputOffset == endOffset)
            break;

        ensureBuffered(isBlocked?BDW_SIZE:RDW_SIZE);
        assertex((numInBuffer != bufferOffset));
        unsigned readSize = numInBuffer - bufferOffset;
        if (readSize + inputOffset > endOffset)
            readSize = (unsigned)(endOffset - inputOffset);
        size32_t size = getTransformRecordSize(buffer+bufferOffset, readSize);

        if (!ensureBuffered(size))
        {
            if (isBlocked)
                throwError1(DFTERR_WrongRECFMvbBlockSize, size);
            else
                throwError1(DFTERR_WrongRECFMvRecordSize, size);
        }
        assertex((numInBuffer != bufferOffset));
        const byte * r;
        if (isBlocked) {
            // now we have all block so loop through records
            size32_t pos=0;
            r = buffer+bufferOffset+BDW_SIZE;
            while (pos+RDW_SIZE<size) {
                unsigned short recsize;
                recsize = getRecordSize(r, RDW_SIZE);  // For error handling
                r += RDW_SIZE;          // 4 bytes (RDW) skipped
                if (recsize<=RDW_SIZE)
                    break;
                recsize -= RDW_SIZE;    // its inclusive
                if (doInputCRC)
                    inputCRC = crc32((const char *)r, recsize, inputCRC);
                target->outputRecord(recsize, r);
                r += recsize;
                pos += recsize+RDW_SIZE;
            }
        }
        else {
            size32_t recsize = size-RDW_SIZE;
            r = buffer+bufferOffset+RDW_SIZE;
            if (doInputCRC)
                inputCRC = crc32((const char *)r, recsize, inputCRC);
            target->outputRecord(recsize, r);
        }
        inputOffset += size;
        bufferOffset += size;
    }

    cursor.inputOffset = inputOffset;
    return (size32_t)(inputOffset - startOffset);
}



//----------------------------------------------------------------------------




CCsvPartitioner::CCsvPartitioner(const FileFormat & _format) : CInputBasePartitioner(_format.maxRecordSize, _format.maxRecordSize)
{
    LOG(MCdebugProgressDetail, unknownJob, "CCsvPartitioner::CCsvPartitioner(_format :'%s', maxRecordSize:%d)", _format.getFileFormatTypeString(), _format.maxRecordSize);
    LOG(MCdebugProgressDetail, unknownJob, "        separator :'%s'", _format.separate.get());
    LOG(MCdebugProgressDetail, unknownJob, "        quote     :'%s'", _format.quote.get());
    LOG(MCdebugProgressDetail, unknownJob, "        terminator:'%s'", _format.terminate.get());
    LOG(MCdebugProgressDetail, unknownJob, "        escape    :'%s'", _format.escape.get());

    maxElementLength = 1;
    format.set(_format);
    const char * separator = format.separate.get();
    if (separator && *separator)
        addActionList(matcher, separator, SEPARATOR, &maxElementLength);

    addActionList(matcher, format.quote.get() ? format.quote.get() : "\"", QUOTE, &maxElementLength);
    addActionList(matcher, format.terminate.get() ? format.terminate.get() : "\\n,\\r\\n", TERMINATOR, &maxElementLength);
    const char * escape = format.escape.get();
    if (escape && *escape)
        addActionList(matcher,  escape, ESCAPE, &maxElementLength);

    matcher.queryAddEntry(1, " ", WHITESPACE);
    matcher.queryAddEntry(1, "\t", WHITESPACE);
    recordStructure.append("RECORD\n");
    isRecordStructurePresent = false;
    fieldCount = 0;
    isFirstRow = true;
    fields.setown(new KeptAtomTable);
}

void CCsvPartitioner::storeFieldName(const char * start, unsigned len)
{
    ++fieldCount;
    recordStructure.append("    STRING ");
    // If record structure present in the first row and we have at least one character
    // long string then it will be this field name.
    // Otherwise we use "fieldx" (where x is the number of this field) as name.
    // This prevents to generate wrong record structure if field name(s) missing:
    // e.g: first row -> fieldA,fieldB,,fieldC,\n

    // Check the field name
    StringBuffer fieldName;
    fieldName.append(start, 0, len);
    fieldName.trim();

    if (isRecordStructurePresent && (0 < fieldName.length() ))
    {
        // Check discovered field name validity
        char act = fieldName.charAt(0);
        if ( !(isalpha(act) || act == '_') )
        {
            fieldName.setCharAt(0, '_');
        }

        for ( unsigned i = 1; i < fieldName.length(); i++)
        {
            act = fieldName.charAt(i);
            if ( !(isalnum(act) || act == '_' || act == '$') )
            {
                fieldName.setCharAt(i, '_');
            }
        }
    }
    else
    {
        fieldName.set("field").append(fieldCount);
    }

    // Check discovered field name uniqueness
    const char * fn = fieldName.str();
    if ( fields->find(fn) != NULL )
    {
        time_t t;
        time(&t);
        fieldName.append('_').append(fieldCount).append('_').append((unsigned)t);
    }

    recordStructure.append(fieldName);
    recordStructure.append(";\n");

    fields->addAtom(fieldName.str());
}

size32_t CCsvPartitioner::getSplitRecordSize(const byte * start, unsigned maxToRead, bool processFullBuffer, bool ateof)
{
    //more complicated processing of quotes etc....
    unsigned quote = 0;
    unsigned quoteToStrip = 0;
    const byte * cur = start;
    const byte * end = start + maxToRead;
    const byte * firstGood = start;
    const byte * lastGood = start;
    const byte * last = start;
    bool lastEscape = false;

    while (cur != end)
    {
        unsigned matchLen;
        unsigned match = matcher.getMatch(end-cur, (const char *)cur, matchLen);
        switch (match & 255)
        {
        case NONE:
            cur++;          // matchLen == 0;
            lastGood = cur;
            break;
        case WHITESPACE:
            //Skip leading whitepace
            if (quote)
                lastGood = cur+matchLen;
            else if (cur == firstGood)
            {
                firstGood = cur+matchLen;
                lastGood = cur+matchLen;
            }
            break;
        case SEPARATOR:
            // Quoted separator
            if (quote == 0)
            {
                if (isFirstRow)
                {
                    storeFieldName((const char*)firstGood, lastGood-firstGood);
                }

                lastEscape = false;
                quoteToStrip = 0;
                firstGood = cur + matchLen;
            }
            lastGood = cur+matchLen;
            break;
        case TERMINATOR:
            if (quote == 0) // Is this a good idea? Means a mismatched quote is not fixed by EOL
            {
               if (isFirstRow)
               {
                   // TODO For further improvement we can use second
                   // row to check discovered record structure (field count).
                   isFirstRow = false;

                   // Process last field
                   storeFieldName((const char*)firstGood, lastGood-firstGood);
                   recordStructure.append("END;");
               }

               if (processFullBuffer)
               {
                   last = cur + matchLen;
                   // Reset to process a new record
                   lastEscape = false;
                   quoteToStrip = 0;
                   firstGood = cur + matchLen;
               }
               else
               {
                   clearBufferOverrun();
                   return (size32_t)(cur + matchLen - start);
               }
            }
            lastGood = cur+matchLen;
            break;
        case QUOTE:
            // Quoted quote
            if (quote == 0)
            {
                if (cur == firstGood)
                {
                    quote = match;
                    firstGood = cur+matchLen;
                }
                lastGood = cur+matchLen;
            }
            else
            {
                if (quote == match)
                {
                    const byte * next = cur + matchLen;
                    //Check for double quotes
                    if ((next != end))
                    {
                        unsigned nextMatchLen;
                        unsigned nextMatch = matcher.getMatch((size32_t)(end-next), (const char *)next, nextMatchLen);
                        if (nextMatch == quote)
                        {
                            quoteToStrip = quote;
                            matchLen += nextMatchLen;
                            lastGood = cur+matchLen;
                        }
                        else
                            quote = 0;
                    }
                    else
                        quote = 0;
                }
                else
                    lastGood = cur+matchLen;
            }
            break;
        case ESCAPE:
            lastEscape = true;
            lastGood = cur+matchLen;
            // If this escape is at the end, proceed to field range
            if (lastGood == end)
                break;

            // Skip escape and ignore the next match
            cur += matchLen;
            match = matcher.getMatch((size32_t)(end-cur), (const char *)cur, matchLen);
            if ((match & 255) == NONE)
                matchLen = 1;
            lastGood += matchLen;
            break;

        }
        cur += matchLen;
    }

    if (processFullBuffer && (last != start))
    {
        return last - start;
    }

    if (!ateof)
        throwError(DFTERR_EndOfRecordNotFound);

    numOfProcessedBytes += (unsigned)(end - start);

    LOG(MCdebugProgress, unknownJob, "CSV splitRecordSize(%d) at end of file", (unsigned) (end - start));

    if (++numOfBufferOverrun > maxNumberOfBufferOverrun)
        throwError1(DFTERR_EndOfCsvRecordNotFound, numOfProcessedBytes);

    return end - start;
}

size32_t CCsvPartitioner::getTransformRecordSize(const byte * start, unsigned maxToRead)
{
    return maxToRead;
}

void CCsvPartitioner::setTarget(IOutputProcessor * _target)
{
    Owned<IOutputProcessor> hook = new CCsvProcessorHook(_target);
    CInputBasePartitioner::setTarget(hook);
}



// A quick version of the csv partitioner that jumps to the split offset, and then searches for a terminator.

CCsvQuickPartitioner::CCsvQuickPartitioner(const FileFormat & _format, bool _noTranslation) : CCsvPartitioner(_format)
{
    LOG(MCdebugProgressDetail, unknownJob, "CCsvQuickPartitioner::CCsvQuickPartitioner(_format.type :'%s', _noTranslation:%d)", _format.getFileFormatTypeString(), _noTranslation);
    noTranslation = _noTranslation;
}

void CCsvQuickPartitioner::findSplitPoint(offset_t splitOffset, PartitionCursor & cursor)
{
    const byte *buffer = bufferBase();
    numInBuffer = bufferOffset = 0;
    if (splitOffset != 0)
    {
        seekInput(splitOffset-thisOffset+thisHeaderSize);

        bool eof;
        if (format.maxRecordSize + maxElementLength > blockSize)
            eof = !ensureBuffered(blockSize);
        else
            eof = !ensureBuffered(format.maxRecordSize + maxElementLength);
        bool fullBuffer = false;
        //Could be end of file - if no elements read.
        if (numInBuffer != bufferOffset)
        {
            //Throw away the first match, incase we hit the \n of a \r\n or something similar.
            if (maxElementLength > 1)
            {
                unsigned matchLen;
                unsigned match = matcher.getMatch(numInBuffer - bufferOffset, (const char *)buffer+bufferOffset, matchLen);
                if ((match & 255) == NONE)
                    bufferOffset++;
                else
                    bufferOffset += matchLen;
            }

            //Could have been single \n at the end of the file....
            if (numInBuffer != bufferOffset)
            {
                if (format.maxRecordSize <= blockSize)
                    bufferOffset += getSplitRecordSize(buffer+bufferOffset, numInBuffer-bufferOffset, fullBuffer, eof);
                else
                {
                    //For large
                    size32_t ensureSize = numInBuffer-bufferOffset;
                    loop
                    {
                        try
                        {
                            //There is still going to be enough buffered for a whole record.
                            eof = !ensureBuffered(ensureSize);
                            bufferOffset += getSplitRecordSize(buffer+bufferOffset, numInBuffer-bufferOffset, fullBuffer, eof);
                            break;
                        }
                        catch (IException * e)
                        {
                            if (ensureSize == format.maxRecordSize)
                                throw;
                            e->Release();
                            LOG(MCdebugProgress, unknownJob, "Failed to find split after reading %d", ensureSize);
                            ensureSize += blockSize;
                            if (ensureSize > format.maxRecordSize)
                                ensureSize = format.maxRecordSize;
                        }
                    }
                    LOG(MCdebugProgress, unknownJob, "Found split after reading %d", ensureSize);
                }
            }
        }
        else if (splitOffset - thisOffset < thisSize)
            throwError2(DFTERR_UnexpectedReadFailure, fullPath.get(), splitOffset-thisOffset+thisHeaderSize);
    }
    else
    {
        // We are in the first part of the file
        bool eof;
        if (format.maxRecordSize + maxElementLength > blockSize)
            eof = !ensureBuffered(blockSize);
        else
            eof = !ensureBuffered(format.maxRecordSize + maxElementLength);
        bool fullBuffer = false;

        // Discover record structure in the first record/row
        getSplitRecordSize(buffer, numInBuffer, fullBuffer, eof);
    }

    cursor.inputOffset = splitOffset + bufferOffset;
    if (noTranslation)
        cursor.outputOffset = cursor.inputOffset - thisOffset;
}



//----------------------------------------------------------------------------

CUtfPartitioner::CUtfPartitioner(const FileFormat & _format) : CInputBasePartitioner(_format.maxRecordSize, _format.maxRecordSize)
{
    LOG(MCdebugProgressDetail, unknownJob, "CUtfPartitioner::CUtfPartitioner(_format.type :'%s', maxRecordSize:%d)", _format.getFileFormatTypeString(), _format.maxRecordSize);
    LOG(MCdebugProgressDetail, unknownJob, "        separator :'%s'", _format.separate.get());
    LOG(MCdebugProgressDetail, unknownJob, "        quote     :'%s'", _format.quote.get());
    LOG(MCdebugProgressDetail, unknownJob, "        terminator:'%s'", _format.terminate.get());

    maxElementLength = 1;
    format.set(_format);
    utfFormat = getUtfFormatType(format.type);
    addUtfActionList(matcher, format.separate ? format.separate.get() : "\\,", SEPARATOR, &maxElementLength, utfFormat);
    addUtfActionList(matcher, format.quote ? format.quote.get() : "\"", QUOTE, &maxElementLength, utfFormat);
    addUtfActionList(matcher, format.terminate ? format.terminate.get() : "\\n,\\r\\n", TERMINATOR, &maxElementLength, utfFormat);
    addUtfActionList(matcher, " ", WHITESPACE, NULL, utfFormat);
    addUtfActionList(matcher, "\t", WHITESPACE, NULL, utfFormat);
    unitSize = format.getUnitSize();

    recordStructure.append("RECORD\n");
    isRecordStructurePresent = false;
    fieldCount = 0;
    isFirstRow = true;
    fields.setown(new KeptAtomTable);
}

void CUtfPartitioner::storeFieldName(const char * start, unsigned len)
{
    ++fieldCount;
    recordStructure.append("    UTF8 ");
    // If record structure present in the first row and we have at least one character
    // long string then it will be this field name.
    // Otherwise we use "fieldx" (where x is the number of this field) as name.
    // This prevents to generate wrong record structure if field name(s) missing:
    // e.g: first row -> fieldA,fieldB,,fieldC,\n

    // Check the field name
    StringBuffer fieldName;
    MemoryBuffer temp;
    if (convertUtf(temp, UtfReader::Utf8, len, start, utfFormat))
    {
        fieldName.append(temp.length(), temp.toByteArray());
        fieldName.trim();
    }

    if (isRecordStructurePresent && (0 < fieldName.length() ))
    {
        // Check discovered field name validity
        char act = fieldName.charAt(0);
        if ( !(isalpha(act) || act == '_') )
        {
            fieldName.setCharAt(0, '_');
        }

        for ( unsigned i = 1; i < fieldName.length(); i++)
        {
            act = fieldName.charAt(i);
            if ( !(isalnum(act) || act == '_' || act == '$') )
            {
                fieldName.setCharAt(i, '_');
            }
        }
    }
    else
    {
        fieldName.append("field").append(fieldCount);
    }

    // Check discovered field name uniqueness
    const char * fn = fieldName.str();
    if ( fields->find(fn) != NULL )
    {
        time_t t;
        time(&t);
        fieldName.append('_').append(fieldCount).append('_').append((unsigned)t);
    }

    recordStructure.append(fieldName);
    recordStructure.append(";\n");

    fields->addAtom(fieldName.str());
}

size32_t CUtfPartitioner::getSplitRecordSize(const byte * start, unsigned maxToRead, bool processFullBuffer, bool ateof)
{
    //If we need more complicated processing...

    unsigned quote = 0;
    unsigned quoteToStrip = 0;
    const byte * cur = start;
    const byte * end = start + maxToRead;
    const byte * firstGood = start;
    const byte * lastGood = start;
    const byte * last = start;
    bool lastEscape = false;

    while (cur != end)
    {
        unsigned matchLen;
        unsigned match = matcher.getMatch(end-cur, (const char *)cur, matchLen);
        switch (match & 255)
        {
        case NONE:
            cur += unitSize;            // matchLen == 0;
            lastGood = cur;
            break;
        case WHITESPACE:
            //Skip leading whitepace
            if (quote)
                lastGood = cur+matchLen;
            else if (cur == firstGood)
            {
                firstGood = cur+matchLen;
                lastGood = cur+matchLen;
            }
            break;
        case SEPARATOR:
            // Quoted separator
            if (quote == 0)
            {
                if (isFirstRow)
                {
                    storeFieldName((const char*)firstGood, lastGood-firstGood);
                }

                lastEscape = false;
                quoteToStrip = 0;
                firstGood = cur + matchLen;
            }
            lastGood = cur+matchLen;
            break;
        case TERMINATOR:
            if (quote == 0) // Is this a good idea? Means a mismatched quote is not fixed by EOL
            {
               if (isFirstRow)
               {
                   // TODO For further improvement we can use second
                   // row to check discovered record structure (field count).
                   isFirstRow = false;

                   // Process last field
                   storeFieldName((const char*)firstGood, lastGood-firstGood);
                   recordStructure.append("END;");
               }

               if (processFullBuffer)
               {
                   last = cur + matchLen;
                   // Reset to process a new record
                   lastEscape = false;
                   quoteToStrip = 0;
                   firstGood = cur + matchLen;
               }
               else
               {
                   clearBufferOverrun();
                   return (size32_t)(cur + matchLen - start);
               }
            }
            lastGood = cur+matchLen;
            break;
        case QUOTE:
            // Quoted quote
            if (quote == 0)
            {
                if (cur == firstGood)
                {
                    quote = match;
                    firstGood = cur+matchLen;
                }
                lastGood = cur+matchLen;
            }
            else
            {
                if (quote == match)
                {
                    const byte * next = cur + matchLen;
                    //Check for double quotes
                    if ((next != end))
                    {
                        unsigned nextMatchLen;
                        unsigned nextMatch = matcher.getMatch((size32_t)(end-next), (const char *)next, nextMatchLen);
                        if (nextMatch == quote)
                        {
                            quoteToStrip = quote;
                            matchLen += nextMatchLen;
                            lastGood = cur+matchLen;
                        }
                        else
                            quote = 0;
                    }
                    else
                        quote = 0;
                }
                else
                    lastGood = cur+matchLen;
            }
            break;
        case ESCAPE:
            lastEscape = true;
            lastGood = cur+matchLen;
            // If this escape is at the end, proceed to field range
            if (lastGood == end)
                break;

            // Skip escape and ignore the next match
            cur += matchLen;
            match = matcher.getMatch((size32_t)(end-cur), (const char *)cur, matchLen);
            if ((match & 255) == NONE)
                matchLen = unitSize;
            lastGood += matchLen;
            break;
        }
        cur += matchLen;
    }

    if (processFullBuffer && (last != start))
    {
        return last - start;
    }


    if (!ateof)
        throwError(DFTERR_EndOfRecordNotFound);

    numOfProcessedBytes += (unsigned)(end - start);

    LOG(MCdebugProgress, unknownJob, "UTF splitRecordSize(%d) at end of file", (unsigned) (end - start));

    if (++numOfBufferOverrun > maxNumberOfBufferOverrun)
        throwError1(DFTERR_EndOfUtfRecordNotFound, numOfProcessedBytes);

    return end - start;
}


void CUtfPartitioner::setTarget(IOutputProcessor * _target)
{
    Owned<IOutputProcessor> hook = new CUtfProcessorHook(format.type, _target);
    CInputBasePartitioner::setTarget(hook);
}



// A quick version of the Utf partitioner that jumps to the split offset, and then searches for a terminator.

CUtfQuickPartitioner::CUtfQuickPartitioner(const FileFormat & _format, bool _noTranslation) : CUtfPartitioner(_format)
{
    LOG(MCdebugProgressDetail, unknownJob, "CUtfQuickPartitioner::CUtfQuickPartitioner(_format.type :'%s', _noTranslation:%d)", _format.getFileFormatTypeString(), _noTranslation);
    noTranslation = _noTranslation;
}

void CUtfQuickPartitioner::findSplitPoint(offset_t splitOffset, PartitionCursor & cursor)
{
    const byte *buffer = bufferBase();
    numInBuffer = bufferOffset = 0;
    if (splitOffset != 0)
    {
        unsigned delta = (unsigned)(splitOffset & (unitSize-1));
        if (delta)
            splitOffset += (unitSize - delta);
        seekInput(splitOffset-thisOffset + thisHeaderSize);
        bool eof;
        if (format.maxRecordSize + maxElementLength > blockSize)
            eof = !ensureBuffered(blockSize);
        else
            eof = !ensureBuffered(format.maxRecordSize + maxElementLength);
        bool fullBuffer = false;
        //Could be end of file - if no elements read.
        if (numInBuffer != bufferOffset)
        {
            //Throw away the first match, incase we hit the \n of a \r\n or something similar.
            if (maxElementLength > unitSize)
            {
                unsigned matchLen;
                unsigned match = matcher.getMatch(numInBuffer - bufferOffset, (const char *)buffer+bufferOffset, matchLen);
                if ((match & 255) == NONE)
                    bufferOffset += unitSize;
                else
                    bufferOffset += matchLen;
            }

            //Could have been single \n at the end of the file....
            if (numInBuffer != bufferOffset)
            {
                if (format.maxRecordSize <= blockSize)
                    bufferOffset += getSplitRecordSize(buffer+bufferOffset, numInBuffer-bufferOffset, fullBuffer, eof);
                else
                {
                    //For large 
                    size32_t ensureSize = numInBuffer-bufferOffset;
                    loop
                    {
                        try
                        {
                            //There is still going to be enough buffered for a whole record.
                            eof = !ensureBuffered(ensureSize);
                            bufferOffset += getSplitRecordSize(buffer+bufferOffset, numInBuffer-bufferOffset, fullBuffer, eof);
                            break;
                        }
                        catch (IException * e)
                        {
                            if (ensureSize == format.maxRecordSize)
                                throw;
                            e->Release();
                            LOG(MCdebugProgress, unknownJob, "Failed to find split after reading %d", ensureSize);
                            ensureSize += blockSize;
                            if (ensureSize > format.maxRecordSize)
                                ensureSize = format.maxRecordSize;
                        }
                    }
                    LOG(MCdebugProgress, unknownJob, "Found split after reading %d", ensureSize);
                }
            }
        }
        else if (splitOffset - thisOffset < thisSize)
            throwError2(DFTERR_UnexpectedReadFailure, fullPath.get(), splitOffset-thisOffset+thisHeaderSize);
    }
    else
    {
        // We are in the first part of the file
        bool eof;
        if (format.maxRecordSize + maxElementLength > blockSize)
            eof = !ensureBuffered(blockSize);
        else
            eof = !ensureBuffered(format.maxRecordSize + maxElementLength);
        bool fullBuffer = false;

        // Discover record structure in the first record/row
        getSplitRecordSize(buffer, numInBuffer, fullBuffer, eof);
    }
    cursor.inputOffset = splitOffset + bufferOffset;
    if (noTranslation)
        cursor.outputOffset = cursor.inputOffset - thisOffset;
}

size32_t CUtfPartitioner::getTransformRecordSize(const byte * start, unsigned maxToRead)
{
    //Need to be careful that multi-byte characters aren't split.
    UtfReader reader(utfFormat, false);
    reader.set(maxToRead, start);
    return reader.getLegalLength();
}

//----------------------------------------------------------------------------

BufferedDirectReader::BufferedDirectReader(size32_t _blockSize, size32_t _bufferSize)
{
    blockSize = _blockSize ? _blockSize : 0x4000;
    bufferSize = _bufferSize ? _bufferSize : blockSize * 2;
    buffer = (byte *)malloc(bufferSize);
    numInBuffer = 0;
    bufferOffset = 0;
}

BufferedDirectReader::~BufferedDirectReader()
{
    free(buffer);
}

size32_t BufferedDirectReader::ensure(size32_t required)
{
    if (bufferOffset + required > numInBuffer)
    {
        if (numInBuffer + blockSize > bufferSize)
        {
            memmove(buffer, buffer+bufferOffset, numInBuffer-bufferOffset);
            numInBuffer -= bufferOffset;
            bufferOffset = 0;
        }

        numInBuffer += stream->read(blockSize, buffer+numInBuffer);
    }
    return (numInBuffer - bufferOffset);
}


XmlSplitter::XmlSplitter(const FileFormat & format)
{
    LOG(MCdebugProgressDetail, unknownJob, "XmlSplitter::XmlSplitter(_format.type :'%s', format.rowTag:'%s')", format.getFileFormatTypeString(), format.rowTag.get());
    maxElementLength = 1;
    utfFormat = getUtfFormatType(format.type);
    StringBuffer openTag, closeTag, endTag, endCloseTag;
    openTag.append("<").append(format.rowTag);
    closeTag.append("</").append(format.rowTag).append(">");
    endTag.append(">");
    endCloseTag.append("/>");

    addUtfActionList(matcher, openTag, OPENTAG, &maxElementLength, utfFormat);
    addUtfActionList(matcher, closeTag, CLOSETAG, &maxElementLength, utfFormat);
    addUtfActionList(matcher, endTag, ENDTAG, &maxElementLength, utfFormat);
    addUtfActionList(matcher, endCloseTag, ENDCLOSETAG, &maxElementLength, utfFormat);
    addUtfActionList(matcher, "\r\n,\n", NEWLINE, &maxElementLength, utfFormat);
    addUtfActionList(matcher, " ,\t", WHITESPACE, &maxElementLength, utfFormat);
    unitSize = format.getUnitSize();
}

size32_t XmlSplitter::getRecordSize(const byte * start, unsigned maxToRead, bool throwIfMissing)
{
    //If we need more complicated processing...
    const byte * cur = start;
    const byte * end = start + maxToRead;
    bool inTag = false;

    while (cur != end)
    {
        unsigned matchLen;
        unsigned match = matcher.getMatch(end-cur, (const char *)cur, matchLen);
        switch (match & 255)
        {
        case NONE:
            cur += unitSize;            // matchLen == 0;
            break;
        case OPENTAG:
            {
                //Need to check we haven't only matched a substring of the name.
                //legal formats for a tag are
                // '<' Name (S Attribute)* S? '/>'
                // '<' Name (S Attribute)* S? '>'
                // So check if next character is whitespace or end of tag.  It won't catch illegal xml, but tough.
                unsigned nextMatchLen;
                const byte * next = cur + matchLen;
                switch (matcher.getMatch(end-next, (const char *)next, nextMatchLen) & 255)
                {
                case WHITESPACE:
                case NEWLINE:
                case ENDTAG:
                case ENDCLOSETAG:
                    //Only start 
                    inTag = true;
                    break;
                }
            }
            break;
        case CLOSETAG:
            cur += matchLen;
            if ((matcher.getMatch(end-cur, (const char *)cur, matchLen) & 255) == NEWLINE)
                cur += matchLen;
            return cur - start;
        case ENDTAG:
            inTag = false;
            break;
        case ENDCLOSETAG:
            if (inTag)
            {
                cur += matchLen;
                if ((matcher.getMatch(end-cur, (const char *)cur, matchLen) & 255) == NEWLINE)
                    cur += matchLen;
                return cur - start;
            }
            break;
        }
        cur += matchLen;
    }

    if (throwIfMissing)
        throwError(DFTERR_EndOfRecordNotFound);
    return end - start;
}


size32_t XmlSplitter::getEndOfRecord(const byte * start, unsigned maxToRead)
{
    //Could be end of file - if no elements read.
    size32_t skipLen = 0;
    if (maxToRead)
    {
        //Throw away the first match, incase we hit the \n of a \r\n or something similar.
        if (maxElementLength > unitSize)
        {
            unsigned matchLen;
            unsigned match = matcher.getMatch(maxToRead, (const char *)start, matchLen);
            if ((match & 255) != NONE)
                skipLen = matchLen;
        }

        //Could have been single \n at the end of the file....
        if (maxToRead != skipLen)
        {
            //There is still going to be enough buffered for a whole record.
            return getRecordSize(start+skipLen, maxToRead - skipLen, false) + skipLen;
        }
    }
    return skipLen;
}


offset_t XmlSplitter::getHeaderLength(BufferedDirectReader & reader)
{
    offset_t startOfHeader = reader.tell();
    offset_t startOfLine = startOfHeader;

    loop
    {
        if (reader.ensure(maxElementLength) == 0)
            throwError(DFTERR_CannotFindFirstXmlRecord);

        bool resetStartOfLine = true;
        unsigned matchLen;
        unsigned match = matcher.getMatch(reader.available(), (const char *)reader.next(), matchLen);
        switch (match & 255)
        {
        case NONE:
            matchLen = unitSize;
            break;
        case OPENTAG:
            {
                unsigned nextMatchLen;
                reader.ensure(matchLen + maxElementLength);
                switch (matcher.getMatch(reader.available()-matchLen, (const char *)(reader.next() + matchLen), nextMatchLen) & 255)
                {
                case WHITESPACE:
                case NEWLINE:
                case ENDTAG:
                case ENDCLOSETAG:
                    if (startOfLine != -1)
                        return startOfLine - startOfHeader;
                    return reader.tell() - startOfHeader;
                }
            }
            break;
        case NEWLINE:
            startOfLine = reader.tell()+matchLen;
            resetStartOfLine = false;
            break;
        case WHITESPACE:
            resetStartOfLine = false;
            break;
        }
        if (resetStartOfLine)
            startOfLine = (offset_t)-1;
        reader.skip(matchLen);
    }
}


offset_t XmlSplitter::getFooterLength(BufferedDirectReader & reader, offset_t size)
{
    offset_t xmlFooterOffset = (offset_t)-1;
    bool inTag = false;

    loop
    {
        if (reader.ensure(maxElementLength) == 0)
        {
            if (xmlFooterOffset == -1)
                throw MakeStringException(DFTERR_CannotFindLastXmlRecord, "Could not find the end of the first record");
            break;
        }

        unsigned matchLen;
        unsigned match = matcher.getMatch(reader.available(), (const char *)reader.next(), matchLen);
        bool matched = false;
        switch (match & 255)
        {
        case NONE:
            matchLen = unitSize;
            break;
        case OPENTAG:
            {
                unsigned nextMatchLen;
                reader.ensure(matchLen + maxElementLength);
                switch (matcher.getMatch(reader.available()-matchLen, (const char *)(reader.next() + matchLen), nextMatchLen) & 255)
                {
                case WHITESPACE:
                case NEWLINE:
                case ENDTAG:
                case ENDCLOSETAG:
                    //Only start 
                    inTag = true;
                    break;
                }
            }
            break;
        case CLOSETAG:
            matched = true;
            break;
        case ENDTAG:
            inTag = false;
            break;
        case ENDCLOSETAG:
            matched = inTag;
            inTag = false;
            break;
        }

        reader.skip(matchLen);
        if (matched)
        {
            xmlFooterOffset = reader.tell();
            reader.ensure(maxElementLength);
            if (matcher.getMatch(reader.available(), (const char *)reader.next(), matchLen) == NEWLINE)
                xmlFooterOffset += matchLen;
        }
    }

    return size - xmlFooterOffset;
}


CJsonInputPartitioner::CJsonInputPartitioner(const FileFormat & _format)
{
    LOG(MCdebugProgressDetail, unknownJob, "CJsonInputPartitioner::CJsonInputPartitioner(format.type :'%s', unitSize:%d)", _format.getFileFormatTypeString(), _format.getUnitSize());

    format.set(_format);
    CriticalBlock block(openfilecachesect);
    if (!openfilecache)
        openfilecache = createFileIOCache(16);
    else
        openfilecache->Link();
    partSeparator.set(",\n");
}

IFileIOCache *CJsonInputPartitioner::openfilecache = NULL;
CriticalSection CJsonInputPartitioner::openfilecachesect;

CJsonInputPartitioner::~CJsonInputPartitioner()
{
    json.clear();
    inStream.clear();
    if (openfilecache) {
        CriticalBlock block(openfilecachesect);
        if (openfilecache->Release())
            openfilecache = NULL;
    }
}

void CJsonInputPartitioner::setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool _compressedInput, const char *_decryptKey)
{
    CPartitioner::setSource(_whichInput, _fullPath, _compressedInput,_decryptKey);
    Owned<IFileIO> inIO;
    Owned<IFile> inFile = createIFile(inputName);
    if (!inFile->exists()) {
        StringBuffer tmp;
        inputName.getRemotePath(tmp);
        throwError1(DFTERR_CouldNotOpenFilePart, tmp.str());
    }
    inIO.setown(openfilecache->addFile(inputName,IFOread));

    if (_compressedInput) {
        Owned<IExpander> expander;
        if (_decryptKey&&*_decryptKey) {
            StringBuffer key;
            decrypt(key,_decryptKey);
            expander.setown(createAESExpander256(key.length(),key.str()));
        }
        inIO.setown(createCompressedFileReader(inIO,expander));
    }

    inStream.setown(createIOStream(inIO));
    json.setown(new JsonSplitter(format, *inStream));
    json->getHeaderLength();
}

CJsonPartitioner::CJsonPartitioner(const FileFormat & _format) : CJsonInputPartitioner(_format)
{
    unitSize = format.getUnitSize();
    utfFormat = getUtfFormatType(format.type);
}


CXmlPartitioner::CXmlPartitioner(const FileFormat & _format) : CInputBasePartitioner(_format.maxRecordSize, _format.maxRecordSize), splitter(_format)
{
    LOG(MCdebugProgressDetail, unknownJob, "CXmlPartitioner::CXmlPartitioner(_format.type :'%s', unitSize:%d)", _format.getFileFormatTypeString(), format.getUnitSize());
    format.set(_format);
    unitSize = format.getUnitSize();
    utfFormat = getUtfFormatType(format.type);
}

size32_t CXmlPartitioner::getSplitRecordSize(const byte * start, unsigned maxToRead, bool processFullBuffer)
{
    return splitter.getRecordSize(start, maxToRead, true);
}

size32_t CXmlPartitioner::getTransformRecordSize(const byte * start, unsigned maxToRead)
{
    //Need to be careful that multi-byte characters aren't split.
    UtfReader reader(utfFormat, false);
    reader.set(maxToRead, start);
    return reader.getLegalLength();
}


void CXmlPartitioner::setTarget(IOutputProcessor * _target)
{
    Owned<IOutputProcessor> hook = new CUtfProcessorHook(format.type, _target);
    CInputBasePartitioner::setTarget(hook);
}



// A quick version of the Utf partitioner that jumps to the split offset, and then searches for a terminator.

CXmlQuickPartitioner::CXmlQuickPartitioner(const FileFormat & _format, bool _noTranslation) : CXmlPartitioner(_format)
{
    LOG(MCdebugProgressDetail, unknownJob, "CXmlQuickPartitioner::CXmlQuickPartitioner(_format.type :'%s', _noTranslation:%d)", _format.getFileFormatTypeString(), _noTranslation);
    noTranslation = _noTranslation;
}

void CXmlQuickPartitioner::findSplitPoint(offset_t splitOffset, PartitionCursor & cursor)
{
    const byte *buffer = bufferBase();
    numInBuffer = bufferOffset = 0;
    if (splitOffset != 0)
    {
        LOG(MCdebugProgressDetail, unknownJob, "CXmlQuickPartitioner::findSplitPoint(splitOffset:%" I64F "d)", splitOffset);
        unsigned delta = (unsigned)(splitOffset & (unitSize-1));
        if (delta)
            splitOffset += (unitSize - delta);
        seekInput(splitOffset-thisOffset + thisHeaderSize);

        if (format.maxRecordSize + splitter.getMaxElementLength() > blockSize)
            ensureBuffered(blockSize);
        else
            ensureBuffered(format.maxRecordSize + splitter.getMaxElementLength());

        if (numInBuffer != bufferOffset)
        {
            size32_t ensureSize = numInBuffer-bufferOffset;
            loop
            {
                size32_t sizeAvailable = numInBuffer - bufferOffset;
                size32_t sizeRecord = splitter.getEndOfRecord(buffer+bufferOffset, sizeAvailable);
                //Stop if found a genuine end, or read past end of file, or read more than enough and not found eof.
                if ((sizeRecord != sizeAvailable) || (splitOffset + sizeRecord >= thisOffset + thisSize))
                {
                    bufferOffset += sizeRecord;
                    break;
                }
                if (sizeAvailable >= format.maxRecordSize)
                {
                    LOG(MCdebugProgressDetail, unknownJob, "CXmlQuickPartitioner::findSplitPoint: record size (>%d bytes) is larger than expected maxRecordSize (%d bytes) [and blockSize (%d bytes)]", sizeRecord, format.maxRecordSize, blockSize);
                    throwError3(DFTERR_EndOfXmlRecordNotFound, splitOffset+bufferOffset, sizeRecord, format.maxRecordSize);
                }
                LOG(MCdebugProgress, unknownJob, "Failed to find split after reading %d", ensureSize);
                ensureSize += blockSize;
                if (ensureSize > format.maxRecordSize)
                    ensureSize = format.maxRecordSize;
                ensureBuffered(ensureSize);
            }
            LOG(MCdebugProgress, unknownJob, "Found split after reading %d", ensureSize);
        }
        else if (splitOffset - thisOffset < thisSize)
            throwError2(DFTERR_UnexpectedReadFailure, fullPath.get(), splitOffset-thisOffset+thisHeaderSize);
    }
    cursor.inputOffset = splitOffset + bufferOffset;
    if (noTranslation)
        cursor.outputOffset = cursor.inputOffset - thisOffset;
}

//----------------------------------------------------------------------------

CRemotePartitioner::CRemotePartitioner(const SocketEndpoint & _ep, const FileFormat & _srcFormat, const FileFormat & _tgtFormat, const char * _slave, const char *_wuid)
    : wuid(_wuid)
{
    LOG(MCdebugProgressDetail, unknownJob, "CRemotePartitioner::CRemotePartitioner(_srcFormat.type :'%s', _tgtFormat.type:'%s', _slave:'%s', _wuid:'%s')", _srcFormat.getFileFormatTypeString(), _tgtFormat.getFileFormatTypeString(), _slave, _wuid);
    ep.set(_ep);
    srcFormat.set(_srcFormat);
    tgtFormat.set(_tgtFormat);
    slave.set(_slave);
}


void CRemotePartitioner::calcPartitions(Semaphore * _sem)
{
    sem = _sem;

#ifdef RUN_SLAVES_ON_THREADS
    start();
#else
    run();
#endif
}


void CRemotePartitioner::callRemote()
{
    bool ok = false;
    try
    {
        StringBuffer url, tmp;
        ep.getUrlStr(url);

        Owned<ISocket> socket = spawnRemoteChild(SPAWNdfu, slave, ep, DAFT_VERSION, queryFtSlaveLogDir(), NULL, wuid);
        if (socket)
        {
            LogMsgJobInfo job(unknownJob);
            MemoryBuffer msg;
            msg.setEndian(__BIG_ENDIAN);

            LOG(MCdebugProgressDetail, job, "Remote partition part %s[%d]", url.str(), whichInput);

            //Send message and wait for response...
            //MORE: they should probably all be sent on different threads....
            msg.append((byte)FTactionpartition);
            passwordProvider.serialize(msg);
            srcFormat.serialize(msg);
            tgtFormat.serialize(msg);
            msg.append(whichInput);
            fullPath.serialize(msg);
            msg.append(totalSize);
            msg.append(thisOffset);
            msg.append(thisSize);
            msg.append(thisHeaderSize);
            msg.append(numParts);
            msg.append(compressedInput);
            unsigned compatflags = 0;                   // compatibility flags (not yet used)
            msg.append(compatflags);
            msg.append(decryptKey);
            //Add extra data at the end to provide backward compatibility
            srcFormat.serializeExtra(msg, 1);
            tgtFormat.serializeExtra(msg, 1);

            if (!catchWriteBuffer(socket, msg))
                throwError1(RFSERR_TimeoutWaitConnect, url.str());

            msg.clear();
            if (!catchReadBuffer(socket, msg, FTTIME_PARTITION))
                throwError1(RFSERR_TimeoutWaitSlave, url.str());

            bool done;
            msg.setEndian(__BIG_ENDIAN);
            msg.read(done);
            msg.read(ok);
            error.setown(deserializeException(msg));
            if (ok)
                deserialize(results, msg);

            msg.clear().append(true);
            catchWriteBuffer(socket, msg);

            LOG(MCdebugProgressDetail, job, "Remote partition calculated %s[%d] ok(%d)", url.str(), whichInput, ok);
        }
        else
        {
            throwError1(DFTERR_FailedStartSlave, url.str());
        }
    }
    catch (IException * e)
    {
        EXCLOG(e, "Calculating partition");
        error.setown(e);
    }

    if (sem)
        sem->signal();
}


void CRemotePartitioner::getResults(PartitionPointArray & partition)
{
    if (error)
        throw error.getLink();

    ForEachItemIn(idx, results)
    {
        PartitionPoint & cur = results.item(idx);
        partition.append(OLINK(cur));
    }
}

int CRemotePartitioner::run()
{
    callRemote();
    return 0;
}

void CRemotePartitioner::setPartitionRange(offset_t _totalSize, offset_t _thisOffset, offset_t _thisSize, unsigned _thisHeaderSize, unsigned _numParts)
{
    totalSize = _totalSize;
    thisOffset = _thisOffset;
    thisSize = _thisSize;
    thisHeaderSize = _thisHeaderSize;
    numParts = _numParts;
}

void CRemotePartitioner::setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool _compressedInput, const char *_decryptKey)
{
    whichInput = _whichInput;
    fullPath.set(_fullPath);
    passwordProvider.addPasswordForFilename(fullPath);
    compressedInput = _compressedInput;
    decryptKey.set(_decryptKey);
}

void CRemotePartitioner::setRecordStructurePresent(bool _recordStructurePresent)
{

}

void CRemotePartitioner::getRecordStructure(StringBuffer & _recordStructure)
{
    _recordStructure.clear();
}


//== Output Processors ======================================================

COutputProcessor::COutputProcessor()
{
    outputOffset = 0;
}

offset_t COutputProcessor::getOutputOffset()
{
    return outputOffset;
}

void COutputProcessor::setOutput(offset_t startOffset, IFileIOStream * _out)
{
    outputOffset = startOffset;
    out.set(_out);
    if (out)
        out->seek(startOffset, IFSbegin);
}

void CFixedOutputProcessor::outputRecord(size32_t len, const byte * data)
{
    outputOffset += recordSize;
    if (len >= recordSize)
        out->write(recordSize, data);
    else
    {
        out->write(len, data);
        size32_t fillLen = recordSize - len;
        void * pad = alloca(fillLen);
        memset(pad, 0, fillLen);
        out->write(fillLen, pad);
    }
}

void CFixedOutputProcessor::updateOutputOffset(size32_t len, const byte * data)
{
    outputOffset += recordSize;
}


//----------------------------------------------------------------------------

CBlockedOutputProcessor::CBlockedOutputProcessor()
{
    outputExtra = 0;
    buffer = NULL;
}

CBlockedOutputProcessor::~CBlockedOutputProcessor()
{
    delete [] buffer;
}

void CBlockedOutputProcessor::finishOutputRecords()
{
    if (outputExtra)
        writeNextBlock();
}

void CBlockedOutputProcessor::finishOutputOffset()
{
    if (outputExtra)
    {
        outputOffset += EFX_BLOCK_SIZE;
        outputExtra = 0;
    }
}

void CBlockedOutputProcessor::setOutput(offset_t startOffset, IFileIOStream * _out)
{
    COutputProcessor::setOutput(startOffset, _out);
    outputExtra = 0;
    if (out && !buffer)
        buffer = new byte[EFX_BLOCK_SIZE];
}

void CBlockedOutputProcessor::outputRecord(size32_t len, const byte * data)
{
    unsigned varLen = len + sizeof(VARIABLE_LENGTH_TYPE);
    if (outputExtra + varLen + sizeof(EFX_BLOCK_HEADER_TYPE) > EFX_BLOCK_SIZE)
        writeNextBlock();

    VARIABLE_LENGTH_TYPE nextLength = len;
    _WINCPYREV(buffer + EFX_BLOCK_HEADER_SIZE + outputExtra, &nextLength, sizeof(nextLength));
    memcpy(buffer + EFX_BLOCK_HEADER_SIZE + outputExtra + sizeof(nextLength), data, len);

    outputExtra += varLen;
}

void CBlockedOutputProcessor::updateOutputOffset(size32_t len, const byte * data)
{
    unsigned varLen = len + sizeof(VARIABLE_LENGTH_TYPE);
    if (outputExtra + varLen + sizeof(EFX_BLOCK_HEADER_TYPE) > EFX_BLOCK_SIZE)
    {
        outputOffset += EFX_BLOCK_SIZE;
        outputExtra = 0;
    }

    outputExtra += varLen;
}

void CBlockedOutputProcessor::writeNextBlock()
{
    EFX_BLOCK_HEADER_TYPE blockLength = outputExtra;
    _WINCPYREV(buffer, &blockLength, EFX_BLOCK_HEADER_SIZE);
    unsigned fillLength = EFX_BLOCK_SIZE - outputExtra - EFX_BLOCK_HEADER_SIZE;
    memset(buffer + EFX_BLOCK_HEADER_SIZE + outputExtra, 0, fillLength);
    out->write(EFX_BLOCK_SIZE, buffer);
    outputOffset += EFX_BLOCK_SIZE;
    outputExtra = 0;
}


//----------------------------------------------------------------------------

void CVariableOutputProcessor::outputRecord(size32_t len, const byte * data)
{
    outputOffset += sizeof(VARIABLE_LENGTH_TYPE) + len;
    VARIABLE_LENGTH_TYPE outLength;
    if (bigendian)
        _WINCPYREV(&outLength, &len, sizeof(VARIABLE_LENGTH_TYPE));
    else
        memcpy(&outLength, &len, sizeof(VARIABLE_LENGTH_TYPE));
    out->write(sizeof(outLength), &outLength);
    out->write(len, data);
}

void CVariableOutputProcessor::updateOutputOffset(size32_t len, const byte * data)
{
    outputOffset += sizeof(VARIABLE_LENGTH_TYPE) + len;
}

//----------------------------------------------------------------------------

void CSimpleOutputProcessor::outputRecord(size32_t len, const byte * data)
{
    outputOffset += len;
    out->write(len, data);
}

void CSimpleOutputProcessor::updateOutputOffset(size32_t len, const byte * data)
{
    outputOffset += len;
}


//----------------------------------------------------------------------------

void CUtfOutputProcessor::outputRecord(size32_t len, const byte * data)
{
    convertUtf(transformed.clear(), utfFormat, len, data, UtfReader::Utf32le);
    unsigned outLen = transformed.length();
    outputOffset += outLen;
    out->write(outLen, transformed.toByteArray());
}

void CUtfOutputProcessor::updateOutputOffset(size32_t len, const byte * data)
{
    convertUtf(transformed.clear(), utfFormat, len, data, UtfReader::Utf32le);
    outputOffset += transformed.length();
}


//----------------------------------------------------------------------------

CRecordSizeOutputProcessor::CRecordSizeOutputProcessor(IRecordSize * _recordSize)
{
    recordSize = _recordSize;
}

void CRecordSizeOutputProcessor::outputRecord(size32_t len, const byte * data)
{
    unsigned mylength = recordSize->getRecordSize(data);
    assertex(len == mylength);
    outputOffset += mylength;
    out->write(len, data);
}

void CRecordSizeOutputProcessor::updateOutputOffset(size32_t len, const byte * data)
{
    unsigned mylength = recordSize->getRecordSize(data);
    assertex(len == mylength);
    outputOffset += mylength;
}


//----------------------------------------------------------------------------

offset_t COutputProcessorHook::getOutputOffset()
{
    return target->getOutputOffset();
}

void COutputProcessorHook::setOutput(offset_t startOffset, IFileIOStream * _out)
{
    target->setOutput(startOffset, _out);
}

void COutputProcessorHook::finishOutputOffset() 
{ 
    target->finishOutputOffset(); 
}

void COutputProcessorHook::finishOutputRecords()
{
    target->finishOutputRecords();
}

void CVariableProcessorHook::outputRecord(size32_t len, const byte * data)
{
    target->outputRecord(len-sizeof(VARIABLE_LENGTH_TYPE), data+sizeof(VARIABLE_LENGTH_TYPE)); 
}

void CVariableProcessorHook::updateOutputOffset(size32_t len, const byte * data) 
{ 
    target->updateOutputOffset(len-sizeof(VARIABLE_LENGTH_TYPE), data+sizeof(VARIABLE_LENGTH_TYPE)); 
}

void CCsvProcessorHook::outputRecord(size32_t len, const byte * data)
{
    UNIMPLEMENTED;
    //target->outputRecord(len-sizeof(terminator-that-matched), data); 
}

void CCsvProcessorHook::updateOutputOffset(size32_t len, const byte * data) 
{ 
    UNIMPLEMENTED;
    //target->updateOutputOffset(len-sizeof(terminator-that-macthed), data); 
}

void CUtfProcessorHook::outputRecord(size32_t len, const byte * data)
{
    convertUtf(transformed.clear(), UtfReader::Utf32le, len, data, utfFormat);
    target->outputRecord(transformed.length(), (const byte *)transformed.toByteArray());
}

void CUtfProcessorHook::updateOutputOffset(size32_t len, const byte * data) 
{ 
    convertUtf(transformed.clear(), UtfReader::Utf32le, len, data, utfFormat);
    target->updateOutputOffset(transformed.length(), (const byte *)transformed.toByteArray());
}

void CBlockedProcessorHook::outputRecord(size32_t len, const byte * data)
{
    assertex(len == EFX_BLOCK_SIZE);
    EFX_BLOCK_HEADER_TYPE blockLength;
    _WINCPYREV(&blockLength, data, sizeof(blockLength));
    data += sizeof(blockLength);
    while (blockLength != 0)
    {
        VARIABLE_LENGTH_TYPE nextLength;
        _WINCPYREV(&nextLength, data, sizeof(nextLength));
        target->outputRecord(nextLength, data+sizeof(nextLength));
        unsigned recLength = nextLength + sizeof(nextLength);
        data += recLength;
        blockLength -= recLength;
    }
}

void CBlockedProcessorHook::updateOutputOffset(size32_t len, const byte * data)
{
    assertex(len == EFX_BLOCK_SIZE);
    EFX_BLOCK_HEADER_TYPE blockLength;
    _WINCPYREV(&blockLength, data, sizeof(blockLength));
    data += sizeof(blockLength);
    while (blockLength != 0)
    {
        VARIABLE_LENGTH_TYPE nextLength;
        _WINCPYREV(&nextLength, data, sizeof(nextLength));
        target->updateOutputOffset(nextLength, data+sizeof(nextLength));
        unsigned recLength = nextLength + sizeof(nextLength);
        data += recLength;
        blockLength -= recLength;
    }
}

//----------------------------------------------------------------------------
IFormatProcessor * createFormatProcessor(const FileFormat & srcFormat, const FileFormat & tgtFormat, bool calcOutput)
{
    IFormatProcessor * partitioner;
    bool sameFormats = srcFormat.equals(tgtFormat);
    LOG(MCdebugProgressDetail, unknownJob, "createFormatProcessor(srcFormat:'%s', tgtFormat:'%s', calcOutput:%d, sameFormats:%d)", srcFormat.getFileFormatTypeString(), tgtFormat.getFileFormatTypeString(), calcOutput, sameFormats);
    switch (srcFormat.type)
    {
    case FFTfixed:
        if (calcOutput && !sameFormats)
            partitioner = new CFixedPartitioner(srcFormat.recordSize);
        else
            partitioner = new CSimpleFixedPartitioner(srcFormat.recordSize, sameFormats);
        break;
    case FFTblocked:
        if (calcOutput && !sameFormats)
            partitioner = new CBlockedPartitioner();
        else
            partitioner = new CSimpleBlockedPartitioner(sameFormats);
        break;
    case FFTrecfmvb:
        partitioner = new CRECFMvbPartitioner(true);
        break;
    case FFTrecfmv:
        partitioner = new CRECFMvbPartitioner(false);
        break;
    case FFTvariable:
        partitioner = new CVariablePartitioner(false);
        break;
    case FFTvariablebigendian:
        partitioner = new CVariablePartitioner(true);
        break;
    case FFTcsv:
        if (calcOutput && !sameFormats)
            partitioner = new CCsvPartitioner(srcFormat);
        else
            partitioner = new CCsvQuickPartitioner(srcFormat, sameFormats);
        break;
    case FFTutf8: case FFTutf8n: case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
        if (srcFormat.markup==FMTxml)
        {
            if (calcOutput && !sameFormats)
                partitioner = new CXmlPartitioner(srcFormat);
            else
                partitioner = new CXmlQuickPartitioner(srcFormat, sameFormats);
        }
        else if (srcFormat.markup==FMTjson)
            partitioner = new CJsonPartitioner(srcFormat);
        else
        {
            if (calcOutput && !sameFormats)
                partitioner = new CUtfPartitioner(srcFormat);
            else
                partitioner = new CUtfQuickPartitioner(srcFormat, sameFormats);
        }
        break;
    default:
        throwError(DFTERR_UnknownFileFormatType);
        break;
    }
    return partitioner;
}

IOutputProcessor * createOutputProcessor(const FileFormat & format)
{
    LOG(MCdebugProgressDetail, unknownJob, "createOutputProcessor(format.type:'%s')", format.getFileFormatTypeString());
    switch (format.type)
    {
    case FFTfixed:
        return new CFixedOutputProcessor(format.recordSize);
    case FFTblocked:
        return new CBlockedOutputProcessor();
    case FFTvariable:
        return new CVariableOutputProcessor(false);
    case FFTvariablebigendian:
        return new CVariableOutputProcessor(true);
    case FFTutf8: case FFTutf8n: case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
        return new CUtfOutputProcessor(format.type);
    default:
        throwError(DFTERR_UnknownFileFormatType);
        return NULL;
    }
}

IFormatPartitioner * createFormatPartitioner(const SocketEndpoint & ep, const FileFormat & srcFormat, const FileFormat & tgtFormat, bool calcOutput, const char * slave, const char *wuid)
{
    bool sameFormats = sameEncoding(srcFormat, tgtFormat);
    LOG(MCdebugProgressDetail, unknownJob, "createFormatProcessor(srcFormat.type:'%s', tgtFormat.type:'%s', calcOutput:%d, sameFormats:%d)", srcFormat.getFileFormatTypeString(), tgtFormat.getFileFormatTypeString(), calcOutput, sameFormats);
    if (sameFormats)
    {
        switch (srcFormat.type)
        {
        case FFTfixed:
            return new CSimpleFixedPartitioner(srcFormat.recordSize, sameFormats);
        case FFTblocked:
            return new CSimpleBlockedPartitioner(sameFormats);
        case FFTcsv:
            if (srcFormat.hasQuote() && srcFormat.hasQuotedTerminator())
                return new CCsvPartitioner(srcFormat);
            else
                return new CCsvQuickPartitioner(srcFormat, sameFormats);
            break;
        case FFTutf: case FFTutf8: case FFTutf8n: case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
            if (srcFormat.markup==FMTxml)
                return new CXmlQuickPartitioner(srcFormat, sameFormats);
            if (srcFormat.markup==FMTjson)
                return new CJsonPartitioner(srcFormat);
            if (srcFormat.hasQuote() && srcFormat.hasQuotedTerminator())
                return new CUtfPartitioner(srcFormat);
            return new CUtfQuickPartitioner(srcFormat, sameFormats);
        }
    }
    if (!calcOutput)
    {
        switch (srcFormat.type)
        {
        case FFTfixed:
            return new CSimpleFixedPartitioner(srcFormat.recordSize, sameFormats);
        case FFTblocked:
            return new CSimpleBlockedPartitioner(sameFormats);
        case FFTvariable:
            return new CVariablePartitioner(false);
        case FFTvariablebigendian:
            return new CVariablePartitioner(true);
        case FFTrecfmvb:
            return new CRECFMvbPartitioner(true);
        case FFTrecfmv:
            return new CRECFMvbPartitioner(false);
        case FFTcsv:
            return new CCsvQuickPartitioner(srcFormat, sameFormats);
        case FFTutf: case FFTutf8: case FFTutf8n: case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
            if (srcFormat.markup==FMTxml)
                return new CXmlQuickPartitioner(srcFormat, sameFormats);
            if (srcFormat.markup==FMTjson)
                return new CJsonPartitioner(srcFormat);
            return new CUtfQuickPartitioner(srcFormat, sameFormats);
        default:
            throwError(DFTERR_UnknownFileFormatType);
            break;
        }
    }
    StringBuffer name;
    if (!slave)
        slave = queryFtSlaveExecutable(ep, name);

    return new CRemotePartitioner(ep, srcFormat, tgtFormat, slave, wuid);
}
