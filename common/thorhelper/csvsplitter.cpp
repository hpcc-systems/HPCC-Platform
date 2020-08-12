/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#include "platform.h"
#include "jregexp.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "junicode.hpp"
#include "jfile.hpp"
#include "eclhelper.hpp"

#ifdef _USE_ICU
#include "unicode/uchar.h"
#endif

#include "csvsplitter.hpp"
#include "eclrtl.hpp"
#include "roxiemem.hpp"
using roxiemem::OwnedRoxieString;

// If you have lines more than 2Mb in length it is more likely to be a bug - so require an explicit override
#define DEFAULT_CSV_LINE_LENGTH 2048
#define MAX_SENSIBLE_CSV_LINE_LENGTH 0x200000

CSVSplitter::CSVSplitter()
{
    lengths = NULL;
    data = NULL;
    numQuotes = 0;
    internalBuffer = NULL;
    maxColumns = 0;
    internalOffset = 0;
}

CSVSplitter::~CSVSplitter()
{
    delete [] lengths;
    delete [] data;
    free(internalBuffer);
}

void CSVSplitter::addQuote(const char * text)
{
    //Allow '' to remove quoting.
    if (text && *text)
        matcher.addEntry(text, QUOTE+(numQuotes++<<8));
}

void CSVSplitter::addSeparator(const char * text)
{
    if (text && *text)
        matcher.addEntry(text, SEPARATOR);
}

void CSVSplitter::addTerminator(const char * text)
{
    matcher.addEntry(text, TERMINATOR);
}

void CSVSplitter::addItem(MatchItem item, const char * text)
{
    if (text)
        matcher.addEntry(text, item);
}

void CSVSplitter::addEscape(const char * text)
{
    matcher.queryAddEntry((size32_t)strlen(text), text, ESCAPE);
}

void CSVSplitter::addWhitespace()
{
    matcher.queryAddEntry(1, " ", WHITESPACE);
    matcher.queryAddEntry(1, "\t", WHITESPACE);
}

void CSVSplitter::reset()
{
    matcher.reset();
    delete [] lengths;
    delete [] data;
    free(internalBuffer);
    lengths = NULL;
    data = NULL;
    numQuotes = 0;
    internalBuffer = NULL;
    internalOffset = 0;
    sizeInternal = 0;
    maxCsvSize = 0;
}

void CSVSplitter::init(unsigned _maxColumns, ICsvParameters * csvInfo, const char * dfsQuotes, const char * dfsSeparators, const char * dfsTerminators, const char * dfsEscapes)
{
    reset();

    maxCsvSize = csvInfo->queryMaxSize();
    maxColumns = _maxColumns;
    lengths = new unsigned [maxColumns+1];      // NB: One larger to remove some tests in main loop...
    data = new const byte * [maxColumns+1];

    unsigned idx;
    unsigned flags = csvInfo->getFlags();
    if (dfsQuotes && (flags & ICsvParameters::defaultQuote))
        addActionList(matcher, dfsQuotes, QUOTE);
    else
    {
        for (idx=0;;idx++)
        {
            OwnedRoxieString text(csvInfo->getQuote(idx));
            if (!text)
                break;
            addQuote(text);
        }
    }

    if (dfsSeparators && (flags & ICsvParameters::defaultSeparate))
        addActionList(matcher, dfsSeparators, SEPARATOR);
    else
    {
        for (idx=0;;idx++)
        {
            OwnedRoxieString text(csvInfo->getSeparator(idx));
            if (!text)
                break;
            addSeparator(text);
        }
    }

    if (dfsTerminators && (flags & ICsvParameters::defaultTerminate))
        addActionList(matcher, dfsTerminators, TERMINATOR);
    else
    {
        for (idx=0;;idx++)
        {
            OwnedRoxieString text(csvInfo->getTerminator(idx));
            if (!text)
                break;
            addTerminator(text);
        }
    }

    if (dfsEscapes && (flags & ICsvParameters::defaultEscape))
        addActionList(matcher, dfsEscapes, ESCAPE);
    else
    {
        for (idx=0;;idx++)
        {
            OwnedRoxieString text(csvInfo->getEscape(idx));
            if (!text)
                break;
            addEscape(text);
        }
    }

    //MORE Should this be configurable??
    if (!(flags & ICsvParameters::preserveWhitespace))
        addWhitespace();
}


void CSVSplitter::init(unsigned _maxColumns, size32_t _maxCsvSize, const char *quotes, const char *separators, const char *terminators, const char *escapes, bool preserveWhitespace)
{
    reset();

    maxCsvSize = _maxCsvSize;
    maxColumns = _maxColumns;
    lengths = new unsigned [maxColumns+1];      // NB: One larger to remove some tests in main loop...
    data = new const byte * [maxColumns+1];

    if (quotes)
        addActionList(matcher, quotes, QUOTE);
    if (separators)
        addActionList(matcher, separators, SEPARATOR);
    if (terminators)
        addActionList(matcher, terminators, TERMINATOR);
    if (escapes)
        addActionList(matcher, escapes, ESCAPE);

    if (!preserveWhitespace)
        addWhitespace();
}

void CSVSplitter::setFieldRange(const byte * start, const byte * end, unsigned curColumn, unsigned quoteToStrip, bool unescape)
{
    size32_t sizeOriginal = (size32_t)(end - start);
    //If the field doesn't contain quotes or escape characters, then we can directly store a pointer to the original.
    if (!quoteToStrip && !unescape)
    {
        lengths[curColumn] = sizeOriginal;
        data[curColumn] = start;
        return;
    }

    // Either quoting or escaping will need to copy into a local buffer.
    size32_t sizeUsed = internalOffset;
    size32_t sizeRequired = sizeUsed + sizeOriginal;
    if (sizeRequired > sizeInternal)
    {
        if (sizeInternal == 0)
            sizeInternal = maxCsvSize;

        //Check again to allow an explicit size to override the maximum sensible line limit
        if (sizeRequired > sizeInternal)
        {
            if (sizeInternal == 0)
                sizeInternal = DEFAULT_CSV_LINE_LENGTH;
            else if (sizeRequired > MAX_SENSIBLE_CSV_LINE_LENGTH)
                throw MakeStringException(99, "CSV File contains a line > %u characters.  Use MAXLENGTH to override the maximum length.", sizeRequired);

            //Cannot overflow as long as MAX_SENSIBLE_CSV_LINE_LENGTH < 0x80...
            while (sizeRequired > sizeInternal)
                sizeInternal *= 2;
        }

        byte * newBuffer = (byte *)realloc(internalBuffer, sizeInternal);
        if (!newBuffer)
            throw MakeStringException(99, "Failed to allocate CSV read buffer of %u bytes", sizeInternal);

        //The buffer has been reallocated, so we need to patch up any fields with pointers into the old buffer
        if (internalBuffer)
        {
            for (unsigned i=0; i < curColumn; i++)
            {
                byte * cur = (byte *)data[i];
                if ((cur >= internalBuffer) && (cur < internalBuffer + sizeInternal))
                    data[i] = (cur - internalBuffer) + newBuffer;
            }
        }
        internalBuffer = newBuffer;
    }

    data[curColumn] = internalBuffer + internalOffset;
    const byte * lastCopied = start;
    const byte *cur;
    for (cur = start; cur != end; )
    {
        unsigned matchLen;
        unsigned match = matcher.getMatch((size32_t)(end-cur), (const char *)cur, matchLen);
        switch (match & 255)
        {
        case NONE:
            matchLen = 1;
            break;
        case WHITESPACE:
        case SEPARATOR:
        case TERMINATOR:
            break;
        case ESCAPE:
            {
                const byte * next = cur + matchLen;
                if (next != end)
                {
                    //Copy all the data up to this escape character, start copying from the next character
                    memcpy(internalBuffer + internalOffset, lastCopied, cur-lastCopied);
                    internalOffset += (cur-lastCopied);
                    lastCopied = cur+matchLen;

                    //Don't treat the next character specially
                    unsigned nextMatchLen;
                    unsigned nextMatch = matcher.getMatch((size32_t)(end-next), (const char *)next, nextMatchLen);
                    if (nextMatchLen == 0)
                        nextMatchLen = 1;
                    matchLen += nextMatchLen;
                }
                break;
            }
        case QUOTE:
            {
                const byte * next = cur + matchLen;
                if ((match == quoteToStrip) && (next != end))
                {
                    unsigned nextMatchLen;
                    unsigned nextMatch = matcher.getMatch((size32_t)(end-next), (const char *)next, nextMatchLen);
                    if (nextMatch == match)
                    {
                        memcpy(internalBuffer + internalOffset, lastCopied, next-lastCopied);
                        internalOffset += (next-lastCopied);
                        matchLen += nextMatchLen;
                        lastCopied = cur+matchLen;
                    }
                }
                break;
            }
        }
        cur += matchLen;
    }

    memcpy(internalBuffer + internalOffset, lastCopied, cur-lastCopied);
    internalOffset += (cur-lastCopied);
    lengths[curColumn] = (size32_t)(internalBuffer + internalOffset - data[curColumn]);
}

unsigned CSVSplitter::splitLine(ISerialStream *stream, size32_t maxRowSize)
{
    if (stream->eos())
        return 0;
    size32_t minRequired = 4096; // MORE - make configurable
    size32_t thisLineLength;
    for (;;)
    {
        size32_t avail;
        const void *peek = stream->peek(minRequired, avail);
        thisLineLength = splitLine(avail, (const byte *)peek);
        if (thisLineLength < avail || avail < minRequired)
            break;

        if (minRequired == maxRowSize)
            throw MakeStringException(99, "Stream contained a line of length greater than %u bytes.", maxRowSize);
        if (avail > minRequired*2)
            minRequired = avail+minRequired;
        else
            minRequired += minRequired;
        if (minRequired >= maxRowSize/2)
            minRequired = maxRowSize;
    }
    return thisLineLength;
}

size32_t CSVSplitter::splitLine(size32_t maxLength, const byte * start)
{
    unsigned curColumn = 0;
    unsigned quote = 0;
    unsigned quoteToStrip = 0;
    const byte * cur = start;
    const byte * end = start + maxLength;
    const byte * firstGood = start;
    const byte * lastGood = start;
    bool lastEscape = false;
    internalOffset = 0;

    while (cur != end)
    {
        unsigned matchLen;
        unsigned match = matcher.getMatch((size32_t)(end-cur), (const char *)cur, matchLen);
        switch (match & 255)
        {
        case NONE:
            cur++;          // matchLen == 0;
            lastGood = cur;
            break;
        case WHITESPACE:
            //Skip leading whitespace
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
            if ((curColumn < maxColumns) && (quote == 0))
            {
                setFieldRange(firstGood, lastGood, curColumn, quoteToStrip, lastEscape);
                lastEscape = false;
                quoteToStrip = 0;
                curColumn++;
                firstGood = cur + matchLen;
            }
            lastGood = cur+matchLen;
            break;
        case TERMINATOR:
            if (quote == 0) // Is this a good idea? Means a mismatched quote is not fixed by EOL
            {
                setFieldRange(firstGood, lastGood, curColumn, quoteToStrip, lastEscape);
                lastEscape = false;
                while (++curColumn < maxColumns)
                {
                    data[curColumn] = cur;
                    lengths[curColumn] = 0;
                }
                return (size32_t)(cur + matchLen - start);
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

    setFieldRange(firstGood, lastGood, curColumn, quoteToStrip, lastEscape);
    while (++curColumn < maxColumns)
        lengths[curColumn] = 0;
    return (size32_t)(end - start);
}


//=====================================================================================================

void CSVOutputStream::beginLine()
{
    clear();
    prefix = NULL;
}

void CSVOutputStream::endLine()
{
    append(terminator);
}

void CSVOutputStream::init(ICsvParameters * args, bool _oldOutputFormat)
{
    if (args->queryEBCDIC())
        throw MakeStringException(99, "EBCDIC CSV output not yet implemented");
    OwnedRoxieString rs;
    quote.set(rs.setown(args->getQuote(0)));
    separator.set(rs.setown(args->getSeparator(0)));
    terminator.set(rs.setown(args->getTerminator(0)));
    escape.set(rs.setown(args->getEscape(0)));
    oldOutputFormat = _oldOutputFormat||!quote.length();
}

void CSVOutputStream::writeUnicode(size32_t len, const UChar * data)
{
    unsigned utf8Length;
    char * utf8Data = NULL;
    rtlUnicodeToCodepageX(utf8Length, utf8Data, len, data, "utf-8");
    writeString(utf8Length, utf8Data);
    rtlFree(utf8Data);
}

#ifndef _USE_ICU
static inline bool u_isspace(UChar next) { return isspace((byte)next); }
#endif

void CSVOutputStream::writeUtf8(size32_t len, const char * data)
{
    append(prefix);
    if (oldOutputFormat) {
        append(quote).append(rtlUtf8Size(len, data), data).append(quote);
    }
    else if (len) {
        // is this OTT?
        // not sure if best way but generate an array of utf8 sizes
        MemoryAttr ma;
        size32_t * cl;
        if (len>256)
            cl = (size32_t *)ma.allocate(sizeof(size32_t)*len);
        else
            cl = (size32_t *)alloca(sizeof(size32_t)*len);
        unsigned start=(unsigned)-1;
        unsigned end=0;
        const byte * s = (const byte *)data;
        unsigned i;
        for (i=0;i<len;i++) {
            const byte *p=s;
            UChar next = readUtf8Character(sizeof(UChar), s);
            cl[i] = (size32_t)(s-p);
            if (next != ' ') {
                end = i;
                if (start==(unsigned)-1)
                    start = i;
            }
        }
        const byte *e=s;
        // do trim
        if (start!=(unsigned)-1) {
            for (i=0;i<start;i++)
                data += *(cl++);
            len -= start;
            end -= start;
            end++;
            while (end<len)
                e -= cl[--len];
        }
        // now see if need quoting by looking for separator, terminator or quote
        // I *think* this can be done with memcmps as has to be exact
        size32_t sl = separator.length();
        size32_t tl = terminator.length();
        size32_t ql = quote.length();
        bool needquote=false;
        s = (const byte *)data;
        for (i=0;i<len;i++) {
            size32_t l = (size32_t)(e-s);
            if (sl&&(l>=sl)&&(memcmp(separator.get(),s,sl)==0)) {
                needquote = true;
                break;
            }
            if (tl&&(l>=tl)&&(memcmp(terminator.get(),s,tl)==0)) {
                needquote = true;
                break;
            }
            if ((l>=ql)&&(memcmp(quote.get(),s,ql)==0)) {
                needquote = true;
                break;
            }
            s+=cl[i];
        }
        if (needquote) {
            append(quote);
            s = (const byte *)data;
            for (i=0;i<len;i++) {
                size32_t l = (size32_t)(e-s);
                if ((l>=ql)&&(memcmp(quote.get(),s,ql)==0))
                    append(quote);
                append(cl[i],(const char *)s);
                s+=cl[i];
            }
            append(quote);
        }
        else
            append((size32_t)(e-(const byte *)data),data);
    }
    prefix = separator;
}

void CSVOutputStream::writeString(size32_t len, const char * data)
{

    append(prefix);
    if (oldOutputFormat) {
        append(quote).append(len, data).append(quote);
    }
    else if (len) {
        // New format (as per GS)
        // first trim
        while (len&&(*data==' ')) {
            len--;
            data++;
        }
        while (len&&(data[len-1]==' '))
            len--;
        // now see if need quoting by looking for separator, terminator or quote
        size32_t sl = separator.length();
        size32_t tl = terminator.length();
        size32_t ql = quote.length();
        bool needquote=false;
        const char *s = data;
        for (unsigned l=len;l>0;l--) {
            if (sl&&(l>=sl)&&(memcmp(separator.get(),s,sl)==0)) {
                needquote = true;
                break;
            }
            if (tl&&(l>=tl)&&(memcmp(terminator.get(),s,tl)==0)) {
                needquote = true;
                break;
            }
            if ((l>=ql)&&(memcmp(quote.get(),s,ql)==0)) {
                needquote = true;
                break;
            }
            s++;
        }
        if (needquote) {
            append(quote);
            const char *s = data;
            for (unsigned l=len;l>0;l--) {
                if ((l>=ql)&&(memcmp(quote.get(),s,ql)==0))
                    append(quote);
                append(*(s++));
            }
            append(quote);
        }
        else
            append(len,data);
    }
    prefix = separator;

}

void CSVOutputStream::writeHeaderLn(size32_t len, const char * data)
{
    append(len,data);
    if (!oldOutputFormat&&len) {
        size32_t tl = terminator.length();
        if ((tl>len)||(memcmp(data+len-tl,terminator.get(),tl)!=0))
            endLine();
    }
}

