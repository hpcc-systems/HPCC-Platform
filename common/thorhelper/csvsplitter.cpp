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

#include "platform.h"
#include "jregexp.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "junicode.hpp"
#include "eclhelper.hpp"

#include "unicode/uchar.h"

#include "csvsplitter.hpp"
#include "eclrtl.hpp"

CSVSplitter::CSVSplitter()
{
    lengths = NULL;
    data = NULL;
    numQuotes = 0;
    internalBuffer = NULL;
    maxColumns = 0;
    curUnquoted = NULL;
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

void CSVSplitter::addEscape(const char * text)
{
    matcher.addEntry(text, ESCAPE);
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
    maxCsvSize = 0;
}

void CSVSplitter::init(unsigned _maxColumns, ICsvParameters * csvInfo, const char * dfsQuotes, const char * dfsSeparators, const char * dfsTerminators, const char * dfsEscapes)
{
    reset();
    maxCsvSize = csvInfo->queryMaxSize();
    internalBuffer = (byte *)malloc(maxCsvSize);

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
            const char * text = csvInfo->queryQuote(idx);
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
            const char * text = csvInfo->querySeparator(idx);
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
            const char * text = csvInfo->queryTerminator(idx);
            if (!text)
                break;
            addTerminator(text);
        }
    }

    // Old workunits won't have queryEscape. MORE: deprecate on the next major version
    if (flags & ICsvParameters::supportsEscape)
    {
        if (dfsEscapes && (flags & ICsvParameters::defaultEscape))
            addActionList(matcher, dfsEscapes, ESCAPE);
        else
        {
            for (idx=0;;idx++)
            {
                const char * text = csvInfo->queryEscape(idx);
                if (!text)
                    break;
                addEscape(text);
            }
        }
    }

    //MORE Should this be configurable??
    if (!(flags & ICsvParameters::preserveWhitespace))
    {
        matcher.queryAddEntry(1, " ", WHITESPACE);
        matcher.queryAddEntry(1, "\t", WHITESPACE);
    }
}

void CSVSplitter::setFieldRange(const byte * start, const byte * end, unsigned curColumn, unsigned quoteToStrip, bool unescape)
{
    // Either quoting or escaping will use the local buffer
    if ((quoteToStrip || unescape) &&
        (unsigned)(curUnquoted - internalBuffer) + (unsigned)(end - start) > maxCsvSize)
        throw MakeStringException(99, "MAXLENGTH for CSV file is not large enough");

    // point to the beginning of the local (possibly changed) buffer, for escaping later
    byte * curUnescaped = curUnquoted;
    if (quoteToStrip)
    {
        data[curColumn] = curUnquoted;
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
                break;
            case TERMINATOR:
                goto done;
            case QUOTE:
                {
                    const byte * next = cur + matchLen;
                    if ((match == quoteToStrip) && (next != end))
                    {
                        unsigned nextMatchLen;
                        unsigned nextMatch = matcher.getMatch((size32_t)(end-next), (const char *)next, nextMatchLen);
                        if (nextMatch == match)
                        {
                            memcpy(curUnquoted, lastCopied, next-lastCopied);
                            curUnquoted += (next-lastCopied);
                            matchLen += nextMatchLen;
                            lastCopied = cur+matchLen;
                        }
                    }
                    break;
                }
            }
            cur += matchLen;
        }
done:
        memcpy(curUnquoted, lastCopied, cur-lastCopied);
        curUnquoted += (cur-lastCopied);
        lengths[curColumn] = (size32_t)(curUnquoted - data[curColumn]);
    }
    else
    {
        lengths[curColumn] = (size32_t)(end-start);
        // Only if ESCAPEs were detected in the input
        if (unescape)
        {
            // Need to copy original to a local string (using allocated buffer)
            memcpy(curUnescaped, start, lengths[curColumn]);
            data[curColumn] = curUnescaped;
            // and update the buffer pointer, to re-use on next iteration
            curUnquoted = curUnescaped + lengths[curColumn];
        }
        else
        {
            data[curColumn] = start;
            return;
        }
    }
    // Un-escape string, if necessary.
    if (unescape)
    {
        byte * cur = curUnescaped; // data[curColumn] is already pointing here one way or another
        byte * end = cur + lengths[curColumn];
        for (; cur < end; cur++)
        {
            unsigned matchLen;
            unsigned match = matcher.getMatch((size32_t)(end-cur), (const char *)cur, matchLen);
            if ((match & 255) == ESCAPE)
            {
                ptrdiff_t restLen = end-cur+matchLen;
                memmove(cur, cur+matchLen, restLen);
                end -= matchLen;
                lengths[curColumn] -= matchLen;
                // Avoid having cur past end
                if (cur == end)
                    break;
            }
        }
    }
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
    curUnquoted = internalBuffer;

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
                    lengths[curColumn] = 0;
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
    quote.set(args->queryQuote(0));
    separator.set(args->querySeparator(0));
    terminator.set(args->queryTerminator(0));
    escape.set(args->queryEscape(0));
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
            if (!u_isspace(next)) {
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

