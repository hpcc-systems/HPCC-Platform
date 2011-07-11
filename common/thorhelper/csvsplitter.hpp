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

#ifndef CSVSPLITTER_INCL
#define CSVSPLITTER_INCL

#ifdef _WIN32
 #ifdef THORHELPER_EXPORTS
  #define THORHELPER_API __declspec(dllexport)
 #else
  #define THORHELPER_API __declspec(dllimport)
 #endif
#else
 #define THORHELPER_API
#endif

#include "jregexp.hpp"
#include "eclhelper.hpp"
#include "unicode/utf.h"

class THORHELPER_API CSVSplitter
{
public:
    CSVSplitter();
    ~CSVSplitter();

    void addQuote(const char * text);
    void addSeparator(const char * text);
    void addTerminator(const char * text);

    void init(unsigned maxColumns, ICsvParameters * csvInfo, const char * dfsQuotes, const char * dfsSeparators, const char * dfsTerminators);
    void reset();
    size32_t splitLine(size32_t maxLen, const byte * start);

    inline unsigned * queryLengths() { return lengths; }
    inline const byte * * queryData() { return data; }

protected:
    void setFieldRange(const byte * start, const byte * end, unsigned curColumn, unsigned quoteToStrip);

protected:
    enum { NONE=0, SEPARATOR=1, TERMINATOR=2, WHITESPACE=3, QUOTE=4 };
    unsigned            maxColumns;
    StringMatcher       matcher;
    unsigned            numQuotes;
    unsigned *          lengths;
    const byte * *      data;
    byte *              unquotedBuffer;
    byte *              curUnquoted;
    unsigned            maxCsvSize;
};

class THORHELPER_API CSVOutputStream : public StringBuffer, implements ITypedOutputStream
{
public:
    void beginLine();
    void writeHeaderLn(size32_t len, const char * data); // no need for endLine
    void endLine();
    void init(ICsvParameters * args, bool _oldOutputFormat);

    virtual void writeReal(double value)                    { append(prefix).append(value); prefix = separator; }
    virtual void writeSigned(__int64 value)                 { append(prefix).append(value); prefix = separator; }
    virtual void writeString(size32_t len, const char * data);
    virtual void writeUnicode(size32_t len, const UChar * data);
    virtual void writeUnsigned(unsigned __int64 value)      { append(prefix).append(value); prefix = separator; }
    virtual void writeUtf8(size32_t len, const char * data);

protected:
    StringAttr separator;
    StringAttr terminator;
    StringAttr quote;
    const char * prefix;
    bool oldOutputFormat;
};

#endif // CSVSPLITTER_INCL
