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

#ifndef CSVSPLITTER_INCL
#define CSVSPLITTER_INCL

#ifdef THORHELPER_EXPORTS
#define THORHELPER_API DECL_EXPORT
#else
#define THORHELPER_API DECL_IMPORT
#endif

#include "jregexp.hpp"
#include "eclhelper.hpp"
#include "unicode/utf.h"

/**
 * CSVSplitter - splits CSV files into fields and rows.
 *
 * CSV files are text based records that can have user defined syntax for quoting,
 * escaping, separating fields and rows. According to RFC-4180, there isn't a
 * standard way of building CSV files, however, there is a set of general rules
 * that most implementations seem to follow. This makes it hard to implement a CSV
 * parser, since even if you follow the RFC, you might not read some files as the
 * producer intended.
 *
 * The general rules are:
 *  * rows are separated by EOL
 *  * fields are separated by comma
 *  * special text must be enclosed by quotes
 *  * there must be a form of escaping quotes
 *
 * However, this implementation allows for user-specified quotes, (field) separators,
 * terminators (row separators), whitespace and (multi-char) escaping sequences, so
 * it should be possible to accommodate most files that deviate from the norm, while
 * still reading the files correctly by default.
 *
 * One important rule is that any special behaviour should be enclosed by quotes, so
 * you don't need to account for escaping separators or terminators when they're not
 * themselves quoted. This, and non-matching quotes should be considered syntax error
 * and the producer should, then, fix their output.
 *
 * Also, many CSV producers (including commercial databases) use slash (\) as escaping
 * char, while the RFC mentions re-using quotes (""). We implement both.
 */
class THORHELPER_API CSVSplitter
{
public:
    CSVSplitter();
    ~CSVSplitter();

    void addQuote(const char * text);
    void addSeparator(const char * text);
    void addTerminator(const char * text);
    void addEscape(const char * text);

    void init(unsigned maxColumns, ICsvParameters * csvInfo, const char * dfsQuotes, const char * dfsSeparators, const char * dfsTerminators, const char * dfsEscapes);
    void reset();
    size32_t splitLine(size32_t maxLen, const byte * start);

    inline unsigned * queryLengths() { return lengths; }
    inline const byte * * queryData() { return data; }

protected:
    void setFieldRange(const byte * start, const byte * end, unsigned curColumn, unsigned quoteToStrip, bool unescape);

protected:
    enum { NONE=0, SEPARATOR=1, TERMINATOR=2, WHITESPACE=3, QUOTE=4, ESCAPE=5 };
    unsigned            maxColumns;
    StringMatcher       matcher;
    unsigned            numQuotes;
    unsigned *          lengths;
    const byte * *      data;
    byte *              internalBuffer;
    size32_t            internalOffset;
    size32_t            sizeInternal;
    size32_t            maxCsvSize;
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
    StringAttr escape;
    const char * prefix;
    bool oldOutputFormat;
};

#endif // CSVSPLITTER_INCL
