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

#ifndef DAFTFORMAT_HPP
#define DAFTFORMAT_HPP

#include "filecopy.hpp"
#include "daft.hpp"
#include "ftbase.ipp"

#define DEFAULT_STD_BUFFER_SIZE         0x10000
#define EFX_BLOCK_HEADER_TYPE   unsigned
#define EFX_BLOCK_HEADER_SIZE   sizeof(EFX_BLOCK_HEADER_TYPE)
#define VARIABLE_LENGTH_TYPE    unsigned
#define EXPECTED_VARIABLE_LENGTH 512            // NOt very criticial

//---------------------------------------------------------------------------

struct PartitionCursor
{
public:
    PartitionCursor(offset_t _inputOffset)  { inputOffset = nextInputOffset = _inputOffset; outputOffset = 0; }
    
    offset_t        inputOffset;
    offset_t        nextInputOffset;
    offset_t        outputOffset;
};

struct TransformCursor
{
public:
    TransformCursor()                       { inputOffset = 0; }

    offset_t        inputOffset;
};

interface IOutputProcessor;
interface IFormatPartitioner : public IInterface
{
public:
//Analysis
    virtual void calcPartitions(Semaphore * sem) = 0;
    virtual void getResults(PartitionPointArray & partition) = 0;
    virtual void setPartitionRange(offset_t _totalSize, offset_t _thisOffset, offset_t _thisSize, unsigned _thisHeaderSize, unsigned _numParts) = 0;
    virtual void setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool compressedInput, const char *decryptKey) = 0;
    virtual void setTarget(IOutputProcessor * _target) = 0;
};

interface IFormatProcessor : public IFormatPartitioner
{
    virtual void setSource(unsigned _whichInput, const RemoteFilename & _fullPath, bool compressedInput, const char *decryptKey) = 0;
    virtual void setTarget(IOutputProcessor * _target) = 0;

    //Processing.
    virtual void beginTransform(offset_t thisOffset, offset_t thisLength, TransformCursor & cursor) = 0;
    virtual void endTransform(TransformCursor & cursor) = 0;
    virtual crc32_t getInputCRC() = 0;
    virtual void setInputCRC(crc32_t value) = 0;
    virtual unsigned transformBlock(offset_t endOffset, TransformCursor & cursor) = 0;
};

interface IOutputProcessor : public IInterface
{
public:
    virtual offset_t getOutputOffset() = 0;
    virtual void setOutput(offset_t startOffset, IFileIOStream * out = NULL) = 0;

    virtual void updateOutputOffset(size32_t len, const byte * data) = 0;
    virtual void finishOutputOffset() = 0;

    virtual void outputRecord(size32_t len, const byte * data) = 0;
    virtual void finishOutputRecords() = 0;

//Record processing
//  virtual void processRecord(size32_t len, byte * data, IFileIOStream * output);
};

typedef IArrayOf<IFormatProcessor> FormatProcessorArray;
typedef IArrayOf<IFormatPartitioner> FormatPartitionerArray;

extern DALIFT_API IFormatProcessor * createFormatProcessor(const FileFormat & srcFormat, const FileFormat & tgtFormat, bool calcOutput);
extern DALIFT_API IOutputProcessor * createOutputProcessor(const FileFormat & format);

extern DALIFT_API IFormatPartitioner * createFormatPartitioner(const SocketEndpoint & ep, const FileFormat & srcFormat, const FileFormat & tgtFormat, bool calcOutput, const char * slave, const char *wuid);


#include <string.h>

class CCsvMatcher
{
    static const char DEFAULT_SEPARATOR_CHAR  = ',';
    static const char DEFAULT_TERMINATOR_CHAR = '\n';
    static const char DEFAULT_QUOTE_CHAR      = '\'';
    static const char DEFAULT_SPACE_CHAR      = ' ';
    static const char DEFAULT_TAB_CHAR        = '\t';

public:
    enum { NONE=0, SEPARATOR=1, TERMINATOR=2, WHITESPACE=3, QUOTE=4 };

    CCsvMatcher() { init(); };
    ~CCsvMatcher() {};

    bool addSeparator(const char * aNewSeparator)
        {
            charMatch[DEFAULT_SEPARATOR_CHAR] = NONE;
            bool success = processParam(aNewSeparator, SEPARATOR);
            if( false == success)
            {
                // Restore default separator
                charMatch[DEFAULT_SEPARATOR_CHAR] = SEPARATOR;
            }
            return success;
        };

    bool addTerminator(const char * aNewTerminator)
        {
            charMatch[DEFAULT_TERMINATOR_CHAR] = NONE;
            bool success = processParam(aNewTerminator, TERMINATOR);
            if( false == success)
            {
                // Restore default separator
                charMatch[DEFAULT_TERMINATOR_CHAR] = TERMINATOR;
            }
            return success;
        };

    void changeQuote(const char aNewQuote)
        {
            // Handle only one quote char
            charMatch[DEFAULT_QUOTE_CHAR] = NONE;
            charMatch[aNewQuote] = QUOTE;
        };

    unsigned char match(unsigned int aIndex)
        {
            return charMatch[aIndex&255];
        };

private:
    void init(void)
        {
            for( int i = 0; i < 256; ++i)
            {
                charMatch[i] = NONE;
            }

            charMatch[DEFAULT_SEPARATOR_CHAR]  = SEPARATOR;
            charMatch[DEFAULT_TERMINATOR_CHAR] = TERMINATOR;
            charMatch[DEFAULT_QUOTE_CHAR]      = QUOTE;
            charMatch[DEFAULT_SPACE_CHAR]      = WHITESPACE;
            charMatch[DEFAULT_TAB_CHAR]        = WHITESPACE;
       };

    void clearAllSpecChar(unsigned aSpecChar)
        {
            for( int i = 0; i < 256; ++i)
            {
                if( charMatch[i] == aSpecChar )
                    charMatch[i] = NONE;
            }
        };

    bool processParam(const char * aParam, unsigned aSpecType)
        {
            char * temp = strdup(aParam);
            bool retVal = true;
            char delim[] = ",";
            char * anchor;
            char * token = strtok_r(temp, delim, &anchor);

            while( NULL != token)
            {
                int len = strlen(token);
                if( (1 < len) && ('\\' != *token))
                {
                    retVal = false;
                    break;
                }
                else
                {
                    char specChar = *token;
                    if( '\\' == specChar )
                    {
                        token++;
                        switch(*token)
                        {
                        case 'b':
                            specChar = '\b';
                            break;

                        case 'f':
                            specChar = '\f';
                            break;

                        case 'n':
                            specChar = '\n';
                            break;

                        case 'r':
                            // Check if process TERMINATOR and token is "\r\n"
                            if((TERMINATOR == aSpecType) && ( '\\' == *(token+1)) && ('n' == *(token+2)) )
                            {
                                specChar = '\n';
                            }
                            else
                            {
                                specChar = '\r';
                            }
                            break;

                        case 't':
                            specChar = '\t';
                            break;

                        case '\\':
                        case '\'':
                            specChar = *token+1;
                            break;

                        }
                    }
                    charMatch[specChar] = aSpecType;
                }

                // Get next token:
                token = strtok_r(NULL, delim, &anchor);
            }

            if(false == retVal)
            {
                // Restore original state
                clearAllSpecChar(aSpecType);
            }
            return retVal;
        }

private:
    unsigned char charMatch[256];

};

#endif
