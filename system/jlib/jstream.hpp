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



#ifndef JSTREAM_HPP
#define JSTREAM_HPP

#include "jiface.hpp"
#include <utility>
#include <vector>

class StringBuffer;

interface jlib_decl IByteOutputStream : public IInterface
{
    virtual void writeByte(byte b) = 0;
    virtual void writeBytes(const void *, int) = 0;
    virtual void writeString(const char *str) = 0;
};

extern jlib_decl IByteOutputStream *createOutputStream(StringBuffer &to);
extern jlib_decl IByteOutputStream *createOutputStream(int handle);


static constexpr size32_t BufferTooSmall = (size32_t)-1;
static constexpr offset_t UnknownOffset = (offset_t)-1;
interface ISerialInputStream : extends IInterface
{
    virtual size32_t read(size32_t len, void * ptr) = 0;            // returns size read, result < len does NOT imply end of file
    virtual void skip(size32_t sz) = 0;
    virtual void get(size32_t len, void * ptr) = 0;                 // exception if no data available
    virtual void reset(offset_t _offset, offset_t _flen) = 0;       // start streaming from a difference section of the input (which may have changed)
                                                                    // throws an error if the input is not seekable (e.g. socket)
    virtual offset_t tell() const = 0;                              // used to implement beginNested
};

interface ICrcSerialInputStream : extends ISerialInputStream // Implemented here to prevent circular reference with jcrc.hpp
{
    virtual unsigned queryCrc() const = 0;
};

interface IBufferedSerialInputStream : extends ISerialInputStream
{
    virtual bool eos() = 0;                                         // no more data - will perform a read if necessary
    virtual const void * peek(size32_t wanted, size32_t &got) = 0;  // try and ensure wanted bytes are available.
                                                                    // if got<wanted then approaching eof
                                                                    // if got>wanted then got is size available in buffer
                                                                    // null may be returned at eos, but not guaranteed
};

/* example of reading a nul terminated string using IBufferedSerialInputStream peek and skip */
extern jlib_decl bool readZeroTerminatedString(StringBuffer & out, IBufferedSerialInputStream & in);
extern jlib_decl const char * queryZeroTerminatedString(IBufferedSerialInputStream & in, size32_t & len);

//return a key value pair - if the key is a null string then return null for the value
extern jlib_decl std::pair<const char *, const char *> peekKeyValuePair(IBufferedSerialInputStream & in, size32_t & len);

//Return a vector of offsets of the starts of null terminated strings - terminated by a null string or end of file.
//Returns a pointer to the base string if valid.
extern jlib_decl const char * peekStringList(std::vector<size32_t> & matches, IBufferedSerialInputStream & in, size32_t & len);

//Return a vector of offsets of the starts of null terminated Attribute Name/Value strings (Value can be empty string)
// - terminated by a null string or end of file.
//Returns a pointer to the base string if valid.
extern jlib_decl const char * peekAttributePairList(std::vector<size32_t> & matches, IBufferedSerialInputStream & in, size32_t & len);


interface ISerialOutputStream : extends IInterface
{
    virtual void put(size32_t len, const void * ptr) = 0;       // throws an error if cannot write the full size.
    virtual void flush() = 0;
    virtual offset_t tell() const = 0;                          // used to implement beginNested
};

interface ICrcSerialOutputStream : extends ISerialOutputStream // Implemented here to prevent circular reference with jcrc.hpp
{
    virtual unsigned queryCrc() const = 0;
};

interface IBufferedSerialOutputStream : extends ISerialOutputStream
{
    virtual byte * reserve(size32_t wanted, size32_t & got) = 0;    // get a pointer to a contiguous block of memory to write to.
    virtual void commit(size32_t written) = 0 ;      // commit the data written to the block returned by reserve
    virtual void suspend(size32_t wanted) = 0;   // Reserve some bytes and prevent data being flushed to the next stage until endNested is called.  May nest.
    virtual void resume(size32_t len, const void * ptr) = 0;  // update the data allocated by suspend and allow flushing.
    virtual void replaceOutput(ISerialOutputStream * newOutput) = 0;
};

interface ICompressor;
interface IExpander;
interface IFileIO;
class MemoryBuffer;

extern jlib_decl ICrcSerialInputStream * createCrcInputStream(ISerialInputStream * input);
extern jlib_decl IBufferedSerialInputStream * createBufferedInputStream(ISerialInputStream * input, size32_t blockReadSize);
extern jlib_decl ISerialInputStream * createDecompressingInputStream(IBufferedSerialInputStream * input, IExpander * decompressor);
extern jlib_decl ISerialInputStream * createSerialInputStream(IFileIO * input);
extern jlib_decl ISerialInputStream * createSerialInputStream(IFileIO * input, offset_t startOffset, offset_t length);
extern jlib_decl ICrcSerialOutputStream * createCrcOutputStream(ISerialOutputStream * output);
extern jlib_decl IBufferedSerialOutputStream * createBufferedOutputStream(ISerialOutputStream * output, size32_t blockWriteSize);
extern jlib_decl IBufferedSerialOutputStream * createThreadedBufferedOutputStream(ISerialOutputStream * output, size32_t blockWriteSize);
extern jlib_decl ISerialOutputStream * createCompressingOutputStream(IBufferedSerialOutputStream * output, ICompressor * compressor);
extern jlib_decl ISerialOutputStream * createSerialOutputStream(IFileIO * output, offset_t offset=0);

extern jlib_decl IBufferedSerialInputStream * createBufferedSerialInputStream(MemoryBuffer & source);
extern jlib_decl IBufferedSerialOutputStream * createBufferedSerialOutputStream(StringBuffer & target);
extern jlib_decl IBufferedSerialOutputStream * createBufferedSerialOutputStream(MemoryBuffer & target);


inline IBufferedSerialOutputStream * createBufferedOutputStream(ISerialOutputStream * output, size32_t blockWriteSize, int threading)
{
    //Threaded version is currently slower unless data is hard to compress or a very large buffer size is being used.
    if (threading != 0)
         // In the future pass threading as a parameter to indicate the number of extra buffers to use by the output thread
        return createThreadedBufferedOutputStream(output, blockWriteSize);
    else
        return createBufferedOutputStream(output, blockWriteSize);
}

inline IBufferedSerialInputStream * createBufferedInputStream(ISerialInputStream * input, size32_t blockReadSize, [[maybe_unused]] bool threaded)
{
    //If a threaded version is implemented it should use async io, rather than a thread to perform the look ahead
    return createBufferedInputStream(input, blockReadSize);
}

extern jlib_decl ISerialInputStream *createProgressStream(ISerialInputStream *stream, offset_t offset, offset_t len, const char *msg, unsigned periodSecs);

#endif
