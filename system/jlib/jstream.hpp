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
    virtual bool eos() = 0;                                         // no more data
    virtual void reset(offset_t _offset, offset_t _flen) = 0;       // input stream has changed - restart reading
    virtual offset_t tell() const = 0;                              // used to implement beginNested
};

interface IBufferedSerialInputStream : extends ISerialInputStream
{
    virtual const void * peek(size32_t wanted, size32_t &got) = 0;   // try and ensure wanted bytes are available.
                                                                    // if got<wanted then approaching eof
                                                                    // if got>wanted then got is size available in buffer
};
/* example of reading a nul terminated string using IBufferedSerialInputStream peek and skip
{
    for (;;) {
        const char *s = peek(1,got);
        if (!s)
            break;  // eof before nul detected;
        const char *p = s;
        const char *e = p+got;
        while (p!=e) {
            if (!*p) {
                out.append(p-s,s);
                skip(p-s+1); // include nul
                return;
            }
            p++;
        }
        out.append(got,s);
        skip(got);
    }
}
*/

interface ISerialOutputStream : extends IInterface
{
    virtual void put(size32_t len, const void * ptr) = 0;       // throws an error if cannot write the full size.
    virtual void flush() = 0;
    virtual offset_t tell() const = 0;                          // used to implement beginNested
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

extern jlib_decl IBufferedSerialInputStream * createBufferedInputStream(ISerialInputStream * input, size32_t blockReadSize);
extern jlib_decl ISerialInputStream * createDecompressingInputStream(IBufferedSerialInputStream * input, IExpander * decompressor);
extern jlib_decl ISerialInputStream * createSerialInputStream(IFileIO * input);
extern jlib_decl IBufferedSerialOutputStream * createBufferedOutputStream(ISerialOutputStream * output, size32_t blockWriteSize);
extern jlib_decl IBufferedSerialOutputStream * createThreadedBufferedOutputStream(ISerialOutputStream * output, size32_t blockWriteSize);
extern jlib_decl ISerialOutputStream * createCompressingOutputStream(IBufferedSerialOutputStream * output, ICompressor * compressor);
extern jlib_decl ISerialOutputStream * createSerialOutputStream(IFileIO * output);
extern jlib_decl IBufferedSerialOutputStream * createBufferedSerialOutputStream(StringBuffer & target);


inline IBufferedSerialOutputStream * createBufferedOutputStream(ISerialOutputStream * output, size32_t blockWriteSize, bool threaded)
{
    //Threaded version is currently slower unless data is hard to compress or a very large buffer size is being used.
    if (threaded)
        return createThreadedBufferedOutputStream(output, blockWriteSize);
    else
        return createBufferedOutputStream(output, blockWriteSize);
}

inline IBufferedSerialInputStream * createBufferedInputStream(ISerialInputStream * input, size32_t blockReadSize, [[maybe_unused]] bool threaded)
{
    //If a threaded version is implemented it should use async io, rather than a thread to perform the look ahead
    return createBufferedInputStream(input, blockReadSize);
}

#endif
