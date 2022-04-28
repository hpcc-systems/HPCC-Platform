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

#include "jliball.hpp"
#include "thorpipe.hpp"
#include "thorxmlread.hpp"
#include "thorxmlwrite.hpp"
#include "thorcommon.ipp"
#include "csvsplitter.hpp"
#include "rtlread_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlformat.hpp"
#include "roxiemem.hpp"

using roxiemem::OwnedRoxieString;

//=====================================================================================================

class CPipeErrorHelper : public Thread, implements IPipeErrorHelper
{
private:
    StringBuffer errorOutput;
    Linked<IPipeProcess> pipe;

public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    int run()
    {
        char buffer[10001];
        int numErrors = 0;
        size32_t read;
        char *errorLine;

        while (true)
        {
            read = pipe->readError(10000,buffer);

            if ((read == 0) || (read == (size32_t)-1))
                break;

            if (numErrors < 100)
            {
                buffer[read] = '\0';
                char *saveptr;
                errorLine = strtok_r(buffer, "\n", &saveptr);
                errorOutput.append(errorLine).newline();
                numErrors++;

                while ((numErrors < 100) && (errorLine = strtok_r(NULL, "\n", &saveptr)))
                {
                    errorOutput.append(errorLine).newline();
                    numErrors++;
                }
            }
        }

        return 0;
    }

    void run(IPipeProcess *_pipe)
    {
        pipe.set(_pipe);
        this->start();
    }

    void wait()
    {
        this->join();
    }

    const char *queryErrorOutput()
    {
        return errorOutput.str();
    }
};

//=====================================================================================================

IPipeErrorHelper * createPipeErrorHelper()
{
    return new CPipeErrorHelper();
}

//=====================================================================================================

#define PIPE_BUFSIZE 0x8000

class CBufferedReadRowStream : implements IReadRowStream, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CBufferedReadRowStream(IEngineRowAllocator * _rowAllocator) : rowAllocator(_rowAllocator)
    {
    }

    virtual bool eos()
    {
        return pipeStream->eos();
    }

    virtual void setStream(ISimpleReadStream * in)
    {
        if (in)
        {
            pipeStream.setown(createSimpleSerialStream(in, PIPE_BUFSIZE));
            rowSource.setStream(pipeStream);
        }
        else
        {
            rowSource.setStream(NULL);
            pipeStream.clear();
        }
    }

protected:
    Owned<ISerialStream> pipeStream;
    CThorStreamDeserializerSource rowSource;
    IEngineRowAllocator * rowAllocator;
};

class CReadRowBinaryStream : public CBufferedReadRowStream
{
public:
    CReadRowBinaryStream(IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * _rowDeserializer)
        : CBufferedReadRowStream(_rowAllocator), rowDeserializer(_rowDeserializer)
    {
    }

    virtual const void * next()
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t size = rowDeserializer->deserialize(rowBuilder, rowSource);
        return rowBuilder.finalizeRowClear(size);
    }

private:
    IOutputRowDeserializer * rowDeserializer;
};

class CReadRowCSVStream : extends CBufferedReadRowStream
{
public:
    CReadRowCSVStream(IEngineRowAllocator * _rowAllocator, ICsvToRowTransformer * _csvTransformer)
        : CBufferedReadRowStream(_rowAllocator), csvTransformer(_csvTransformer)
    {
        ICsvParameters * csvInfo = csvTransformer->queryCsvParameters();
        //MORE: This value is never used.  Should it be asserting(headerLines == 0)
        unsigned int headerLines = csvInfo->queryHeaderLen();
        size32_t max = csvInfo->queryMaxSize();

        const char * quotes = NULL;
        const char * separators = NULL;
        const char * terminators = NULL;
        const char * escapes = NULL;
        csvSplitter.init(csvTransformer->getMaxColumns(), csvInfo, quotes, separators, terminators, escapes);
    }

    virtual const void * next()
    {
        size32_t maxRowSize = 10*1024*1024; // MORE - make configurable
        unsigned thisLineLength = csvSplitter.splitLine(pipeStream, maxRowSize);
        if (thisLineLength)
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned thisSize;
            unsigned __int64 fpos=0;
            thisSize = csvTransformer->transform(rowBuilder, csvSplitter.queryLengths(), (const char * *)csvSplitter.queryData(), fpos);
            pipeStream->skip(thisLineLength);
            if (thisSize)
                return rowBuilder.finalizeRowClear(thisSize);
        }
        return nullptr;
    }

private:
    ICsvToRowTransformer * csvTransformer;
    CSVSplitter csvSplitter;    
};


class CReadRowXMLStream : implements IReadRowStream, implements IXMLSelect, implements IThorDiskCallback, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CReadRowXMLStream(IEngineRowAllocator * _rowAllocator, IXmlToRowTransformer * _xmlTransformer, const char * _iteratorPath, unsigned _pipeFlags)
        : rowAllocator(_rowAllocator), xmlTransformer(_xmlTransformer), iteratorPath(_iteratorPath), pipeFlags(_pipeFlags)
    {
    }

    virtual void setStream(ISimpleReadStream * _in)
    {
        in.set(_in);
        bool noRoot = (pipeFlags & TPFreadnoroot) != 0;
        bool useContents = (pipeFlags & TPFreadusexmlcontents) != 0;
        if (in)
            xmlParser.setown(createXMLParse(*in, iteratorPath, *this, noRoot?ptr_noRoot:ptr_none, useContents));
        else
            xmlParser.clear();
    }

    //iface IXMLSelect
    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        lastMatch.set(&entry);
    }

    virtual bool eos()
    {
        return !ensureNext();
    }


    virtual const void * next()
    {
        for (;;)
        {
            if (!ensureNext())
                return NULL;
            
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned sizeGot = xmlTransformer->transform(rowBuilder, lastMatch, this);
            lastMatch.clear();
            if (sizeGot)
                return rowBuilder.finalizeRowClear(sizeGot);
        }
    }

    bool ensureNext()
    {
        while (!lastMatch && xmlParser)
        {
            if (!xmlParser->next())
                return false;
        }
        return lastMatch != NULL;
    }
            

//interface IThorDiskCallback
    virtual unsigned __int64 getFilePosition(const void * row) { return 0; }
    virtual unsigned __int64 getLocalFilePosition(const void * row) { return 0; }
    virtual const char * queryLogicalFilename(const void * row) { return ""; }
    virtual const byte * lookupBlob(unsigned __int64 id) override { throwUnexpected(); }


private:
    IXmlToRowTransformer * xmlTransformer;
    IEngineRowAllocator * rowAllocator;
    Owned<ISimpleReadStream> in;
    Owned<IXMLParse> xmlParser;
    Owned<IColumnProvider> lastMatch;
    StringAttr iteratorPath;
    unsigned pipeFlags;
};

IReadRowStream *createReadRowStream(IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * _rowDeserializer, IXmlToRowTransformer * _xmlTransformer, ICsvToRowTransformer * _csvTransformer, const char * iteratorPath, unsigned pipeFlags)
{
    if (_xmlTransformer)
        return new CReadRowXMLStream(_rowAllocator, _xmlTransformer, iteratorPath, pipeFlags);
    else if (_csvTransformer)
        return new CReadRowCSVStream(_rowAllocator, _csvTransformer);
    else
        return new CReadRowBinaryStream(_rowAllocator, _rowDeserializer);
}

//=====================================================================================================

// MORE - should really split into three implementations - XML, CSV and RAW
class THORHELPER_API CPipeWriteXformHelper : implements IPipeWriteXformHelper, public CInterface //Transforms output before being written to pipe. Currently CSV and XML output supported
{
    CSVOutputStream csvWriter;
    IHThorCsvWriteExtra * csvWriterExtra;
    IHThorXmlWriteExtra * xmlWriterExtra;
    IOutputRowSerializer *rawSerializer;
    StringBuffer header;
    StringBuffer footer;
    StringBuffer rowTag;
    unsigned flags;
public:
    CPipeWriteXformHelper(unsigned _flags, IHThorXmlWriteExtra * _xmlWriterExtra, IHThorCsvWriteExtra * _csvWriterExtra, IOutputRowSerializer *_rawSerializer)
        : flags(_flags), xmlWriterExtra(_xmlWriterExtra), csvWriterExtra(_csvWriterExtra), rawSerializer(_rawSerializer) {};
    IMPLEMENT_IINTERFACE;

    virtual void writeHeader(IPipeProcess * pipe)
    {
        if (header.length())
            pipe->write(header.length(),header.str());
    }
    virtual void writeFooter(IPipeProcess * pipe)
    {
        if (footer.length())
            pipe->write(footer.length(),footer.str());
    }
    virtual void ready()
    {
        if (flags & TPFwritexmltopipe)
        {
            assertex(xmlWriterExtra);
            OwnedRoxieString xmlpath(xmlWriterExtra->getXmlIteratorPath());
            if (!xmlpath)
                rowTag.append("Row");
            else
            {
                const char *path = xmlpath;
                if (*path == '/') 
                    path++;
                if (strchr(path, '/')) 
                    UNIMPLEMENTED;              // more what do we do with /mydata/row
                rowTag.append(path);
            }

            //getHeader/footer can return a tag name, or NULL (indicates to use the default tag), or "" (do not use header/footer)
            if (!(flags & TPFwritenoroot))
            {
                OwnedRoxieString hdr(xmlWriterExtra->getHeader());
                if (hdr == NULL)
                    header.append("<Dataset>\n");
                else
                    header.append(hdr);

                OwnedRoxieString ftr(xmlWriterExtra->getFooter());
                if (ftr == NULL) 
                    footer.append("</Dataset>\n");
                else
                    footer.append(ftr);
            }
        }
        else if (flags & TPFwritecsvtopipe)
        {
            assertex(csvWriterExtra);
            ICsvParameters * csv = csvWriterExtra->queryCsvParameters(); 
            csvWriter.init(csv, false);
            OwnedRoxieString hdr(csv->getHeader());
            if (hdr) 
            {
                csvWriter.beginLine();
                csvWriter.writeHeaderLn(strlen(hdr), hdr);
                header.append(csvWriter.str());
            }

            OwnedRoxieString ftr(csv->getFooter());
            if (ftr) 
            {
                csvWriter.beginLine();
                csvWriter.writeHeaderLn(strlen(ftr), ftr);//MORE: no writeFooterLn method, is writeHeaderLn ok?
                footer.append(csvWriter.str());
            }
        }
    }

    virtual void writeTranslatedText(const void * row, IPipeProcess * pipe)
    {
        if (xmlWriterExtra)
        {
            CommonXmlWriter xmlWriter(xmlWriterExtra->getXmlFlags());
            xmlWriter.outputBeginNested(rowTag, false);
            xmlWriterExtra->toXML((const byte *)row, xmlWriter);
            xmlWriter.outputEndNested(rowTag);
            pipe->write(xmlWriter.length(), xmlWriter.str());
        }
        else if (csvWriterExtra)
        {
            csvWriter.beginLine();
            csvWriterExtra->writeRow((const byte *)row, &csvWriter);
            csvWriter.endLine();
            pipe->write(csvWriter.length(), csvWriter.str());
        }
        else
        {
            MemoryBuffer myBuff;
            CThorDemoRowSerializer serializerTarget(myBuff);
            rawSerializer->serialize(serializerTarget, (const byte *) row);
            pipe->write(myBuff.length(), myBuff.toByteArray());
        }
    }
};

extern THORHELPER_API IPipeWriteXformHelper *createPipeWriteXformHelper(unsigned _flags, IHThorXmlWriteExtra * _xmlWriterExtra, IHThorCsvWriteExtra * _csvWriterExtra, IOutputRowSerializer *_rawSerializer)
{
    return new CPipeWriteXformHelper(_flags, _xmlWriterExtra, _csvWriterExtra, _rawSerializer);
}
//=====================================================================================================

