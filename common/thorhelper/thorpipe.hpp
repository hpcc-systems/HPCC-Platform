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

#ifndef __THORPIPERROR_HPP_
#define __THORPIPERROR_HPP_

#ifdef THORHELPER_EXPORTS
 #define THORHELPER_API DECL_EXPORT
#else
 #define THORHELPER_API DECL_IMPORT
#endif

#include "jthread.hpp"
#include "csvsplitter.hpp"

interface IPipeErrorHelper : extends IInterface
{
public:
    virtual void run(IPipeProcess *pipe) = 0;
    virtual void wait() = 0;
    virtual const char * queryErrorOutput() = 0;
};

extern THORHELPER_API IPipeErrorHelper * createPipeErrorHelper();

interface IReadRowStream : public IInterface
{
public:
    virtual const void * next() = 0;
    virtual void setStream(ISimpleReadStream * in) = 0;
    virtual bool eos() = 0;
};

interface IEngineRowAllocator;
interface IOutputRowDeserializer;
interface IXmlToRowTransformer;
interface ICsvToRowTransformer;

interface IPipeWriteXformHelper : extends IInterface
{
    virtual void writeHeader(IPipeProcess * pipe) = 0;
    virtual void writeFooter(IPipeProcess * pipe) = 0;
    virtual void ready() = 0;
    virtual void writeTranslatedText(const void * row, IPipeProcess * pipe) = 0;
};

extern THORHELPER_API IReadRowStream *createReadRowStream(IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * _rowDeserializer, IXmlToRowTransformer * _xmlTransformer, ICsvToRowTransformer * _csvTransformer, const char * iteratorPath, unsigned pipeFlags);
extern THORHELPER_API IPipeWriteXformHelper *createPipeWriteXformHelper(unsigned _flags, IHThorXmlWriteExtra * _xmlWriterExtra, IHThorCsvWriteExtra * _csvWriterExtra, IOutputRowSerializer *_rawSerializer);

#endif /* __THORPIPERROR_HPP_ */
