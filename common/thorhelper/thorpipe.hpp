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

#ifndef __THORPIPERROR_HPP_
#define __THORPIPERROR_HPP_

#ifdef _WIN32
 #ifdef THORHELPER_EXPORTS
  #define THORHELPER_API __declspec(dllexport)
 #else
  #define THORHELPER_API __declspec(dllimport)
 #endif
#else
 #define THORHELPER_API
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
