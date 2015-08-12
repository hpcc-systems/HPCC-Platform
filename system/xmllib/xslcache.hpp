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

#ifndef XSLCACHE_HPP_INCL
#define XSLCACHE_HPP_INCL

#include "jliball.hpp"
#include "xslprocessor.hpp"

//Default cachetimeout 12 hours
#define XSLT_DEFAULT_CACHETIMEOUT 43200

enum IO_Type 
{
    IO_TYPE_FILE = 0,
    IO_TYPE_BUFFER = 1
};

interface IXslBuffer : public IInterface
{
public:
    virtual void compile() = 0;
    virtual bool isCompiled() const = 0;
    virtual IO_Type getType() = 0;
    virtual const char* getFileName() = 0;
    virtual char* getBuf() = 0;
    virtual int getLen() = 0;
    virtual StringArray& getIncludes() = 0;
    virtual const char *getCacheId()=0;
};

interface IXslCache : public IInterface
{
public:
    virtual IXslBuffer* getCompiledXsl(IXslBuffer* xslbuffer, bool replace) = 0;
    virtual void setCacheTimeout(int timeout) = 0;
};

IXslCache* getXslCache();
IXslCache* getXslCache2();

#endif
