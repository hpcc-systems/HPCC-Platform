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
