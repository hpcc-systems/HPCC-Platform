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
#ifndef ECLPLUS_HPP
#define ECLPLUS_HPP

#include "jlib.hpp"
#include "jprop.hpp"
#include "workunit.hpp"
#include "eclplus.ipp"
#include "ws_workunits.hpp"

#define SDS_TIMEOUT 10000

enum Commands 
{
    list        = 0,
    dump        = 1,
    nuke        = 2,
    view        = 3,
    execute     = 4,
    checksyntax = 5,
    generateECL = 6
};

interface IResultSetCursor;
interface IResultSetMetaData;

interface IFormatType  : extends IInterface
{
    virtual const char * getDataDelimiter()      = 0;
    virtual const char * getValueSeparator()     = 0;
    virtual const char * getNameValueSeparator() = 0;
    virtual const char * getRecordSeparator()    = 0;
    virtual bool displayNamesHeader()            = 0;
    virtual bool embedNames()                    = 0;
    virtual bool displayRecordNumber()           = 0;
    virtual bool displayQueryNumber()            = 0;
    virtual bool displayKeys()                   = 0;
    virtual bool getDisplayBoolean()             = 0;
    virtual bool isBinary()                      = 0;
    virtual void setStartRowNumber(int num)      = 0;
    virtual int getStartRowNumber()              = 0;
    virtual void printBody(FILE* fp, int len, char* txt) = 0;
    virtual void printHeader(FILE* fp, const char* name) = 0;
    virtual void printFooter(FILE* fp) = 0;
};

extern IEclPlusHelper * createEclPlusHelper(IProperties * globals);
extern IFormatType * createFormatter(IProperties * globals);

IClientWsWorkunits * createWorkunitsClient(IProperties * _globals);

#endif // ECLPLUS_HPP
