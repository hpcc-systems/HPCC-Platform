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
