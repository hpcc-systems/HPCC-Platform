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
#ifndef QUERYHELPER_HPP
#define QUERYHELPER_HPP

#include "jlib.hpp"
#include "eclplus.hpp"
#include "formattypes.ipp"

static volatile bool ctrlcpressed = false;

bool ControlHandler();

class QueryHelper : implements IEclPlusHelper
{
public:
    IMPLEMENT_IINTERFACE;

    QueryHelper(IProperties * globals, IFormatType * format);
    virtual ~QueryHelper();

    virtual bool doit(FILE * fp);

    bool doSubmitWorkUnit(FILE * fp, const char * wuid, const char* cluster);

protected:
    Owned<IProperties> globals;
    Owned<IFormatType> format;
    Owned<IClientWsWorkunits> wuclient;
};

class RerunHelper : extends QueryHelper
{
public:
    RerunHelper(IProperties * globals, IFormatType * format) : QueryHelper(globals, format) {};
    virtual bool doit(FILE * fp);
};


#endif // QUERYHELPER_HPP
