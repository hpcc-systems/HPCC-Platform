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
