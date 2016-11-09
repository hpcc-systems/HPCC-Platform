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
#ifndef DUMPHELPER_HPP
#define DUMPHELPER_HPP

#include "jlib.hpp"
#include "eclplus.hpp"
#include "formattypes.ipp"

class DumpHelper : implements IEclPlusHelper
{
public:
    DumpHelper(IProperties * globals, IFormatType * format);
    virtual ~DumpHelper();

    virtual bool doit(FILE * fp);

private:
    Owned<IProperties> globals;
    Owned<IFormatType> format;
    Owned<IClientWsWorkunits> wuclient;
};

class GraphHelper : implements IEclPlusHelper
{
public:
    GraphHelper(IProperties * globals, IFormatType * format);
    virtual ~GraphHelper();

    virtual bool doit(FILE * fp);

private:
    Owned<IProperties> globals;
    Owned<IFormatType> format;
    Owned<IClientWsWorkunits> wuclient;
};

class StatsHelper : implements IEclPlusHelper
{
public:
    StatsHelper(IProperties * globals, IFormatType * format);

    virtual bool doit(FILE * fp);

private:
    Owned<IProperties> globals;
    Owned<IFormatType> format;
    Owned<IClientWsWorkunits> wuclient;
};
#endif // DUMPHELPER_HPP
