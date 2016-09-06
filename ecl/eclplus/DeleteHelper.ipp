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
#ifndef DELETEHELPER_HPP
#define DELETEHELPER_HPP

#include "jlib.hpp"
#include "eclplus.hpp"
#include "formattypes.ipp"
#include "ws_workunits.hpp"

class DeleteHelper : implements IEclPlusHelper
{
public:
    DeleteHelper(IProperties * globals, IFormatType * format);
    virtual ~DeleteHelper();

    virtual bool doit(FILE * fp);

protected:
    Owned<IProperties> globals;
    Owned<IFormatType> format;
    Owned<IClientWsWorkunits> wuclient;
};

class AbortHelper : implements IEclPlusHelper
{
public:
    AbortHelper(IProperties * globals, IFormatType * format);
    virtual bool doit(FILE * fp);

protected:
    Owned<IProperties> globals;
    Owned<IFormatType> format;
    Owned<IClientWsWorkunits> wuclient;
};
#endif // DELETEHELPER_HPP
