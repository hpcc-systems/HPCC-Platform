/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#ifndef LISTHELPER_HPP
#define LISTHELPER_HPP

#include "jlib.hpp"
#include "eclplus.hpp"
#include "formattypes.ipp"

class ListHelper : implements IEclPlusHelper
{
public:
    IMPLEMENT_IINTERFACE;

    ListHelper(IProperties * globals, IFormatType * format);
    virtual ~ListHelper();

    virtual bool doit(FILE * fp);

private:
    StringBuffer &doFormat(StringBuffer &s, IConstECLWorkunit * w);

    Owned<IProperties> globals;
    Owned<IFormatType> format;
    Owned<IClientWsWorkunits> wuclient;
};

#endif // LISTHELPER_HPP
