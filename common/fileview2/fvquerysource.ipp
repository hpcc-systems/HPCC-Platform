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

#ifndef FVQUERYSOURCE_IPP
#define FVQUERYSOURCE_IPP

#include "fvdatasource.hpp"
#include "dllserver.hpp"
#include "hqlexpr.hpp"
#include "eclhelper.hpp"

#include "fvsource.ipp"
#include "dadfs.hpp"

class QueryDataSource : public PagedDataSource
{
public:
    QueryDataSource(IConstWUResult * _wuResult, const char * _wuid, const char * _username, const char * _password);
    ~QueryDataSource();

    virtual bool init();
    virtual bool loadBlock(__int64 startRow, offset_t startOffset);

protected:
    bool createBrowseWU();
    virtual void improveLocation(__int64 row, RowLocation & location);

protected:
    StringAttr browseWuid;
    StringAttr username;
    StringAttr password;
};


#endif
