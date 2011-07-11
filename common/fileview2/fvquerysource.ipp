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
    StringAttr cluster;
    StringAttr username;
    StringAttr password;
};


#endif
