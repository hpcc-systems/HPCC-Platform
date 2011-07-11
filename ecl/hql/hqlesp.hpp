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
#ifndef HQLESP_HPP
#define HQLESP_HPP

#include "hql.hpp"

extern HQL_API bool retrieveWebServicesInfo(IWorkUnit *workunit, HqlLookupContext & ctx);
extern HQL_API bool retrieveWebServicesInfo(IWorkUnit *workunit, const char * queryText, HqlLookupContext & ctx);
extern HQL_API IPropertyTree * retrieveWebServicesInfo(const char * queryText, HqlLookupContext & ctx);

#endif
