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

#ifndef _SYSTEM_ERROR_LIST_HPP__
#define _SYSTEM_ERROR_LIST_HPP__

// Define start and end ranges for error codes that may be thrown, written to a log, or returned in any manner
// create a range that will be big enough

// actual error codes do not belong in this file

#define ERRORID_UNKNOWN         999      // Right now, this value is used to filter out error messages which have 
                                                    // not been verified for well-defined error code and message content. For
                                                    // example, if an error code is not greater than this value, ECLWatch displays 
                                                    // the error as "internal system error".

#define ECL_WARN_START          1000
#define ECL_WARN_END            1099

#define ROXIEMM_ERROR_START     1300

#define ROXIE_ERROR_START      1400   // roxie is already using this start value
#define ROXIE_ERROR_END        1799

#define ROXIE_MGR_START        1800   // used in esp service to communicate / gather info for roxie
#define ROXIE_MGR_END          1999

#define HQL_ERROR_START         2000    // 2000..3999 in hql, 4000..4999 in hqlcpp
#define HQL_ERROR_END           4999

#define WORKUNIT_ERROR_START    5000
#define WORKUNIT_ERROR_END      5099

#define WUWEB_ERROR_START       5500
#define WUWEB_ERROR_END         5599

#define XSLT_ERROR_START       5600
#define XSLT_ERROR_END         5699

#define REMOTE_ERROR_START      8000    // dafilesrv etc - see common/remote/remoteerr.hpp
#define REMOTE_ERROR_END        8099

#define DISPATCH_ERROR_START   9000
#define DISPATCH_ERROR_END     9399

#define QUERYREGISTRY_ERROR_START   9600
#define QUERYREGISTRY_ERROR_END     9799

#define JVM_API_ERROR_START    10000
#define JVM_API_ERROR_END      10499

#define PKG_PROCESS_ERROR_START  11000
#define PKG_PROCESS_ERROR_END    11100

#define ECLWATCH_ERROR_START    20000
#define ECLWATCH_ERROR_END      29999



#endif

