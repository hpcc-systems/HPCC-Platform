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

#ifndef _THIRDPARTY_H_
#define _THIRDPARTY_H_

#ifdef _DEBUG
// When creating debug builds for developer use, you can disable the use of varions third party libraries using the flags below
// The main purpose of this would be for use on a platform or machine where the appropriate third-party support has not been installed
// DO NOT release any version of this file where any of the followinf are uncommented without careful consideration
// DO NOT define any of these outside of the _DEBUG section without even more careful consideration

//#define _NO_MYSQL     // Allow system to build without mysql client library support
//#define _NO_SYBASE        // Allow system to build without sybaseclient library support
//#define _NO_SAMI      // Allow system to build without SAMI/Agentxx etc

#endif

#define NO_LINUX_SSL

#endif //_THIRDPARTY_H_

