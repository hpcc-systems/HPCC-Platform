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

#ifndef PACKAGEPROCESS_ERRORS_H
#define PACKAGEPROCESS_ERRORS_H

#include "errorlist.h"

#define PKG_NAME_EXISTS   PKG_PROCESS_ERROR_START
#define PKG_MISSING_PARAM   PKG_PROCESS_ERROR_START+1
#define PKG_DALI_LOOKUP_ERROR    PKG_PROCESS_ERROR_START+2
#define PKG_MISSING_DALI_LOOKUP_IP  PKG_PROCESS_ERROR_START+3
#define PKG_SET_NOT_DEFINED   PKG_PROCESS_ERROR_START+4
#define PKG_ACTIVATE_NOT_FOUND   PKG_PROCESS_ERROR_START+5
#define PKG_DEACTIVATE_NOT_FOUND   PKG_PROCESS_ERROR_START+6
#define PKG_DELETE_NOT_FOUND   PKG_PROCESS_ERROR_START+7

#endif
