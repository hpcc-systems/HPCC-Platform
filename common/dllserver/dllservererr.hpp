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

#ifndef DLLSERVEERR_HPP
#define DLLSERVEERR_HPP

#define ERR_DSV_FIRST                           9000
#define ERR_DSV_LAST                            9009

#define DSVERR_CouldNotFindDll                  9000
#define DSVERR_DllLibIpMismatch                 9001
#define DSVERR_NoAssociatedLib                  9002
#define DSVERR_LibNotLocal                      9003

//---- Text for all errors (make it easy to internationalise) ---------------------------

#define DSVERR_CouldNotFindDll_Text             "Could not find location of %s"
#define DSVERR_DllLibIpMismatch_Text            "The DLL and Library have a mismatched IP address"
#define DSVERR_NoAssociatedLib_Text             "No Library associated with DLL %s"
#define DSVERR_LibNotLocal_Text                 "Library for %s not copied locally"

#endif
