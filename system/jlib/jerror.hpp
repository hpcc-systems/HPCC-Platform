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


#ifndef JERROR_HPP
#define JERROR_HPP

#include "jexcept.hpp"
#include "jerrorrange.hpp"

/* Errors generated in jlib */

#define JLIBERR_BadlyFormedDateTime             1000
#define JLIBERR_BadUtf8InArguments              1001
#define JLIBERR_InternalError                   1002

//---- Text for all errors (make it easy to internationalise) ---------------------------

#define JLIBERR_BadlyFormedDateTime_Text        "Badly formatted date/time '%s'"
#define JLIBERR_BadUtf8InArguments_Text         "The utf separators/terminators aren't valid utf-8"

#endif
