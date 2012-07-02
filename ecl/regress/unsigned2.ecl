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

unsigned8 bigUnsigned := 18446744073709551615;

integer1 intTen := 10;
unsigned1 uTen := 10;

bigUnsigned % intTen; // -1
bigUnsigned % uTen; // -1

(unsigned8)bigUnsigned % intTen; // -1
(unsigned8)bigUnsigned % uTen; // -1

bigUnsigned % (unsigned1)intTen; // -1
bigUnsigned % (unsigned1)uTen; // -1

(unsigned8)bigUnsigned % (unsigned1)intTen; // -1 (unsigned8)bigUnsigned % (unsigned1)uTen; // -1

// casting to unsigned8 produces correct result, but casting to any other unsigned does not bigUnsigned % (unsigned8)intTen; // 5 bigUnsigned % (unsigned8)uTen; // 5

(unsigned8)bigUnsigned % (unsigned8)intTen; // 5 (unsigned8)bigUnsigned % (unsigned8)uTen; // 5
