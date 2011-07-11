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

#option ('globalFold', false);
cast(x) := (string6)x;
y := TRANSFER(x'12345d', DECIMAL5);
cast(y);

(string6)y;

(string6)(TRANSFER(x'12345a', DECIMAL5));
(string6)(TRANSFER(x'12345b', DECIMAL5));
(string6)(TRANSFER(x'12345c', DECIMAL5));
(string6)(TRANSFER(x'12345d', DECIMAL5));
(string6)(TRANSFER(x'12345e', DECIMAL5));
(string6)(TRANSFER(x'12345f', DECIMAL5));

(string6)(TRANSFER(x'12345a', DECIMAL5)) = ' 12345';
(string6)(TRANSFER(x'12345b', DECIMAL5)) = '-12345';
(string6)(TRANSFER(x'12345c', DECIMAL5)) = ' 12345';
(string6)(TRANSFER(x'12345d', DECIMAL5)) = '-12345';
(string6)(TRANSFER(x'12345e', DECIMAL5)) = ' 12345';
(string6)(TRANSFER(x'12345f', DECIMAL5)) = ' 12345';
