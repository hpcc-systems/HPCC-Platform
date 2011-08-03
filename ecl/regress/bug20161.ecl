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

PSTRING := TYPE
EXPORT string load(string rawdata) := rawdata[2..TRANSFER(rawdata[1], unsigned integer1)];
EXPORT integer8 physicallength(string ecldata) := TRANSFER(LENGTH(ecldata), unsigned integer1) + 1;
EXPORT string store(string ecldata) := TRANSFER(LENGTH(ecldata), string1) + ecldata;
END;

r := { unsigned integer8 id, PSTRING firstname, PSTRING lastname };

ds := dataset([{1,'Gavin','Hawthorn'},{2,'Jim','Peck'}], r);
output(ds);

