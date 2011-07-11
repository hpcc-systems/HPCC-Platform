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






string10 s10 := '' : stored('s10');
string20 s20 := '' : stored('s20');
string60 s60 := '' : stored('s60');
unicode10 u10 := U'' : stored('u10');


output(trim(s10));
output(trim((string20)s10));
output(trim((string20)s20));
output(trim((string20)s60));

output(trim((string20)u10));
