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


string headerPrefix := '' : stored('headerPrefix');
string headerSuffix := '' : stored('headerSuffix');
string xmlRowTag := 'row' : stored('xmlRowTag');

d := DATASET('d3', { string x, string y, string z } , FLAT);

output(d,,'o1',csv(heading(headerPrefix)));
output(d,,'o2',csv(heading(headerPrefix, headerSuffix)));
output(d,,'o3',csv(heading(headerPrefix, single)));
output(d,,'o4',csv(heading(headerPrefix, headerSuffix, single)));


output(d,,'x1',xml(heading(headerPrefix)));
output(d,,'x2',xml(heading(headerPrefix, headerSuffix)));
output(d,,'x3',xml(xmlRowTag, heading(headerPrefix, headerSuffix)));
output(d,,'x4',xml(opt, xmlRowTag, heading(headerPrefix, headerSuffix), trim));
