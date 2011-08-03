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

import lib_stringlib;

InValid_CITY(string s) :=
    WHICH(s[1]=' ' and length(trim(s))>0,
        stringlib.stringfind('"\'',s[1],1)<>0 and stringlib.stringfind('"\'',s[length(trim(s))],1)<>0,
        stringlib.stringtouppercase(s)<>s,length(trim(s))<>length(trim(stringlib.stringfilter(s,'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\' <>{}[]-^=!+&,./'))),
        ~(length(trim(s)) = 0 or length(trim(s)) between 3 and 4 or length(trim(s)) = 6 or length(trim(s)) between 8 and 9 or length(trim(s)) >= 11));


invalid_city('AA');



