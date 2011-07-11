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
#link ('hthor');
import lib_stringlib;

loadxml('<AA><BB>1</BB><CC>2</CC></AA>');

#DECLARE (x,y)

#SET (x, RANDOM()*0)
#SET (y, stringlib.GetDateYYYYMMDD()[1..4])

%'x'%; '\n';
%'y'%; '\n';
(integer8)RANDOM(); '\n';
(string10)stringlib.GetDateYYYYMMDD(); '\n';
