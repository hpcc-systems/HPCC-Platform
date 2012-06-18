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
// Test calling stuff from template command
// Everything in default module works

LOADXML('<xml></xml>');

export YorN(boolean flag) := MAP(flag = true => 'Y', 'N');
EXPORT testmacro(x) := MACRO
  x*2
ENDMACRO;


s1 := 'ECL call external func: ' + StringLib.getBuildInfo();
s2 := 'ECL call ECL func: ' + YorN(true);
s3 := 'ECL call ECL macro: ' + testmacro(3);

s1;
s2;
s3;


#DECLARE(s1,s2,s3)

#SET(s1, 'TMPT call external func: ' + StringLib.getBuildInfo())

#SET(s2, 'TMPT call ECL func: ' + YorN(true))

#SET(s3, 'TMPT call ECL macro: ' + testmacro(3))

%'s1'%;
%'s2'%;
%'s3'%;
