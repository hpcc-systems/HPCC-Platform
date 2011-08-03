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
// Everything in local scope works

MyStringLib := SERVICE
   integer4 TestExternalFunc(integer4 x) :
            pure,C,library='dab',entrypoint='rtlTestExternalFunc';
END;

integer myfunc(x) := 2 * x;

macro1(x) := MACRO
  x * 2
ENDMACRO;

s1 := 'ECL call local external func: ' + MyStringLib.TestExternalFunc(3);
s2 := 'ECL call local ECL func: ' + myfunc(3);
s3 := 'ECL call local macro: ' + macro1(3);


s1;
s2;
s3;

LOADXML('<xml>dummy</xml>');

#DECLARE(s1,s2,s3,s1x,s2x,s3x)

#SET(s1, 'TMPT call local external func: ' + MyStringLib.TestExternalFunc(3))
#SET(s2, 'TMPT call local ECL func: ' + myfunc(3))
#SET(s3, 'TMPT call local ECL macro: ' + macro1(3))

%'s1'%;
%'s2'%;
%'s3'%;
