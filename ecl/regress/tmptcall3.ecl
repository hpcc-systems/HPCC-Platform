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
// Everything in MODULE scope (Shared or Exported) works

SHARED MyStringLib := SERVICE
   Integer4 TestExternalFunc(Integer4 x) :
            pure,c,library='dab',entrypoint='rtlTestExternalFunc';
END;

SHARED integer myfunc(x) := 2 * x;

SHARED macro1(x) := MACRO
  x * 2
ENDMACRO;

EXPORT MyStringLibEx := SERVICE
   Integer4 TestExternalFunc(Integer4 x) :
            pure,c,library='dab',entrypoint='rtlTestExternalFunc';
END;

EXPORT integer myfuncEx(x) := 2 * x;

EXPORT macro1Ex(x) := MACRO
  x * 2
ENDMACRO;

s1 := 'ECL call shared external func: ' + MyStringLib.TestExternalFunc(3);
s2 := 'ECL call shared ECL func: ' + myfunc(3);
s3 := 'ECL call shared macro: ' + macro1(3);

s1x := 'ECL call exported external func: ' + MyStringLibEx.TestExternalFunc(3);
s2x := 'ECL call exported ECL func: ' + myfuncEx(3);
s3x := 'ECL call exported macro: ' + macro1Ex(3);

s1;
s2;
s3;
s1x;
s2x;
s3x;

LOADXML('<xml>dummy</xml>');

#DECLARE(s1,s2,s3,s1x,s2x,s3x)

#SET(s1,   'TMPT call shared external func: ' + MyStringLib.TestExternalFunc(3))
#SET(s2, 'TMPT call Shared ECL func: ' + myfunc(3))
#SET(s3, 'TMPT call Shared ECL macro: ' + macro1(3))


#SET(s1x, 'TMPT call exported external func: ' + MyStringLibEx.TestExternalFunc(3))
#SET(s2x, 'TMPT call exported ECL func: ' + myfuncEx(3))
#SET(s3x, 'TMPT call exported ECL macro: ' + macro1Ex(3))

%'s1'%;
%'s2'%;
%'s3'%;
%'s1x'%;
%'s2x'%;
%'s3x'%;

