/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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

