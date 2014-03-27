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



#DECLARE(s1,s2,s3,s1x,s2x,s3x)

#SET(s1, 'TMPT call local external func: ' + MyStringLib.TestExternalFunc(3))
#SET(s2, 'TMPT call local ECL func: ' + myfunc(3))
#SET(s3, 'TMPT call local ECL macro: ' + macro1(3))

%'s1'%;
%'s2'%;
%'s3'%;
