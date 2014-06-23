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

