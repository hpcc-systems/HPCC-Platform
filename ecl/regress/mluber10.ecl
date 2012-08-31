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
andsearch := 'OSAMA;BIN;LADEN';

MAC_String2Set(a, b, c, outset) :=
MACRO
set of string30 outset := [a, b, c];
ENDMACRO;

MAC_String2Set(andsearch[1..StringLib.StringFind(andsearch, ';', 1)-1],
                         andsearch[StringLib.StringFind(andsearch, ';', 1)+1..
                                   StringLib.StringFind(andsearch, ';', 2)-1],
                         andsearch[StringLib.StringFind(andsearch, ';', 2)+1..],
                         outAnd)

output(outAnd[1]);
output(outAnd[2]);
output(outAnd[3]);
