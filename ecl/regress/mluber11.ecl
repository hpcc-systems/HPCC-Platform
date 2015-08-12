/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
STRING100 and_terms1 := 'ONE;TWO;THREE';

STRING and1 := StringLib.StringToUpperCase(and_terms1);

MAC_String2Set(a, b, c, outset) :=
MACRO
set of string30 outset := [a, b, c];
ENDMACRO;

MAC_String2Set((and1[1..StringLib.StringFind(and1, ';', 1)-1]),
                (and1[StringLib.StringFind(and1, ';', 1)+1..StringLib.StringFind(and1, ';', 2)-1]),
                (and1[StringLib.StringFind(and1, ';', 2)+1..]),
                         and1Out)

STRING30 test := 'ONE';

output(test IN and1Out);
