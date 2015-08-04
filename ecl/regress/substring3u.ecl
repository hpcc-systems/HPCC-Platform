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
unicode x := U'abc def  ' : stored('x');
unicode y := U'def ghi' : stored('y');

integer one := 1 : stored('one');
integer four := 4 : stored('four');
integer five := 5 : stored('five');
integer six := 6 : stored('six');
integer seven := 7 : stored('seven');
integer ten := 10 : stored('ten');

'The following should be true:';
x[four] = y[four];
y[1..four] = x[five..];
y[1..four] = x[five..ten];
y[1..four] = x[five..seven];
x[five..four] = y[ten..];

'The following should be false:';
x[five..seven] = y[1..five];
x[five..seven] = y[one..five];
