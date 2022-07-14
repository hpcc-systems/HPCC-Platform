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


import Std.Str;

string MyFunc(string s) := DEFINE MAP(
    REGEXFIND('--', s) => Str.FindReplace(s, '--', '-'),
    REGEXFIND(' - ', s) => Str.FindReplace(s, ' - ', ' '),
    s);

string MyFunc2(string s) := DEFINE FUNCTION
    first := s[1];
    last := s[LENGTH(s)-1];
    RETURN first + last + last + first;
END;

string text1 := 'abc -- def -- g - hi' : stored('text1');
string text2 := 'abc - def - ghi' : stored('text2');


ordered(
    output(MyFunc(text1));
    output(MyFunc(text2));
    output(MyFunc2(text1));
);
