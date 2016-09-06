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
cast(x) := (string6)x;
y := TRANSFER(x'12345d', DECIMAL5);
cast(y);

(string6)y;

(string6)(TRANSFER(x'12345a', DECIMAL5));
(string6)(TRANSFER(x'12345b', DECIMAL5));
(string6)(TRANSFER(x'12345c', DECIMAL5));
(string6)(TRANSFER(x'12345d', DECIMAL5));
(string6)(TRANSFER(x'12345e', DECIMAL5));
(string6)(TRANSFER(x'12345f', DECIMAL5));

(string6)(TRANSFER(NOFOLD(x'12345a'), DECIMAL5));
(string6)(TRANSFER(NOFOLD(x'12345b'), DECIMAL5));
(string6)(TRANSFER(NOFOLD(x'12345c'), DECIMAL5));
(string6)(TRANSFER(NOFOLD(x'12345d'), DECIMAL5));
(string6)(TRANSFER(NOFOLD(x'12345e'), DECIMAL5));
(string6)(TRANSFER(NOFOLD(x'12345f'), DECIMAL5));

(string6)(TRANSFER(x'12345a', DECIMAL5)) = ' 12345';
(string6)(TRANSFER(x'12345b', DECIMAL5)) = '-12345';
(string6)(TRANSFER(x'12345c', DECIMAL5)) = ' 12345';
(string6)(TRANSFER(x'12345d', DECIMAL5)) = '-12345';
(string6)(TRANSFER(x'12345e', DECIMAL5)) = ' 12345';
(string6)(TRANSFER(x'12345f', DECIMAL5)) = ' 12345';
