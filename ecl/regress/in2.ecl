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
string1 getcity1(string1 city_char) := if(NOFOLD(city_char) in
['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R'
,'S','T','U','V','W','X','Y','Z'], city_char, ' ');

string1 getcity2(string1 city_char) := if(NOFOLD((string) city_char) in
['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R'
,'S','T','U','V','W','X','Y','Z'], city_char, ' ');

getcity1('A');
getcity2('A');
