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

EXPORT string1 Source_Group(string2 sr) := MAP(sr  IN  ['MA', 'RV', 'ED', 'WD', 'ID', 'MD', 'ND', 'FD', 'OD', 'TD', 'VD', 'CD', 'RD', 'AD', 'AV', 'EV', 'IV', 'XV', 'FV', 'TV', 'MV', 'WV', 'OV', 'SV', 'NV'] => 'A',
     sr  IN  ['JD', 'PD'] => 'B',
     sr  IN  ['MI', 'EQ', 'NC', 'TC', 'TU', 'DE', 'TB', 'GO', 'FA', 'FB', 'AK', 'FF', 'FE'] => ' ',
     sr  IN  ['UT', 'UW', 'MU'] => 'U',
     sr  IN  ['BA'] => 'D',
     sr  IN  ['DC', 'MW'] => 'E',
     'C');

EXPORT string1 Source_Group2(string2 sr) :=
    CASE(sr,
    'MA'=>'A', 'RV'=>'A', 'ED'=>'A', 'WD'=>'A', 'ID'=>'A', 'MD'=>'A', 'ND'=>'A', 'FD'=>'A', 'OD'=>'A', 'TD'=>'A', 'VD'=>'A', 'CD'=>'A', 'RD'=>'A', 'AD'=>'A', 'AV'=>'A', 'EV'=>'A', 'IV'=>'A', 'XV'=>'A', 'FV'=>'A', 'TV'=>'A', 'MV'=>'A', 'WV'=>'A', 'OV'=>'A', 'SV'=>'A', 'NV'=>'A',
    'JD'=>'B', 'PD'=>'B',
    'MI'=>' ', 'EQ'=>' ', 'NC'=>' ', 'TC'=>' ', 'TU'=>' ', 'DE'=>' ', 'TB'=>' ', 'GO'=>' ', 'FA'=>' ', 'FB'=>' ', 'AK'=>' ', 'FF'=>' ', 'FE'=>' ',
    'UT'=>'U', 'UW'=>'U', 'MU'=>'U',
    'BA'=>'D',
    'DC'=>'E', 'MW'=>'E',
    'C');

EXPORT boolean Source_is_XQMZ(string2 src) := Source_Group(src)  IN  ['A', 'B'];



namesRecord :=
            RECORD
string20        surname;
string10        forename;
string2         code;
            END;

namesTable := dataset('x',namesRecord,FLAT);

output(namesTable(Source_is_XQMZ(code)));
