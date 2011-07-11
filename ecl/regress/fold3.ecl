/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
