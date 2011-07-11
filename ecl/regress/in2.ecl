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

#option ('globalFold', false);
string1 getcity1(string1 city_char) := if(city_char in
['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R'
,'S','T','U','V','W','X','Y','Z'], city_char, ' ');

string1 getcity2(string1 city_char) := if((string) city_char in
['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R'
,'S','T','U','V','W','X','Y','Z'], city_char, ' ');

getcity1('A');
getcity2('A');
