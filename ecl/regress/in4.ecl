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

Layout_FormerName := record
 big_endian unsigned integer4 person_id;
 big_endian unsigned integer1 fnm_type;
 string25 fnm_surname;
 string15 fnm_first_name;
 string15 fnm_middle_name;
 string2 fnm_suffix;
end;

FormerName := dataset('former_name', Layout_FormerName, flat);
Output(formername( person_id in[1,10,105,116,117,119,129,135,139,142,146,3,30,46,64,66,7,73,74,75,85,88,9,92,98])); 
