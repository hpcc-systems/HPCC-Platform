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
person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, string40 per_last_name}, thor);
daterng := '198101200106';
(integer)(daterng[5..6]);
output(person,{(integer)(daterng[5..6])});

#IF (daterng[-1..5] = '19810')
'Correct';
#ELSE
'Incorrect';
#END

#IF ((ebcdic string)daterng[-1..5] = (ebcdic string)'19810')
'Correct';
#ELSE
'Incorrect';
#END

