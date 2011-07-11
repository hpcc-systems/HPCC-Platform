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


namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable2 := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);

string4 _FILTER_LIMIT_i := '2' : stored('filterLimit');  // not for filter
unsigned4 _FILTER_LIMIT := IF((unsigned4)_FILTER_LIMIT_i=0,3,(unsigned4)_FILTER_LIMIT_i);   // not for filter

output(_FILTER_LIMIT_i);
output(_FILTER_LIMIT);
output(choosen(namesTable2, _FILTER_LIMIT));
