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

namesRecord := RECORD 
string20 surname; 
string10 forename; 
varstring20 vsurname; 
integer2 age := 25; 
END; 

names := dataset([{'Halliday','Gavin','Halliday',10}],namesRecord); 

/*
// don't break the line: may lose the memory leak!
//output(names, { 'ab'='ab', 'ab' = 'ab ', 'ab' = 'ab '[1..3+0], = ', 'ab'[1..2+0] = 'ab '[1..3+0], true },'out.d00'); },'out.d00'); },'out.d00'); },'out.d00'); 
*/

output(names, { 'ab' = 'ab '[1..3+0],}
