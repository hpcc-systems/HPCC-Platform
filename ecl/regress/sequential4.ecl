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

#option ('resourceSequential', true);

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('names',namesRecord,FLAT);


d1 := dataset([{'Halliday','Gavin',34}], namesRecord);
d2 := dataset([{'Halliday','Liz',34}], namesRecord);
d3 := dataset([{'Halliday','Abigail',2}], namesRecord);

boolean change := false : stored('change');

o1 := output(d1,,'names',overwrite);
o2 := output(d2,,'names',overwrite);
o2b := output(d3,,'names',overwrite);
o3 := if(change, o2);
o3b := if(false, o2b);
o4 := output(namesTable);

sequential(o1, o3, o3b, o4);
