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

namesTable := dataset('x',namesRecord,FLAT);

ret  := function
abc := ['1','2'];

d := distribute(namesTable, HASH(
#IF ('1' in abc)
surname,
#end
#IF ('2' in abc)
forename,
#end
#IF ('3' in abc)
age,
#end
0));

return d;
end;

output(ret,,'out.d00',overwrite);

#if (0)
//Not supported because #IF are bound early, rather than later

ret2(set of string abc)  := function

//
d := distribute(namesTable, HASH(
#IF ('1' in abc)
surname,
#end
#IF ('2' in abc)
forename,
#end
#IF ('3' in abc)
age,
#end
0));

return d;
end;

output(ret2('2','3',,'out.d00',overwrite);
#end


