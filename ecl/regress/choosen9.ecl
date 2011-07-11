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



idRecord := { unsigned id };

namesRecord := 
            RECORD
string20        name;
dataset(idRecord)   x{maxcount(100)};
dataset(idRecord)   y{maxcount(100)};
            END;

namesTable := nofold(dataset([
        {'Gavin', [1,2,3,4], [6,7,8,9]},
        {'John', [1,2], [6,7]},
        {'Jim', [1,2,3], [6,7]},
        {'Jimmy', [1,2,3,4,5,6,7,8], [6,7]}], namesRecord));


p := project(namesTable, transform(namesRecord, self.x := choosen(nofold(left.x)+nofold(left.y), 5); self.y := []; self := left));

output(p);




id2Record := { string id{maxlength(100)} };

names2Record := 
            RECORD
string20        name;
dataset(id2Record)  x{maxcount(100)};
dataset(id2Record)  y{maxcount(100)};
            END;

names2Table := nofold(dataset([
        {'Gavin', ['1','2bcd','3','4'], ['6','7','8','9']},
        {'John', ['1','2bcd'], ['6','7']},
        {'Jim', ['1','2bcd','3'], ['6','7']},
        {'Jimmy', ['1','2bcd','3','4','5','6','7','8'], ['6','7']}], names2Record));


p2 := project(names2Table, transform(names2Record, self.x := choosen(nofold(left.x)+nofold(left.y), 5); self.y := []; self := left));

output(p2);
