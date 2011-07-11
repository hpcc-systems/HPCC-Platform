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

Simple := RECORD
    INTEGER number;
    STRING1 letter;
    UNSIGNED seg;
END;

ds1 := DATASET([{1,'A',10},{1,'B',10},{3,'C',10},{1,'D',10},{1,'E',10},
               {1,'F',11},{1,'G',11}, {1,'H',11},{1,'I',11},{1,'J',11}], Simple);

ds2 := DATASET([{1,'E',ERROR('Shouldn\'t have read me, but THOR may find it hard not to!')},{1,'J',11}], Simple);

storedTrue := true : stored('storedTrue');
storedFalse := false : stored('storedFalse');

//Even nastier (esp. for thor).  Ensure that a side-effect in another child graph isn't evaluated if the condition isn't going to be met!
//sort,many is there to try and ensure it is split by a heavyweigth activity
dsa := nofold(sort(nofold(ds2), number, many));
dsax := nofold(sort(nofold(dsa), letter, seg, many));
f1a := dsax(number < 3);
f2a := f1a(storedFalse);
f3a := f2a(letter not in ['G','H','I']);
f4a := table(f3a, { letter, sm := seg*number });
output(f4a);            // Should generate a single filter project activity

rec := { unsigned id; };
failDataset := if(not storedTrue, dataset([1], rec), fail(rec, 'Side effect called for activity that shouldn\'t have been started'));

ds3 := DATASET([{1,'A',10},{failDataset[1].id,'B',10}], Simple);
dsb := nofold(sort(nofold(ds3), number, many));
dsbx := nofold(sort(nofold(dsb), letter, seg, many));
f1b := dsbx(number < 3);
f2b := f1b(storedFalse);
f3b := f2b(letter not in ['G','H','I']);
f4b := table(f3b, { letter, sm := seg*number });
output(f4b);            // Should generate a single filter project activity

