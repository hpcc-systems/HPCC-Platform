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

#option('supportFilterProject', 1);

Simple := RECORD
    INTEGER number;
    STRING1 letter;
    UNSIGNED seg;
END;

ds1 := DATASET([{1,'A',10},{1,'B',10},{3,'C',10},{1,'D',10},{1,'E',10},
               {1,'F',11},{1,'G',11}, {1,'H',11},{1,'I',11},{1,'J',11}], Simple);

ds2 := DATASET([{1,'E',ERROR('Shouldn\'t have read me!')},{1,'J',11}], Simple);

storedTrue := true : stored('storedTrue');
storedFalse := false : stored('storedFalse');

dsa := nofold(ds1);
f1a := dsa(number < 3);
f2a := f1a(storedTrue);
f3a := f2a(letter not in ['G','H','I']);
f4a := table(f3a, { letter, sm := seg*number });
output(f4a);            // Should generate a single filter project activity

dsb := nofold(ds2);
f1b := dsb(number < 3);
f2b := f1b(storedFalse);
f3b := f2b(letter not in ['G','H','I']);
f4b := table(f3b, { letter, sm := seg*number });
output(f4b);            // Should generate a single filter project activity, and short circuited readings


rec := { unsigned id; };
failDataset := if(not storedTrue, dataset([1], rec), fail(rec, 'Side effect called for activity that shouldn\'t have been started'));

//The onCreate() for this dataset will retriee a value from failDataset.  We need to make sure that dependencies for conditional
//activities aren't evaluated, the following should cause a failure if they are.
//May cause interesting problems....
ds3 := DATASET([{1,'A',10},{failDataset[1].id,'B',10}], Simple);
dsc := nofold(ds3);
f1c := dsc(number < 3);
f2c := f1c(storedFalse);
f3c := f2c(letter not in ['G','H','I']);
f4c := table(f3c, { letter, sm := seg*number });
output(f4c);            // Should generate a single filter project activity, and short circuited readings


