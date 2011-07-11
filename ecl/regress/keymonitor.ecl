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

rec :=
  record
    string2 state;
  end;

rawfile := dataset(dataset([{'FL'}], rec), 'regress::stringkey', THOR, preload);
//output(rawfile,overwrite);

infile := rawfile;

doCompare(other, other2) := macro
#uniquename(o1)
#uniquename(o2)
#uniquename(o3)
#uniquename(o4)
#uniquename(o5)
#uniquename(o6)
#uniquename(o7)
#uniquename(o8)
#uniquename(o9)
%o1% := output(infile(keyed(state = other)));
//%o9% := output(infile(keyed(state = other or state = other2)));
%o2% := output(infile(keyed(state != other)));
%o3% := output(infile(keyed(state >= other)));
%o4% := output(infile(keyed(state <= other)));
%o5% := output(infile(keyed(state > other)));
%o6% := output(infile(keyed(state < other)));
%o7% := output(infile(keyed(state between other and other2)));
%o8% := output(infile(keyed(state not between other and other2)));
sequential(%o1%,%o2%,%o3%,%o4%,%o5%,%o6%,%o7%,%o8%);
endmacro;

doCompareIn(other) := macro
#uniquename(o1)
#uniquename(o2)
#uniquename(o3)
#uniquename(o4)
#uniquename(o5)
#uniquename(o6)
#uniquename(o7)
#uniquename(o8)
#uniquename(o9)
%o1% := output(infile(keyed(state in other)));
%o2% := output(infile(keyed(state not in other)));
sequential(%o1%,%o2%);
endmacro;

string1 compare1a := 'F';
string1 compare1b := 'L';
string2 compare2a := 'FL';
string2 compare2b := 'OH';
string4 compare4a := 'FLOR';
string4 compare4b := 'OHIO';
string4 compare4c := 'FL  ';
string4 compare4d := 'OH  ';

string1 stored1a := 'F' : stored('stored1a');
string1 stored1b := 'L' : stored('stored1b');
string2 stored2a := 'FL' : stored('stored2a');
string2 stored2b := 'OH' : stored('stored2b');
string4 stored4a := 'FLOR' : stored('stored4a');
string4 stored4b := 'LA  ' : stored('stored4b');
string storedxa := 'FL  ' : stored('storedxa');
string storedxb := 'LA  ' : stored('storedxb');

doCompare(compare1a, compare1b);
doCompare(compare2a, compare2b);
doCompare(compare4a, compare4b);
doCompare(compare4c, compare4d);

doCompare(stored1a, stored1b);
doCompare(stored2a, stored2b);
doCompare(stored4a, stored4b);
doCompare(storedxa, storedxb);


set of string1 setcompare1 := ['F','L'];
set of string2 setcompare2 := ['FL','OH'];
set of string4 setcompare4a := ['FLOR','OHIO'];
set of string4 setcompare4b := ['FL  ','OH  '];

set of string1 setstored1 := ['F', 'L'] : stored('setstored1b');
set of string2 setstored2 := ['FL', 'OH'] : stored('setstored2b');
set of string4 setstored4 := ['FLOR', 'LA  '] : stored('setstored4b');
set of string setstoredx := ['FL  ', 'LA  '] : stored('setstoredxb');

doCompareIn(setcompare1);
doCompareIn(setcompare2);
doCompareIn(setcompare4a);
doCompareIn(setcompare4b);

doCompareIn(setstored1);
doCompareIn(setstored2);
doCompareIn(setstored4);
doCompareIn(setstoredx);
