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

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
unsigned8       filepos{virtual(fileposition)}
            END;

namesTable := dataset('x',namesRecord,FLAT);

i := index(namesTable, { surname, forename, filepos } ,'\\seisint\\person.name_first.key');

string searchNameLow := 'Halliday' : stored('SearchNameLow');
string searchNameHigh := 'Halliday' : stored('SearchNameHigh');

dsx := i(surname >= searchNameLow and surname < searchNameHigh);
x := catch(dsx, skip);
output(x);

string20 searchNameLow20 := 'Halliday' : stored('SearchNameLow20');
string20 searchNameHigh20 := 'Halliday' : stored('SearchNameHigh20');

dsy := i(surname >= searchNameLow20 and surname < searchNameHigh20);
y := catch(dsy, onfail(transform(recordof(dsy), self.surname := FAILMESSAGE; self.filepos := FAILCODE; self := [])));

output(y);

z := catch(dsy, fail('Something awful happened'));

output(z);
