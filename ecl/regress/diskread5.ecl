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

personRecord := RECORD
//unsigned8 __filepos__{virtual(fileposition)};
unsigned4 house_id;
unsigned4 person_id;
string20  surname;
string20  forename;
    END;

personDataset := DATASET('person',personRecord,FLAT);


r := RECORD
 personDataset;
 unsigned4 id := 0;
 END;

r ta(personDataset le) := TRANSFORM
        SELF.id := 0;
        SELF := le;
    END;

a := project(hint(personDataset,tigerwoods), ta(LEFT),hint(gogogo(99)));

b := a(surname > 'Hawthorn');

output(b,,'out.d00');
output(hint(personDataset,outputonlyOnce),,'out.d00');