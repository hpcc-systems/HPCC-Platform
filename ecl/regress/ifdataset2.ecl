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
unsigned4 house_id;
unsigned4 person_id;
string20  surname;
string20  forename;
string1   pcode;
    END;

personDataset := DATASET('person',personRecord,FLAT,OPT);
seedDataset := DATASET('person',personRecord,FLAT,OPT);

personRecord t(personRecord l, personRecord r) := TRANSFORM
    SELF := r;
  END;
seeds := JOIN(personDataset, seedDataset, LEFT.pcode = RIGHT.pcode, t(LEFT, RIGHT));

seedPersonDataset := personDataset + seeds;

cond := count(personDataset) > 1000;
Wanted := IF( cond, seedPersonDataset, personDataset);

output(Wanted);
