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


nameRecord := RECORD
string10            forename;
string10            surname;
unsigned4           age;
              END;

nameDataset := DATASET('in.d00', nameRecord, FLAT);


familyRecord := RECORD
string10            surname;
unsigned1           numPeople;
string10            forenames,dim(self.numPeople);
                END;

outRecord gatherFamily(familyRecord l, namesRecord r, unsigned c) :=
                TRANSFORM
                    SELF.surname := l.surname);
                    SELF.numPeople := l.numPeople+1;
                    SELF.forenames[c] := r.forename;
                END;

families := DENORMALIZE(nameDataset, surname, normalizeAddresses(LEFT, RIGHT, COUNTER));

output(families,,'out.d00');

