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
unsigned            age;
              END;

personRecord := RECORD
nameRecord;
nameRecord          mother;
nameRecord          father;
                END;

testDataset := DATASET('inagg.d00', personRecord, FLAT);


outRecord := RECORD
nameRecord;
nameRecord          parent;
                END;

outRecord normalizeAddresses(namesRecord l) := 
                TRANSFORM
                    SELF.parent.age := IF(l.mother.age > l.father.age, l.mother.age, l.father.age);
                    SELF.parent := l.father;
                    SELF := l;
                END;

normalizedNames := project(testDataset, normalizeAddresses(LEFT));

output(normalizedNames,,'out.d00');