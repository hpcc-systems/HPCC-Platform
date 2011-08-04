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
              END;

personRecord := RECORD
nameRecord;
nameRecord          mother;
nameRecord          father;
                END;

testDataset := DATASET('inagg.d00', testRecord, FLAT);


outRecord :=
            RECORD
unsigned1       kind;
nameRecord;
            END;


outRecord normalizeAddresses(namesRecord l, integer c) :=
                TRANSFORM
                    SELF.kind := c;
                    SELF := CHOOSE(c, l, l.mother, l.father);
                END;

normalizedNames := normalize(testDataset, 3, normalizeAddresses(LEFT, COUNTER));

output(normalizedNames,,'out.d00');