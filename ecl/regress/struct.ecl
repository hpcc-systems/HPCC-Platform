/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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