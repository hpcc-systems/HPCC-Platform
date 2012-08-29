/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

