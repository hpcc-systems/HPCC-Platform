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

personRecord := RECORD,maxlength(999)
string20            surname;
ebcdic string20     forename;
unsigned2           age;
boolean             hasPhone;
                    ifblock(self.hasPhone)
string                  phone;
                    END;
                END;


addressRecord := record
integer4            id;
personRecord        primaryName;
                    ifblock(self.primaryName.age = 0)
unsigned8               dob;
                    end;
dataset(personRecord) secondary{maxcount(10)};
                 end;


addressTable := dataset('address', addressRecord, thor);
addressIndex := index(addressTable, { id }, { addressTable }, 'addressIndex');

BUILDINDEX(addressIndex);
