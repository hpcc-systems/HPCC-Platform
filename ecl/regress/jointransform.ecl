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


nameRecord :=
            RECORD
string20        surname;
string10        forename;
            END;


leftRecord := RECORD
    unsigned4   uid;
    nameRecord  name;
    unsigned    age;
END;

rightRecord := RECORD
    unsigned4   uid;
    nameRecord  name;
    string      address;
    udecimal8   dob;
END;

leftDs := DATASET('people', leftRecord, thor);
rightDs := DATASET('addresses', rightRecord, thor);

output(join(leftDs, rightDs, LEFT.uid = RIGHT.uid, full only));
