/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

outRecord :=
    RECORD
        string name{xpath('Name')};
        unsigned6 id{xpath('ADL')};
        real8 score;
    END;

string myUserName := '' : stored('myUserName');
string myPassword := '' : stored('myPassword');

outRecord genDefault1() := TRANSFORM
    SELF.name := FAILMESSAGE;
    SELF.id := FAILCODE;
    SELF.score := (real8)FAILMESSAGE('ip');
    END;


//Test all the different PERSIST variants.
output(SOAPCALL('ip', 'service', { string20 surname := 'Hawthorn', string20 forename := 'Gavin';}, DATASET(outRecord), PERSIST));
output(SOAPCALL('ip', 'service', { string20 surname := 'Hawthorn', string20 forename := 'Gavin';}, DATASET(outRecord), PERSIST(true)));
output(SOAPCALL('ip', 'service', { string20 surname := 'Hawthorn', string20 forename := 'Gavin';}, DATASET(outRecord), PERSIST(false)));
output(SOAPCALL('ip', 'service', { string20 surname := 'Hawthorn', string20 forename := 'Gavin';}, DATASET(outRecord), PERSIST(0)));
output(SOAPCALL('ip', 'service', { string20 surname := 'Hawthorn', string20 forename := 'Gavin';}, DATASET(outRecord), PERSIST(99)));
