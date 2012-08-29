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

rec :=      RECORD
string          userid;
string          password;
unsigned        purpose{xpath('glb-purpose')};
string          xxfunction;
unsigned4       version;
boolean         verifySSNS{xpath('verify-ssns')};
decimal15       ssn;
boolean         bankruptcy;
boolean         blind;
boolean         includeDayOfBirth{xpath('include-day-of-birth')};
            END;


test := dataset('~file::127.0.0.1::temp::bpssearch.log', rec, XML('query', NOROOT));
output(choosen(test,100));

