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

ppersonRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

pperson1 := DATASET('in.d00', ppersonRecord, THOR);

s1 := sort(pperson1, surname);

d1 := DISTRIBUTE(nofold(s1), HASH(surname)) : PERSIST('per1');

f1 := d1(surname<>'');

s2 := sort(f1, forename);

// output(s1, , 'out1.d00', OVERWRITE);
// output(s2, , 'out1.d00', OVERWRITE);
output(s2);
