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

//Test combining funnels

householdRecord := RECORD
unsigned4 house_id;
string20  address1;
string20  zip;
string2   st;
    END;


householdDataset := DATASET('household',householdRecord,FLAT);

x := householdDataset + householdDataset(st = 'NY') + householdDataset + householdDataset(st = 'FL');

output(x,,'out1.d00');

x2 := (householdDataset & householdDataset(st = 'NY')) + (householdDataset & householdDataset(st = 'FL'));
output(x2,,'out2.d00');

x3 := (householdDataset & householdDataset(st = 'NY')) & (householdDataset & householdDataset(st = 'FL'));
output(x3,,'out3.d00');

x4 := householdDataset & householdDataset(st = 'NY') & householdDataset + householdDataset(st = 'FL');
output(x4,,'out4.d00');

x5 := (householdDataset && householdDataset(st = 'NY')) && (householdDataset && householdDataset(st = 'FL'));
output(x5,,'out5.d00');
