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

sillyrec := record
string25 name;
string200 contents;
end;

ds := dataset('~thor::updatetest::datafile', sillyrec, thor);

ds1 := dataset([{'Megan', 'Baltimore'}], sillyrec);
ds2 := sort(ds, name) + sort(ds1, name);
output(ds2,, '~thor::updatetest::datafile', overwrite);
// Results in crc error reading the file, and the contents are then
// two copies of the "Megan", "Baltimore" data
/*
 // Code to create the original data set
ds := dataset([{'Bruce', 'Germantown QW'}], sillyrec);

output(ds,,'updatetest::datafile', overwrite);
*/