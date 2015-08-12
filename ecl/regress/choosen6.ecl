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

fooSet := DATASET([{1},{2},{3},{4}], {INTEGER1 x});

OUTPUT(fooSet);

s2 := CHOOSEN(fooSet, 7, 6);
COUNT(s2); // zero records as expected

s3 := CHOOSEN(fooSet, 7, 2);
OUTPUT(s3); // two records as expected

s4 := CHOOSEN(fooSet, ALL, 2);
OUTPUT(s4); // No records?  Why???
COUNT(s4);


fooSet2 := DATASET([{1},{2},{3},{4},{5}], {INTEGER1 x});

output(fooSet2[6..]);
output(fooSet2[2..9]);
output(fooSet2[2..]);
output(fooSet2[..3]);
