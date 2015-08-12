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

subRec := record
big_endian unsigned4 id;
packed integer2 score;
        end;

rec := record
unsigned6    did;
subRec       primary;
string       unkeyable;
subRec       secondary;
        end;


unsigned4 searchId := 0 : stored('searchId');
unsigned4 secondaryScore := 0 : stored('secondaryScore');

rawfile := dataset('~thor::rawfile', rec, THOR, preload);

// Combine matches
filtered := rawfile(
 keyed(secondary.id = searchId),
 keyed(primary.score = secondaryScore)
);

output(filtered)
