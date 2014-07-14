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

import $.setup;
sq := setup.sq('hthor');
//Sample query for pulling across some sample related payload indexes

i1 := index({ string40 forename, string40 surname }, { unsigned4 id }, sq.PersonIndexName);
i2 := index({ unsigned4 id }, { unsigned8 filepos }, sq.PersonIndexName+'ID');

ds1 := i1(KEYED(forename = 'Liz'));
ds2 := JOIN(ds1, i2, KEYED(LEFT.id = RIGHT.id));
ds3 := FETCH(sq.PersonExDs, ds2, RIGHT.filepos);

output(ds1);
output(ds2);
output(ds3);
