/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

namesRecord :=
            RECORD
string      txt;
            END;

ds1 := nofold(dataset([],namesRecord));
output(ds1, named('ds'), extend);

ds2 := dataset(nofold([]),namesRecord);
output(ds2, named('ds'), extend);

ds3 := DATASET(NOFOLD([]), {string txt});
output(ds3,named('ds'),extend);

ds4 := dataset(NOFOLD(ALL), {string txt});
output(ds4, named('ds'), extend);
