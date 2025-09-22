/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

// HPCC-32793

#OPTION('enableClusterHopping', TRUE);

MyLayout := RECORD
    UNSIGNED4   id;
    UNSIGNED4   n;
END;

MakeData(UNSIGNED4 numRows) := DATASET(numRows, TRANSFORM(MyLayout, SELF.id := HASH(COUNTER), SELF.n := RANDOM()), DISTRIBUTED);

ds := MakeData(1000) : INDEPENDENT;

// Assumes BWR submitted to thor named 'mythor1'

x := EVALUATE('mythor2', SORT(ds, id));
y := EVALUATE(ECLAGENT, SORT(ds, n));

OUTPUT(x+y);
