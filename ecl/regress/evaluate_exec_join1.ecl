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

ds1 := NOFOLD(MakeData(10000));
ds2 := NOFOLD(MakeData(1000));

JoinData(DATASET(MyLayout) d1, DATASET(MyLayout) d2) := JOIN
    (
        d1,
        d2,
        LEFT.id = RIGHT.id,
        TRANSFORM
            (
                {
                    UNSIGNED4   id,
                    UNSIGNED4   n1,
                    UNSIGNED4   n2
                },
                SELF.id := LEFT.id,
                SELF.n1 := LEFT.n,
                SELF.n2 := RIGHT.n
            )
    );

OUTPUT(EVALUATE('mythor1', JoinData(ds1, ds2)), NAMED('mythor1'));
OUTPUT(EVALUATE('mythor2', JoinData(ds1, ds2)), NAMED('mythor2'));
OUTPUT(EVALUATE(ECLAGENT, JoinData(ds1, ds2)), NAMED('ecl_agent'));
