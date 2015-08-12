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

aaa := DATASET('aaa',{STRING1 fa; }, FLAT);
/*
distibuted_aaa := DISTRIBUTE(aaa, RANDOM());
distibuted_aaax := DISTRIBUTED(aaa, RANDOM());

/*
//OUTPUT(SORT(aaa, fa, LOCAL));
OUTPUT(SORT(distibuted_aaa, fa, LOCAL));
OUTPUT(SORT(distibuted_aaax, fa, LOCAL));
*/
/*
bbb := DATASET('aaa',{STRING1 fb; }, FLAT);

RECORD ResultRec := RECORD
    aaa.fa;
    END;

ResultRec Trans(aaa x, bbb y) := TRANSFORM
    SELF := x;
    END;

r1 := JOIN(aaa, bbb, aaa.fa = bbb.fb, LOCAL);
r2 := SORT(distibuted_aaa, f1, LOCAL);
r3 := SORT(distibuted_aaax, f1, LOCAL);
*/
