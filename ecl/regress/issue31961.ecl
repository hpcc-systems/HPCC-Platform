/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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



strRec := { string x; };

myRec := RECORD
    STRING a;
    STRING b;
    STRING c;
    STRING d;
    DATASET(strRec) e;
END;


makeDs(unsigned start) := DATASET(100, TRANSFORM(strRec, SELF.x := (STRING)(start + counter)));

myRec t(unsigned cnt) := TRANSFORM
    SELF.a := (STRING)cnt;
    SELF.b := (STRING)HASH32(cnt);
    SELF.c := (STRING)HASH64(cnt);
    SELF.d := (STRING)HASH64(cnt+1);
    SELF.e := makeDs(cnt);
END;

i := DATASET(1000000, t(COUNTER));
s := SORT(i, a, HINT(sortCompBlkSz(1000)), LOCAL);
c := COUNT(NOFOLD(s));
output(c);
