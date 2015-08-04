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

export DataLib := SERVICE
  string    PreferredFirst(const string scr)  : c, pure, entrypoint='dataCalcPreferredFirst';
END;

layout :=
RECORD
    STRING str;
END;


ds1 := DATASET([{'MARK'}, {'ED'}], layout);
ds2 := DATASET([{'MARK'}, {'ED'}], layout);
s1 := SORT(DISTRIBUTE(ds1, HASH(str)), str, local);
s2 := SORT(DISTRIBUTE(ds2, HASH(str)), str, local);
ss1 := SORTED(s1, str, local);
ss2 := SORTED(s2, str, local);



p1 := SORT(DISTRIBUTE(ds1, HASH(datalib.preferredfirst(str))), datalib.preferredfirst(str), local);
p2 := SORT(DISTRIBUTE(ds2, HASH(datalib.preferredfirst(str))), datalib.preferredfirst(str), local);
sp1 := SORTED(p1, datalib.preferredfirst(str), local);
sp2 := SORTED(p2, datalib.preferredfirst(str), local);

j := JOIN(ss1, ss2, LEFT.str = RIGHT.str, LOCAL);
jp := JOIN(sp1, sp2, datalib.preferredfirst(LEFT.str) = datalib.preferredfirst(RIGHT.str), LOCAL);

//output(j);
output(jp);
