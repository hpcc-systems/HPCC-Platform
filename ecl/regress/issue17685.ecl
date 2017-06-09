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
#option ('targetClusterType', 'roxie');

{unsigned2 a,unsigned2 b,unsigned2 c,unsigned2 d,unsigned2 e} t(unsigned cnt) := TRANSFORM
    SELF.a := cnt;
    SELF.b := cnt % 10;
    SELF.c := HASH64(cnt);
    SELF.d := HASH32(cnt) % 10;
    SELF.e := HASH(cnt) % 10;
END;

ds := dataset(200, t(COUNTER));

s1 := SORT(ds, (unsigned8)d,(unsigned8)e,RECORD);
f1 := NOCOMBINE(s1);

s2 := SORT(f1, (unsigned8)d,(unsigned8)e,b,RECORD);
f2 := NOCOMBINE(s2);
s3 := SORTED(f2, (unsigned8)d,(unsigned8)e,b,RECORD, ASSERT,LOCAL);
output(s3);
