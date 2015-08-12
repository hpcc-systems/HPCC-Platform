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

r1 := record
  data8 d;
  end;

dr1 := dataset('fred',r1,flat);
dr2 := dataset('ethel',r1,flat);

r1 proj(r1 l,r1 r) := transform
self := l;
  end;

j := join(dr1,dr2,(data2)left.d=(data2)right.d AND left.d[3] =
right.d[3], proj(left,right));

output(j)