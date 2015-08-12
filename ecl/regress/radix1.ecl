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

rec := record
      string10  key;
      string10  seq;
      string80  fill;
       end;

cvt(string1 x) := (>unsigned1<)x-32;
scale(integer x, string1 y) := (x * 95 + cvt(y));
radix(string10 key) := scale(scale(scale(cvt(key[1]), key[2]), key[3]), key[4]);
divisor := global((95*95*95*95+CLUSTERSIZE-1) DIV CLUSTERSIZE);

in := DATASET('nhtest::terasort1',rec,FLAT);

// radix sort (using distribute then local sort)
d := DISTRIBUTE(in,radix(key) DIV divisor);
s := SORT(d,key,LOCAL,UNSTABLE);

OUTPUT(s,,'terasortrad_out',OVERWRITE);
