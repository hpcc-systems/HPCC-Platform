/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

// Testing code generation of the various forms of PARTITION/BLOOM

rec := RECORD
   string1 a;
   string3 b;
   string1 c;
   unsigned d;
END;

boolean useBloom := true : stored('useBloom');

d := dataset('fred', rec, FLAT);

buildindex(d, {a, b}, { c },'ix1', overwrite, PARTITION(a,b), BLOOM(b));
buildindex(d, {a, b => c },'ix2', overwrite, PARTITION(a,b), BLOOM(b));
buildindex(d, ,'ix3', overwrite, PARTITION(a,b), BLOOM(b));

i1 := index(d, {a,b,c,d}, 'index1');
i2 := index(d, {a,b,c,d}, 'index2', PARTITION(a,b), BLOOM(c));

buildindex(i1, overwrite, PARTITION(c), BLOOM(a, b));  // NOTE - should use values from BUILD
buildindex(i2, overwrite, PARTITION(c), BLOOM(a, b));  // NOTE - should use values from INDEX

// Various forms of BLOOM

buildindex(i1, overwrite, BLOOM(a), BLOOM(b), BLOOM(a,b));
buildindex(i1, overwrite, BLOOM(false, a), BLOOM(true, b));
buildindex(i1, overwrite, BLOOM(useBloom, a));
buildindex(i1, overwrite, BLOOM(a, LIMIT(10), PROBABILITY(0.1)), BLOOM(b, LIMIT(12), PROBABILITY(0.2)));

