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

d := dataset('fred', rec, FLAT);

buildindex(d, {a, b => c },'ix2', overwrite, BLOOM(a),bloom(a));  // Must be unique

i1 := index(d, {a,b,c,d}, 'index1', BLOOM(a),bloom(a));
i2 := index(d, {a,b,c,d}, 'index2');
i3 := index(d, {a,b,c,d}, 'index3', BLOOM(b));
buildindex(i1, overwrite); // Should report the index duplicated bloom issue - shame it's not sooner...
buildindex(i2, overwrite, BLOOM(a),bloom(a));
buildindex(i3, overwrite, BLOOM(a),bloom(a));  // Won't report an error, because the index definition overrides (not ideal)
