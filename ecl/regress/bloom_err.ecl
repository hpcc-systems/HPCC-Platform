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

buildindex(d, {a, b => c },'ix2', overwrite, BLOOM(c));  // Must be keyed
buildindex(d, {a, b => c },'ix2', overwrite, BLOOM(b,a));  // Must be in order
buildindex(d, {a, b => c },'ix2', overwrite, BLOOM(a),bloom(a));  // Must be unique
buildindex(d, {a, b => c },'ix2', overwrite, BLOOM(a,a));  // Must not repeat fields
buildindex(d, {a, b => c },'ix2', overwrite, BLOOM(a,PROBABILITY(10)));  // Probability out of range
