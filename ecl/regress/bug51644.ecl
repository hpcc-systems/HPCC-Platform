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

layout := {
  string s,
};

ds := DATASET([{'cacacacat. caaat'}],layout);

pattern p1 := pattern('ca+t'); // matches cacacacat
pattern p2 := pattern('c\\a+t'); // matches cat and caaat
pattern p3 := pattern('c(a)+t'); // matches cat and caaat

rule r1 := p1;
rule r2 := p2;
rule r3 := p3;

results := {
  string a := matchtext;
};

output(PARSE(ds, s, r1, results));
output(PARSE(ds, s, r2, results));
output(PARSE(ds, s, r3, results));
