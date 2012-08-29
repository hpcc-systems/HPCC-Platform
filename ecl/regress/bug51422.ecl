/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
     unicode u1{maxlength(100)},
};
layout2 := {
     unicode u1{maxlength(100)};
     unsigned pos;
     unsigned len;
};
ds := dataset([{u'abcd\u6c34ef'}], layout);

rule r1 := pattern(u'[\u0035-\u007a]'); // match a-f, not \u6c34
rule r2 := pattern(u'[\u0034-\u007a]'); // match a-f not \u6c34
rule r3 := pattern(u'\u6c34'); // produces a match for \u6c34
rule r4 := pattern(u'\u0034'); // don't match on \u6c34
rule r5 := pattern(u'[\u6c34]'); // match on \u6c34
rule r6 := pattern(u'[\u0034]'); // no match on \u6c34

layout2 t1(layout rec) := transform
     self.u1 := matchunicode;
     self.pos := matchPOSITION;
     self.len := matchLENGTH;
end;
sequential(
  output(parse(ds, u1, r1, t1(left))),
  output(parse(ds, u1, r2, t1(left))),
  output(parse(ds, u1, r3, t1(left))),
  output(parse(ds, u1, r4, t1(left))),
  output(parse(ds, u1, r5, t1(left))),
  output(parse(ds, u1, r6, t1(left)))
);
