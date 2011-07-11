/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
