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
