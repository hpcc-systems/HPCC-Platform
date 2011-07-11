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

/*--SOAP--
<message name="Foobar_Service">
  <part name="checkornot-nomatter" type="xsd:boolean"/>
</message>
*/

#option ('targetClusterType', 'roxie');

export foobar_service := macro

subrec1 := record
    string1 a;
    string1 b;
end;

subrec2 := record
    string1 c;
    string1 d;
end;

rec1 := record
    integer seq;
    integer foobar;
    dataset(subrec1) s1;
    dataset(subrec2) s2;
end;

rec2 := record
    integer seq;
    string5 foo;
    dataset(subrec1) s1;
    dataset(subrec2) s2;
end;

df := dataset([{'a','b'},{'c','d'}],subrec1);
dfb := dataset([{'e','f'},{'h','i'}],subrec1);
df2 := dataset([{'1','2'},{'3','4'}],subrec2);
df2b := dataset([{'5','6'}],subrec2);

myrecs1 := nofold(dataset([{1,2,df,df2}],rec1));
myrecs2 := nofold(dataset([{1,'foo',dfb,df2b}],rec2));

rec1 jn(myrecs1 L, myrecs2 R) := transform
    self.s1 := R.s1;
    self.s2 := R.s2;
    self := L;
end;

outf := join(myrecs1,myrecs2,left.seq = right.seq,jn(LEFT,RIGHT));

output(outf);

endmacro;

foobar_service();
