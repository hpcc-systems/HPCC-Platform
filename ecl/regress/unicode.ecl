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

#option ('optimizeGraph', false);
#option ('foldAssign', false);
#option ('globalFold', false);
__set_debug_option__('optimizeDiskRead', 0);

'This is some sample text in little endian unicode';
U'éëéë';
one := 1 : stored('one');
x := U'éëéë';

x[1..1] = x[3..3];
x[1..one] = x[2+one..2+one];

en1 := u'llama';
en2 := u'lucia';
en3 := u'maria';
es1 := (UNICODE_es__TRADITIONAL)en1;
es2 := (UNICODE_es__TRADITIONAL)en2;
es3 := (UNICODE_es__TRADITIONAL)en3;
en1 < en2;
en2 < en3;
es1 > es2;
es2 < es3;

r1 := record
string10 s10;
unicode10 u10a;
unicode10 u10b;
unicode10 u10c;
unicode20 u20a;
  end;

t1 := dataset('in.d00', r1, THOR);

r1 tran1(r1 l) := transform
    self.s10 := (string)l.u10a;
    self.u10a := (unicode)l.s10;
    self.u10b := (unicode)('!' + l.s10[1..5] + '!');
    self.u10c := (unicode)(U'x' +  U'ß' + U'was' + U'τετελεσται');
    self.u20a := U'die straße';
    end;

t2 := project(t1, tran1(left));

r1 tran2(r1 l) := transform
    self.s10 := '!'+(string)(l.u10a[one..5]+U'x')+'!';
    self.u10a := (U'x' +  U'ß' + x[one..one+one] + U'τετελεσται')[2..one*8];
    self.u10b := U'ß' + (unicode)l.s10[one..4] + U'ß';
    self.u10c := (unicode)(one+9);
    self.u20a := (unicode)(one+1.1);
    self := l;
    end;

t3 := project(t2, tran2(left));

r1 tran3(r1 l) := transform
    self.u10a := U'x' + (unicode)one;
    self.u10b := U'x' + (unicode)(one + 1.1);
    self.s10 :=  IF(l.u10a IN [U'a',U'b',U'c',U'd',U'e',U'τ',U'ε',U'τ',U'ε',U'λ',U'ε',U'σ',U'τ',U'α',U'ι'], 'a', 'b');
    self.u10c := IF((varunicode)l.u10a IN [U'a',(varunicode)U'b',(varunicode)U'c',(varunicode)U'd',(varunicode)U'e',(varunicode)U'τ',(varunicode)U'ε',(varunicode)U'τ',(varunicode)U'ε',(varunicode)U'λ',(varunicode)U'ε',(varunicode)U'σ',(varunicode)U'τ',(varunicode)U'α',(varunicode)U'ι'], U'a', U'b');
    self.u20a := [U'a',U'b',U'c',U'd',U'e',U'τ',U'ε',U'τ',U'ε',U'λ',U'ε',U'σ',U'τ',U'α',U'ι'][length(trim(l.s10))] +
        [U'a',(varunicode)U'b',(varunicode)U'c',(varunicode)U'd',(varunicode)U'e',(varunicode)U'τ',(varunicode)U'ε',(varunicode)U'τ',(varunicode)U'ε',(varunicode)U'λ',(varunicode)U'ε',(varunicode)U'σ',(varunicode)U'τ',(varunicode)U'α',(varunicode)U'ι'][length(trim(l.u10a))];
    self := l;
    end;

t4 := project(t3, tran3(left));

output(t4,,'out.d00');

rloc := record, locale('es__traditional')
    unicode10 ux;
    unicode10 uy;
end;

rloc tranloc(rloc l) := transform
    self.ux := if(l.ux < l.uy, l.ux, l.uy);
    self.uy := if(l.ux < l.uy, l.uy, l.ux);
end;

tloc1 := dataset('inloc.d00', rloc, THOR);

tloc2 := project(tloc1, tranloc(left));

output(tloc2,,'outloc.d00');
