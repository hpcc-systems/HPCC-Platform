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

string s1 := 'ABCDEFGHIJK' : stored('s1');
varstring v1 := 'ABCDEFGHIJK' : stored('v1');
unicode u1 := U'ABCDEFGHIJK' : stored('u1');
varunicode w1 := U'ABCDEFGHIJK' : stored('w1');
qstring q1 := Q'ABCDEFGHIJK' : stored('q1');
data d1 := D'ABCDEFGHIJK' : stored('d1');

integer st1 := 1 : stored('st1');
integer st2 := 2 : stored('st2');
integer st3 := 3 : stored('st3');
integer st4 := 4 : stored('st4');
integer st5 := 5 : stored('st5');
integer st6 := 6 : stored('st6');
integer st10 := 10 : stored('st10');
integer st13 := 13 : stored('st13');

output('-----------QStrings-----------');
q1[1] = Q'A';
q1[2] = Q'B';
q1[3..4] = Q'CD';
q1[5..6] = Q'EF';
q1[1..length(q1)-5] = Q'ABCDEF';
q1[5..] = Q'EFGHIJK';
q1[10..13] = Q'JK  ';
q1[st10..st13] = Q'JK  ';

Q'X'+q1[st1]+Q'X' = Q'XAX';
Q'X'+q1[st2]+Q'X' = Q'XBX';
Q'X'+q1[st3..st4]+Q'X' = Q'XCDX';
Q'X'+q1[st5..st6]+Q'X' = Q'XEFX';
Q'X'+q1[st1..length(q1)-st5]+Q'X' = Q'XABCDEFX';
Q'X'+q1[st5..]+Q'X' = Q'XEFGHIJKX';
Q'X'+q1[st10..st13]+Q'X' = Q'XJK  X';

Output('-----------Data-----------');
d1[1] = D'A';
d1[2] = D'B';
d1[3..4] = D'CD';
d1[5..6] = D'EF';
d1[1..length(d1)-5] = D'ABCDEF';
d1[5..] = D'EFGHIJK';
d1[10..13] = X'4A4B0000';

D'X'+d1[st1]+D'X' = D'XAX';
D'X'+d1[st2]+D'X' = D'XBX';
D'X'+d1[st3..st4]+D'X' = D'XCDX';
D'X'+d1[st5..st6]+D'X' = D'XEFX';
D'X'+d1[st1..length(d1)-st5]+D'X' = D'XABCDEFX';
D'X'+d1[st5..]+D'X' = D'XEFGHIJKX';
D'X'+d1[st10..st13]+D'X' = X'584A4B000058';

Output('-----------String-----------');
s1[1] = 'A';
s1[2] = 'B';
s1[3..4] = 'CD';
s1[5..6] = 'EF';
s1[1..length(s1)-5] = 'ABCDEF';
s1[5..] = 'EFGHIJK';
s1[10..13] = 'JK  ';

'X'+s1[st1]+'X' = 'XAX';
'X'+s1[st2]+'X' = 'XBX';
'X'+s1[st3..st4]+'X' = 'XCDX';
'X'+s1[st5..st6]+'X' = 'XEFX';
'X'+s1[st1..length(s1)-st5]+'X' = 'XABCDEFX';
'X'+s1[st5..]+'X' = 'XEFGHIJKX';
'X'+s1[st10..st13]+'X' = 'XJK  X';

Output('-----------VarString-----------');
v1[1] = 'A';
v1[2] = 'B';
v1[3..4] = 'CD';
v1[5..6] = 'EF';
v1[1..length(v1)-5] = 'ABCDEF';
v1[5..] = 'EFGHIJK';
v1[10..13] = 'JK  ';

'X'+v1[st1]+'X' = 'XAX';
'X'+v1[st2]+'X' = 'XBX';
'X'+v1[st3..st4]+'X' = 'XCDX';
'X'+v1[st5..st6]+'X' = 'XEFX';
'X'+v1[st1..length(v1)-st5]+'X' = 'XABCDEFX';
'X'+v1[st5..]+'X' = 'XEFGHIJKX';
'X'+v1[st10..st13]+'X' = 'XJK  X';

Output('-----------Unicode-----------');
u1[1] = U'A';
u1[2] = U'B';
u1[3..4] = U'CD';
u1[5..6] = U'EF';
u1[1..length(u1)-5] = U'ABCDEF';
u1[5..] = U'EFGHIJK';
u1[10..13] = U'JK  ';

U'X'+u1[st1]+U'X' = U'XAX';
U'X'+u1[st2]+U'X' = U'XBX';
U'X'+u1[st3..st4]+U'X' = U'XCDX';
U'X'+u1[st5..st6]+U'X' = U'XEFX';
U'X'+u1[st1..length(u1)-st5]+U'X' = U'XABCDEFX';
U'X'+u1[st5..]+U'X' = U'XEFGHIJKX';
U'X'+u1[st10..st13]+U'X' = U'XJK  X';

Output('-----------VarUnicode-----------');
w1[1] = U'A';
w1[2] = U'B';
w1[3..4] = U'CD';
w1[5..6] = U'EF';
w1[1..length(w1)-5] = U'ABCDEF';
w1[5..] = U'EFGHIJK';
w1[10..13] = U'JK  ';

U'X'+w1[st1]+U'X' = U'XAX';
U'X'+w1[st2]+U'X' = U'XBX';
U'X'+w1[st3..st4]+U'X' = U'XCDX';
U'X'+w1[st5..st6]+U'X' = U'XEFX';
U'X'+w1[st1..length(w1)-st5]+U'X' = U'XABCDEFX';
U'X'+w1[st5..]+U'X' = U'XEFGHIJKX';
U'X'+w1[st10..st13]+U'X' = U'XJK  X';

r := record
string11 sf := 'ABCDEFGHIJK';// : stored('s1');
varstring11 vf := 'ABCDEFGHIJK';// : stored('v1');
unicode11 uf := U'ABCDEFGHIJK';// : stored('u1');
varunicode11 wf := U'ABCDEFGHIJK';// : stored('w1');
qstring11 qf := Q'ABCDEFGHIJK';// : stored('q1');
data11 df := D'ABCDEFGHIJK';
   end;

d := dataset([r],r);

t1 := table(d,
        record

qstring1 qf1 := qf[1];
qstring1 qf2 := qf[2];
qstring2 qf3 := qf[3..4];
qstring2 qf4 := qf[5..6];
qstring8 qf5 := qf[1..length(qf)-5];
qstring7 qf6 := qf[5..];
qstring4 qf7 := qf[10..13];

qstring1 zqf1 := qf[st1];
qstring1 zqf2 := qf[st2];
qstring2 zqf3 := qf[st3..st4];
qstring2 zqf4 := qf[st5..st6];
qstring8 zqf5 := qf[st1..length(qf)-5];
qstring7 zqf6 := qf[st5..];
qstring4 zqf7 := qf[st10..st13];

data1 df1 := df[1];
data1 df2 := df[2];
data2 df3 := df[3..4];
data2 df4 := df[5..6];
data8 df5 := df[1..length(df)-5];
data7 df6 := df[5..];
data4 df7 := df[10..13];

data1 zdf1 := df[st1];
data1 zdf2 := df[st2];
data2 zdf3 := df[st3..st4];
data2 zdf4 := df[st5..st6];
data8 zdf5 := df[st1..length(df)-5];
data7 zdf6 := df[st5..];
data4 zdf7 := df[st10..st13];

string1 sf1 := sf[1];
string1 sf2 := sf[2];
string2 sf3 := sf[3..4];
string2 sf4 := sf[5..6];
string8 sf5 := sf[1..length(sf)-5];
string7 sf6 := sf[5..];
string4 sf7 := sf[10..13];

string1 zsf1 := sf[st1];
string1 zsf2 := sf[st2];
string2 zsf3 := sf[st3..st4];
string2 zsf4 := sf[st5..st6];
string8 zsf5 := sf[st1..length(sf)-5];
string7 zsf6 := sf[st5..];
string4 zsf7 := sf[st10..st13];

varstring1 vf1 := vf[1];
varstring1 vf2 := vf[2];
varstring2 vf3 := vf[3..4];
varstring2 vf4 := vf[5..6];
varstring8 vf5 := vf[1..length(vf)-5];
varstring7 vf6 := vf[5..];
varstring4 vf7 := vf[10..13];

varstring1 zvf1 := vf[st1];
varstring1 zvf2 := vf[st2];
varstring2 zvf3 := vf[st3..st4];
varstring2 zvf4 := vf[st5..st6];
varstring8 zvf5 := vf[st1..length(vf)-5];
varstring7 zvf6 := vf[st5..];
varstring4 zvf7 := vf[st10..st13];

unicode1 uf1 := uf[1];
unicode1 uf2 := uf[2];
unicode2 uf3 := uf[3..4];
unicode2 uf4 := uf[5..6];
unicode8 uf5 := uf[1..length(uf)-5];
unicode7 uf6 := uf[5..];
unicode4 uf7 := uf[10..13];

unicode1 zuf1 := uf[st1];
unicode1 zuf2 := uf[st2];
unicode2 zuf3 := uf[st3..st4];
unicode2 zuf4 := uf[st5..st6];
unicode8 zuf5 := uf[st1..length(uf)-5];
unicode7 zuf6 := uf[st5..];
unicode4 zuf7 := uf[st10..st13];

varunicode1 wf1 := wf[1];
varunicode1 wf2 := wf[2];
varunicode2 wf3 := wf[3..4];
varunicode2 wf4 := wf[5..6];
varunicode8 wf5 := wf[1..length(wf)-5];
varunicode7 wf6 := wf[5..];
varunicode4 wf7 := wf[10..13];

varunicode1 zwf1 := wf[st1];
varunicode1 zwf2 := wf[st2];
varunicode2 zwf3 := wf[st3..st4];
varunicode2 zwf4 := wf[st5..st6];
varunicode8 zwf5 := wf[st1..length(wf)-5];
varunicode7 zwf6 := wf[st5..];
varunicode4 zwf7 := wf[st10..st13];
        end);


t2 := table(t1,
            record
qf1 = Q'A';
qf2 = Q'B';
qf3 = Q'CD';
qf4 = Q'EF';
qf5 = Q'ABCDEF';
qf6 = Q'EFGHIJK';
qf7 = Q'JK  ';

zqf1 = Q'A';
zqf2 = Q'B';
zqf3 = Q'CD';
zqf4 = Q'EF';
zqf5 = Q'ABCDEF';
zqf6 = Q'EFGHIJK';
zqf7 = Q'JK  ';

df1 = D'A';
df2 = D'B';
df3 = D'CD';
df4 = D'EF';
df5 = D'ABCDEF';
df6 = D'EFGHIJK';
df7 = X'4A4B0000';

zdf1 = D'A';
zdf2 = D'B';
zdf3 = D'CD';
zdf4 = D'EF';
zdf5 = D'ABCDEF';
zdf6 = D'EFGHIJK';
zdf7 = X'4A4B0000';

sf1 = 'A';
sf2 = 'B';
sf3 = 'CD';
sf4 = 'EF';
sf5 = 'ABCDEF';
sf6 = 'EFGHIJK';
sf7 = 'JK  ';

zsf1 = 'A';
zsf2 = 'B';
zsf3 = 'CD';
zsf4 = 'EF';
zsf5 = 'ABCDEF';
zsf6 = 'EFGHIJK';
zsf7 = 'JK  ';

vf1 = 'A';
vf2 = 'B';
vf3 = 'CD';
vf4 = 'EF';
vf5 = 'ABCDEF';
vf6 = 'EFGHIJK';
vf7 = 'JK  ';

zvf1 = 'A';
zvf2 = 'B';
zvf3 = 'CD';
zvf4 = 'EF';
zvf5 = 'ABCDEF';
zvf6 = 'EFGHIJK';
zvf7 = 'JK  ';

uf1 = U'A';
uf2 = U'B';
uf3 = U'CD';
uf4 = U'EF';
uf5 = U'ABCDEF';
uf6 = U'EFGHIJK';
uf7 = U'JK  ';

zuf1 = U'A';
zuf2 = U'B';
zuf3 = U'CD';
zuf4 = U'EF';
zuf5 = U'ABCDEF';
zuf6 = U'EFGHIJK';
zuf7 = U'JK  ';

wf1 = U'A';
wf2 = U'B';
wf3 = U'CD';
wf4 = U'EF';
wf5 = U'ABCDEF';
wf6 = U'EFGHIJK';
wf7 = U'JK  ';

zwf1 = U'A';
zwf2 = U'B';
zwf3 = U'CD';
zwf4 = U'EF';
zwf5 = U'ABCDEF';
zwf6 = U'EFGHIJK';
zwf7 = U'JK  ';

    end);

output(t2);


