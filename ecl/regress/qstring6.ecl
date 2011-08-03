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

rec1 :=     RECORD
qstring20       qf1;
string20        sf1;
varstring20     vf1;
data20          df1;
unicode20       uf1;
varunicode20    wf1;
            END;


bound1 := 11 : stored('boound1');
bound2 := 11 : stored('boound2');
bound3 := 11 : stored('boound3');
bound4 := 11 : stored('boound4');
bound5 := 11 : stored('boound5');
bound6 := 11 : stored('boound6');

test := nofold(dataset([
                {'Gavin','Hawthorn','Slapdash Lane','Great',U'Chishilll',U'Herts'}
                ], rec1));


x := table(test, {

'--to qstring--';
qstring12 qqf := qf1[1..bound1];
qstring12 qsf := sf1[1..bound1];
qstring12 qvf := vf1[1..bound1];
qstring12 qdf := df1[1..bound1];
qstring12 quf := uf1[1..bound1];
qstring12 qwf := wf1[1..bound1];

'--to string--';
string12 sqf := qf1[1..bound2];
string12 ssf := sf1[1..bound2];
string12 svf := vf1[1..bound2];
string12 sdf := df1[1..bound2];
string12 suf := uf1[1..bound2];
string12 swf := wf1[1..bound2];

'--to varstring--';
varstring12 vqf := qf1[1..bound3];
varstring12 vsf := sf1[1..bound3];
varstring12 vvf := vf1[1..bound3];
varstring12 vdf := df1[1..bound3];
varstring12 vuf := uf1[1..bound3];
varstring12 vwf := wf1[1..bound3];

'--to data--';
data12 dqf := qf1[1..bound4];
data12 dsf := sf1[1..bound4];
data12 dvf := vf1[1..bound4];
data12 ddf := df1[1..bound4];
data12 duf := uf1[1..bound4];
data12 dwf := wf1[1..bound4];

'--to unicode--';
unicode12 uqf := qf1[1..bound5];
unicode12 usf := sf1[1..bound5];
unicode12 uvf := vf1[1..bound5];
unicode12 udf := df1[1..bound5];
unicode12 uuf := uf1[1..bound5];
unicode12 uwf := wf1[1..bound5];

'--to varunicode--';
varunicode12 wqf := qf1[1..bound6];
varunicode12 wsf := sf1[1..bound6];
varunicode12 wvf := vf1[1..bound6];
varunicode12 wdf := df1[1..bound6];
varunicode12 wuf := uf1[1..bound6];
varunicode12 wwf := wf1[1..bound6];

'Assignments';

'--to qstring--';
qstring12 qqf2 := qf1;
qstring12 qsf2 := sf1;
qstring12 qvf2 := vf1;
qstring12 qdf2 := df1;
qstring12 quf2 := uf1;
qstring12 qwf2 := wf1;

'--to string--';
string12 sqf2 := qf1;
string12 ssf2 := sf1;
string12 svf2 := vf1;
string12 sdf2 := df1;
string12 suf2 := uf1;
string12 swf2 := wf1;

'--to varstring--';
varstring12 vqf2 := qf1;
varstring12 vsf2 := sf1;
varstring12 vvf2 := vf1;
varstring12 vdf2 := df1;
varstring12 vuf2 := uf1;
varstring12 vwf2 := wf1;

'--to data--';
data12 dqf2 := qf1;
data12 dsf2 := sf1;
data12 dvf2 := vf1;
data12 ddf2 := df1;
data12 duf2 := uf1;
data12 dwf2 := wf1;

'--to unicode--';
unicode12 uqf2 := qf1;
unicode12 usf2 := sf1;
unicode12 uvf2 := vf1;
unicode12 udf2 := df1;
unicode12 uuf2 := uf1;
unicode12 uwf2 := wf1;

'--to varunicode--';
varunicode12 wqf2 := qf1;
varunicode12 wsf2 := sf1;
varunicode12 wvf2 := vf1;
varunicode12 wdf2 := df1;
varunicode12 wuf2 := uf1;
varunicode12 wwf2 := wf1;

});
output(x);
