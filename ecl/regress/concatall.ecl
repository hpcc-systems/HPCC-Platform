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


namesRecord :=
            RECORD
data10      surnameD;
data10      forenameD;
string10    surnameS;
string10        forenameS;
varstring10     surnameV;
varstring10     forenameV;
qstring10       surnameQ;
qstring10       forenameQ;
ebcdic string10 surnameE;
ebcdic string10     forenameE;
            END;


rtl := service
string str2StrX(const data src) : eclrtl,library='eclrtl',entrypoint='rtlStrToStrX';
    end;


namesTable := dataset('x',namesRecord,FLAT);

outrec      :=  RECORD
data12  d12d        := namesTable.forenameD + namesTable.surnameD + '00';
data22  d22d        := namesTable.forenameD + namesTable.surnameD + '01';
data32  d32d        := namesTable.forenameD + namesTable.surnameD + '02';
data12  d12s        := namesTable.forenameS + namesTable.surnameS + '03';
data22  d22s        := namesTable.forenameS + namesTable.surnameS + '04';
data32  d32s        := namesTable.forenameS + namesTable.surnameS + '05';
data12  d12v        := namesTable.forenameV + namesTable.surnameV + '06';
data22  d22v        := namesTable.forenameV + namesTable.surnameV + '07';
data32  d32v        := namesTable.forenameV + namesTable.surnameV + '08';
data12  d12q        := namesTable.forenameQ + namesTable.surnameQ + '09';
data22  d22q        := namesTable.forenameQ + namesTable.surnameQ + '0a';
data32  d32q        := namesTable.forenameQ + namesTable.surnameQ + '0b';
data12  d12e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'0c';
data22  d22e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'0d';
data32  d32e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'0e';
string12    s12d        := namesTable.forenameD + namesTable.surnameD + 's1';
string22    s22d        := namesTable.forenameD + namesTable.surnameD + 's2';
string32    s32d        := namesTable.forenameD + namesTable.surnameD + 's3';
string12    s12s        := namesTable.forenameS + namesTable.surnameS + 's4';
string22    s22s        := namesTable.forenameS + namesTable.surnameS + 's5';
string32    s32s        := namesTable.forenameS + namesTable.surnameS + 's6';
string12    s12v        := namesTable.forenameV + namesTable.surnameV + 's7';
string22    s22v        := namesTable.forenameV + namesTable.surnameV + 's8';
string32    s32v        := namesTable.forenameV + namesTable.surnameV + 's9';
string12    s12q        := namesTable.forenameQ + namesTable.surnameQ + 'sa';
string22    s22q        := namesTable.forenameQ + namesTable.surnameQ + 'sb';
string32    s32q        := namesTable.forenameQ + namesTable.surnameQ + 'sc';
string12    s12e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'sd';
string22    s22e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'se';
string32    s32e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'sf';
varstring12 v12d        := namesTable.forenameD + namesTable.surnameD + 'v1';
varstring22 v22d        := namesTable.forenameD + namesTable.surnameD + 'v2';
varstring32 v32d        := namesTable.forenameD + namesTable.surnameD + 'v3';
varstring12 v12s        := namesTable.forenameS + namesTable.surnameS + 'v4';
varstring22 v22s        := namesTable.forenameS + namesTable.surnameS + 'v5';
varstring32 v32s        := namesTable.forenameS + namesTable.surnameS + 'v6';
varstring12 v12v        := namesTable.forenameV + namesTable.surnameV + 'v7';
varstring22 v22v        := namesTable.forenameV + namesTable.surnameV + 'v8';
varstring32 v32v        := namesTable.forenameV + namesTable.surnameV + 'v9';
varstring12 v12q        := namesTable.forenameQ + namesTable.surnameQ + 'va';
varstring22 v22q        := namesTable.forenameQ + namesTable.surnameQ + 'vb';
varstring32 v32q        := namesTable.forenameQ + namesTable.surnameQ + 'vc';
varstring12 v12e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'vd';
varstring22 v22e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'ve';
varstring32 v32e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'vf';
qstring12   q12d        := namesTable.forenameD + namesTable.surnameD + 'q1';
qstring22   q22d        := namesTable.forenameD + namesTable.surnameD + 'q2';
qstring32   q32d        := namesTable.forenameD + namesTable.surnameD + 'q3';
qstring12   q12s        := namesTable.forenameS + namesTable.surnameS + 'q4';
qstring22   q22s        := namesTable.forenameS + namesTable.surnameS + 'q5';
qstring32   q32s        := namesTable.forenameS + namesTable.surnameS + 'q6';
qstring12   q12v        := namesTable.forenameV + namesTable.surnameV + 'q7';
qstring22   q22v        := namesTable.forenameV + namesTable.surnameV + 'q8';
qstring32   q32v        := namesTable.forenameV + namesTable.surnameV + 'q9';
qstring12   q12q        := namesTable.forenameQ + namesTable.surnameQ + 'qa';
qstring22   q22q        := namesTable.forenameQ + namesTable.surnameQ + 'qb';
qstring32   q32q        := namesTable.forenameQ + namesTable.surnameQ + 'qc';
qstring12   q12e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'qd';
qstring22   q22e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'qe';
qstring32   q32e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'qf';
ebcdic string12 e12d        := namesTable.forenameD + namesTable.surnameD + 'e1';
ebcdic string22 e22d        := namesTable.forenameD + namesTable.surnameD + 'e2';
ebcdic string32 e32d        := namesTable.forenameD + namesTable.surnameD + 'e3';
ebcdic string12 e12s        := namesTable.forenameS + namesTable.surnameS + 'e4';
ebcdic string22 e22s        := namesTable.forenameS + namesTable.surnameS + 'e5';
ebcdic string32 e32s        := namesTable.forenameS + namesTable.surnameS + 'e6';
ebcdic string12 e12v        := namesTable.forenameV + namesTable.surnameV + 'e7';
ebcdic string22 e22v        := namesTable.forenameV + namesTable.surnameV + 'e8';
ebcdic string32 e32v        := namesTable.forenameV + namesTable.surnameV + 'e9';
ebcdic string12 e12q        := namesTable.forenameQ + namesTable.surnameQ + 'ea';
ebcdic string22 e22q        := namesTable.forenameQ + namesTable.surnameQ + 'eb';
ebcdic string32 e32q        := namesTable.forenameQ + namesTable.surnameQ + 'ec';
ebcdic string12 e12e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'ed';
ebcdic string22 e22e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'ee';
ebcdic string32 e32e        := namesTable.forenameE + namesTable.surnameE + (ebcdic string)'ef';
                END;

output(namesTable,outrec,'out.d00');

