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

#option ('foldAssign', false);
#option ('globalFold', false);


namesRecord :=
            RECORD
string20        forename;
string20        surname;
            END;

namesTable := DATASET([
        { 'Aasm', 'Rbuin' },
        { 'Agfsd', 'Jsiak' },
        { 'Agudsfa', 'Bruon' },
        { 'Asda', 'Dsehpatne' },
        { 'Afan', 'Mtaoklc' },
        { 'Asdfdsir', 'Measure' },
        { 'Aasdf', 'Btiwonick' },
        { 'Adfa', 'Jcaskon' },
        { 'Aasdfan', 'Querido' },
        { 'adsfaa', 'Rmoan' },
        { 'Adfadi', 'Wilson' },
        { 'Adi', 'Chahande' },
        { 'Asafasd', 'Fishbeck' },
        { 'Ansdfny', 'Scott' },
        { 'Aasdfny', 'Tyson' },
        { 'Armando', 'Escalante' },
        { 'Asasfa', 'Fail' },
        { 'Basdfra', 'Johnson' },
        { 'Basda', 'Messner' },
        { 'Ben', 'Ppaita' },
        { 'Ben', 'Ray   ' },
        { 'Benjamin', 'Ng' },
        { 'Bill', 'Hasa' },
        { 'Bill', 'Otsradner' },
        { 'Jim', 'Rodriquez' },
        { 'Stuart', 'Vilanueva' }
        ], namesRecord);

diskNamesRecord :=
            RECORD
                namesRecord;
unsigned8       filepos{virtual(fileposition)};
unsigned8       localfilepos{virtual(localfileposition)}
            END;

string1 hex1(unsigned4 i) := ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'][i+1];
string2 hex2(unsigned4 i) := hex1(i DIV 0x10) + hex1(i & 0x0F);
string4 hex4(unsigned4 i) := hex2(i DIV 0x100) + hex2(i & 0xFF);
string8 hex8(unsigned4 i) := hex4(i DIV 0x10000) + hex4(i & 0xFFFF);
string16 hex16(unsigned8 i) := hex8(i DIV (unsigned8)0x100000000) + hex8(i & 0xFFFFFFFF);

diskTable := dataset('~names', diskNamesRecord, thor);

sequential(
output(sort(namesTable, surname),,'~names',overwrite),
output(diskTable, { surname, forename, filepos, hex16(localfilepos)} )
);
