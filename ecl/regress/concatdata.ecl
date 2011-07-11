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
data20      surname;
data10      forename;
integer2        age := 25;
            END;


rtl := service
string str2StrX(const data src) : eclrtl,library='eclrtl',entrypoint='rtlStrToStrX';
    end;


namesTable := dataset('x',namesRecord,FLAT);

namesTable2 := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);

outrec      :=  RECORD
data42          text := namesTable2.surname[1..1+2];
data42          text2 := (namesTable2.surname[1..1+2]+' '+namesTable2.surname[1..1+2]+' '+namesTable2.surname[1..1+2]);
data10          text3 := 'Gav' + 'in' + ' ' + 'Hall';
data42          text4 := rtl.str2strX(D'Gavin');
data42          text5 := namesTable2.surname[1..2+1] + namesTable2.surname[1..2+1];
data42          text6 := rtl.str2strX(namesTable2.surname[1..2] + namesTable2.surname[1..2]);
                END;
output(namesTable2,outrec,'out.d00');

outr := RECORD
data42          text4;
        END;

outr x(namesRecord l) := transform
SELF.text4 := (data42)rtl.str2strX(D'Gavin');
    END;

//output(project(namesTable2,x(LEFT)),,'out.d00');
