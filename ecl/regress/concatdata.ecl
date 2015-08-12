/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
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
