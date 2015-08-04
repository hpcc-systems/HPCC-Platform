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

//import dt;
export pstring := type
    export integer physicallength(string x) := transfer(x[1], unsigned1)+1;
    export string load(string x) := x[2..transfer(x[1], unsigned1)+1];
    export string store(string x) := transfer(length(x), string1)+x;
end;


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
pstring     fulladdress := '';
                ifblock(SELF.age > 16)
string20            casino := 'Unknown';
                END;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin',31, '10 Slapdash Lane', 'Las Vegas'},
        {'Hawthorn','Mia',30,'Ditto'},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

//output(namesTable, THOR);
output(namesTable,,'x.out');
