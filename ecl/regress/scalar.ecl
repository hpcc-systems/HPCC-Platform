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

export display :=
    SERVICE
        echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
    END;


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesTable2 := nofold(dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'Hawthorn','Emma',30},
        {'X','Z'}], namesRecord));

display.echo('**** ' + (string)count(namesTable2) + ' ****');
display.echo('**** ' + (string)sum(namesTable2, namesTable2.age) + ' ****');

display.echo('**** ' + (string)min(namesTable2, namesTable2.age) + ' ****');
display.echo('**** ' + (string)max(namesTable2, namesTable2.age) + ' ****');
display.echo('**** ' + (string)ave(namesTable2, namesTable2.age) + ' ****');
display.echo('**** ' + (string)count(group(namesTable2,namesTable2.surname)) + ' ****');
//output(namesTable2,{sum(group,age)},'out.d00');

z := group(namesTable2,surname,all);

output(z,{surname,count(group)},'out.d00');

output(__OS__);
