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
