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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;


namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

distribution(namesTable, surname, forename, NAMED('Stats'));

x := dataset(row(transform({string line}, SELF.line := WORKUNIT('Stats', STRING))));

extractedValueRec :=
            record
string              value;
unsigned            cnt;
            end;

extractedRec := record
string              name;
unsigned            cnt;
dataset(extractedValueRec) values;
                END;


extractedRec t1 := transform
    SELF.name := XMLTEXT('@name');
    SELF.cnt := (unsigned)XMLTEXT('@distinct');
    SELF.values := XMLPROJECT('Value', transform(extractedValueRec, SELF.value := XMLTEXT(''), SELF.cnt := (unsigned)XMLTEXT('@count')))(cnt > 1);
    end;

p := parse(x, line, t1, xml('/XML/Field'));
output(p);

