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

export display := SERVICE
 echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
END;

namesRecord :=
            RECORD
unsigned4       holeid;
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset('base1',namesRecord,THOR);
namesTable2 := dataset('base2',namesRecord,THOR);
namesTable3 := dataset('base3',namesRecord,THOR);
namesTable4 := dataset('base4',namesRecord,THOR);
namesTable5 := dataset('base5',namesRecord,THOR);

count(namesTable1);
count(namesTable1);
count(namesTable1) + count(namesTable1);
count(namesTable2) + count(namesTable3);
count(namesTable2) + count(namesTable3);

display.echo('Total records = ' + (string)count(namesTable4));
