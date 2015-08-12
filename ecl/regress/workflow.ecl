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

#option('generateLogicalGraph', true);

export personRecord := RECORD
unsigned4 personid;
string1 sex;
string33 forename;
string33 surname;
string4 yob;
unsigned4 salary;
string20 security;
unsigned1 numNotes;
    END;

export personDataset := DATASET('person',personRecord,THOR);

export display :=
    SERVICE
        echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
    END;


//x := count(personDataset) : stored('gavin');
a := count(personDataset);
x := count(personDataset) : success(display.echo('success')),
                        failure(display.echo('failed')), failure(display.echo('failed again')),
                        recovery(display.echo('retry once')), recovery(display.echo('retry 5 times'),5),
                        stored('gavin');

x + x;
