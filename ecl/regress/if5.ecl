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

#option ('targetClusterType', 'roxie');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);
s := sort(namesTable, forename);


ds1 := table(dataset([{0}],{integer i}),
{
'--',
if(count(s) > 0, s[1].age, 0),
if(count(s) > 0, s[1].forename, ''),
if(count(s) >= 1, s[1].forename, ''),
if(count(s) > 0, s[2].age, 0),
if(count(s) > 1, s[2].forename, ''),
'--'
});

output(ds1);


ds2 := table(dataset([{5}],{integer i}),
{
'--',
if(count(s) > 1, s[1].forename, ''),
if(count(s) > 3, s[2].age, 0),          // can't optimize to s[2].age, since differ for count=2/3
if(count(s) > 3, s[2].forename, ''),
'--'
});

output(ds2);


