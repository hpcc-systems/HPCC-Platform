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

#option ('childQueries', true);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset([
        {'Hawthorn','Mia',34},
        {'Hawthorn','Gavin',35}], namesRecord);

namesTable2 := dataset([
        {'Hawthorn','Nathan',2},
        {'Hawthorn','Abigail',2}], namesRecord);


combinedRecord :=
            record
unsigned        id;
unsigned        maxChildren;
dataset(namesRecord)    parents{maxcount(5)};
dataset(namesRecord, choosen(self.maxChildren)) children;
            end;

infile := dataset([0,1,2,3],{unsigned id});     // make sure the child queries are executed multiple times.

combinedRecord t1(infile l) :=
        TRANSFORM
            SELF := l;
            self.maxChildren := 2;
            SELF.parents := global(namesTable1, few);
            SELF.children := global(namesTable2, many);
        END;

p1 := project(infile, t1(LEFT));
output(p1);
