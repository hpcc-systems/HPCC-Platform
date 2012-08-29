/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//NoThor
//Test for bug 25636

namesRecord := 
            RECORD
string20        surname;
            END;

idRecord := record
boolean include;
dataset(namesRecord) people{maxcount(20)};
    end;


ds := dataset([
        {false,[{'Gavin'},{'Liz'}]},
        {true,[{'Richard'},{'Jim'}]},
        {false,[]}], idRecord);

idRecord t(idRecord l) := transform
    sortedPeople := sort(l.people, surname);
    self.people := if(not l.include, sortedPeople(l.include)) + sortedPeople;
    self := l;
end;

output(project(ds, t(left)));
