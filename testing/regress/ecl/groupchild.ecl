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

//Test iteration of grouped datasets in child queries.

#option ('groupedChildIterators', true);

childRecord := RECORD
string      name;
unsigned    dups := 0;
end;


namesRecord := 
            RECORD
string20        surname;
dataset(childRecord) children;
            END;


ds := dataset(
[
    {'Gavin', 
        [{'Smith'},{'Jones'},{'Jones'},{'Doe'},{'Smith'}]
    },
    {'John',
        [{'Bib'},{'Bob'}]
    }
], namesRecord);

namesRecord t(namesRecord l) := transform
    deduped := dedup(group(SORT(l.children, name), name), name);
    cnt(string search) := count(table(deduped(name != search), {count(group)}));
    notdoe := HAVING(deduped, LEFT.name != 'Doe');
    self.children := group(project(notdoe, transform(childRecord, self.dups := cnt(left.name); self.name := left.name)));
    self := l;
end;


output(project(nofold(ds), t(left)));



