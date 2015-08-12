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
#option ('testLCR', true);

childRec := { unsigned6 id; };

rec := record
unsigned4       id;
string5         name;
dataset(childRec) ids{maxcount(10)} := _EMPTY_(childRec);
end;


names := dataset([
    {1, 'Gavin'},
    {2, 'Richard'},
    {3, 'Tony'},
    {4, 'Isaac'}], rec) : stored('in', few);

rec t(rec l) := transform
    self.id := l.id + names[1].id;
    self := l;
end;

p := project(names, t(LEFT));

output(p);
