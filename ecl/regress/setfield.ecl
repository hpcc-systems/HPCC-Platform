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

isAll := true : stored('isAll');

filterSet := IF(isAll, ALL, []);

d := dataset([{'gavin',[]},{'richard',['RKC']},{'Jim',filterSet}], { string15 name, set of string20 aka});

output(d);


d2 := dataset([{'gavin',[]},{'richard',[]},{'Jim',[]}], { string15 name, set of string20 aka});

output(project(d2, transform(recordof(d2), SELF.aka := IF(LEFT.name = 'Jim', filterSet, []); self := LEFT)));
