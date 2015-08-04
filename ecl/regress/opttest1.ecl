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

#option ('optimizeGraph', false);
rec := record
 string10 lname;
 string10 fname;
end;

head := dataset('infile',rec,flat);


x1 := table(head, {lname, fname, unsigned4 cnt := 1 });

x2 := sort(x1, lname);

x3 := table(x2, {fname, lname, cnt});

x4 := x3(lname > 'Hawthorn');

output(x4);

/*
i) swap filter x4 with table x3.
ii) swap filter x4' with sort x2
iii) swap filter x4'' with table x1
*/
