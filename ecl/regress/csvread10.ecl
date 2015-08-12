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


inRecord :=
            RECORD
unsigned        line;
string20        className;
string50        helper;
unsigned        size;
            END;

inTable := dataset('~file::127.0.0.1::temp::temp.csv',inRecord,csv(separator(':'),utf8));

s := sort(inTable, -line);
withSizes := iterate(s, transform(inRecord, self.size := left.line - right.line; self := right))(size < 10000000);

output(sort(withSizes, -size),,'~withSizes',csv,overwrite);

t := table(withSizes, { unsigned cnt := count(group), unsigned sz := sum(group, size), helper }, helper );;
output(sort(t, cnt, sz));
output(sort(t, sz, cnt));
