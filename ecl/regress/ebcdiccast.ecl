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






integer8 i := 12345 : stored('i');
integer8 j := 12345 : stored('j');

rec := record
integer x;
ebcdic string s;
    end;

ds := dataset('ds', rec, thor);
ds2 := table(ds,
            {
            ebcdic string s2 := if(x < 10, (ebcdic string)i, (ebcdic string)j);
            data s3 := if(x < 10, (data)i, (data)j);
            });
output(ds2);
