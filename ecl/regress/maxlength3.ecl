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

r1 := record
    string      s1{maxlength(30)};
    string      s2{maxlength(30)};
    string      s3{maxlength(30)};
end;

output(dataset('ds1', r1, csv),named('r1'));

r2 := record,maxsize(30+12)
    string      s1{maxlength(30)};
    string      s2{maxlength(30)};
    string      s3{maxlength(30)};
end;

output(dataset('ds2', r2, csv),named('r2'));
