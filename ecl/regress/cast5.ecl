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






string10 s10 := '' : stored('s10');
string20 s20 := '' : stored('s20');
string60 s60 := '' : stored('s60');
unicode10 u10 := U'' : stored('u10');


output(trim(s10));
output(trim((string20)s10));
output(trim((string20)s20));
output(trim((string20)s60));

output(trim((string20)u10));
