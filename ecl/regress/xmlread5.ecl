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


rec :=      RECORD
integer         duration{xpath('@duration')};
string          name{xpath('@name')};
            END;

timings := dataset('~file::127.0.0.1::temp::time.out', rec, XML('/All/Timings/Timing'));

output(topn(timings(name != 'EclServer: totalTime'), 30, -duration));
summary := table(timings, { integer total := sum(group, duration); name;}, name);
so := sort(summary, -total, name);
output(so);
