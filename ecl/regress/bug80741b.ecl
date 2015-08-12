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

unicode u1 := u'abc' : stored('u1');
data dataCast := (data)u1;
data dataTransfer := transfer(u1, data);

// expected 610062006300, but outputs 616263 // I'm guessing it's casting to STRING as an intermediate between UNICODE and DATA.
output(dataCast, named('cast_uni2data'));

// outputs 610062006300 as expected
output(dataTransfer, named('transfer_uni2data'));


data d1 := x'610062006300' : stored('d1');
unicode uniCast := (unicode)d1;
unicode uniTransfer := (>unicode<)d1;

// expected abc, but looks like it outputs a<null>b<null>c<null>
output(uniCast, named('cast_data2uni'));

// expected abc, but looks like it outputs abc<null><null><null>
output(uniTransfer, named('transfer_data2uni'));

unicode uniTransfer2 := transfer(dataTransfer, unicode); // expected abc, but looks like it outputs abc<null><null><null><null><null><null><null><null><null>
output(uniTransfer2, named('transfer_uni2data2uni'));

unicode q1 := u'abc';
qstring qstringTransfer := transfer(u1, qstring);

output(qstringTransfer, named('transfer_uni2qstring'));
