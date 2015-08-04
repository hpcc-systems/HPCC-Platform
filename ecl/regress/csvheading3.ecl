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


string headerPrefix := '' : stored('headerPrefix');
string headerSuffix := '' : stored('headerSuffix');
string xmlRowTag := 'row' : stored('xmlRowTag');

d := DATASET('d3', { string x, string y, string z } , FLAT);

output(d,,'o1',csv(heading(headerPrefix)));
output(d,,'o2',csv(heading(headerPrefix, headerSuffix)));
output(d,,'o3',csv(heading(headerPrefix, single)));
output(d,,'o4',csv(heading(headerPrefix, headerSuffix, single)));


output(d,,'x1',xml(heading(headerPrefix)));
output(d,,'x2',xml(heading(headerPrefix, headerSuffix)));
output(d,,'x3',xml(xmlRowTag, heading(headerPrefix, headerSuffix)));
output(d,,'x4',xml(opt, xmlRowTag, heading(headerPrefix, headerSuffix), trim));
