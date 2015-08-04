/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

a := [2,3,5,7];
a;
a[2..3];
a[..3];
a[2..];
a[3..*];

b := ['Richard', 'Gavin', 'Jake', 'Gordon', 'John', 'Tony', 'Mark', 'Attila', 'Jamie'];
b;
b[5..];
b[..4];
b[3..6];
b[7..*];

c := [2.2,3.3,5.5,7.7];
c;
c[2..3];
c[..3];
c[2..];
c[3..*];
