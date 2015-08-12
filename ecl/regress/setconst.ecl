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

#option ('globalFold', false);
//The following should be true:

[1]=[1];
[]=[];
[1]>[];
[1,2]>[1];
[2]>[1,2];

['Gavin','Hawthorn']>=['Elizabeth','Hawthorn'];
['Gavin','Hawthorn']>=['Gavin   ','Hall'];

[1]=[1];
[]<[1];
[1]<[1,2];
[1,2]<[2];

boolean z(set of integer a) := a >= [1,2,3];
boolean y(set of unsigned4 b) := z(b);

y([1,2,3]);
y([1,2,4]);

integer c2(set of integer a, set of integer b) := (a <=> b);
integer setcompare(set of unsigned4 a, set of unsigned4 b) := c2(a, b);


setcompare([1,2,3],[1,2,3]) = 0;
setcompare([],[]) = 0;
setcompare([1,2],[1,2,3]) < 0;
setcompare([1,2,2],[1,2,3]) < 0;
setcompare([1,2,3],[1,2]) > 0;
setcompare([1,2,4],[1,2,3]) > 0;


//The following should be false:

[1]=[2];
[1]=[1,2];
[2,1]=[2];
[1]<[];
[1,2]<[1];
[2]<[1,2];

3 <=> 4;
