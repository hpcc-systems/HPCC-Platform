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

// Horrible test for success/failure workflow requring sequential/conditional children.

s1 := false : stored('s1');

o1 := output(100) : persist('o1');
o1b := output(99) : persist('o1b');

o2 := sequential(o1, o1b) : independent;

o3 := output(101) : persist('o3');

of := output(-1);

o4 := if (s1, o2, o3) : failure(of);



of1 := output(-2) : persist('of1');
of2 := output(-3) : persist('of2');

ofs := sequential(of1, of2);

o5 := o4 : failure(ofs, LABEL('When things go wrong'));


o5;
