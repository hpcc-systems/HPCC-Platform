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




AddNumbers(integer x, integer y) := define function return x+y; end;
AddNumbers2(integer x, integer y) := define x+y;

unknown := 99 : stored('unknown');


AddUnknown(integer x) := define function return x+unknown; end;

one := 1 : stored('one');
two := 2 : stored('two');


func2(unsigned x, unsigned y) := AddUnknown(x * y);

output(AddNumbers(one, two));
output(AddUnknown(one));
output(func2(two, two));


output(AddNumbers(1, 2));
output(AddNumbers2(AddNumbers(3, 4),AddNumbers(5,6)));
output(AddUnknown(1));
output(func2(2, 2));
