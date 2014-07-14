/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#STORED('unsignedeight',18446744073709551615)
unsigned8 eight := 0 : stored('unsignedeight');
output(eight);

#STORED('unsignedeightstr','18446744073709551615')
string eight_as_str := '' : stored('unsignedeightstr');
unsigned8 eight_cast := (unsigned8) eight_as_str;

output(eight_cast);

#STORED('x1',0x7fffffffffffffff)
unsigned8 x1 := 0 : stored('x1');
output(x1);

#STORED('x2',0x8000000000000000)
unsigned8 x2 := 0 : stored('x2');
output(x2);

#STORED('x3',0xFFFFFFFFFFFFFFFF)
unsigned8 x3 := 0 : stored('x3');
output(x3);

#STORED('x4',0b0111111111111111111111111111111111111111111111111111111111111111)
unsigned8 x4 := 0 : stored('x4');
output(x4);

#STORED('x5',0b1000000000000000000000000000000000000000000000000000000000000000)
unsigned8 x5 := 0 : stored('x5');
output(x5);

#STORED('x6',0b1111111111111111111111111111111111111111111111111111111111111111)
unsigned8 x6 := 0 : stored('x6');
output(x6);

