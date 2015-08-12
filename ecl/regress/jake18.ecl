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

#option('optimizeDiskRead', 0);

tRec1 :=
    record
        String1 S;
        Integer4 K;
        Integer4 H;
        Integer1 B1;
        Integer1 B2;
        String84 FILL;
        Integer4 CRC;
        String1 E;
    end;


tRec3 :=
    record
        Integer4 K1;
        Integer4 K2;
    end;


DS1 := dataset ('~test::testfile2 ', tRec1, flat);



fun1 := DS1&DS1;
fun2 := DS1&DS1;
fun3 := fun1+fun2+DS1;


count(fun3);
