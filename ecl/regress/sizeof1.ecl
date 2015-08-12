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

// basic type
n1 := sizeof(integer8);
n1x := sizeof(unsigned integer8);
n2 := sizeof(boolean);
n2x := sizeof(ebcdic string10);

// basic type variable
integer4 i := 3;
n3 := sizeof(i);
boolean b := true;
n4 := sizeof(b);

// expr
m1 := sizeof('abc');
m2 := sizeof(123);
m3 := sizeof(true);
m4 := sizeof('abcd'+'def');
m5 := sizeof(1+3000);
m6 := sizeof((integer8)23);

// dataset

rec := record
   boolean bx;
   string3 sx;
   integer2 ix;
end;

ds := dataset('ds', rec, flat);

n5 := sizeof(ds);
n6 := sizeof(rec);
n7 := sizeof(ds.bx);

n := n1+n1x+n2+n2x+n3+n4+n4+n5+n6+n7;

n;
m1 + m2 + m3 + m4 + m5 + m6;
