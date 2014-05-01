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

inRecord :=
            RECORD
unsigned        id;
unsigned        key;
unsigned        len;
string          extra;
            END;

unsigned minExtra := 100;
unsigned maxExtra := 200000;
unsigned num := 20000000;

inRecord makeRow(unsigned c) := TRANSFORM
    r10000 := RANDOM() % 100000;
    x := r10000 / 100000;
    e := 1- (EXP(x)-1) / (EXP(1)-1);
    len := e * (maxExtra - minExtra) + minExtra;

    SELF.id := c;
    SELF.key := HASH64(c);
    SELF.len := len;
    SELF.extra := ''[1..len];
END;

ds := DATASET(num, makeRow(COUNTER));


s := SORT(ds, key % 10000);

gr := GROUP(NOFOLD(s), (key % 10000) DIV 100);

s2 := SORT(gr, key);

x1 := CHOOSEN(s2, 1, GROUPED);
x2 := GROUP(x1);
x3 := SORT(NOFOLD(x2), key);

t := CHOOSEN(NOFOLD(x3), 1);
OUTPUT(TABLE(t), { id, key});
