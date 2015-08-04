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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTableX := dataset('x',namesRecord,FLAT);
namesTableY := dataset('y',namesRecord,FLAT);


c1 := false : stored('c1');
c2 := false : stored('c2');
c3 := false : stored('c3');
c4 := false : stored('c4');
c5 := false : stored('c5');
c6 := false : stored('c6');
c7 := false : stored('c7');
c8 := false : stored('c8');

ds1 := namesTableX(age > 1);
ds2:= namesTableY(age > 2);

i1 := if(c1, ds1, ds2);
i2 := if(c2, i1, ds2);
output(i2);

ds3 := namesTableX(age > 3);
ds4:= namesTableY(age > 4);

i3 := if(c3, ds3, ds4);
i4 := if(c4, i3, ds3);
output(i4);

ds5 := namesTableX(age > 5);
ds6:= namesTableY(age > 6);

i5 := if(c5, ds5, ds6);
i6 := if(c6, ds6, i5);
output(i6);

ds7 := namesTableX(age > 7);
ds8:= namesTableY(age > 8);

i7 := if(c7, ds7, ds8);
i8 := if(c8, ds7, i7);
output(i8);

