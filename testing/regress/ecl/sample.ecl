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

SomeFile := DATASET([{'A'},{'B'},{'C'},{'D'},{'E'},
                     {'F'},{'G'},{'H'},{'I'},{'J'},
                     {'K'},{'L'},{'M'},{'N'},{'O'},
                     {'P'},{'Q'},{'R'},{'S'},{'T'},
                     {'U'},{'V'},{'W'},{'X'},{'Y'}],
                    {STRING1 Letter});

Set1 := SAMPLE(SomeFile,5,1);
Set2 := SAMPLE(SomeFile,5,2);
Set3 := SAMPLE(SomeFile,5,3);
Set4 := SAMPLE(SomeFile,5,4);
Set5 := SAMPLE(SomeFile,5,5);

OUTPUT(Set1);
OUTPUT(Set2);
OUTPUT(Set3);
OUTPUT(Set4);
OUTPUT(Set5);
/* When run on hthor, the expected results are:
            Set1:       Set2:       Set3:       Set4:       Set5:
    Rec#    Letter      Letter      Letter      Letter      Letter
    1       A           B           C           D           E
    2       F           G           H           I           J
    3       K           L           M           N           O
    4       P           Q           R           S           T
    5       U           V           W           X           Y
*/