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

Set1 := ENTH(SomeFile,2,10,1);
Set2 := ENTH(SomeFile,2,10,2);
Set3 := ENTH(SomeFile,2,10,3);
Set4 := ENTH(SomeFile,2,10,4);
Set5 := ENTH(SomeFile,2,10,5);
Set6 := ENTH(SomeFile,5); // semantically equivalent to ENTH(SomeFile,5,COUNT(SomeFile),1);
Set7 := ENTH(SomeFile,0); // no rows output
Set8 := ENTH(SomeFile,0,5); // no rows output
Set9 := ENTH(SomeFile,5,0); // all rows output
Set10 := ENTH(SomeFile,0,0); // special case, no rows output
EmptyFile := SomeFile(Letter = 'Z');

OUTPUT(Set1);
OUTPUT(Set2);
OUTPUT(Set3);
OUTPUT(Set4);
OUTPUT(Set5);
OUTPUT(Set6);
OUTPUT(Set7);
OUTPUT(Set8);
OUTPUT(Set9);
OUTPUT(Set10);

/* The expected results for sets 1--5 are:
            Set1:       Set2:       Set3:       Set4:       Set5:
    Rec#    Letter      Letter      Letter      Letter      Letter
    1       E           D           C           B           A
    2       J           I           H           G           F
    3       O           N           M           L           K
    4       T           S           R           Q           P
    5       Y           X           W           V           U
*/
