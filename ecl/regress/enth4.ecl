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

SomeFile := DATASET([{'A'},{'B'},{'C'},{'D'},{'E'},
                     {'F'},{'G'},{'H'},{'I'},{'J'},
                     {'K'},{'L'},{'M'},{'N'},{'O'},
                     {'P'},{'Q'},{'R'},{'S'},{'T'},
                     {'U'},{'V'},{'W'},{'X'},{'Y'}],
                    {STRING1 Letter});

Set0 := ENTH(SomeFile,2,10,0);
Set1 := ENTH(SomeFile,2,10,1);
Set2 := ENTH(SomeFile,2,10,2);
Set3 := ENTH(SomeFile,2,10,3);
Set4 := ENTH(SomeFile,2,10,4);
Set5 := ENTH(SomeFile,2,10,5);
Set6 := ENTH(SomeFile,2,10,6);
Set7 := ENTH(SomeFile,2);

OUTPUT(Set0);
OUTPUT(Set1);
OUTPUT(Set2);
OUTPUT(Set3);
OUTPUT(Set4);
OUTPUT(Set5);
OUTPUT(Set6);
OUTPUT(Set7);

