/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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