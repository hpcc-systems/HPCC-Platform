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

