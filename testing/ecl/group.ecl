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

MyRec := RECORD
    STRING1 Value1;
    STRING1 Value2;
END;

SomeFile := DATASET([{'C','G'},
                     {'C','C'},
                     {'A','X'},
                     {'B','G'},
                     {'A','B'}],MyRec);

SortedRecs  := SORT(SomeFile,Value1);

GroupedRecs1 := GROUP(SomeFile,Value1);     // unsorted
GroupedRecs2 := GROUP(SortedRecs,Value1);   // sorted

SortedRecs1 := SORT(GroupedRecs1,Value2);
SortedRecs2 := SORT(GroupedRecs1,-Value2);
SortedRecs3 := SORT(GroupedRecs2,Value2);
SortedRecs4 := SORT(GroupedRecs2,-Value2);

OUTPUT(SortedRecs1);
OUTPUT(SortedRecs2);
OUTPUT(SortedRecs3);
OUTPUT(SortedRecs4);

/*
SortedRecs1 results in:
    Rec#    Value1  Value2
    1       C       C
    2       C       G
    3       A       X
    4       B       G
    5       A       B

SortedRecs2 results in:
    Rec#    Value1  Value2
    1       C       G
    2       C       C
    3       A       X
    4       B       G
    5       A       B

SortedRecs3 results in:
    Rec#    Value1  Value2
    1       A       B
    2       A       X
    3       B       G
    4       C       C
    5       C       G

SortedRecs4 results in:
    Rec#    Value1  Value2
    1       A       X
    2       A       B
    3       B       G
    4       C       G
    5       C       C
*/