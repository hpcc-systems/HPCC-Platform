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

//nothor
//nothorlcr
MyRec := RECORD
    STRING1 Value1;
    STRING1 Value2;
END;

SomeFile := DATASET([{'C','G'},
             {'C','C'},
             {'A','X'},
             {'B','G'},
             {'A','B'}],MyRec);

Dedup1 := DEDUP(SomeFile, 
                LEFT.Value2 IN ['G','C','X'] AND RIGHT.Value2 IN ['X','B','C'] ,ALL);

/*
Processes as:   LEFT   vs.  RIGHT
                1 (G)       2 (C)       - keep both
                1 (G)       3 (X)       - lose 3 (RIGHT rec)
                1 (G)       4 (G)       - keep both
                1 (G)       5 (B)       - lose 5 (RIGHT rec)

                2 (C)       1 (G)       - keep both
                2 (C)       4 (G)       - keep both

                3 (X)       1 (G)       - keep 1 (3 already "gone")
                3 (X)       2 (C)       - lose 2 
                3 (X)       4 (G)       - keep 4 (3 already "gone")

                4 (G)       1 (G)       - keep both

                5 (B)       1 (G)       - keep 1 (5 already "gone")
                5 (B)       4 (G)       - keep 4 (5 already "gone")
Result set is:
    Rec#    Value1  Value2
    1       C       G
    4       B       G
*/

Dedup2 := DEDUP(SomeFile, 
                LEFT.Value2 IN ['G','C'] AND RIGHT.Value2 IN ['X','B'] ,ALL);

/*
Processes as:   LEFT   vs.  RIGHT
                1 (G)       2 (C)       - keep both
                1 (G)       3 (X)       - lose 3 (RIGHT rec)
                1 (G)       4 (G)       - keep both
                1 (G)       5 (B)       - lose 5 (RIGHT rec)

                2 (C)       1 (G)       - keep both
                2 (C)       4 (G)       - keep both

                3 (X)       1 (G)       - keep 1 (3 already "gone")
                3 (X)       2 (C)       - keep 2 (3 already "gone")
                3 (X)       4 (G)       - keep 4 (3 already "gone")

                4 (G)       1 (G)       - keep both
                4 (G)       2 (C)       - keep both

                5 (B)       1 (G)       - keep 1 (5 already "gone")
                5 (B)       2 (C)       - keep 2 (5 already "gone")
                5 (B)       4 (G)       - keep 4 (5 already "gone")
Result set is:
    Rec#    Value1  Value2
    1       C       G
    2       C       C
    4       B       G
*/

Dedup3 := DEDUP(SomeFile, 
                LEFT.Value2 IN ['X','B'] AND RIGHT.Value2 IN ['G','C'],ALL);

/*
Processes as:   LEFT   vs.  RIGHT
                1 (G)       2 (C)       - keep both
                1 (G)       3 (X)       - keep both
                1 (G)       4 (G)       - keep both
                1 (G)       5 (B)       - keep both

                2 (C)       1 (G)       - keep both
                2 (C)       3 (X)       - keep both
                2 (C)       4 (G)       - keep both
                2 (C)       5 (B)       - keep both

                3 (X)       1 (G)       - lose RIGHT rec 1
                3 (X)       2 (C)       - lose RIGHT rec 2
                3 (X)       4 (G)       - lose RIGHT rec 4
                3 (X)       5 (B)       - keep both

                4 (G)       3 (X)       - keep 3 (4 already "gone")
                4 (G)       5 (B)       - keep 5 (4 already "gone")

                5 (B)       3 (X)       - keep both
Result set is:
    Rec#    Value1  Value2
    3       A       X
    5       A       B
*/

output(Dedup1);
output(Dedup2);
output(Dedup3);
