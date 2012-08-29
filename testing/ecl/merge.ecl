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

SomeFile1 := DATASET([{1,'A'},{1,'B'},{1,'C'},{1,'D'},{1,'E'},
                     {1,'F'},{1,'G'},{1,'H'},{1,'I'},{1,'J'},
                     {1,'K'},{1,'L'},{1,'M'},{1,'N'},{1,'O'},
                     {1,'P'},{1,'Q'},{1,'R'},{1,'S'},{1,'T'},
                     {1,'U'},{1,'V'},{1,'W'},{1,'X'},{1,'Y'}],
                    {integer1 number,STRING1 Letter});

SomeFile2 := DATASET([{2,'A'},{2,'B'},{2,'C'},{2,'D'},{2,'E'},
                     {2,'F'},{2,'G'},{2,'H'},{2,'I'},{2,'J'},
                     {2,'K'},{2,'L'},{2,'M'},{2,'N'},{2,'O'},
                     {2,'P'},{2,'Q'},{2,'R'},{2,'S'},{2,'T'},
                     {2,'U'},{2,'V'},{2,'W'},{2,'X'},{2,'Y'}],
                    {integer1 number,STRING1 Letter});

SomeFile3 := DATASET([{3,'A'},{3,'B'},{3,'C'},{3,'D'},{3,'E'},
                     {3,'F'},{3,'G'},{3,'H'},{3,'I'},{3,'J'},
                     {3,'K'},{3,'L'},{3,'M'},{3,'N'},{3,'O'},
                     {3,'P'},{3,'Q'},{3,'R'},{3,'S'},{3,'T'},
                     {3,'U'},{3,'V'},{3,'W'},{3,'X'},{3,'Y'}],
                    {integer1 number,STRING1 Letter});

m1 := merge(sort(SomeFile1,letter,number),sort(SomeFile2,letter,number),local);
m2 := merge(sort(SomeFile1,letter,number),sort(SomeFile2,letter,number),sort(SomeFile3,letter,number),local);

output(m1);
output(m2);