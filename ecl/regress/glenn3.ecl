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

#option ('globalFold', false);
INTEGER1 i1     := 65;
INTEGER1 i1a    := 0x41;
INTEGER1 i1b    := 41x;
INTEGER1 i1c    := 0b01000001;
INTEGER1 i1d    := 01000001b;


OUTPUT(REJECTED(i1=i1a,i1=i1b,i1=i1c,i1=i1d));
OUTPUT(REJECTED(i1a=i1b,i1a=i1c,i1a=i1d));
OUTPUT(REJECTED(i1b=i1c,i1b=i1d));
OUTPUT(REJECTED(i1c=i1d));

OUTPUT((TRANSFER(i1,STRING1)));
OUTPUT((TRANSFER(i1a,STRING1)));
OUTPUT((TRANSFER(i1b,STRING1)));
OUTPUT((TRANSFER(i1c,STRING1)));
OUTPUT((TRANSFER(i1d,STRING1)));
