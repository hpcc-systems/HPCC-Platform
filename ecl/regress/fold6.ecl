/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

// Test folding of comparisons of unsigned values against zero

unsigned4 a := nofold(3); // : STORED('a');
ASSERT(MAX(a, (unsigned4) 0) = a);
ASSERT(a >= 0);
ASSERT(0 <= a);

BIG_ENDIAN unsigned4 ba := nofold(3); // : STORED('ba');

ASSERT(MAX(ba, (BIG_ENDIAN unsigned4) 0) = ba);
ASSERT(ba >= 0);
ASSERT(0 <= ba);

unsigned decimal4 da := nofold(3); // : STORED('da');

ASSERT(MAX(da, (unsigned decimal4) 0) = da);
ASSERT(da >= 0);
ASSERT(0 <= da);
