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

OUTPUT(HASH(0));
OUTPUT(HASH(1234567890));
OUTPUT(HASH(0987654321));
OUTPUT(HASH('abcdefghijklmnopqrstuvwxyz'));
OUTPUT(HASH('!@#$%^&*--_+=-0987654321~`'));
OUTPUT(HASH64(0));
OUTPUT(HASH64(1234567890));
OUTPUT(HASH64(0987654321));
OUTPUT(HASH64('abcdefghijklmnopqrstuvwxyz'));
OUTPUT(HASH64('@#$%^&*---+=-0987654321~`'));
OUTPUT(HASHCRC(0));
OUTPUT(HASHCRC(1234567890));
OUTPUT(HASHCRC(0987654321));
OUTPUT(HASHCRC('abcdefghijklmnopqrstuvwxyz'));
OUTPUT(HASHCRC('!@#$%^&*--_+=-0987654321~`'));

OUTPUT(ATAN2(0.0,1.0));
OUTPUT(ATAN2(0.0,-1.0));
OUTPUT(ATAN2(-1.0,0.0));
OUTPUT(ATAN2(1.0,0.0));
OUTPUT(ATAN2(1.0,1.0));
OUTPUT(ATAN2(2.0,2.0));
OUTPUT(ATAN2(3.0,3.0));
OUTPUT(ATAN2(4.0,4.0));
OUTPUT(ATAN2(-1.0,1.0));
OUTPUT(ATAN2(-2.0,2.0));
OUTPUT(ATAN2(-3.0,3.0));
OUTPUT(ATAN2(-4.0,4.0));
OUTPUT(ATAN2(-1.0,-1.0));
OUTPUT(ATAN2(-2.0,-2.0));
OUTPUT(ATAN2(-3.0,-3.0));
OUTPUT(ATAN2(-4.0,-4.0));
OUTPUT(ATAN2(1.0,-1.0));
OUTPUT(ATAN2(2.0,-2.0));
OUTPUT(ATAN2(3.0,-3.0));
OUTPUT(ATAN2(4.0,-4.0));
