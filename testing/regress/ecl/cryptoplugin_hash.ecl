/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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
import Std;

output(Std.Crypto.SupportedHashAlgorithms());

output('SHA1 tests');
hashModuleSHA1 := Std.Crypto.Hashing('sha1');
DATA hashedValue     := hashModuleSHA1.Hash((DATA)'SHA1The quick brown fox jumps over the lazy dog');
DATA hashedValueEx   := hashModuleSHA1.Hash((DATA)'SHA1The most beautiful thing in the world is, of course, the world itself!');
DATA hashedValueExEx := hashModuleSHA1.Hash((DATA)'SHA1The most beautiful thing in the world is, of course, the world itself!');

output( hashedValue );
output( hashedValueEx );
output( hashedValueExEx);


output('SHA224 tests');
hashModuleSHA224 := Std.Crypto.Hashing('sha224');
DATA hashedValue224 := hashModuleSHA224.Hash((DATA)'SHA224The quick brown fox jumps over the lazy dog');
output( hashedValue224 );

output('SHA256 tests');
hashModuleSHA256 := Std.Crypto.Hashing('sha256');
DATA hashedValue256 := hashModuleSHA256.Hash((DATA)'SHA256The quick brown fox jumps over the lazy dog');
output( hashedValue256 );

output('SHA384 tests');
hashModuleSHA384 := Std.Crypto.Hashing('sha256');
DATA hashedValue384 := hashModuleSHA384.Hash((DATA)'SHA384The quick brown fox jumps over the lazy dog');
output( hashedValue384 );

output('SHA512 tests');
hashModuleSHA512 := Std.Crypto.Hashing('sha512');
DATA hashedValue512 := hashModuleSHA512.Hash((DATA)'SHA512The quick brown fox jumps over the lazy dog');
output( hashedValue512 );


//Try again on previously instantiated modules
output('Combined SHA* tests');

DATA RehashedValue := hashModuleSHA1.Hash((DATA)'SHA1The quick brown fox jumps over the lazy dog');
output( RehashedValue );

DATA RehashedValue224 := hashModuleSHA224.Hash((DATA)'SHA224The quick brown fox jumps over the lazy dog');
output( RehashedValue224 );

DATA RehashedValue256 := hashModuleSHA256.Hash((DATA)'SHA256The quick brown fox jumps over the lazy dog');
output( RehashedValue256 );

DATA RehashedValue384 := hashModuleSHA384.Hash((DATA)'SHA384The quick brown fox jumps over the lazy dog');
output( RehashedValue384 );

DATA RehashedValue512 := hashModuleSHA512.Hash((DATA)'SHA512The quick brown fox jumps over the lazy dog');
output( RehashedValue512 );


