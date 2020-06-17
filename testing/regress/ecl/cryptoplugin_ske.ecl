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

output(Std.Crypto.SupportedSymmetricCipherAlgorithms());

output('Testing SymmetricEncryption aes-256-cbc');
SymmModule256 := Std.Crypto.SymmetricEncryption( 'aes-256-cbc', '01234567890123456789012345678901' );
DATA dat256 := SymmModule256.Encrypt( (DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule256.Decrypt(dat256) );

DATA dat256Ex := SymmModule256.Encrypt( (DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
output( (STRING)SymmModule256.Decrypt(dat256Ex) );

output('Testing SymmetricEncryption aes-192-cbc');
SymmModule192 := Std.Crypto.SymmetricEncryption( 'aes-192-cbc', '012345678901234567890123' );
DATA dat192 := SymmModule192.Encrypt( (DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule192.Decrypt(dat192) );

output('Testing SymmetricEncryption aes-128-cbc');
SymmModule128 := Std.Crypto.SymmetricEncryption( 'aes-128-cbc', '0123456789012345' );
DATA dat128 := SymmModule128.Encrypt( (DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule128.Decrypt(dat128) );


output('Testing SymmEncryption aes-256-cbc');
SymmModule256p := Std.Crypto.SymmEncryption( 'aes-256-cbc', (DATA)'01234567890123456789012345678901' );
DATA dat256p := SymmModule256p.Encrypt( (DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule256p.Decrypt(dat256p) );

DATA dat256Exp := SymmModule256p.Encrypt( (DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
output( (STRING)SymmModule256p.Decrypt(dat256Exp) );

output('Testing SymmEncryption aes-192-cbc');
SymmModule192p := Std.Crypto.SymmEncryption( 'aes-192-cbc', (DATA)'012345678901234567890123' );
DATA dat192p := SymmModule192p.Encrypt( (DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule192p.Decrypt(dat192p) );

output('Testing SymmEncryption aes-128-cbc');
SymmModule128p := Std.Crypto.SymmEncryption( 'aes-128-cbc', (DATA)'0123456789012345' );
DATA dat128p := SymmModule128p.Encrypt( (DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule128p.Decrypt(dat128p) );