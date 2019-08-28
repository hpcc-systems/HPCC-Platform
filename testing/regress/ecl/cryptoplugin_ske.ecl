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

output('aes-256-cbc tests');
SymmModule256 := Std.Crypto.SymmetricEncryption( 'aes-256-cbc', '01234567890123456789012345678901' );
DATA dat256 := SymmModule256.Encrypt( (DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule256.Decrypt(dat256) );

DATA dat256Ex := SymmModule256.Encrypt( (DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
output( (STRING)SymmModule256.Decrypt(dat256Ex) );

output('aes-192-cbc tests');
SymmModule192 := Std.Crypto.SymmetricEncryption( 'aes-192-cbc', '012345678901234567890123' );
DATA dat192 := SymmModule192.Encrypt( (DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule192.Decrypt(dat192) );

output('aes-128-cbc tests');
SymmModule128 := Std.Crypto.SymmetricEncryption( 'aes-128-cbc', '0123456789012345' );
DATA dat128 := SymmModule128.Encrypt( (DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule128.Decrypt(dat128) );

//Try again on previously instantiated modules

output('aes-xxx-cbc combined tests');
DATA dat256ExEx := SymmModule256.Encrypt( (DATA)'256The quick brown fox jumps over the lazy dog');
output ( (STRING)SymmModule256.Decrypt(dat256ExEx) );
output ( (STRING)SymmModule256.Decrypt(dat256ExEx) );
output ( (STRING)SymmModule256.Decrypt(dat256ExEx) );

DATA dat192Ex := SymmModule192.Encrypt( (DATA)'192The quick brown fox jumps over the lazy dog');
output ( (STRING)SymmModule192.Decrypt(dat192Ex) );
output ( (STRING)SymmModule192.Decrypt(dat192Ex) );
output ( (STRING)SymmModule192.Decrypt(dat192Ex) );

DATA dat128Ex := SymmModule128.Encrypt( (DATA)'128The quick brown fox jumps over the lazy dog');
output( (STRING)SymmModule128.Decrypt(dat128Ex) );
output( (STRING)SymmModule128.Decrypt(dat128Ex) );
output( (STRING)SymmModule128.Decrypt(dat128Ex) );
