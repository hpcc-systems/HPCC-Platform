/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the 'License');
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an 'AS IS' BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */
import Std;

STRING pubKey := '-----BEGIN PUBLIC KEY-----' + '\n' +
'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAr64RncTp5pV0KMnWRAof' + '\n' +
'od+3AUS/IDngT39j3Iovv9aI2N8g4W5ipqhKftRESmzQ6I/TiUQcmi42soUXmCeE' + '\n' +
'BHqlMDydw9aHOQG17CB30GYsw3Lf8iZo7RC7ocQE3OcRzH0eBkOryW6X3efWnMoy' + '\n' +
'hIR9MexCldF+3WM/X0IX0ApSs7kuVPVG4Yj202+1FVO/XNwjMukJG5ASuxpYAQvv' + '\n' +
'/oKj6q7kInEIvhLiGfcm3bpTzWQ66zVz3z/huLbEXEy5oj2fQaC5E3s5mdpk/CW3' + '\n' +
'J6Tk4NY3NySWzE/2/ZOWxZdR79XC+goNL6v/5gPI8B/a3Z8OeM2PfSZwPMnVuvU0' + '\n' +
'bwIDAQAB' + '\n' +
'-----END PUBLIC KEY-----';


STRING privKey := '-----BEGIN RSA PRIVATE KEY-----' + '\n' +
'MIIEowIBAAKCAQEAr64RncTp5pV0KMnWRAofod+3AUS/IDngT39j3Iovv9aI2N8g' + '\n' +
'4W5ipqhKftRESmzQ6I/TiUQcmi42soUXmCeEBHqlMDydw9aHOQG17CB30GYsw3Lf' + '\n' +
'8iZo7RC7ocQE3OcRzH0eBkOryW6X3efWnMoyhIR9MexCldF+3WM/X0IX0ApSs7ku' + '\n' +
'VPVG4Yj202+1FVO/XNwjMukJG5ASuxpYAQvv/oKj6q7kInEIvhLiGfcm3bpTzWQ6' + '\n' +
'6zVz3z/huLbEXEy5oj2fQaC5E3s5mdpk/CW3J6Tk4NY3NySWzE/2/ZOWxZdR79XC' + '\n' +
'+goNL6v/5gPI8B/a3Z8OeM2PfSZwPMnVuvU0bwIDAQABAoIBAQCnGAtNYkOOu8wW' + '\n' +
'F5Oid3aKwnwPytF211WQh3v2AcFU17qle+SMRi+ykBL6+u5RU5qH+HSc9Jm31AjW' + '\n' +
'V1yPrdYVZInFjYIJCPzorcXY5zDOmMAuzg5PBVV7VhUA0a5GZck6FC8AilDUcEom' + '\n' +
'GCK6Ul8mR9XELBFQ6keeTo2yDu0TQ4oBXrPBMN61uMHCxh2tDb2yvl8Zz+EllADG' + '\n' +
'70pztRWNOrCzrC+ARlmmDfYOUgVFtZin53jq6O6ullPLzhkm3/+QFRGYWsFgQB6J' + '\n' +
'Z9HJtW5YB47RT5RbLHKXeMc6IJW+d+5HrzgTdK79P7wAZk8JCIDyHe2AaNAUzc/G' + '\n' +
'sB0cNeURAoGBAOKtaVFa6z2F4Q+koMBXCt4m7dCJnaC+qthF249uEOIBeF3ds9Fq' + '\n' +
'f0jhhvuV0OcN8lYbR/ZlYRJDUs6mHh/2BYSkdeaLKojXTxKR2bA4xQk5dtJCdoPf' + '\n' +
'0c15AlTgOYk2oNXP/azDICJYT/cdvIdUL9P4IoZthu1FjwG266GacEnNAoGBAMZn' + '\n' +
'1wRUXS1dbqemoc+g48wj5r3/qsIG8PsZ2Y8W+oYW7diNA5o6acc8YPEWE2RbJDbX' + '\n' +
'YEADBnRSdzzOdo0JEj4VbNZEtx6nQhBOOrtYKnnqHVI/XOz3VVu6kedUKdBR87KC' + '\n' +
'eCzO1VcEeZtsTHuLO4t7NmdHGqNxTV+jLvzBoQsrAoGAI+fOD+nz6znirYSpRe5D' + '\n' +
'tW67KtYxlr28+CcQoUaQ/Au5kjzE9/4DjXrT09QmVAMciNEnc/sZBjiNzFf525wv' + '\n' +
'wZP/bPZMVYKtbsaVkdlcNJranHGUrkzswbxSRzmBQ5/YmCWrDAuYcnhEqmMWcuU9' + '\n' +
'8jiS13JP9hOXlHDyIBYDhV0CgYBV6TznuQgnzp9NpQ/H8ijxilItz3lHTu4mLMlR' + '\n' +
'9mdAjMkszdLTg5uuE+z+N8rp17VUseoRjb3LvLG4+MXIyDbH/0sDdPm+IjqvCNDR' + '\n' +
'spmh9MgBh0JbsbWaZK0s9/qrI/FcSLZ04JLsfRmTPU/Y5y8/dHjYO6fDQhp44RZF' + '\n' +
'iCqNxQKBgHf7KZIOKgV4YNyphk1UYWHNz8YY5o7WtaQ51Q+kIbU8PRd9rqJLZyk2' + '\n' +
'tKf8e6z+wtKjxi8GKQzE/IdkQqiFmB1yEjjRHQ81WS+K5NnjN1t0IEscJqOAwv9s' + '\n' +
'iIhG5ueb6xoj/N0LuXa8loUT5aChKWxRHEYdegqU48f+qxUcJj9R' + '\n' +
'-----END RSA PRIVATE KEY-----';

output('PKIEncryption Tests enumerating supported algorithms');
output(Std.Crypto.SupportedPublicKeyAlgorithms());

encModule := Std.Crypto.PublicKeyEncryptionFromBuffer('RSA', pubKey, privKey, 'Passphrase');

//Digital Signature tests
output('Testing PKE PublicKeyEncryptionFromBuffer Digital Signatures');
DATA signature := encModule.Sign((DATA)'The quick brown fox jumps over the lazy dog');
output( TRUE = encModule.VerifySignature(signature, (DATA)'The quick brown fox jumps over the lazy dog'));
output(FALSE = encModule.VerifySignature(signature, (DATA)'Your Name Here'));

DATA bogus := (DATA)'Not a valid signaturexxx';
output(FALSE = encModule.VerifySignature(bogus, (DATA)'Not a valid signaturexxx'));

DATA sig256Ex := encModule.Sign((DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
output(TRUE = encModule.VerifySignature(sig256Ex, (DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,'));


//Encrypt/Decrypt tests
output('Testing PKE PublicKeyEncryptionFromBuffer Encrypt/Decrypt');
DATA encrypted := encModule.Encrypt((DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)encModule.Decrypt(encrypted) );

DATA sigBuff := encModule.Sign((DATA)'The quick brown fox jumps over the lazy dog');
output(encModule.VerifySignature(sigBuff, (DATA)'The quick brown fox jumps over the lazy dog'));//TRUE

DATA pkiDat := encModule.Encrypt((DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)encModule.Decrypt(pkiDat) );



PKEncMod := Std.Crypto.PKEncryptionFromBuffer('RSA', pubKey, privKey, (DATA)'Passphrase');

//Digital Signature tests
output('Testing PKE PKEncryptionFromBuffer Digital Signatures');
DATA signature2 := PKEncMod.Sign((DATA)'The quick brown fox jumps over the lazy dog');
output( TRUE = PKEncMod.VerifySignature(signature2, (DATA)'The quick brown fox jumps over the lazy dog'));
output(FALSE = PKEncMod.VerifySignature(signature2, (DATA)'Your Name Here'));

DATA bogus2 := (DATA)'Not a valid signaturexxx';
output(FALSE = PKEncMod.VerifySignature(bogus2, (DATA)'Not a valid signaturexxx'));

DATA sig256Ex2 := PKEncMod.Sign((DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
output(TRUE = PKEncMod.VerifySignature(sig256Ex2, (DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,'));


//Encrypt/Decrypt tests
output('Testing PKE PKEncryptionFromBuffer Encrypt/Decrypt');
DATA encrypted2 := PKEncMod.Encrypt((DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)PKEncMod.Decrypt(encrypted2) );

DATA sigBuff2 := PKEncMod.Sign((DATA)'The quick brown fox jumps over the lazy dog');
output(PKEncMod.VerifySignature(sigBuff2, (DATA)'The quick brown fox jumps over the lazy dog'));//TRUE

DATA pkiDat2 := PKEncMod.Encrypt((DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)PKEncMod.Decrypt(pkiDat2) );
