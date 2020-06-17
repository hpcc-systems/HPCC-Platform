/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

/*###############################
    Create key files for testing
#################################*/
PKey :=  RECORD
  STRING  Key;
END;

dPubKey :=  DATASET([{
'-----BEGIN PUBLIC KEY-----' + '\n' +
'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAr64RncTp5pV0KMnWRAof' + '\n' +
'od+3AUS/IDngT39j3Iovv9aI2N8g4W5ipqhKftRESmzQ6I/TiUQcmi42soUXmCeE' + '\n' +
'BHqlMDydw9aHOQG17CB30GYsw3Lf8iZo7RC7ocQE3OcRzH0eBkOryW6X3efWnMoy' + '\n' +
'hIR9MexCldF+3WM/X0IX0ApSs7kuVPVG4Yj202+1FVO/XNwjMukJG5ASuxpYAQvv' + '\n' +
'/oKj6q7kInEIvhLiGfcm3bpTzWQ66zVz3z/huLbEXEy5oj2fQaC5E3s5mdpk/CW3' + '\n' +
'J6Tk4NY3NySWzE/2/ZOWxZdR79XC+goNL6v/5gPI8B/a3Z8OeM2PfSZwPMnVuvU0' + '\n' +
'bwIDAQAB' + '\n' +
'-----END PUBLIC KEY-----' + '\n'
}],PKey);

OUTPUT(dPubKey,,'~regress::certificates::pubkey.pem', CSV(SEPARATOR(''), TERMINATOR('')), OVERWRITE);



PRKey :=  RECORD
  STRING  Key;
END;

dPrivKey :=  DATASET([{
'-----BEGIN RSA PRIVATE KEY-----' + '\n' +
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
'-----END RSA PRIVATE KEY-----' + '\n'
}],PRKey);

OUTPUT(dPrivKey,,'~regress::certificates::privkey.pem', CSV(SEPARATOR(''), TERMINATOR('')), OVERWRITE);


/*###############################
    Begin tests
#################################*/
output('PKIEncryption Tests enumerating supported public key algorithms');
output(Std.Crypto.SupportedPublicKeyAlgorithms());


encModuleLFN := Std.Crypto.PublicKeyEncryptionFromLFN('RSA', '~regress::certificates::pubkey.pem', '~regress::certificates::privkey.pem', '');

//Digital Signature tests
output('Testing PKI PublicKeyEncryptionFromLFN Digital Signatures');

DATA sig1 := encModuleLFN.Sign((DATA)'The quick brown fox jumps over the lazy dog');
output(encModuleLFN.VerifySignature(sig1, (DATA)'This should fail'));
output(encModuleLFN.VerifySignature(sig1, (DATA)'The quick brown fox jumps over the lazy dog'));//this should pass

DATA sig2 := encModuleLFN.Sign((DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
DATA sig3 := encModuleLFN.Sign((DATA)'The most beautiful thing in the world is, of course, the world itself!');

output(encModuleLFN.VerifySignature(sig1, (DATA)'The quick brown fox jumps over the lazy dog'));//this should pass
output(encModuleLFN.VerifySignature(sig2, (DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,'));//this should pass
output(encModuleLFN.VerifySignature(sig3, (DATA)'The most beautiful thing in the world is, of course, the world itself!'));//this should pass



//Encrypt/Decrypt tests
output('Testing PKI PublicKeyEncryptionFromLFN Encrypt/Decrypt');

DATA enc1 := encModuleLFN.Encrypt((DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)encModuleLFN.Decrypt(enc1) );

DATA enc2 := encModuleLFN.Encrypt((DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
DATA enc3 := encModuleLFN.Encrypt((DATA)'The most beautiful thing in the world is, of course, the world itself!');//this should use the cached key file
output( (STRING)encModuleLFN.Decrypt(enc2) );//this should use the cached key file
output( (STRING)encModuleLFN.Decrypt(enc3) );//this should use the cached key file

output( (STRING)encModuleLFN.Decrypt(enc3) );//this should use the cached key file
output( (STRING)encModuleLFN.Decrypt(enc2) );//this should use the cached key file
output( (STRING)encModuleLFN.Decrypt(enc1) );//this should use the cached key file




encModuleLFN2 := Std.Crypto.PKEncryptionFromLFN('RSA', '~regress::certificates::pubkey.pem', '~regress::certificates::privkey.pem', (DATA)'PassPhrase');

//Digital Signature tests
output('Testing PKI PKEncryptionFromLFN Digital Signatures');

DATA sig12 := encModuleLFN2.Sign((DATA)'The quick brown fox jumps over the lazy dog');
output(encModuleLFN2.VerifySignature(sig12, (DATA)'This should fail'));
output(encModuleLFN2.VerifySignature(sig12, (DATA)'The quick brown fox jumps over the lazy dog'));//this should pass

DATA sig22 := encModuleLFN2.Sign((DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
DATA sig32 := encModuleLFN2.Sign((DATA)'The most beautiful thing in the world is, of course, the world itself!');

output(encModuleLFN2.VerifySignature(sig12, (DATA)'The quick brown fox jumps over the lazy dog'));//this should pass
output(encModuleLFN2.VerifySignature(sig22, (DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,'));//this should pass
output(encModuleLFN2.VerifySignature(sig32, (DATA)'The most beautiful thing in the world is, of course, the world itself!'));//this should pass



//Encrypt/Decrypt tests
output('Testing PKI PKEncryptionFromLFN Encrypt/Decrypt');

DATA enc12 := encModuleLFN2.Encrypt((DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)encModuleLFN2.Decrypt(enc12) );

DATA enc22 := encModuleLFN2.Encrypt((DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
DATA enc32 := encModuleLFN2.Encrypt((DATA)'The most beautiful thing in the world is, of course, the world itself!');//this should use the cached key file
output( (STRING)encModuleLFN2.Decrypt(enc22) );//this should use the cached key file
output( (STRING)encModuleLFN2.Decrypt(enc32) );//this should use the cached key file

output( (STRING)encModuleLFN2.Decrypt(enc32) );//this should use the cached key file
output( (STRING)encModuleLFN2.Decrypt(enc22) );//this should use the cached key file
output( (STRING)encModuleLFN2.Decrypt(enc12) );//this should use the cached key file

