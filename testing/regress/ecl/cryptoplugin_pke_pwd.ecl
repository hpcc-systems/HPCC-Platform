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

PKey :=  RECORD
  STRING  Key;
END;

/*Private key generated first, using
   openssl genrsa -aes128 -passout pass:ThisIsMyPassphrase -out priv.pem 1024
*/

STRING privKey := '-----BEGIN RSA PRIVATE KEY-----' + '\n' +
'Proc-Type: 4,ENCRYPTED' + '\n' +
'DEK-Info: AES-128-CBC,27840180591F7545A3BC6AC26017B5E2' + '\n' +
'' + '\n' +
'JZ7kSTs0chmd3TmPTWQW3NM9dtfgJN59cecq8UzNeDfNdXQYU5WwhPFebpqX6K4H' + '\n' +
'hRJ4pKaFCS39+ib68Yalwb5T+vru9t6WHhJkbGcl41bz6U0aXs3FCEEGFUEngVWu' + '\n' +
'lonE8YjbeC+kiE7UlnGiFweteTJNlzbsFfa0w3U/6/tkfbd6ZDbriEhUvrbp1EPw' + '\n' +
'JAAZDs9MNCqs2S76VqqWHyWhVI32lgauVRqDNZTZDSnXF9/huUUSuK8fLK4G68Jz' + '\n' +
'0gSb7AeR9/AaJgg1FVUantmX7Ja60qLQW4O6DzTJgTGtuKEhaX3wNjpH5aKw8Ifn' + '\n' +
'gVdZrm9hBKGQCxC5JjVjcrKRXKjj7iKf+d0UN57q9BlKcqw+r+ET2Lqf2jnm1XTt' + '\n' +
'O1i6VkEGCZxSKdy2jb0d1kHNJonXyrW7mukfclO2LKqDwWYr2efu4wv0Dt9ttWeA' + '\n' +
'jL6taU7O3aGwjTibLW8qcneWKQogIwnmvY2TsDTtL7Pr+zXpIeBOvuu9+IEGV5nm' + '\n' +
'j4pVrlApKDF7+hhhYyevJSEfnImCwgeji3pZ5CnFEYASBMEGZmGmWtJyZ/sDkrTe' + '\n' +
'RjyOV22NaHWtu7HISaOgU/inG8NwGsOL91osnmE+hB07vr44Blaz2oHQhtEZb35k' + '\n' +
'YLeP+sf4MK0iQy/aKnLcZBHig8/m4MIPNKgpHu/MJ03pbiNiUzV34q4IkuvQiEFf' + '\n' +
'9N/p4HHRx6789Ndf1b8+iW0VtftfTt/HXYnSw2I1InFfB8KmnC20gYIQEorGPUuX' + '\n' +
'32yYXSjYNdyWZI52PwX57LD/A5YwkuTowib5MyYFoA2Po51B9bHNCTwzN1RTfGbH' + '\n' +
'-----END RSA PRIVATE KEY-----' + '\n';

/*Public key generated using
   openssl rsa -in priv.pem -passin pass:ThisIsMyPassphrase -pubout -out pub.pem
*/

STRING pubKey := '-----BEGIN PUBLIC KEY-----' + '\n' +
'MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCWnKkGM0l3Y6pKhxMq87hAGBL6' + '\n' +
'FfEo2HC6XCSQuaAMLkdf7Yjn3FpvFIEO6A1ZYJy70cT8+HOFta+sSUyMn2fDc5cv' + '\n' +
'VdX8v7XCycYXEBeZ4KsTCHHPCUoO/nxNbxhNz09T8dx/JsIH50LHipR6FTLTSCXR' + '\n' +
'N9KVLaPXs5DdQx6PjQIDAQAB' + '\n' +
'-----END PUBLIC KEY-----' + '\n';

//--------------
//BEGIN TESTING
//--------------

encModule := Std.Crypto.PKEncryptionFromBuffer('RSA', pubKey, privKey, (DATA)'ThisIsMyPassphrase');

//Digital Signature

//Digital Signature tests
output('Testing PKE PublicKeyEncryptionFromBuffer Digital Signatures');
DATA signature := encModule.Sign((DATA)'The quick brown fox jumps over the lazy dog');
output( TRUE = encModule.VerifySignature(signature, (DATA)'The quick brown fox jumps over the lazy dog'));
output(FALSE = encModule.VerifySignature(signature, (DATA)'Your Name Here'));

DATA bogus := (DATA)'Not a valid signaturexxx';
output(FALSE = encModule.VerifySignature(bogus, (DATA)'Not a valid signaturexxx'));

DATA sig256Ex := encModule.Sign((DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,');
output(TRUE = encModule.VerifySignature(sig256Ex, (DATA)'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTTUVWXYZ`~!@#$%^&*()_-+=|}]{[":;?/>.<,'));

//encrypt/decrypt

DATA enc1 := encModule.Encrypt((DATA)'The quick brown fox jumps over the lazy dog');
output( (STRING)encModule.Decrypt(enc1) );

DATA enc2 := encModule.Encrypt((DATA)'Hello World!');
output( (STRING)encModule.Decrypt(enc2) );

DATA enc3 := encModule.Encrypt((DATA)'I like eggs');
output( (STRING)encModule.Decrypt(enc3) );

