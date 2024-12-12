/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the License);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an AS IS BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

Import Std;

EXPORT TestOpenSSL := MODULE

    EXPORT STRING PLAINTEXT := 'Colorless green ideas sleep furiously.';
    EXPORT STRING CIPHERS_CIPHER := 'aes-256-cbc';
    EXPORT STRING PASSPHRASE := 'mypassphrase';

    EXPORT STRING RSA_PUBLIC_1 :=
    '-----BEGIN RSA PUBLIC KEY-----' + '\n' +
    'MIICCgKCAgEAuLYYTmPMzp13999s7bUAaJZVON+9k/2PDBF5AfHtQ40FmxwRdG3d' + '\n' +
    'CeUcbhb8Nuw3YizS7WgdZBtjyi7hZ146aHBDXVXhJlInR3y44hAnYB/sOccxQYWQ' + '\n' +
    '6xOZ6VpZ7MnJl4j2OFmoc62QAwQuqizXhfFOtd2KfTubKsMPR1tWrWRC6zlDBkYV' + '\n' +
    'D677MqQyhAuZvCjYO5E5dPmoVVxZaBxjsTjElEqtjkRdROG1D0PJY+8XkWP/76ns' + '\n' +
    'ieNsTb9R0umN4x4+lzsHSB4tcGICx+S7cW+FmJzyGlf6J4YWEq4VqoIQ7MI8O8Hj' + '\n' +
    'fuR3jdZwubZQrFzKtvWv3i3Dm16+JxmtK6qjhJhGzlXchBk+j8eOUaWOV0sBMLPo' + '\n' +
    'sVEapY+SmISxj0sn8yLSfbUs5ivHaEMNaZq6BM1Db5y7MEOANv2FplvT9oW7UECg' + '\n' +
    '7D7PUZnrBZf16FiNxwXnxK/Sn049y/2nFnHPxACWZVVtXN9N64GZPEP1/vYyYFCN' + '\n' +
    'yt0mdYjJLtf6z87YYLM4kHMNqnbPJKOQahvRC4fW4/QfQYFjLwHFNppUo9D/bx2t' + '\n' +
    'sCmDEWc7cGQE081tQep34rpTel0wkePhxajmSlRNyVtv3ZXph1Hl1XoOj17QLdyw' + '\n' +
    'WjCAqcMKrVHnOkyKo2lTNhY3bsu6PfEe4dTTia3/WlPJe0nGa0SUuvkCAwEAAQ==' + '\n' +
    '-----END RSA PUBLIC KEY-----';
    EXPORT STRING RSA_PRIVATE_1 :=
    '-----BEGIN RSA PRIVATE KEY-----' + '\n' +
    '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' + '\n' +
    '-----END RSA PRIVATE KEY-----';

    EXPORT STRING RSA_PUBLIC_2 :=
    '-----BEGIN RSA PUBLIC KEY-----' + '\n' +
    'MIICCgKCAgEA4AhX7olMaerm6IZEQQ3yhb4HWOtbMNuDcr8GEnjvxCP7JubdsJ82' + '\n' +
    'v9Agft1dmMPpqMrcoH0F06hzyyHBcnyFYAKsBG8v8nNFI9nxD/Yy4V86ROymQP4y' + '\n' +
    '1OJNXhXQE3vO65lDRm5aru0cVKKymSDhD0lW+fFUYQdKm6U5yanFHFSlDpjs3dUl' + '\n' +
    'fmXrFkqzmMNs/jfHkxQy10aTcTh0aGucz5mlW/nbOiz9pcQULZU/WFcz+j8/cLr7' + '\n' +
    'oLUbwz4ZjJ3y+DLhFQF4cCdU9iein80pLwLvKiY6abQ/kBGrXDCHIH0UXQg/pjTF' + '\n' +
    '+gVXI3phYBq60vbQvzpqdW6lUJov/E6GKc49aDN3Fe8t476Hm1KW2KqO55Ym3WGZ' + '\n' +
    'q5JS9kNBBqvNQ3ZX4GMxVwJkRBBWViV/wIWQQYbxpoGdqjMosRtJ9+xBJD+W7TB6' + '\n' +
    '/cokgWz6OtYWKmlAEnGO1e4V6eokAlv5HfQRlQHFSafvpVs0B+CljfAtnxkO+4hy' + '\n' +
    'CisLsgT5RjheBrIimqPrGXmYsPVR7Kh/zaNUV8MVufqBd49zsVzyGclcLUZ8mTxN' + '\n' +
    'bly8yoSvoMGZ0P1BgEmhhvfHE/SGkPG0PSBnEKOQmw9gsN3t+dJWi41cSrnMnmmW' + '\n' +
    'JkTtyrvCsP0csQVBhgzCMoXrIL4Xooiu2qK5pZ3LdMBJPiV7TuVHZakCAwEAAQ==' + '\n' +
    '-----END RSA PUBLIC KEY-----';
    EXPORT STRING RSA_PRIVATE_2 :=
    '-----BEGIN RSA PRIVATE KEY-----' + '\n' +
    'Proc-Type: 4,ENCRYPTED' + '\n' +
    'DEK-Info: AES-128-CBC,48EDA073E8D55C8DF0D7FF600AB45FC1' + '\n' +    '\n' +
    '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' + '\n' +
    '-----END RSA PRIVATE KEY-----';

    EXPORT UNSIGNED8 staticSalt := 123456789;
    EXPORT DATA staticIV := (DATA)'0123456789012345';

    EXPORT hash_sha256 := Std.OpenSSL.Digest.Hash((DATA)PLAINTEXT, 'sha256');
    EXPORT hash_sha512 := Std.OpenSSL.Digest.Hash((DATA)PLAINTEXT, 'sha512');
    EXPORT encrypt_my_iv_my_salt := Std.OpenSSL.Ciphers.Encrypt((DATA)PLAINTEXT, CIPHERS_CIPHER, (DATA)PASSPHRASE, salt := (>DATA<)staticSalt, iv := staticIV);
    EXPORT encrypt_default_iv_my_salt := Std.OpenSSL.Ciphers.Encrypt((DATA)PLAINTEXT, CIPHERS_CIPHER, (DATA)PASSPHRASE, salt := (>DATA<)staticSalt);
    EXPORT encrypt_my_iv_default_salt := Std.OpenSSL.Ciphers.Encrypt((DATA)PLAINTEXT, CIPHERS_CIPHER, (DATA)PASSPHRASE, iv := staticIV);
    EXPORT encrypt_default_iv_default_salt := Std.OpenSSL.Ciphers.Encrypt((DATA)PLAINTEXT, CIPHERS_CIPHER, (DATA)PASSPHRASE);
    EXPORT encrypt_rsa := Std.OpenSSL.PublicKey.Encrypt((DATA)PLAINTEXT, RSA_PUBLIC_1);
    EXPORT encrypt_rsa_passphrase := Std.OpenSSL.PublicKey.Encrypt((DATA)PLAINTEXT, RSA_PUBLIC_2);
    EXPORT seal_rsa := Std.OpenSSL.PublicKey.RSASeal((DATA)PLAINTEXT, [RSA_PUBLIC_1, RSA_PUBLIC_2]);
    EXPORT signed_rsa_sha256 := Std.OpenSSL.PublicKey.Sign((DATA)PLAINTEXT, (DATA)'', RSA_PRIVATE_1);
    EXPORT signed_rsa_sha256_passphrase := Std.OpenSSL.PublicKey.Sign((DATA)PLAINTEXT, (DATA)PASSPHRASE, RSA_PRIVATE_2, 'SHA256');

    // Fails to sign with the wrong passphrase and throws an exception
    // There is no clean way to capture this exception, so it is not tested here
    // EXPORT signed_rsa_sha256_wrong_passphrase := Std.OpenSSL.PublicKey.Sign((DATA)PLAINTEXT, (DATA)'notmypassphrase', RSA_PRIVATE_2, 'SHA256'); Fails with Error: -1: Error within loading a pkey: error:1C800064:Provider routines::bad decrypt

    EXPORT TestDigests := [
        ASSERT(COUNT(Std.OpenSSL.Digest.AvailableAlgorithms()) > 0, 'No digest algorithms available');

        ASSERT(Std.Str.ToHexPairs(hash_sha256) = '0F6BE6CC79C301CEA386173C0919FEE806E78075618656D24F733AA5052EBF6D');
        ASSERT(LENGTH(hash_sha256) = 32);
        ASSERT(Std.Str.ToHexPairs(hash_sha512) = '9E309515371114774EE9E3C216888AFA89CC6616AF2E583601BA6E84750943B246E2CA5A518B44096A3A5929A1A50BF842117676DAFA5435CDF981DCA1344F8F');
        ASSERT(LENGTH(hash_sha512) = 64);

        ASSERT(TRUE)
    ];

    EXPORT TestCiphers := [
        ASSERT(COUNT(Std.OpenSSL.Ciphers.AvailableAlgorithms()) > 0, 'No cipher algorithms available');

        ASSERT(Std.OpenSSL.Ciphers.IVSize(CIPHERS_CIPHER) = 16);
        ASSERT(Std.OpenSSL.Ciphers.SaltSize(CIPHERS_CIPHER) = 8);

        ASSERT(Std.Str.ToHexPairs(encrypt_my_iv_my_salt) = '7D99A971120AF361404F93F27CABB44BEDC9333BF84C3DFDBC803F3CEE7A4BA1A5BE0C2FD98A507E718988A140F2B29D');
        ASSERT(LENGTH(encrypt_my_iv_my_salt) = 48);
        ASSERT((STRING)Std.OpenSSL.Ciphers.Decrypt(encrypt_my_iv_my_salt, CIPHERS_CIPHER, (DATA)PASSPHRASE, salt := (>DATA<)staticSalt, iv := staticIV) = PLAINTEXT);
        ASSERT(LENGTH(encrypt_default_iv_my_salt) = 48);
        ASSERT((STRING)Std.OpenSSL.Ciphers.Decrypt(encrypt_default_iv_my_salt, CIPHERS_CIPHER, (DATA)PASSPHRASE, salt := (>DATA<)staticSalt) = PLAINTEXT);
        ASSERT(LENGTH(encrypt_my_iv_default_salt) = 48);
        ASSERT((STRING)Std.OpenSSL.Ciphers.Decrypt(encrypt_my_iv_default_salt, CIPHERS_CIPHER, (DATA)PASSPHRASE, iv := staticIV) = PLAINTEXT);
        ASSERT(LENGTH(encrypt_default_iv_default_salt) = 48);
        ASSERT((STRING)Std.OpenSSL.Ciphers.Decrypt(encrypt_default_iv_default_salt, CIPHERS_CIPHER, (DATA)PASSPHRASE) = PLAINTEXT);

        ASSERT(TRUE)
    ];

    EXPORT TestRSA := [
        ASSERT(LENGTH(encrypt_rsa) = 512);
        ASSERT((STRING)Std.OpenSSL.PublicKey.Decrypt((DATA)encrypt_rsa, (DATA)'', RSA_PRIVATE_1) = PLAINTEXT);
        ASSERT((STRING)Std.OpenSSL.PublicKey.Decrypt((DATA)encrypt_rsa_passphrase, (DATA)PASSPHRASE, RSA_PRIVATE_2) = PLAINTEXT);
        ASSERT(LENGTH(seal_rsa) = 1100);
        ASSERT((STRING)Std.OpenSSL.PublicKey.RSAUnseal((DATA)seal_rsa, (DATA)'', RSA_PRIVATE_1) = PLAINTEXT);
        ASSERT((STRING)Std.OpenSSL.PublicKey.RSAUnseal((DATA)seal_rsa, (DATA)PASSPHRASE, RSA_PRIVATE_2) = PLAINTEXT);

        ASSERT(Std.Str.ToHexPairs(signed_rsa_sha256) = '399F5FCB9B1A3C02D734BF3F1C9CB480681126B0F7505697E045ECF22E11A47FADCF7E2BA73761AD6345F6702AAD230957AFA1B0B3C8C29FC537AACA68' +
        '13B79DE0CDEF82B4BB6183D0637583D0E2AA78892EE190D7AB9CB20F9AC36D91DB2994A07F0C17A0CFB5AF0385EA4ABA9723FC72FB08081AE4C6C83E659AEBA1C103FB6F4E831EFDAA4CE037A3874D59664B1DE90' +
        '313B474897E2BF7D48CF09E19EC706A83B3E86B9F8A5F15BE01BA6A7E0BDC78877D8C60870E4713CCB87EC93A6C47E8EBF522E35BAEC85A1FC0209397CE0C62564B79104514D042F435FF0DF4B4500C8CE0F3AD76' +
        '4C26FDAFD14C493D303860C6A9AE7D539F42855C077EE682F6BD234CE035AAFADA5B5E90021D6D82D9030E687234D71E95CA701FDD6E097764443FEC237744FC3530E0AB3715F28B510766F73B6F56DDE7219AA2A' +
        'A77A41AF1D34200CC63F3D35E89398536F4BAA3A5D66E3C9BDF0C3AFFE9AE618413C3EAC2A980DF6B363D6F6F93BCB2D02D5BB4CCFF912CEDEE66FCF916F289C34CA2D45DFD14E545AA8D6CE591F455D993A7E6E4' +
        'C573C9EBE9FE472131A39D60ED53624745A79A29B31D07DE38D1FA64D3DB32EF447F62B64F8A07C012E55C06551F5C60509797A4521DC4E7CB33F9C0759E6B46DA6B758C86E26507CA9D79933532BE923FC449842' +
        '17A203F97B97606E18E9F4949DC5E6C21705A89844B316093EFD8C0CC');
        ASSERT(LENGTH(signed_rsa_sha256) = 512);
        ASSERT(Std.OpenSSL.PublicKey.VerifySignature((DATA)signed_rsa_sha256, (DATA)PLAINTEXT, RSA_PUBLIC_1, 'sha256') = TRUE);
        ASSERT(Std.OpenSSL.PublicKey.VerifySignature((DATA)((UNSIGNED)signed_rsa_sha256 + 1), (DATA)PLAINTEXT, RSA_PUBLIC_1, 'sha256') = FALSE);
        ASSERT(Std.Str.ToHexPairs(signed_rsa_sha256_passphrase) = '448BD2397EB945D508E81A0AE45A01BB9799CAEDC8EEA779798BB07B5CB0C7D3FD571FF602298214F68B5215F039CEE1E2D6D75112A5CCD' +
        'A95875C2774779893101907F0F2BD7C259CB2A0519FAE1A015F48D025A446D69C9E50EA8DB0EC071E53178E6550E13E52ACDA9466D012590AAB358F25E68E91AAEC63E1323823CF48004D27406236079C0610347A' +
        '9A6F4B9B58496DE430C0DDF4BFBD9DC333910EA14F3D8E9F7ADCF8FF9BE4C2AE4735CFE38C2B7F6D08313FA6CDF9E836B7156566851E65165907B74DB1A45D4C404423E5AF34C3972231AE4F18455C90448B0459F' +
        '3365D037F997FDEB48458646BAE2F756E0A0A0EC68F4676F5426DB568ABE0914592CF0320202063C3F2A4850E7C3BDB801CE9CA8055FFAD5BFFCA2BD3EEC85FDC8C72DA109A7A879097F3B5B1BB106C655DE12C3D' +
        '1F09DFDD98BF6CA52E021EEC7C39D07C982FDBA7888A88EFC506ADD915CAD4AEC9E6BED8598254322D684E5621DF97F557DFB585D424A877461EBA33C8AFFE0354DBBE9C283E11B03AEBD2974FB876F27292834D3' +
        '012A8A61A76990085F386495BE729B16D6646D754D3F0AEAD89BA044A8BB9813437F344A579DA4676438CDB7BB98EA396C98E86CBB3FCA1BC46A392E23A52AE177F6C798793146081FD0FD637570D9A718C148E17' +
        'FFC502936F4FC09E35592B4B7C2FFB1DFE6B7F4CC766595D47A630AB1A58CDD11716');
        ASSERT(LENGTH(signed_rsa_sha256_passphrase) = 512);
        ASSERT(Std.OpenSSL.PublicKey.VerifySignature((DATA)signed_rsa_sha256_passphrase, (DATA)PLAINTEXT, RSA_PUBLIC_2, 'SHA256') = TRUE);
        ASSERT(Std.OpenSSL.PublicKey.VerifySignature((DATA)((UNSIGNED)signed_rsa_sha256_passphrase + 1), (DATA)PLAINTEXT, RSA_PUBLIC_2, 'SHA256') = FALSE);

        // Fails to sign with the wrong passphrase and throws an exception
        // There is no clean way to capture this exception, so it is not tested here
        // ASSERT(LENGTH(signed_rsa_sha256_wrong_passphrase) = 0); // fails with "Error: -1: Error within loading a pkey: error:1C800064:Provider routines::bad decrypt"

        ASSERT(TRUE)
    ];
END;
