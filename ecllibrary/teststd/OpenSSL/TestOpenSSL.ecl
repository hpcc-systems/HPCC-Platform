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
    'MIICCgKCAgEAtHYNEH8DswZUtx3Xk90cPCLtf3vl7awjZvI4V/yJkOb5U/sQRgsw' + '\n' +
    '60F84i8Okg3yBxGIEDiZ/cWKBYfQGsycbm+zljIHh0tt8S7hH9iwFiZB41urPZ4Q' + '\n' +
    'zSs84h05SZA9nCVPQOpFYi0I8n6QrGf9eokJf/jO/dZcvsIDYqtiLfTYWMLKibrY' + '\n' +
    'HgP9lIKxJieSmbrX4spYBEf+B9cSQqHBwThAP1mvkIzD2eGuYlQV6hXZb2dSXn8l' + '\n' +
    'Lakgq06mvkCxYglv6GEHKPrEd6KCZKgBLKZsEabvd5+eWXL4VcaAdXjQgRIa5aV2' + '\n' +
    'AJCk5V1ClMraJuw1aw62+2GTi7bTum4t+GJQdTfDSYreTBIZLUzHwlRnlbMrsthd' + '\n' +
    'aVWct3KId2BT/ntqmalBTcu+nK7H3XOEQGrFdQo2tQ459guchYqxk8QFZCVTkNzw' + '\n' +
    'Ky7SyJV9dqDDUhQQSTvwv42CbwpwbqSynE682h1IQThgk1LuDX/IIQRupIgbyJKo' + '\n' +
    'WrFYoNalhyoeXCgQT7gH2y5YdeoGf3oN7dn4HUDFdW2FOYPzpsSMLVr1MUhxelJc' + '\n' +
    'Ru1RUsJDwfkXBUOZM7ChgwGfQYWGjFcEOPK6AbUlSTER2vP3t5R/QPzN6uzih10y' + '\n' +
    'LFFeUxMuvLnmgpKSU9bVsXbN2D4hmInZW8vDRKugNeTrtaV6KkUus5sCAwEAAQ==' + '\n' +
    '-----END RSA PUBLIC KEY-----';
    EXPORT STRING RSA_PRIVATE_2 :=
    '-----BEGIN RSA PRIVATE KEY-----' + '\n' +
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
    EXPORT encrypt_rsa := Std.OpenSSL.RSA.Encrypt((DATA)PLAINTEXT, RSA_PUBLIC_1);
    EXPORT seal_rsa := Std.OpenSSL.RSA.Seal((DATA)PLAINTEXT, [RSA_PUBLIC_1, RSA_PUBLIC_2]);
    EXPORT signed_rsa_sha256 := Std.OpenSSL.RSA.Sign((DATA)PLAINTEXT, (DATA)PASSPHRASE, RSA_PRIVATE_1, 'sha256');

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
        ASSERT((STRING)Std.OpenSSL.RSA.Decrypt((DATA)encrypt_rsa, RSA_PRIVATE_1) = PLAINTEXT);
        ASSERT(LENGTH(seal_rsa) = 1112);
        ASSERT((STRING)Std.OpenSSL.RSA.Unseal((DATA)seal_rsa, RSA_PRIVATE_1) = PLAINTEXT);
        ASSERT((STRING)Std.OpenSSL.RSA.Unseal((DATA)seal_rsa, RSA_PRIVATE_2) = PLAINTEXT);

        ASSERT(Std.Str.ToHexPairs(signed_rsa_sha256) = '399F5FCB9B1A3C02D734BF3F1C9CB480681126B0F7505697E045ECF22E11A47FADCF7E2BA73761AD6345F6702AAD230957AFA1B0B3C8C29FC537AACA68' +
        '13B79DE0CDEF82B4BB6183D0637583D0E2AA78892EE190D7AB9CB20F9AC36D91DB2994A07F0C17A0CFB5AF0385EA4ABA9723FC72FB08081AE4C6C83E659AEBA1C103FB6F4E831EFDAA4CE037A3874D59664B1DE90' +
        '313B474897E2BF7D48CF09E19EC706A83B3E86B9F8A5F15BE01BA6A7E0BDC78877D8C60870E4713CCB87EC93A6C47E8EBF522E35BAEC85A1FC0209397CE0C62564B79104514D042F435FF0DF4B4500C8CE0F3AD76' +
        '4C26FDAFD14C493D303860C6A9AE7D539F42855C077EE682F6BD234CE035AAFADA5B5E90021D6D82D9030E687234D71E95CA701FDD6E097764443FEC237744FC3530E0AB3715F28B510766F73B6F56DDE7219AA2A' +
        'A77A41AF1D34200CC63F3D35E89398536F4BAA3A5D66E3C9BDF0C3AFFE9AE618413C3EAC2A980DF6B363D6F6F93BCB2D02D5BB4CCFF912CEDEE66FCF916F289C34CA2D45DFD14E545AA8D6CE591F455D993A7E6E4' +
        'C573C9EBE9FE472131A39D60ED53624745A79A29B31D07DE38D1FA64D3DB32EF447F62B64F8A07C012E55C06551F5C60509797A4521DC4E7CB33F9C0759E6B46DA6B758C86E26507CA9D79933532BE923FC449842' +
        '17A203F97B97606E18E9F4949DC5E6C21705A89844B316093EFD8C0CC');
        ASSERT(LENGTH(signed_rsa_sha256) = 512);
        ASSERT(Std.OpenSSL.RSA.VerifySignature((DATA)signed_rsa_sha256, (DATA)PLAINTEXT, (DATA)PASSPHRASE, RSA_PUBLIC_1, 'sha256') = TRUE);
        ASSERT(Std.OpenSSL.RSA.VerifySignature((DATA)((UNSIGNED)signed_rsa_sha256 + 1), (DATA)PLAINTEXT, (DATA)PASSPHRASE, RSA_PUBLIC_1, 'sha256') = FALSE);

        ASSERT(TRUE)
    ];
END;
