/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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
'MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCWnKkGM0l3Y6pKhxMq87hAGBL6' + '\n' +
'FfEo2HC6XCSQuaAMLkdf7Yjn3FpvFIEO6A1ZYJy70cT8+HOFta+sSUyMn2fDc5cv' + '\n' +
'VdX8v7XCycYXEBeZ4KsTCHHPCUoO/nxNbxhNz09T8dx/JsIH50LHipR6FTLTSCXR' + '\n' +
'N9KVLaPXs5DdQx6PjQIDAQAB' + '\n' +
'-----END PUBLIC KEY-----' + '\n';

//--------------
//Vault Example
//--------------

DATA vaultData := x'227ACC7749A442CFBA6404AD59304DD608E3D1544B293221FA0A9E44AAAD272A3EEFF15387ABB54F1F375D35C034BB03623A5100942764356046DDDBA9963F7DDF1B7FED431769815F02BA2FDB4D1ECE7E5835FA392AB7FE5292979F80B469A062750F79039633CA60EDE01D292ED4B364C1BA7E8F1301F5DB33883872945A70';
VARSTRING vaultKey := (VARSTRING) getSecret('vault-example', 'crypt.key');

vaultEncModule := Std.Crypto.PKEncryptionFromBuffer('RSA', pubKey, vaultKey);

output( (STRING)vaultEncModule.Decrypt(vaultData), named('vault_message'));
