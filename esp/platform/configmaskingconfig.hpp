/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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


// Configuration is currently tailored to the sensitive data present in the legacy
// config files. It is expected to be expanded or changed to support containerized
// configurations in the future.
//
// There are several assumptions made by the rules in the configuration:
// 1. They rely on the default case-insensitive matching behavior of the masking
//    plugin.
// 2. The default of full value masking is needed.
// 3. They match the suffix of the XML attribute name. For example, the rule for
//    the LDAPVault value type matches both 'hpccAdminVaultId' and 'ldapAdminVaultId'
//    attributes.
//
constexpr const char* configMaskerConfig = R"!!!(
maskingPlugin:
  library: datamasker
  entryPoint: newPartialMaskSerialToken
  profile:
    - domain: 'urn:hpcc:platform:configs'
      version: 1
      valueType:
        - name: password
          rule:
            - startToken: 'password="'
              endToken: '"'
              matchCase: false
              contentType: xml
            - startToken: 'passphrase="'
              endToken: '"'
              matchCase: false
              contentType: xml
        - name: LDAPVault
          rule:
            - startToken: 'AdminVaultId="'
              endToken: '"'
              matchCase: false
              contentType: xml
        - name: LDAPKey
          rule:
            - startToken: 'AdminSecretKey="'
              endToken: '"'
              matchCase: false
              contentType: xml
        - name: LDAPUser
          rule:
            - startToken: 'systemCommonName="'
              endToken: '"'
              matchCase: false
              contentType: xml
            - startToken: 'systemUser="'
              endToken: '"'
              matchCase: false
              contentType: xml
            - startToken: 'ldapUser="'
              endToken: '"'
              matchCase: false
              contentType: xml
        - name: SSHUser
          rule:
            - startToken: 'SSHusername="'
              endToken: '"'
              matchCase: false
              contentType: xml
        - name: Certificate
          rule:
            - startToken: 'CA_Certificates_Path="'
              endToken: '"'
              matchCase: false
              contentType: xml
            - startToken: 'certificateFileName="'
              endToken: '"'
              matchCase: false
              contentType: xml
            - startToken: 'privateKeyFileName="'
              endToken: '"'
              matchCase: false
              contentType: xml
            - startToken: 'siteCertificate="'
              endToken: '"'
              matchCase: false
              contentType: xml
)!!!";
