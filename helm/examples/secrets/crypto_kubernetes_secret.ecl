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

//--------------
//K8S Example
//--------------

DATA k8sData := x'5C62E1843162330ED7BDAB7F37E50F892A669B54B8A466ED421F14954AA0505BA9EADAC4DA1D1FB1FD53EBDCF729D1049F893B3EE53ECCE48813A546CF58EBBB26EF5B9247002F7A8D1F90C3C372544501A126CEFC4B385BF540931FC0224D4736E4E1E4CF0C67D035063900887C240C8C4F365F0186ED0515E98B23C75E482A';
VARSTRING k8sKey := (VARSTRING) getSecret('k8s-example', 'crypt.key');

OUTPUT( (STRING)Std.OpenSSL.PublicKey.RSAUnseal(k8sData, (DATA)'', k8sKey), NAMED('k8s_message'));
