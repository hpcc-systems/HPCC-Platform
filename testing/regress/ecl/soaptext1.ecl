/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

//nothor
//nohthor

//version callDirect=true
//version callViaEsp=false,persistConnection=false,encryptConnection=false
//version callViaEsp=false,persistConnection=true,encryptConnection=false
//xversion callViaEsp=false,persistConnection=false,encryptConnection=true
//xversion callViaEsp=false,persistConnection=true,encryptConnection=true
//version callViaEsp=true,persistConnection=false,encryptConnection=false
//version callViaEsp=true,persistConnection=true,encryptConnection=false
//xversion callViaEsp=true,persistConnection=false,encryptConnection=true
//xversion callViaEsp=true,persistConnection=true,encryptConnection=true

import ^ as root;

//Simplified configuration
string roxieIP := '.' : STORED('remoteRoxieService');
string espIP :=  '.' : STORED('remoteEspService');
boolean defaultEncryptConnection :=     false : stored('encryptConnection');
boolean defaultPersistConnection := false : stored('persistConnection');
boolean defaultCallDirect := false : stored('callDirect');
boolean defaultCallViaEsp := false : stored('callViaEsp');

//Simplified call options - doesn't provide all combinations that should be tested eventually
boolean encryptConnection := #IFDEFINED(root.encryptConnection, defaultEncryptConnection);
boolean persistConnection := #IFDEFINED(root.persistConnection, defaultPersistConnection);
boolean callDirect := #IFDEFINED(root.callDirect, defaultCallDirect);
boolean callViaEsp := #IFDEFINED(root.callViaEsp, defaultCallViaEsp);

//--- end of version configuration ---

import common.SoapTextTest;

#stored ('searchWords', 'one,and,sheep,when,richard,king');

string searchWords := '' : stored('searchWords');
unsigned documentLimit := 3 : stored('documentLimit');
unsigned maxResults := 50;

//----------------

ConnectionType := SoapTextTest.ConnectionType;

SoapTextTest.CallOptionsRecord initCallOptions() := TRANSFORM
    SELF.connectionToRoxie := IF(encryptConnection, ConnectionType.Encrypted, ConnectionType.Plain);
    SELF.connectionToEsp := IF(encryptConnection, ConnectionType.Encrypted, ConnectionType.Plain);
    SELF.persistConnectToRoxie := persistConnection;
    SELF.persistConnectToEsp := persistConnection;
    SELF.connectDirectToRoxie := NOT callViaEsp;
    SELF.embedServiceCalls := callDirect;
    SELF := [];
END;

SoapTextTest.ConfigOptionsRecord initConfigOptions() := TRANSFORM
    SELF.remoteEspUrl := espIP;
    SELF.remoteRoxieUrl := roxieIP;
    SELF := [];
END;


callOptions := ROW(initCallOptions());
configOptions := ROW(initConfigOptions());

SoapTextTest.runMainService(searchWords, documentLimit, maxResults, callOptions, configOptions);
