/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

// This is here to ensure that roxie fails gracefully when a packet that is too large
// is sent from the master to the slave. Note that the limit is not applied when localSlave is set,
// so the test is meaningless - in order to ensure that the results still match on such systems,
// we simply fake the expected output.

//nothor
//nothorlcr
//nohthor

import Std.Str;

string s10 := (string10)'' : stored ('s10');
string s100 := s10 + s10 + s10 + s10 + s10 + s10 + s10 + s10 + s10 + s10 : stored ('s100');
string s1000 := s100 + s100 + s100 + s100 + s100 + s100 + s100 + s100 + s100 + s100 : stored ('s1000');
string s10000 := s1000 + s1000 + s1000 + s1000 + s1000 + s1000 + s1000 + s1000 + s1000 + s1000 : stored ('s10000');
string s100000 := s10000 + s10000 + s10000 + s10000 + s10000 + s10000 + s10000 + s10000 + s10000 + s10000 : stored ('s100000');
string myStoredString := 'x' + s100000 + 'x';


rec := { string x{maxlength(100)}, unsigned value };

expected := DATASET([{'Packet length', 1446}], rec);

ds := dataset([{'a',1},{'b',2},{'c',3},{'d',4}], rec);


f(dataset(rec) infile) := infile(x != myStoredString);

rec FailTransform := transform
  self.x := FAILMESSAGE[1..13]; 
  self.value := FAILCODE
END;

caught := catch(allnodes(f(ds)), onfail(FailTransform));

string localSlave := getEnv('control:localSlave');

output(IF(localSlave='1' OR Str.toLowerCase(localSlave)='true', expected, caught));

