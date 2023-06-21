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

//class=roxieserviceaccess
//nothor
//nohthor
//nohthorlcr

#option('generateGlobalId', true);

string targetIP := '127.0.0.1';
string blackListedPort := '6666';

string blacklistedTargetURL1 := 'http://' + targetIP + ':6666';
string blacklistedTargetURL2 := 'http://' + targetIP + ':6667';
string blacklistedTargetURL3 := 'http://' + targetIP + ':6668';

d := dataset([{'FRED'},{'WILMA'},{'WILMA2'},{'WILMA3'},{'WILMA4'},{'WILMA5'},{'WILMA6'}], {string unkname});

ServiceOutRecord :=
    RECORD
        string name;
        unsigned4 id;
    END;

ServiceOutRecord doError(d l, integer a) := TRANSFORM
  SELF.name := 'ERROR: ' + failmessage;
  SELF.id := a;
END;

sequential(
  output(SOAPCALL(d, blacklistedTargetURL1, 'soapbase2', { unkname }, 
                  DATASET(ServiceOutRecord),
                  onFail(doError(LEFT, 3)),
                  HINT(blacklistdelay(20)),
                  RETRY(0), 
                  PARALLEL(1),
                  log('SOAP1: ' + unkname, 'TAIL: ' + unkname),
                  TIMEOUT(1))),
  output(SOAPCALL(d, blacklistedTargetURL2, 'soapbase2', { unkname }, 
                  DATASET(ServiceOutRecord),
                  onFail(doError(LEFT, 3)),
                  HINT(noblacklist),
                  RETRY(0), 
                  PARALLEL(1),
                  log('SOAP2: ' + unkname, 'TAIL: ' + unkname),
                  TIMEOUT(1))),
  output(SOAPCALL(d, blacklistedTargetURL3, 'soapbase2', { unkname }, 
                  DATASET(ServiceOutRecord),
                  onFail(doError(LEFT, 3)),
                  HINT(blacklisterror('No')),
                  RETRY(0), 
                  PARALLEL(1),
                  log('SOAP3: ' + unkname, 'TAIL: ' + unkname),
                  TIMEOUT(1))),
);
