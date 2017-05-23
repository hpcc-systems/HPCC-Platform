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

//nohthor
//nothor

//version targetIP='127.0.0.1',goodPort='9876',blacListedPort='9875'

string targetIP := '.' : stored('targetIP');
string goodPort := '9876' : stored('goodPort');
string blacListedPort := '9875' : stored('blacListedPort');

string targetURL := 'http://' + targetIP + ':' + goodPort;
string doubleTargetURL := 'http://'+ targetIP +':' + goodPort + '|http://'+ targetIP +':' + goodPort;
string blacklistedTargetURL := 'http://'+ targetIP +':' + goodPort + '|http://' + targetIP + ':' + blacListedPort;

d := dataset([{'FRED'},{'WILMA'}], {string unkname});

ServiceOutRecord :=
    RECORD
        string name;
        data pic;
        unsigned4 id{xpath('r')};
        unsigned4 novalue;
    END;

// simple query->dataset form
output(SORT(SOAPCALL(targetURL,'soapbase', { string unkname := 'FRED' }, dataset(ServiceOutRecord), LOG('simple')),record));

// double query->dataset form
output(SORT(SOAPCALL(doubleTargetURL,'soapbase', { string unkname := 'FRED' }, dataset(ServiceOutRecord)),record));

// simple dataset->dataset form
output(sort(SOAPCALL(d, targetURL,'soapbase', { unkname }, DATASET(ServiceOutRecord)),record));

// double query->dataset form
ServiceOutRecord doError(d l) := TRANSFORM
  //SELF.name := 'ERROR: \'' + l.unkname + '\'-\'' + failmessage[1..18] + '\''; //if (l.unkname='FRED' AND failmessage[1..18]='blacklisted socket','blacklisted socket', failmessage);
  SELF.name := 'ERROR: ' + if (l.unkname='FRED' AND (failmessage[1..18]='blacklisted socket' OR failmessage[1..18]='connection failed '),'blacklisted socket', failmessage[1..18]);
  SELF.pic := x'01020304';
  SELF.id := if (l.unkname='FRED' AND failcode=-3,-1,failcode);
  SELF.novalue := 0;
END;

ServiceOutRecord doError2 := TRANSFORM
  SELF.name := 'ERROR: ' + failmessage;
  SELF.pic := x'01020304';
  SELF.id := failcode;
  SELF.novalue := 0;
END;

ServiceOutRecord doError3(d l) := TRANSFORM
  SELF.name := 'ERROR: ' + failmessage;
  SELF.pic := x'01020304';
  SELF.id := if (l.unkname='FRED' AND failcode=-3,-1,failcode);
  SELF.novalue := 0;
END;

// Test some failure cases

output(SORT(SOAPCALL(d, blacklistedTargetURL,'soapbase', { unkname }, DATASET(ServiceOutRecord), onFail(doError(LEFT)),RETRY(0), log('SOAP: ' + unkname),TIMEOUT(1)), record));
output(SORT(SOAPCALL(targetURL,'soapbase', { string unkname := 'FAIL' }, dataset(ServiceOutRecord),onFail(doError2),RETRY(0), LOG(MIN)),record));
output(SORT(SOAPCALL(d, targetURL,'soapbaseNOSUCHQUERY', { unkname }, DATASET(ServiceOutRecord), onFail(doError3(LEFT)),MERGE(25),PARALLEL(1),RETRY(0), LOG(MIN)), record));

childRecord := record
unsigned            id;
    end;

FullServiceOutRecord :=
    RECORD
        string name;
        data pic;
        unsigned4 id{xpath('r')};
        dataset(childRecord) ids{maxcount(5)};
        unsigned4 novalue;
    END;

//leak children when linked counted rows are enabled, because not all records are read
//Use a count so the results are consistent, and nofold to prevent the code generator removing the child dataset...
output(count(nofold(choosen(SOAPCALL(d, targetURL,'soapbase', { string unkname := d.unkname+'1' }, dataset(FullServiceOutRecord)),1))));
output(count(nofold(choosen(SOAPCALL(d, targetURL,'soapbase', { string unkname := d.unkname+'1' }, dataset(FullServiceOutRecord), merge(3)),2))));
