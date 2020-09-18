/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

//class=spray
//class=roxieserviceaccess
//version targetIP='127.0.0.1',goodPort='9876',blacListedPort='9875'

#option('generateGlobalId', true);

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
output(SORT(HTTPCALL(targetURL,'soapbase', { string unkname := 'FRED' }, dataset(ServiceOutRecord), JSON, XPATH('*/Results', ROWPATH('result_1/Row'), ExcPath('Exception')), LOG('simple'), HTTPHEADER('HPCC-Global-Id','12345678900'), HTTPHEADER('HPCC-Caller-Id','1111')),record));

// noroot /no service name -- recreate root
output(SORT(HTTPCALL(targetURL,'', { string unkname := 'FRED' }, dataset(ServiceOutRecord), JSON(NOROOT, HEADING('{"soapbase":{', '}}')), XPATH('*/Results', ROWPATH('result_1/Row'), ExcPath('Exception')), LOG('simple'), HTTPHEADER('HPCC-Global-Id','12345678900'), HTTPHEADER('HPCC-Caller-Id','1111')),record));

// double query->dataset form
output(SORT(HTTPCALL(doubleTargetURL,'soapbase', { string unkname := 'FRED' }, dataset(ServiceOutRecord), JSON, XPATH('soapbaseResponse/Results', ROWPATH('*/Row'), ExcPath('Exception')), LOG, HTTPHEADER('HPCC-Global-Id','12345678900'), HTTPHEADER('HPCC-Caller-Id','2222')),record));

// simple dataset->dataset form
output(sort(HTTPCALL(d, targetURL,'soapbase', { unkname }, DATASET(ServiceOutRecord), JSON, XPATH('*', ROWPATH('Results/result_1/Row'), ExcPath('Results/Exception')), HTTPHEADER('HPCC-Global-Id','12345678900'), HTTPHEADER('HPCC-Caller-Id','3333')),record));

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

output(SORT(HTTPCALL(d, blacklistedTargetURL,'soapbase', { unkname }, DATASET(ServiceOutRecord), JSON, XPATH('*', ROWPATH('Results/result_1/Row'), ExcPath('Results/Exception')), onFail(doError(LEFT)),RETRY(0), log('SOAP: ' + unkname),TIMEOUT(1)), record));

output(SORT(HTTPCALL(targetURL,'soapbase', { string unkname := 'FAIL' }, dataset(ServiceOutRecord), JSON, XPATH('soapbaseResponse/Results', ROWPATH('result_1/Row'), ExcPath('Exception')),onFail(doError2),RETRY(0), LOG(MIN), HTTPHEADER('Global-Id','12345678900'), HTTPHEADER('Caller-Id','4444')),record));

output(SORT(HTTPCALL(d, targetURL,'soapbaseNOSUCHQUERY', { unkname }, DATASET(ServiceOutRecord), JSON, XPATH('*/Results', ROWPATH('result_1/Row'), ExcPath('Exception')), onFail(doError3(LEFT)),PARALLEL(1),RETRY(0), LOG(MIN)), record));

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
output(count(nofold(choosen(HTTPCALL(d, targetURL,'soapbase', { string unkname := d.unkname+'1' }, dataset(FullServiceOutRecord), JSON, XPATH('soapbaseResponse/Results', ROWPATH('result_1/Row'), ExcPath('Exception'))),1))));
output(count(nofold(choosen(HTTPCALL(d, targetURL,'soapbase', { string unkname := d.unkname+'1' }, dataset(FullServiceOutRecord), JSON, XPATH('soapbaseResponse/Results', ROWPATH('result_1/Row'), ExcPath('Exception'))),2))));
