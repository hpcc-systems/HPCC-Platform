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


d := dataset([{'FRED'},{'WILMA'}], {string unkname});

ServiceOutRecord := 
    RECORD
        string name;
        data pic;
        unsigned4 id{xpath('r')};
        unsigned4 novalue;
    END;

// simple query->dataset form
output(SORT(SOAPCALL('http://127.0.0.1:9876','soapbase', { string unkname := 'FRED' }, dataset(ServiceOutRecord), LOG('simple')),record));

// double query->dataset form
output(SORT(SOAPCALL('http://127.0.0.1:9876|http://127.0.0.1:9876','soapbase', { string unkname := 'FRED' }, dataset(ServiceOutRecord)),record));

// simple dataset->dataset form
output(sort(SOAPCALL(d, 'http://127.0.0.1:9876','soapbase', { unkname }, DATASET(ServiceOutRecord)),record));

// double query->dataset form
ServiceOutRecord doError(d l) := TRANSFORM
  SELF.name := 'ERROR: ' + if (l.unkname='FRED' AND failmessage='connection failed 127.0.0.1:9875','blacklisted socket 127.0.0.1:9875', failmessage);
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

// Test some failure cases

output(SORT(SOAPCALL(d, 'http://127.0.0.1:9876|http://127.0.0.1:9875','soapbase', { unkname }, DATASET(ServiceOutRecord), onFail(doError(LEFT)),RETRY(0), log('SOAP: ' + unkname),TIMEOUT(1)), record));
output(SORT(SOAPCALL('http://127.0.0.1:9876','soapbase', { string unkname := 'FAIL' }, dataset(ServiceOutRecord),onFail(doError2),RETRY(0), LOG(MIN)),record));
output(SORT(SOAPCALL(d, 'http://127.0.0.1:9876','soapbaseNOSUCHQUERY', { unkname }, DATASET(ServiceOutRecord), onFail(doError(LEFT)),MERGE(25),PARALLEL(1),RETRY(0), LOG(MIN)), record));

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
output(count(nofold(choosen(SOAPCALL(d, 'http://127.0.0.1:9876','soapbase', { string unkname := d.unkname+'1' }, dataset(FullServiceOutRecord)),1))));
output(count(nofold(choosen(SOAPCALL(d, 'http://127.0.0.1:9876','soapbase', { string unkname := d.unkname+'1' }, dataset(FullServiceOutRecord), merge(3)),2))));

