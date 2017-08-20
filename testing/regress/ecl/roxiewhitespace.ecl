/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#OPTION('writeInlineContent', true);

//nothor
//nohthor

NameRec := RECORD
  string First;
  string Last;
END;

AddressRec := RECORD
  string City;
  string State;
  integer ZipCode;
END;

PersonRec := RECORD
  NameRec Name;
  AddressRec Address;
END;

PersonRecOut := RECORD
  NameRec Name;
  string FullName {xpath('name/<>')};
  AddressRec Address;
END;

peeps_send := DATASET([{{'  Joe  ', '  Doe  '}, {'Fresno', 'CA', 11111}}], PersonRec);

roxieEchoTestRequestRecord := RECORD
  DATASET(PersonRec) Peeps {XPATH('Peeps/Row')} := peeps_send;
END;

exceptionRec := RECORD
  string Source {xpath('Source')};
  integer Code {xpath('Code')};
  string Message {xpath('Message')};
END;

roxieEchoTestResponseRecord := RECORD
  DATASET(PersonRecOut) Peeps {XPATH('Dataset/Row')} := DATASET([], PersonRecOut);
  exceptionRec Exception {XPATH('Exception')};
END;

roxieEchoTestResponseRecord doFail() := TRANSFORM
  self.Exception.CODE := IF (FAILCODE=0, 0, ERROR(FAILCODE, FAILMESSAGE));
  self.Exception.Message := FAILMESSAGE;
  self.Exception.Source := 'Test';
END;

string TargetIP := '.' : stored('TargetIP');
string TargetURL := 'http://' + TargetIP + ':9876';

soapcallResult := SOAPCALL(TargetURL, 'roxie_keepwhitespace', roxieEchoTestRequestRecord,
    DATASET(roxieEchoTestResponseRecord),
    LITERAL,
    XPATH('*/Results/Result'),
    RESPONSE(NOTRIM),
    ONFAIL(doFail()));

OUTPUT(soapcallResult, NAMED('keepResult'));

roxieEchoTestStripRequest := RECORD
  roxieEchoTestRequestRecord;
  boolean _stripWhitespaceFromStoredDataset := true;
END;

stripResult := SOAPCALL(TargetURL, 'roxie_keepwhitespace', roxieEchoTestStripRequest,
    DATASET(roxieEchoTestResponseRecord),
    LITERAL,
    XPATH('*/Results/Result'),
    RESPONSE(NOTRIM),
    ONFAIL(doFail()));

OUTPUT(stripResult, NAMED('stripResult'));


//Test handling of roxie 500 server too busy
//if roxie thread counts increase greatly we could increase PARALLEL value, or make it dynamic somehow
d :=DATASET(3000, TRANSFORM(roxieEchoTestRequestRecord, SELF.peeps := peeps_send));

OUTPUT(count(sort(SOAPCALL(d, TargetURL,'roxie_keepwhitespace', roxieEchoTestRequestRecord, DATASET(roxieEchoTestResponseRecord), PARALLEL(300)), record)), NAMED('serverTooBusy'));

InlinePersonRec := RECORD
  string Name {XPATH('Name/<>')};
  string Address {XPATH('Address<>')};
END;

inline_peeps_send := DATASET([{'<first>a</first><last>b</last>', '<address><city>c</city><state>s</state><zipcode>9</zipcode></address>'}], InlinePersonRec);

inlineEchoTestRequestRecord := RECORD
  DATASET(InlinePersonRec) Peeps {XPATH('Peeps/Row')} := inline_peeps_send;
END;

inlineResult := SOAPCALL(TargetURL, 'roxie_keepwhitespace', inlineEchoTestRequestRecord,
    DATASET(roxieEchoTestResponseRecord),
    LITERAL,
    XPATH('*/Results/Result'),
    RESPONSE(NOTRIM),
    ONFAIL(doFail()));

OUTPUT(inlineResult, NAMED('inlineXmlResult'));
