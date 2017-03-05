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

peeps_send := DATASET([
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 11111}}
], PersonRec);

roxieEchoTestRequestRecord := RECORD
  DATASET(PersonRec) Peeps {XPATH('Peeps/Row')} := peeps_send;
END;

exceptionRec := RECORD
  string Source {xpath('Source')};
  integer Code {xpath('Code')};
  string Message {xpath('Message')};
END;

roxieEchoTestResponseRecord := RECORD
  DATASET(PersonRec) Peeps {XPATH('Dataset/Row')} := DATASET([], PersonRec);
  exceptionRec Exception {XPATH('Exception')};
END;

roxieEchoTestResponseRecord doFail() := TRANSFORM
  self.Exception.CODE := IF (FAILCODE=0, 0, ERROR(FAILCODE, FAILMESSAGE));
  self.Exception.Message := FAILMESSAGE;
  self.Exception.Source := 'Test';
END;

string TargetIP := '.' : stored('TargetIP');
string TargetURL := 'http://' + TargetIP + ':9876';

gzipResult := SOAPCALL(TargetURL, 'roxie_echo', roxieEchoTestRequestRecord,
    DATASET(roxieEchoTestResponseRecord),
    LITERAL,
    XPATH('*/Results/Result'),
    RESPONSE(NOTRIM),
    HTTPHEADER('Content-Encoding', 'gzip'),
    HTTPHEADER('Accept-Encoding', 'gzip'),
    ONFAIL(doFail()));

OUTPUT(gzipResult, NAMED('gzipResult'));

deflateResult := SOAPCALL(TargetURL, 'roxie_echo', roxieEchoTestRequestRecord,
    DATASET(roxieEchoTestResponseRecord),
    LITERAL,
    XPATH('*/Results/Result'),
    RESPONSE(NOTRIM),
    HTTPHEADER('Content-Encoding', 'deflate'),
    HTTPHEADER('Accept-Encoding', 'deflate'),
    ONFAIL(doFail()));

OUTPUT(deflateResult, NAMED('deflateResult'));

xdeflateResult := SOAPCALL(TargetURL, 'roxie_echo', roxieEchoTestRequestRecord,
    DATASET(roxieEchoTestResponseRecord),
    LITERAL,
    XPATH('*/Results/Result'),
    RESPONSE(NOTRIM),
    HTTPHEADER('Content-Encoding', 'x-deflate'),
    HTTPHEADER('Accept-Encoding', 'x-deflate'),
    ONFAIL(doFail()));

OUTPUT(xdeflateResult, NAMED('xdeflatepResult'));

