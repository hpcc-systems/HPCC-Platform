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
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 22222}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 33333}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 44444}},
  {{'Joeseph', 'Johnson'}, {'Fresno', 'CA', 55555}}
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

//Use 'remote-mtls' to establish secure communication using the remote issuer
string TargetURL := 'remote-mtls:https://roxie1.hpcc1:19876';

remoteResult := SOAPCALL(TargetURL, 'roxie_echo', roxieEchoTestRequestRecord,
    DATASET(roxieEchoTestResponseRecord),
    LITERAL,
    XPATH('*/Results/Result'),
    RESPONSE(NOTRIM),
    ONFAIL(doFail()));

OUTPUT(remoteResult, NAMED('remoteResult'));
