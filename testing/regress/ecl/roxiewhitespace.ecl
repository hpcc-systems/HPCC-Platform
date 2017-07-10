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

