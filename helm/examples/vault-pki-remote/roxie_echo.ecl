//nothor
//nohthor
//publish

NameRec := RECORD
  string10 First;
  string15 Last;
END;

AddressRec := RECORD
  string10 City;
  string2 State;
  integer4 ZipCode;
END;

PersonRec := RECORD
  NameRec Name;
  AddressRec Address;
END;

peeps := DATASET([], PersonRec) : STORED('peeps', FEW);

OUTPUT(peeps, NAMED('Peeps'));

