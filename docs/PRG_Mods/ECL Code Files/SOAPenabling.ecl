IMPORT $.DeclareData AS ProgGuide;
     
EXPORT SOAPenabling() := FUNCTION
  STRING30 lname_value := '' : STORED('LastName');
  STRING30 fname_value := '' : STORED('FirstName');
  IDX  := ProgGuide.IDX__Person_LastName_FirstName;
  Base := ProgGuide.Person.FilePlus;
  Fetched := IF(fname_value = '',
           FETCH(Base,IDX(LastName=lname_value),RIGHT.RecPos),
                FETCH(Base,IDX(LastName=lname_value,
      FirstName=fname_value),RIGHT.RecPos));
  RETURN OUTPUT(CHOOSEN(Fetched,2000));
END;
