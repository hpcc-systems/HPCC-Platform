//
//  Example code - use without restriction.  
//
IMPORT $;

OutRec1 := $.DeclareData.Layout_Person;
RoxieIP := 'http://127.0.0.1:8002/WsEcl/soap/query/roxie/roxieoverview1.1';
svc     := 'RoxieOverview1.1';

InputRec := RECORD
  STRING30 LastName := 'KLYDE';
	STRING30 FirstName := '';
END;	

//1 rec in, recordset out
ManyRec1 := SOAPCALL(RoxieIP,
                     svc,
										 InputRec,
										 DATASET(OutRec1));
										 
OUTPUT(ManyRec1);		
