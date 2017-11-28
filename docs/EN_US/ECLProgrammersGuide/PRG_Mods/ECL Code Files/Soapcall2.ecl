//
//  Example code - use without restriction.  
//
IMPORT $;

OutRec1 := $.DeclareData.Layout_Person;
RoxieIP := 'http://127.0.0.1:8002/WsEcl/soap/query/roxie/roxieoverview1.1';
svc     := 'RoxieOverview1.1';

//recordset in, recordset out
InRec := RECORD
  STRING30 LastName{XPATH('LastName')};
	STRING30 FirstName{XPATH('FirstName')};
END;	
					
InputDataset := DATASET([{'TRAYLOR','CISSY'},
                         {'KLYDE','CLYDE'},
												 {'SMITH','DAR'},
												 {'BOWEN','PERCIVAL'},
												 {'ROMNEY','GEORGE'}],Inrec);

ManyRec2 := SOAPCALL(InputDataset,
										 RoxieIP,
										 svc,
										 Inrec,
										 TRANSFORM(LEFT),
										 DATASET(OutRec1),
										 ONFAIL(SKIP));								 

OUTPUT(ManyRec2);		
