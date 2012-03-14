//
//  Example code - use without restriction.  
//
IMPORT $; 
IDX := $.DeclareData.IDX__Person_State_City_Zip_LastName_FirstName_Payload;

Filter := IDX.State = 'LA' AND IDX.City = 'ABBEVILLE';

OUTPUT(CHOOSEN(SORT(IDX(Filter),FirstName),5));			//the old way 
OUTPUT(CHOOSEN(STEPPED(IDX(Filter),FirstName),5));	//Smart Stepping
