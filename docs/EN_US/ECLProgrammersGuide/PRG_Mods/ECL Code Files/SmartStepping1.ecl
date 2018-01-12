//
//  Example code - use without restriction.  
//
IMPORT $; 
IDX := $.DeclareData.IDX__Person_State_City_Zip_LastName_FirstName_Payload;

Filter := IDX.State = 'LA' AND IDX.City = 'ABBEVILLE';
	//filter by the leading index elements
	//and sort the output by a trailing element

OUTPUT(SORT(IDX(Filter),FirstName),all);		//the old way 

OUTPUT(STEPPED(IDX(Filter),FirstName),all);	//Smart Stepping

