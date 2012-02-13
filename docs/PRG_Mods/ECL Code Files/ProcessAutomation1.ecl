//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

CSVfile1 := '~$.DeclareData::AUTOMATION::People1_CSV';
CSVfile2 := '~$.DeclareData::AUTOMATION::People2_CSV';
XMLfile1 := '~$.DeclareData::AUTOMATION::People1_XML';
XMLfile2 := '~$.DeclareData::AUTOMATION::People2_XML';

O1 := OUTPUT($.DeclareData.ds1,
             {PersonID,FirstName,LastName,MiddleInitial,Gender,Street,City,State,Zip},
						 CSVfile1,CSV,OVERWRITE);
O2 := OUTPUT($.DeclareData.ds2,
             {PersonID,FirstName,LastName,MiddleInitial,Gender,Street,City,State,Zip},
						 CSVfile2,CSV,OVERWRITE);
O3 := OUTPUT($.DeclareData.ds1,
             {PersonID,FirstName,LastName,MiddleInitial,Gender,Street,City,State,Zip},
						 XMLfile1,XML,OVERWRITE);
O4 := OUTPUT($.DeclareData.ds2,
             {PersonID,FirstName,LastName,MiddleInitial,Gender,Street,City,State,Zip},
						 XMLfile2,XML,OVERWRITE);

CSVds1 := DATASET(CSVfile1,$.DeclareData.Layout_Person,CSV);
CSVds2 := DATASET(CSVfile2,$.DeclareData.Layout_Person,CSV);
XMLds1 := DATASET(XMLfile1,$.DeclareData.Layout_Person,XML('Dataset/Row'));
XMLds2 := DATASET(XMLfile2,$.DeclareData.Layout_Person,XML('Dataset/Row'));

P1 := PARALLEL(O1,O2,O3,O4);

P2 := PARALLEL(Std.File.Despray($.DeclareData.SubFile1,$.DeclareData.LZ_IP,$.DeclareData.LZ_Dir + 'People1.d00',,,,TRUE), 
               Std.File.Despray($.DeclareData.SubFile2,$.DeclareData.LZ_IP,$.DeclareData.LZ_Dir + 'People2.d00',,,,TRUE),
               Std.File.Despray(CSVfile1,$.DeclareData.LZ_IP,$.DeclareData.LZ_Dir + 'People1.csv',,,,TRUE),
               Std.File.Despray(CSVfile2,$.DeclareData.LZ_IP,$.DeclareData.LZ_Dir + 'People2.csv',,,,TRUE),
               Std.File.Despray(XMLfile1,$.DeclareData.LZ_IP,$.DeclareData.LZ_Dir + 'People1.xml',,,,TRUE),
               Std.File.Despray(XMLfile2,$.DeclareData.LZ_IP,$.DeclareData.LZ_Dir + 'People2.xml',,,,TRUE));
							 
SEQUENTIAL(P1,P2)
							 
							 