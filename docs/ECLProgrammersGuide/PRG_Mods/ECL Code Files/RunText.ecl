//
//  Example code - use without restriction.  
//
IMPORT $;

MyRec := RECORD
  STRING1 value1;
  STRING1 value2;
END;
D := DATASET([{'A','B'},
		  {'B','C'},
		  {'A','D'},
		  {'B','B'},
		  {'A','C'},
		  {'B','D'},
		  {'A','B'},
		  {'C','C'},
		  {'C','D'},
		  {'A','A'}],MyRec);

$.DeclareData.MAC_CountFieldValues(D,Value1)
$.DeclareData.MAC_CountFieldValues(D,Value2)
