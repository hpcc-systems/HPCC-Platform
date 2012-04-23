MyRec := RECORD
	STRING1 Value1;
	STRING1 Value2;
END;

SomeFile := GROUP(DATASET([{'C','G'},
					 {'C','C'},
					 {'A','X'},
					 {'B','G'},
					 {'A','B'}],MyRec),TRUE);
distribution(SomeFile);

/*
<XML>
<Field name="Value1" distinct="3">
 <Value count="2">A</Value>
 <Value count="1">B</Value>
 <Value count="2">C</Value>
</Field>
<Field name="Value2" distinct="4">
 <Value count="1">B</Value>
 <Value count="1">C</Value>
 <Value count="2">G</Value>
 <Value count="1">X</Value>
</Field>
</XML>

*/