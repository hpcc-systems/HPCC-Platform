//
//  Example code - use without restriction.  
//
SimpleRandomSample(InFile,UID_Field,SampleSize,Result) := MACRO

	//build a table of the UIDs
	#uniquename(Layout_Plus_RecID)
	%Layout_Plus_RecID% := RECORD
		UNSIGNED8 RecID := 0;
		InFile.UID_Field;
	END;

	#UNIQUENAME(InTbl)
	%InTbl% := TABLE(InFile,%Layout_Plus_RecID%);

	//then assign unique record IDs to the table entries
	#uniquename(IDRecs)
	%Layout_Plus_RecID% %IDRecs%(%Layout_Plus_RecID% L, INTEGER C) := TRANSFORM
		SELF.RecID := C;
		SELF := L;
	END;

	#UNIQUENAME(UID_Recs)
	%UID_Recs% := PROJECT(%InTbl%,%IDRecs%(LEFT,COUNTER));

	//discover the number of records
	#UNIQUENAME(WholeSet)
	%WholeSet% := COUNT(InFile) : GLOBAL;

	//then generate the unique record IDs to include in the sample
	#UNIQUENAME(BlankSet)
	%BlankSet%  := DATASET([{0}],{UNSIGNED8 seq});

	#UNIQUENAME(SelectEm)
	TYPEOF(%BlankSet%) %SelectEm%(%BlankSet% L, INTEGER c) := TRANSFORM
		SELF.seq := ROUNDUP(%WholeSet% * (((RANDOM() % 100000)+1) / 100000));
	END;
		
	#UNIQUENAME(selected)
	%selected% := NORMALIZE( %BlankSet%, SampleSize, %SelectEm%(LEFT, COUNTER));

	//then filter the original dataset by the selected UIDs
	#UNIQUENAME(SetSelectedRecs)
	%SetSelectedRecs% := SET(%UID_Recs%(RecID IN SET(%selected%,seq)),UID_Field);

	result := infile(UID_Field IN %SetSelectedRecs% );

ENDMACRO;

//*****************************************************
SomeFile := DATASET([{'A1'},{'B1'},{'C1'},{'D1'},{'E1'},
                     {'F1'},{'G1'},{'H1'},{'I1'},{'J1'},
                     {'K1'},{'L1'},{'M1'},{'N1'},{'O1'},
                     {'P1'},{'Q1'},{'R1'},{'S1'},{'T1'},
                     {'U1'},{'V1'},{'W1'},{'X1'},{'Y1'},
			               {'A2'},{'B2'},{'C2'},{'D2'},{'E2'},
                     {'F2'},{'G2'},{'H2'},{'I2'},{'J2'},
                     {'K2'},{'L2'},{'M2'},{'N2'},{'O2'},
                     {'P2'},{'Q2'},{'R2'},{'S2'},{'T2'},
                     {'U2'},{'V2'},{'W2'},{'X2'},{'Y2'},
			               {'A3'},{'B3'},{'C3'},{'D3'},{'E3'},
                     {'F3'},{'G3'},{'H3'},{'I3'},{'J3'},
                     {'K3'},{'L3'},{'M3'},{'N3'},{'O3'},
                     {'P3'},{'Q3'},{'R3'},{'S3'},{'T3'},
                     {'U3'},{'V3'},{'W3'},{'X3'},{'Y3'},
			               {'A4'},{'B4'},{'C4'},{'D4'},{'E4'},
                     {'F4'},{'G4'},{'H4'},{'I4'},{'J4'},
                     {'K4'},{'L4'},{'M4'},{'N4'},{'O4'},
                     {'P4'},{'Q4'},{'R4'},{'S4'},{'T4'},
                     {'U4'},{'V4'},{'W4'},{'X4'},{'Y4'}
			   ],{STRING2 Letter});

ds := DISTRIBUTE(SomeFile,HASH(letter[2]));
SimpleRandomSample(ds,Letter,6, res1)
SimpleRandomSample(ds,Letter,6, res2)
SimpleRandomSample(ds,Letter,6, res3)
SimpleRandomSample(ds,Letter,6, res4)
SimpleRandomSample(ds,Letter,6, res5)
SimpleRandomSample(ds,Letter,6, res6)
SimpleRandomSample(ds,Letter,6, res7)
SimpleRandomSample(ds,Letter,6, res8)
SimpleRandomSample(ds,Letter,6, res9)
SimpleRandomSample(ds,Letter,6, res10)
SimpleRandomSample(ds,Letter,6, res11)
SimpleRandomSample(ds,Letter,6, res12)
SimpleRandomSample(ds,Letter,6, res13)
SimpleRandomSample(ds,Letter,6, res14)
SimpleRandomSample(ds,Letter,6, res15)
SimpleRandomSample(ds,Letter,6, res16)
SimpleRandomSample(ds,Letter,6, res17)
SimpleRandomSample(ds,Letter,6, res18)
SimpleRandomSample(ds,Letter,6, res19)
SimpleRandomSample(ds,Letter,6, res20)
SimpleRandomSample(ds,Letter,6, res21)
SimpleRandomSample(ds,Letter,6, res22)
SimpleRandomSample(ds,Letter,6, res23)
SimpleRandomSample(ds,Letter,6, res24)
SimpleRandomSample(ds,Letter,6, res25)
SimpleRandomSample(ds,Letter,6, res26)
SimpleRandomSample(ds,Letter,6, res27)
SimpleRandomSample(ds,Letter,6, res28)
SimpleRandomSample(ds,Letter,6, res29)
SimpleRandomSample(ds,Letter,6, res30)
SimpleRandomSample(ds,Letter,6, res31)
SimpleRandomSample(ds,Letter,6, res32)
SimpleRandomSample(ds,Letter,6, res33)
SimpleRandomSample(ds,Letter,6, res34)
SimpleRandomSample(ds,Letter,6, res35)
SimpleRandomSample(ds,Letter,6, res36)
SimpleRandomSample(ds,Letter,6, res37)
SimpleRandomSample(ds,Letter,6, res38)
SimpleRandomSample(ds,Letter,6, res39)
SimpleRandomSample(ds,Letter,6, res40)

OUTPUT(CHOOSEN(res1,5));
OUTPUT(CHOOSEN(res2,5));
OUTPUT(CHOOSEN(res3,5));
OUTPUT(CHOOSEN(res4,5));
OUTPUT(CHOOSEN(res5,5));
OUTPUT(CHOOSEN(res6,5));
OUTPUT(CHOOSEN(res7,5));
OUTPUT(CHOOSEN(res8,5));
OUTPUT(CHOOSEN(res9,5));
OUTPUT(CHOOSEN(res10,5));
OUTPUT(CHOOSEN(res11,5));
OUTPUT(CHOOSEN(res12,5));
OUTPUT(CHOOSEN(res13,5));
OUTPUT(CHOOSEN(res14,5));
OUTPUT(CHOOSEN(res15,5));
OUTPUT(CHOOSEN(res16,5));
OUTPUT(CHOOSEN(res17,5));
OUTPUT(CHOOSEN(res18,5));
OUTPUT(CHOOSEN(res19,5));
OUTPUT(CHOOSEN(res20,5));
OUTPUT(CHOOSEN(res21,5));
OUTPUT(CHOOSEN(res22,5));
OUTPUT(CHOOSEN(res23,5));
OUTPUT(CHOOSEN(res24,5));
OUTPUT(CHOOSEN(res25,5));
OUTPUT(CHOOSEN(res26,5));
OUTPUT(CHOOSEN(res27,5));
OUTPUT(CHOOSEN(res28,5));
OUTPUT(CHOOSEN(res29,5));
OUTPUT(CHOOSEN(res30,5));
OUTPUT(CHOOSEN(res31,5));
OUTPUT(CHOOSEN(res32,5));
OUTPUT(CHOOSEN(res33,5));
OUTPUT(CHOOSEN(res34,5));
OUTPUT(CHOOSEN(res35,5));
OUTPUT(CHOOSEN(res36,5));
OUTPUT(CHOOSEN(res37,5));
OUTPUT(CHOOSEN(res38,5));
OUTPUT(CHOOSEN(res39,5));
OUTPUT(CHOOSEN(res40,5));
