//
//  Example code - use without restriction.  
//
HexStr2Decimal(STRING HexIn) := FUNCTION

  //type re-definitions to make code more readable below
	BE1 := BIG_ENDIAN UNSIGNED1;
	BE2 := BIG_ENDIAN UNSIGNED2;
	BE3 := BIG_ENDIAN UNSIGNED3;
	BE4 := BIG_ENDIAN UNSIGNED4;
	BE5 := BIG_ENDIAN UNSIGNED5;
	BE6 := BIG_ENDIAN UNSIGNED6;
	BE7 := BIG_ENDIAN UNSIGNED7;
	BE8 := BIG_ENDIAN UNSIGNED8;

	TrimHex	:= TRIM(HexIn,ALL);
  HexLen  := LENGTH(TrimHex);
	UseHex	:= IF(HexLen % 2 = 1,'0','') + TrimHex;

  //a sub-function to translate two hex chars to a packed hex format
	STRING1 Str2Data(STRING2 Hex) := FUNCTION
		UNSIGNED1 N1 := CASE(Hex[1],
												 '0'=>00x,'1'=>10x,'2'=>20x,'3'=>30x,
												 '4'=>40x,'5'=>50x,'6'=>60x,'7'=>70x,
												 '8'=>80x,'9'=>90x,'A'=>0A0x,'B'=>0B0x,
												 'C'=>0C0x,'D'=>0D0x,'E'=>0E0x,'F'=>0F0x,00x);
		UNSIGNED1 N2 := CASE(Hex[2],
												 '0'=>00x,'1'=>01x,'2'=>02x,'3'=>03x,
												 '4'=>04x,'5'=>05x,'6'=>06x,'7'=>07x,
												 '8'=>08x,'9'=>09x,'A'=>0Ax,'B'=>0Bx,
												 'C'=>0Cx,'D'=>0Dx,'E'=>0Ex,'F'=>0Fx,00x);
		RETURN (>STRING1<)(N1 | N2);
	END;	

  UseHexLen  := LENGTH(TRIM(UseHex));
	InHex2  := Str2Data(UseHex[1..2]);
	InHex4  := InHex2  + Str2Data(UseHex[3..4]);
	InHex6  := InHex4  + Str2Data(UseHex[5..6]);
	InHex8  := InHex6  + Str2Data(UseHex[7..8]);
	InHex10 := InHex8  + Str2Data(UseHex[9..10]);;
	InHex12 := InHex10 + Str2Data(UseHex[11..12]);
	InHex14 := InHex12 + Str2Data(UseHex[13..14]);
	InHex16 := InHex14 + Str2Data(UseHex[15..16]);

  RETURN CASE(UseHexLen,
	            2 =>(STRING)(>BE1<)InHex2,
	            4 =>(STRING)(>BE2<)InHex4,
							6 =>(STRING)(>BE3<)InHex6,
							8 =>(STRING)(>BE4<)InHex8,
							10=>(STRING)(>BE5<)InHex10,
							12=>(STRING)(>BE6<)InHex12,
							14=>(STRING)(>BE7<)InHex14,
							16=>(STRING)(>BE8<)InHex16,
							'ERROR');
END;

OUTPUT(HexStr2Decimal('0101'));								 // 257   
OUTPUT(HexStr2Decimal('FF'));									 // 255   
OUTPUT(HexStr2Decimal('FFFF'));								 // 65535   
OUTPUT(HexStr2Decimal('FFFFFF'));							 // 16777215   
OUTPUT(HexStr2Decimal('FFFFFFFF'));						 // 4294967295   
OUTPUT(HexStr2Decimal('FFFFFFFFFF'));					 // 1099511627775   
OUTPUT(HexStr2Decimal('FFFFFFFFFFFF'));				 // 281474976710655   
OUTPUT(HexStr2Decimal('FFFFFFFFFFFFFF'));			 // 72057594037927935   
OUTPUT(HexStr2Decimal('FFFFFFFFFFFFFFFF'));		 // 18446744073709551615  
OUTPUT(HexStr2Decimal('FFFFFFFFFFFFFFFFFF'));	 // ERROR 

