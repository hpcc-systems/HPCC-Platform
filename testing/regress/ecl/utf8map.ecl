IMPORT Std;
FilePath := 'MyTestFile';
FileLayout :=
{ utf8 MyValue }
;
MyFile := nofold(dataset(['Permanent','FixedTerm'], FileLayout));
y := MyFile[1].MyValue;

UseMap(UTF8 myValue) := FUNCTION
RETURN MAP(
myValue = U'Permanent' => 2,
//Std.Uni.CompareAtStrength(myValue, U'SomethingElse' , 5) = 0 => 4,
myValue = U'Fixed term contract' => 3,
myValue = U'Fixed Term' => 3,
1
);
END;

UseCase(UTF8 myValue) := FUNCTION
  RETURN CASE(myValue,
      U'Permanent' => 2,
      U'Fixed term contract' => 3,
      U'Fixed Term' => 3,
      1
  );
END;

x := u'Permanent';
x = y;  // true
Std.Uni.CompareAtStrength(x,y,5) = 0; // true
IF(y = U'Permanent',  2, 1);  // 2
IF(Std.Uni.CompareAtStrength(x,y,5) = 0, 2, 1); // 2
UseCase( x ); // 2
UseCase( y ); // 2
UseMap( x );  // 2
UseMap( y );  // Should be 2
