//Reproduce problem converting utf8 to string resulting in zero length string
DATA10 y := x'02000000E385A4E385A4';

r := { Utf8 value; };

y2 := TRANSFER(y, r);
ds := DATASET(y2);
output(ds, { (string)value; } );
