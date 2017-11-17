IMPORT Std;
  
CommonData := RECORD
  STRING fname;
  STRING lname;
END;

Parcelled := RECORD
  CommonData;
  unsigned1 type;
  DATA parcel;
END;

MyInputRec1 := RECORD
  CommonData;
  unsigned1 age;
  string    nickname;
END;

indata1 := DATASET([{ 'Richard', 'Chapman', 52, 'rkc' }, { 'Gavin', 'Halliday', 35, 'CodeMaster' }], MyInputRec1);

MyInputRec2 := RECORD
  CommonData;
  string middlename;
  unsigned salary;
END;

indata2 := DATASET([{ 'Richard', 'Chapman', 'Kenneth', 2000000 }, { 'Gavin', 'Halliday', 'Charles', 10000 }], MyInputRec2);

wrapped1 := PROJECT(indata1, TRANSFORM(Parcelled, SELF.parcel := Std.Type.vrec(MyInputRec1-common).serialize(LEFT), SELF.Type:=1, SELF:= LEFT)); 
wrapped2 := PROJECT(indata2, TRANSFORM(Parcelled, SELF.parcel := Std.Type.vrec(MyInputRec2-common).serialize(LEFT), SELF.Type:=2, SELF:= LEFT)); 

AllData := wrapped1 + wrapped2;

OUTPUT(alldata);
'---';
// Can 'peek' inside the serialised data if you want...
OUTPUT(alldata(type=1 AND Std.Type.vrec(MyInputRec1-common).deserializedField(parcel, 'age').readInt() > 50));
'---';

unwrapped1 := PROJECT(AllData(type=1), TRANSFORM(MyInputRec1, SELF:=Std.Type.vrec(MyInputRec1-common).deserialize(LEFT.parcel), SELF := LEFT));
unwrapped2 := PROJECT(AllData(type=2), TRANSFORM(MyInputRec2, SELF:=Std.Type.vrec(MyInputRec2-common).deserialize(LEFT.parcel), SELF := LEFT));

OUTPUT(unwrapped1);
'---';
OUTPUT(unwrapped2);
'---';
// Arbitrary transforms - of individual rows
output(PROJECT(unwrapped1, TRANSFORM(MyInputRec2, SELF := Std.Type.vrec(MyInputRec2).translateRow(LEFT))));
'---';
// Arbitrary transforms - of entire dataset
output(Std.Type.vrec(MyInputRec2).translate(unwrapped1));

