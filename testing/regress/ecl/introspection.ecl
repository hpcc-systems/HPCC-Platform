IMPORT Std;

myrec := RECORD
  string20 desc       { default('default') };
  boolean b           { default(true)};
  unsigned4 n1        { default(1)};
  unsigned4 n0        { default(0)};
  unsigned8 n2        { default(2)};
  real r1             { default(3.14)};
  decimal4_2 d1       { default(-3.14)};
  udecimal4_2 d2      { default(3.14)};
  string8 s1          { default('string01') };
  varstring v1        { default('v1') };
  varstring5 v2       { default('v2') };
  unicode8 u1         { default(u'€unicode') };
  varunicode vu1      { default(u'€vu1') };
  varunicode5 vu2     { default(u'€vu2') };
  data da1            { default(d'11223344') };
  data8 da2           { default(d'55667788') };
  string tail         { default('end') };
END;

namerec := RECORD
  string name;
END;

fieldnames := DATASET(Std.Type.vrec(myrec).fieldNames(), namerec);

ds := DATASET([{'Description'}], myrec);

keyvalue := RECORD
  string  key;
  string  value;
  integer ivalue;
  real8   rvalue;
  boolean bvalue;
  utf8    uvalue;
  data    dvalue;
END;

keyValue dumpValue(myrec l, namerec r) := TRANSFORM
  SELF.key := r.name;
  SELF.value := Std.Type.rec(l).field(r.name).readString();
  SELF.ivalue := Std.Type.rec(l).field(r.name).readInt();
  SELF.rvalue := Std.Type.rec(l).field(r.name).readReal();
  SELF.bvalue := Std.Type.rec(l).field(r.name).readBool();
  SELF.uvalue := Std.Type.rec(l).field(r.name).readUtf8();
  SELF.dvalue := Std.Type.rec(l).field(r.name).readData();
END;

vrec := Std.Type.vrec(myrec);

keyValue dumpValue2(DATA l, namerec r) := TRANSFORM
  SELF.key := r.name;
  SELF.value := vrec.deserializedField(l, r.name).readString();
  SELF.ivalue := vrec.deserializedField(l, r.name).readInt();
  SELF.rvalue := vrec.deserializedField(l, r.name).readReal();
  SELF.bvalue := vrec.deserializedField(l, r.name).readBool();
  SELF.uvalue := vrec.deserializedField(l, r.name).readUtf8();
  SELF.dvalue := vrec.deserializedField(l, r.name).readData();
END;

drec := Std.Type.drec(vrec.getBinaryTypeInfo());

keyValue dumpValue3(DATA l, namerec r) := TRANSFORM
  SELF.key := r.name;
  SELF.value := drec.deserializedField(l, r.name).readString();
  SELF.ivalue :=drec.deserializedField(l, r.name).readInt();
  SELF.rvalue := drec.deserializedField(l, r.name).readReal();
  SELF.bvalue := drec.deserializedField(l, r.name).readBool();
  SELF.uvalue := drec.deserializedField(l, r.name).readUtf8();
  SELF.dvalue := drec.deserializedField(l, r.name).readData();
END;

OUTPUT(join(ds, fieldnames, true, dumpvalue(LEFT,RIGHT), ALL));
OUTPUT(join(ds, fieldnames, true, dumpvalue2(Std.Type.rec(LEFT).serialize(), RIGHT), ALL));
OUTPUT(join(ds, fieldnames, true, dumpvalue3(Std.Type.rec(LEFT).serialize(), RIGHT), ALL));
