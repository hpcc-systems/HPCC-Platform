/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std;

EXPORT TestType := MODULE

  SHARED rec1 := RECORD
    STRING s1;
  END;

  SHARED rec2 := RECORD
    string  s;
    string8 s2;
    boolean b;
    integer i;
    integer4 i4;
    unsigned ui;
    unsigned4 ui4;
    real r;
    real4 r4;
    decimal4_2 d42;
    udecimal4_2 ud42;
    set of string2 set_s2;
    qstring q;
    qstring5 q5;
    varstring v;
    varstring5 v5;
    unicode u;
    unicode8 u8;
    varunicode vu;
    varunicode5 vu5;
    data da;
    data8 da8;
    packed integer pi;
    packed integer4 pi4;
    big_endian integer bi;
    big_endian unsigned integer4 bui4;
    rec1 subrec;
  END;

  SHARED vrec1 := Std.Type.VRec(rec1);
  SHARED vrec2 := Std.Type.VRec(rec2);
  SHARED drec2 := Std.Type.DRec(vrec2.getBinaryTypeInfo());

  EXPORT TestSerialize := [
    ASSERT(vrec1.getBinaryTypeInfo() = x'5052B035DD691DBF6B1300000004040000000D04400004017331000004040000', CONST);
    ASSERT(vrec1.getJsonTypeInfo() = 
'''{
 "ty1": {
  "fieldType": 1028,
  "length": 0
 },
 "fieldType": 1037,
 "length": 4,
 "fields": [
  {
   "name": "s1",
   "type": "ty1",
   "flags": 1028
  }
 ]
}''', CONST);

    ASSERT(vrec1.numFields()=1);

    ASSERT(vrec2.getFieldName(0)='', CONST);
    ASSERT(vrec2.getFieldName(1)='s', CONST);
    ASSERT(vrec2.getFieldName(2)='s2', CONST);
    ASSERT(vrec2.getFieldName(100)='', CONST);
    
    ASSERT(vrec2.getFieldNum('sx')=0, CONST);
    ASSERT(vrec2.getFieldNum('s')=1, CONST);
    ASSERT(vrec2.getFieldNum('S')=1, CONST);
 
    ASSERT(vrec2.getFieldType('sx')='', CONST);
    ASSERT(vrec2.getFieldType('s')='STRING', CONST);
    ASSERT(vrec2.getFieldType('S')='STRING', CONST);
    ASSERT(vrec2.getFieldType('s2')='STRING8', CONST);
    ASSERT(vrec2.getFieldType('b')='BOOLEAN', CONST);
    ASSERT(vrec2.getFieldType('i')='INTEGER8', CONST);
    ASSERT(vrec2.getFieldType('i4')='INTEGER4', CONST);
    ASSERT(vrec2.getFieldType('ui')='UNSIGNED8', CONST);
    ASSERT(vrec2.getFieldType('ui4')='UNSIGNED4', CONST);
    ASSERT(vrec2.getFieldType('r')='REAL8', CONST);
    ASSERT(vrec2.getFieldType('r4')='REAL4', CONST);
    ASSERT(vrec2.getFieldType('d42')='DECIMAL4_2', CONST);
    ASSERT(vrec2.getFieldType('ud42')='UDECIMAL4_2', CONST);
    ASSERT(vrec2.getFieldType('set_s2')='SET OF STRING2', CONST);
    ASSERT(vrec2.getFieldType('q')='QSTRING', CONST);
    ASSERT(vrec2.getFieldType('q5')='QSTRING5', CONST);
    ASSERT(vrec2.getFieldType('v')='VARSTRING', CONST);
    ASSERT(vrec2.getFieldType('v5')='VARSTRING5', CONST);
    ASSERT(vrec2.getFieldType('u')='UNICODE', CONST);
    ASSERT(vrec2.getFieldType('u8')='UNICODE8', CONST);
    ASSERT(vrec2.getFieldType('vu')='VARUNICODE', CONST);
    ASSERT(vrec2.getFieldType('vu5')='VARUNICODE5', CONST);
    ASSERT(vrec2.getFieldType('da')='DATA', CONST);
    ASSERT(vrec2.getFieldType('da8')='DATA8', CONST);
    ASSERT(vrec2.getFieldType('pi')='PACKED INTEGER', CONST);
    ASSERT(vrec2.getFieldType('pi4')='PACKED INTEGER', CONST);  // NOTE - size specified is not recorded.
    ASSERT(vrec2.getFieldType('bi')='BIG_ENDIAN INTEGER8', CONST);
    ASSERT(vrec2.getFieldType('bui4')='BIG_ENDIAN UNSIGNED4', CONST);
    ASSERT(vrec2.getFieldType('subrec.s1')='STRING', CONST);
    ASSERT(vrec2.fieldNames()=['s','s2','b','i','i4','ui','ui4','r','r4','d42','ud42','set_s2','q','q5','v','v5','u','u8','vu','vu5','da','da8','pi','pi4','bi','bui4','subrec.s1']);  // Can't const-fold this check, for some reason
    
    // As above, but passing in the serialized form of the type info
    
    ASSERT(drec2.getFieldName(0)='', CONST);
    ASSERT(drec2.getFieldName(1)='s', CONST);
    ASSERT(drec2.getFieldName(2)='s2', CONST);
    ASSERT(drec2.getFieldName(100)='', CONST);
    
    ASSERT(drec2.getFieldNum('sx')=0, CONST);
    ASSERT(drec2.getFieldNum('s')=1, CONST);
    ASSERT(drec2.getFieldNum('S')=1, CONST);
 
    ASSERT(drec2.getFieldType('sx')='', CONST);
    ASSERT(drec2.getFieldType('s')='STRING', CONST);
    ASSERT(drec2.getFieldType('S')='STRING', CONST);
    ASSERT(drec2.getFieldType('s2')='STRING8', CONST);
    ASSERT(drec2.getFieldType('b')='BOOLEAN', CONST);
    ASSERT(drec2.getFieldType('i')='INTEGER8', CONST);
    ASSERT(drec2.getFieldType('i4')='INTEGER4', CONST);
    ASSERT(drec2.getFieldType('ui')='UNSIGNED8', CONST);
    ASSERT(drec2.getFieldType('ui4')='UNSIGNED4', CONST);
    ASSERT(drec2.getFieldType('r')='REAL8', CONST);
    ASSERT(drec2.getFieldType('r4')='REAL4', CONST);
    ASSERT(drec2.getFieldType('d42')='DECIMAL4_2', CONST);
    ASSERT(drec2.getFieldType('ud42')='UDECIMAL4_2', CONST);
    ASSERT(drec2.getFieldType('set_s2')='SET OF STRING2', CONST);
    ASSERT(drec2.getFieldType('q')='QSTRING', CONST);
    ASSERT(drec2.getFieldType('q5')='QSTRING5', CONST);
    ASSERT(drec2.getFieldType('v')='VARSTRING', CONST);
    ASSERT(drec2.getFieldType('v5')='VARSTRING5', CONST);
    ASSERT(drec2.getFieldType('u')='UNICODE', CONST);
    ASSERT(drec2.getFieldType('u8')='UNICODE8', CONST);
    ASSERT(drec2.getFieldType('vu')='VARUNICODE', CONST);
    ASSERT(drec2.getFieldType('vu5')='VARUNICODE5', CONST);
    ASSERT(drec2.getFieldType('da')='DATA', CONST);
    ASSERT(drec2.getFieldType('da8')='DATA8', CONST);
    ASSERT(drec2.getFieldType('pi')='PACKED INTEGER', CONST);
    ASSERT(drec2.getFieldType('pi4')='PACKED INTEGER', CONST);  // NOTE - size specified is not recorded.
    ASSERT(drec2.getFieldType('bi')='BIG_ENDIAN INTEGER8', CONST);
    ASSERT(drec2.getFieldType('bui4')='BIG_ENDIAN UNSIGNED4', CONST);
    ASSERT(drec2.getFieldType('subrec.s1')='STRING', CONST);
    ASSERT(drec2.fieldNames()=['s','s2','b','i','i4','ui','ui4','r','r4','d42','ud42','set_s2','q','q5','v','v5','u','u8','vu','vu5','da','da8','pi','pi4','bi','bui4','subrec.s1']);  // Can't const-fold this check, for some reason
    ASSERT(drec2.numFields()=vrec2.numFields(), CONST);
    ASSERT(TRUE, CONST)
  ];
END;
