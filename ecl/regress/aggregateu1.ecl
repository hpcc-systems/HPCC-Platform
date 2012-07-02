inRec := RECORD
  UNSIGNED1 ID;
  STRING text;
END;

myDS := DATASET([{1,'A'},{1,'B'},{2,'C'},{2,'D'},{3,'E'},{2,'F'}],inRec);

outRec := RECORD
  UNSIGNED1 ID;
  STRING contents;
END;

outRec mainXF(inRec L, outRec R) := TRANSFORM
  SELF.ID := L.ID;
  SELF.contents := R.contents + IF(R.contents <> '', ',', '') + L.text;
END;

outRec mergeXF(outRec R1, outRec R2) := TRANSFORM
  SELF.ID := R1.ID;
  SELF.contents := R1.contents + ',' + R2.contents;
END;

OUTPUT(AGGREGATE(myDS, outRec, mainXF(LEFT, RIGHT), mergeXF(RIGHT1, RIGHT2),LEFT.ID));
