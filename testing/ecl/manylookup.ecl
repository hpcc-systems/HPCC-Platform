rec := RECORD
 unsigned key;
END;

lhs := dataset([{1},{2},{3},{4},{5}], rec);
rhs := dataset([
  {1},{2},{2},{2},{2},{3},{4},
  {1},{2},{2},{2},{2},{3},{4}
  ], rec);

j1 := JOIN(lhs, rhs, LEFT.key=RIGHT.key, TRANSFORM(rec, SELF:=LEFT), MANY LOOKUP);
j2 := JOIN(lhs, rhs, LEFT.key=RIGHT.key, TRANSFORM(rec, SELF:=LEFT), MANY LOOKUP, HINT(usefewtable));
j3 := JOIN(lhs, rhs, LEFT.key=RIGHT.key, TRANSFORM(rec, SELF:=LEFT), LOOKUP);

output(NOFOLD(j1));
output(NOFOLD(j2));
output(NOFOLD(j3));
