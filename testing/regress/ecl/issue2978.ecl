Work1 := RECORD
  UNSIGNED4 key;
  UNSIGNED4 pos;
  UNSIGNED4 len;
  UNICODE tok;
END;

ds := DATASET([{1,  1, 3,  u'abc'}
              ,{1,  4, 1,  u','}
              ,{1,  6, 1,  u'Z'}
              ,{2,  1, 3,  u'abc'}
              ,{2,  5, 3,  u'def'}], Work1);

 Work1 roll1(Work1 cumm, Work1 curr) := TRANSFORM
  SELF.tok := cumm.tok + IF(cumm.pos + cumm.len < curr.pos, u' ', u'') + curr.tok;
  SELF.len := cumm.len + IF(cumm.pos + cumm.len < curr.pos, 1, 0) + curr.len;
  SELF := cumm;
 END;

d1 := ROLLUP(ds, roll1(LEFT,RIGHT), key, LOCAL);
OUTPUT(d1);
