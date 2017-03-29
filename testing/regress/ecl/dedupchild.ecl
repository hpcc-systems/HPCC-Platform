cRec := RECORD
 unsigned4 cid;
END;

pRec := RECORD
 unsigned4 id;
 DATASET(cRec) kids;
END;

pSetSize := 2000;

cRec makeC(unsigned4 c) := TRANSFORM
  SELF.cid := c;
END;

pRec makeP(unsigned4 c) := TRANSFORM
  SELF.id := c;
  SELF.kids := DATASET(1+(c%10), makeC(COUNTER%2));
END;

pSet := DATASET(pSetSize, makeP(COUNTER), DISTRIBUTED);

kids  := pSet.kids;

outRec := RECORD
 unsigned val1;
 unsigned val2;
 unsigned val3;
 unsigned val4;
 unsigned val5;
END;

outRec doTrans(pRec l) := TRANSFORM
 SortedKids := SORT(l.kids, cid);
 DedupKids1 := DEDUP(SortedKids, cid);
 DedupKids2 := DEDUP(l.kids, cid, ALL);
 DedupKids3 := DEDUP(l.kids(cid<99999), cid, ALL); // filter to prevent CSE of dedup
 DedupKids4 := DEDUP(l.kids(cid<99998), cid, ALL); // filter to prevent CSE of dedup
 
 SELF.val1 := SUM(DedupKids1, cid);
 SELF.val2 := SUM(DedupKids2, cid);
 SELF.val3 := IF(EXISTS(DedupKids1), 1, 0);
 SELF.val4 := IF(EXISTS(DedupKids3), 1, 0);
 SELF.val5 := COUNT(CHOOSEN(DedupKids4, 2));
END;

p := PROJECT(pSet, doTrans(LEFT));

DATASET([{'SumDedupVals', SUM(p, val1)},
         {'SumDedupAllVals', SUM(p, val2)},
         {'ExistsDedupTotal', SUM(p, val3)},
         {'ExistsDedupAllTotal', SUM(p, val4)},
         {'ChoosenDedupAllTotal', SUM(p, val5)} ], {string type, unsigned8 val});
