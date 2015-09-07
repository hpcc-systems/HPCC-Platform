testforward := MODULE, FORWARD

Ins001_Structure:=RECORD
 INTEGER2 ordernumber;
 STRING orderdate;
 STRING requireddate;
 STRING shippeddate;
END;
;

EXPORT Ins001_dsOutput := DATASET('~birt::orders',Ins001_Structure,THOR);

EXPORT Ins003_dsOutput:=PROJECT(Ins001_dsOutput,TRANSFORM({RECORDOF(Ins001_dsOutput),STRING year,STRING shippedyear},
SELF.year:=LEFT.orderdate[1..4];
SELF.shippedyear:=LEFT.shippeddate[1..4];
SELF:=LEFT;));

  EXPORT x := Ins003_dsOutput;

END;

output(testforward.x);
