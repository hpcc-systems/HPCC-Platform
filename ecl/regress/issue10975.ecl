import java;
subrec := RECORD
  integer4 subi;
END;

jret := RECORD
  set of string sset;
  DATASET(subrec) sub;
end;

DATASET(jret) passDataset2(DATASET(jret) d) := IMPORT(java, 'JavaCat.passDataset2:([LJavaCat;)Ljava/util/Iterator;');

ds := DATASET(
  [
   {['Hello from ECL'], []}
  ], jret);

output(passDataset2(ds));
