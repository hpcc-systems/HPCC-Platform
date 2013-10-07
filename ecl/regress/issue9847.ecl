testMod(dataset ds) := MODULE
  tmpRecord := record
    dataset(recordof(ds)) child_ds1 := [];
  end;
  export tmpDataset := dataset([{ds}], tmpRecord);
END;

r := record
  string20 s20;
end;
d := dataset([{'aaa'}], r);
blarg := testMod(d);
blarg.tmpDataset;
