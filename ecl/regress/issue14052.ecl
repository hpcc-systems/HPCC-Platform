layout1 := {
  unsigned4 doc_id;
  unicode uni_val{maxlength(30)}
};

derived1 := {
  layout1 ref;
  layout1 def;
  unsigned4 doc_id;
};

inline := dataset([
   {1,u'one',3,u'three',3}
  ,{2,u'two',4,u'four',4}
  ,{3,u'three',5,u'five',5}
  ,{4,u'four',6,u'six',6}
],derived1);

getStuff(dataset(derived1) ds1) := function

  ds1s := sort(ds1,ref.doc_id,def.doc_id,local);

  ds1sb := ds1s(doc_id % 2 = 1);

  ds1m := merge(ds1s(doc_id % 2 = 0),
                ds1sb,
                sorted(ref.doc_id,def.doc_id),
                // dedup,   // fails to compile with or without...leaving out for simplicity
                local);
  // This will work
  // ds1m := dedup(sort(ds1s(doc_id % 2 = 0)+ds1sb,ref.doc_id,def.doc_id,local),ref.doc_id,def.doc_id,local);

  ds2 := project(ds1m,transform(layout1,
                                self.doc_id:= left.def.doc_id;
                                self.uni_val := left.def.uni_val));

  debug() := parallel(
     evaluate('')
    ,output(ds1m,,named('ds1m'))
  );

  ret := module
    export debugger := debug();
    export finalOut := ds2;
    // export finalOut := ds1m;  // works
  end;

  return ret;
end;

example := getStuff(inline);

// example.debugger;  // works if not commented
output(example.finalout,all);
