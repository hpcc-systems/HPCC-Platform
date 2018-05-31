r := RECORD
    UNSIGNED id;
    STRING name;
END;

testPassParameterMeta(streamed dataset(r) ds) := EMBED(C++ : passparametermeta)
  if (metaDs.queryRecordAccessor(true).getNumFields() != 2) {
  	rtlFail(0,"Meta data incorrect");
  }
ENDEMBED;

ds := DATASET([{1,'Name'}], r);
testPassParameterMeta(ds);