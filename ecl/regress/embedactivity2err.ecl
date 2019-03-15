rec := { string f1; };

UNSIGNED4 myfunc(STREAMED DATASET(rec) recs) := EMBED(C++: activity)
  #body
  return 0 
ENDEMBED;

myfunc(DATASET([{'f1'}], rec));
