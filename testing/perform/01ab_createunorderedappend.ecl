import perform.system;
import perform.format;
import perform.files;

LOADXML('<xml/>');

ds(unsigned i) := DATASET(system.simpleRecordCount DIV system.SplitWidth, format.createSimple(COUNTER+i), DISTRIBUTED);

dsAll := DATASET([], format.simpleRec)

#declare(i)
#set(I,1)
#loop
  + ds(%I%)
  #set(I,%I%+1)
  #if (%I%>system.SplitWidth)
    #break
  #end
#end
;

cnt := COUNT(NOFOLD(dsAll));

OUTPUT(cnt);
