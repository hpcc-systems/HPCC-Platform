import perform.system;
import perform.format;
import perform.files;

LOADXML('<xml/>');

ds := files.generateSimple();

#declare(i)
#set(I,0)
#loop
#uniquename(cnt)
%cnt% := COUNT(NOFOLD(ds((id1 % system.SplitWidth) = %I%)));
OUTPUT(%cnt%);
  #set(I,%I%+1)
  #if (%I%>=system.SplitWidth)
    #break
  #end
#end
