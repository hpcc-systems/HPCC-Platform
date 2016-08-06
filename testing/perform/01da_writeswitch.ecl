import perform.system, perform.format, perform.files;
LOADXML('<xml/>');

ds := files.diskSimple(false);

#declare(i)
#set(I,0)
#loop
#uniquename(stream)
%stream% := NOFOLD(ds((id1 % system.SplitWidth) = %I%));
OUTPUT(%stream%,,files.simpleName+'_uncompressed_' + %'I'%,OVERWRITE);
  #set(I,%I%+1)
  #if (%I%>=system.SplitWidth)
    #break
  #end
#end
