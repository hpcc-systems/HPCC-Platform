import Std.System.Thorlib;
import Std.File AS FileServices;

string fname_prefix := '~thor::icecream';
string key_prefix := '~thor::icecream::key';

myrec := record
                string20                name;
                string20                password;
                string20                flavor;
end;

df := dataset([{'JOHN','PASS','CHOCOMINT'},{'JOE','PASS','CHOCOLATE'},
                                                {'JANE','PASS','DOUBLE CHOCOLATE'}],myrec);
df2 := dataset([{'ALFRED','PASS','ROCKY ROAD'},
                                                {'ALLISON','PASS','PISTACHIO'},
                                                {'ANDREA','PASS','PEANUT BRITTLE'}],myrec);

df3 := dataset([{'ERIK','PASS','LEMON'},
                                                {'ERIKA','PASS','STRAWBERRY'},
                                                {'ESTHER','PASS','ORANGE'},
                                                {'EVE','PASS','WATERMELON'},
                                                {'ETHAN','PASS','GRAPE'},
                                                {'EVAN','PASS','LIME'}],
                                                myrec);


STRING fname := fname_prefix + '::icecream_superfile';
if (FileServices.SuperFileExists (fname), FileServices.clearsuperfile(fname), FileServices.createsuperfile(fname));

STRING kname := key_prefix + '::icecream_superkeyfile';
if (FileServices.SuperFileExists (kname), FileServices.clearsuperfile(kname), FileServices.createsuperfile(kname));

// save input datasets as logical files in THOR and add them to the super files
create_files := parallel (output(df,,fname_prefix + '::icecream1', named ('df'), overwrite),
                          output(df2,,fname_prefix + '::icecream2', named ('df2'), overwrite),
                          output(df3,,fname_prefix + '::icecream3', named ('df3'), overwrite));

a4 := fileservices.addsuperfile(fname_prefix + '::icecream_superfile', fname_prefix + '::icecream1');
a5 := fileservices.addsuperfile(fname_prefix + '::icecream_superfile', fname_prefix + '::icecream3');

file_miniqry_base := dataset(fname_prefix + '::icecream1',{myrec,unsigned8 __fpos {virtual (fileposition)}},flat);
key_miniqry := index(file_miniqry_base,{name,password,flavor,__fpos},'~thor::icecream::key::icecream_name_password');

file_miniqry_base_2 := dataset(fname_prefix + '::icecream2',{myrec,unsigned8 __fpos {virtual (fileposition)}},flat);
key_miniqry_2 := index(file_miniqry_base_2,{name,password,flavor,__fpos},'~thor::icecream::key::icecream_name_password_2');

file_miniqry_base_3 := dataset(fname_prefix +'::icecream3',{myrec,unsigned8 __fpos {virtual (fileposition)}},flat);
key_miniqry_3 := index(file_miniqry_base_3,{name,password,flavor,__fpos},'~thor::icecream::key::icecream_name_password_3');

// build indices, add them to the superfiles:
build_indices := parallel (buildindex (key_miniqry,overwrite),
                           buildindex (key_miniqry_2,overwrite),
                           buildindex (key_miniqry_3,overwrite));

c := fileservices.addsuperfile(key_prefix + '::icecream_superkeyfile',key_prefix + '::icecream_name_password');
c1 := fileservices.addsuperfile(key_prefix + '::icecream_superkeyfile',key_prefix + '::icecream_name_password_3');


sequential(create_files, a4, a5, build_indices,c,c1);
