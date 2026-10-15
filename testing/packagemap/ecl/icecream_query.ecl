import lib_stringlib;

string20 nm := 'JOE' : stored('Name');
string20 p := 'PASS' : stored('Password');
varstring newstring := stringlib.StringToUpperCase(p);
myrec := record
                string20                name;
                string20                password;
                string20                flavor;
end;

file_miniqry_base := dataset('~thor::icecream::icecream_superfile',{myrec,unsigned8 __fpos {virtual (fileposition)}},flat);
i := index(file_miniqry_base,{name,password,flavor,__fpos},'~thor::icecream::key::icecream_superkeyfile', OPT);

output(sort(i, record));
res0 := i(name = nm, password = newstring)[1].flavor;
res := if (res0 = '','Not Found',res0);
output(res);
