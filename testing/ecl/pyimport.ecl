import python;
string pcat(string a, string b) := IMPORT(Python, '/opt/HPCCSystems/examples/python/python_cat.cat');
pcat('Hello ', 'world!');
