import python;
string pcat(string a, string b) := IMPORT(Python, '/opt/HPCCSystems/examples/embed/python_cat.cat');
pcat('Hello ', 'world!');
