IMPORT python;

integer a(integer val) := EMBED(python)
val+1
ENDEMBED;
string a2(string val) := EMBED(python)
val+'1'
ENDEMBED;

string a3(varstring val) := EMBED(python)
val+'1'
ENDEMBED;

integer b(integer val) := EMBED(python, 'val-2');

a(10);
a2('Hello');
a3('world');
b(10);
