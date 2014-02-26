import java;
string jcat(string a, string b) := IMPORT(java, 'JavaCat.cat:(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;' : classpath('/opt/HPCCSystems/moreclasses/'));

/*
integer jadd(integer a, integer b) := IMPORT(java, 'JavaCat.add:(II)I');
integer jaddl(integer a, integer b) := IMPORT(java, 'JavaCat.addL:(II)J');
integer jaddi(integer a, integer b) := IMPORT(java, 'JavaCat.addI:(II)Ljava/lang/Integer;');

real jfadd(real4 a, real4 b) := IMPORT(java, 'JavaCat.fadd:(FF)F');
real jdadd(real a, real b) := IMPORT(java, 'JavaCat.dadd:(DD)D');
real jdaddD(real a, real b) := IMPORT(java, 'JavaCat.daddD:(DD)Ljava/lang/Double;');
*/
jcat('Hello ', 'world!');
/*
jadd(1,2);
jaddl(3,4);
jaddi(5,6);

jfadd(1,2);
jdadd(3,4);
jdaddD(5,6);
*/
