import java;

/*
 This example illustrates various calls to Java functions defined in the Java module JavaCat.
 The source of JavaCat can be found in the examples directory - it can be compiled to JavaCat.class
 using

   javac JavaCat

 and the resulting file JavaCat.class should be placed in /opt/HPCCSystems/classes (or somewhere else
 where it can be located via the standatd Java CLASSPATH environment variable.
 */


string jcat(string a, string b) := IMPORT(java, 'JavaCat.cat:(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;');

integer jadd(integer a, integer b) := IMPORT(java, 'JavaCat.add:(II)I');
integer jaddl(integer a, integer b) := IMPORT(java, 'JavaCat.addL:(II)J');
integer jaddi(integer a, integer b) := IMPORT(java, 'JavaCat.addI:(II)Ljava/lang/Integer;');

real jfadd(real4 a, real4 b) := IMPORT(java, 'JavaCat.fadd:(FF)F');
real jdadd(real a, real b) := IMPORT(java, 'JavaCat.dadd:(DD)D');
real jdaddD(real a, real b) := IMPORT(java, 'JavaCat.daddD:(DD)Ljava/lang/Double;');

jcat('Hello ', 'world!');
jadd(1,2);
jaddl(3,4);
jaddi(5,6);

jfadd(1,2);
jdadd(3,4);
jdaddD(5,6);
