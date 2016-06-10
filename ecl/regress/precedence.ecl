boolean b1 := false : stored('b1');
boolean b2 := false : stored('b2');
boolean b3 := false : stored('b3');

unsigned u1 := 0 : stored('u1');
unsigned u2 := 0 : stored('u2');
unsigned u3 := 0 : stored('u3');

b1 and b2 or b3;
(b1 and b2) or b3;
b1 and (b2 or b3);

u1 & u2 | u3;
(u1 & u2) | u3;
u1 & (u2 | u3);
