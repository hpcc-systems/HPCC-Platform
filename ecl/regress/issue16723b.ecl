STRING str := 'aaa';

irow() := TRANSFORM({STRING one,STRING two}, SELF.one := 'str'; SELF.two := str);

DATASET([irow()]);
