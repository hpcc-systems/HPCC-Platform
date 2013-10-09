x := module

    export e := enum(a,b,c);
    export t := unsigned2;
    export pattern p := 'h';
    export r:= { unsigned id; };

    export f(unsigned i) := i * 10;
    export hello(string who = 'world') := 'hello ' + who;
end;

output(x);
