ds := dataset([1,2,3,4,5], { unsigned i; });

r := record
unsigned i;
dataset({unsigned i}) next;
end;

r t(unsigned value) := transform
    childDs := dataset([value, value+1, value+2], { unsigned i });
    withSideEffect := WHEN(childDs, output(choosen(childDs,1),named('trace'),extend));
    SELF.next := withSideEffect;
    SELF.i := value;
END;

p := PROJECT(ds, t(LEFT.i));
output(p);
