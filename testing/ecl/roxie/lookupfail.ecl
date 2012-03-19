d := dataset([{'bad'},{'worse'}], {string f});

d success := transform
  self.f := 'Success';
end;

l := NOFOLD(LIMIT(NOFOLD(d), 1));

j := JOIN(d,l,LEFT.f=right.f);

CATCH(j,ONFAIL(success));
