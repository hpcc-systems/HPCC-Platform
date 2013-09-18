enumType := unsigned2;

enumP := enum(unsigned2,
              p1       = 1,
              p2       = 2,
              MaxValue = 0xFFFF);

enumV := enum(enumType,
              v1       = 1,
              v2       = 2,
              v3       = 3,
              MaxValue = 0xFFFF);

rec := {
  enumType e,
  enumP    p,
  enumV    v,
};

ds := dataset([{1,2,3}],rec);

output(ds,,'~enum_test::temp',overwrite);
