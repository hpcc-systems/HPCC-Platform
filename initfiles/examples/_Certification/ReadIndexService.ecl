/* ******************************************************************************
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/

IMPORT $ AS Certification;
IMPORT Std;

EXPORT ReadIndexService() := FUNCTION
  STRING10 lnamein_value := '' : STORED('lnamein');
  STRING10 fnamein_value := '' : STORED('fnamein');
  UNSIGNED1 prangein_value := 0 : STORED('prangein');
  STRING10 streetin_value := '' : STORED('streetin');
  UNSIGNED1 zipsin_value := 0 : STORED('zipsin');
  UNSIGNED1 agein_value := 0 : STORED('agein');
  STRING2 birth_statein_value := '' : STORED('birth_statein');
  STRING3 birth_monthin_value := '' : STORED('birth_monthin');

  dat := Certification.DataFile;
  i := Certification.IndexFile;

  TYPEOF(dat) xt(dat l, i r) := TRANSFORM
    SELF.__filepos := r.__filepos;
    SELF := l;
  END;

  lname_filter := KEYED(i.lname = Std.Str.ToUpperCase(lnamein_value));
  fname_filter := KEYED(fnamein_value='' OR
                        i.fname = Std.Str.ToUpperCase(fnamein_value));
  prange_filter := KEYED(prangein_value=0 OR
                         i.prange = prangein_value);
  street_filter := KEYED(streetin_value='' OR
                         i.street = Std.Str.ToUpperCase(streetin_value));
  zips_filter := KEYED(zipsin_value=0 OR
                       i.zips = zipsin_value);
  age_filter := KEYED(agein_value=0 OR
                      i.age = agein_value);
  birth_state_filter := KEYED(birth_statein_value='' OR
                              i.birth_state = Std.Str.ToUpperCase(birth_statein_value));
  birth_month_filter := KEYED(birth_monthin_value='' OR
                              i.birth_month = Std.Str.ToUpperCase(birth_monthin_value));

  IDX_filter := lname_filter AND 
                fname_filter AND
                prange_filter AND
                street_filter AND
                zips_filter AND
                age_filter AND 
                birth_state_filter AND
                birth_month_filter;

  o := FETCH(dat, i(IDX_filter), RIGHT.__filepos, xt(LEFT, RIGHT));
  return OUTPUT(CHOOSEN(o,100));
end;
