t := MODULE
  EXPORT a := OUTPUT('a');
  EXPORT c := OUTPUT('c');
  EXPORT b := OUTPUT('b');
  EXPORT d := MODULE
    EXPORT a := OUTPUT('a1');
    EXPORT c := OUTPUT('c1');
    EXPORT b := OUTPUT('b1');
  END;
END;

EVALUATE(t);
EVALUATE(t, c);
