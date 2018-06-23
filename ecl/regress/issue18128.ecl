DictionaryRec := RECORD
    STRING key => STRING value;
END;

/*
MyFunc(STRING k, DICTIONARY(DictionaryRec) dict) := FUNCTION
    RETURN dict[k].value;
END;
*/

MyFunc(STRING k, DICTIONARY(DictionaryRec) dict = DICTIONARY([], DictionaryRec)) := FUNCTION
    RETURN dict[k].value;
END;

d := DICTIONARY([{'foo' => 'bar'}], DictionaryRec);

MyFunc('foo', d);
