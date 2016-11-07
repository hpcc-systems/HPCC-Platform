

childRec := RECORD
     DECIMAL7_2 value;
END;

mainRec := RECORD
    UDECIMAL8 id;
    DECIMAL7_2 total;
    DATASET(childRec) children;
END;


ds := DATASET([
        {1, 1.0,[1.1,2.3,4.5,6.7]},
        {2, 3.45,[-1,10.30]}], mainRec);


childRec tc(childRec l) := TRANSFORM,SKIP(l.value = 0)  // Skip forces into a child query
    SELF.value := l.value * 2D;
END;

mainRec t(mainRec l) := TRANSFORM
    p := NOFOLD(PROJECT(l.children, tc(LEFT)));
    SELF.id := l.id * 2;
    SELF.total := l.total + COUNT(p) + SUM(p, value);
    SELF.children := p;
END;

OUTPUT(PROJECT(ds, t(LEFT)));
