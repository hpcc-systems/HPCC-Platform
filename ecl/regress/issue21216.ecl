R123456789 := RECORD
     UNSIGNED1 u1;
END;
R2 := RECORD
    R123456789 InnerR1;
END;
#DECLARE(xmlOfRecord)
#EXPORT(xmlOfRecord,R2)
%'xmlOfRecord'%;
