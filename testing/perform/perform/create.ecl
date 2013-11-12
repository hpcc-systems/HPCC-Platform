import perform.system, perform.format;

export create := MODULE
    export orderedAppend(splitWidth) := FUNCTIONMACRO
        import perform.system, perform.format;
        LOCAL ds(unsigned i) := DATASET(system.simpleRecordCount DIV SplitWidth, format.createSimple(COUNTER+i), DISTRIBUTED);

        LOCAL dsAll := DATASET([], format.simpleRec)

        #declare(i)
        #set(I,0)
        #loop
          + ds(%I%)
          #set(I,%I%+1)
          #if (%I%>=SplitWidth)
            #break
          #end
        #end
        ;

        RETURN dsAll;
    ENDMACRO;
END;
