//#when(legacy)

import Std.Date;

STRING10 DateTimeDifference(STRING20 sd1,STRING20 sd2) := FUNCTION

      /////////////////////////////////////////////////////////////////////////////////////
      // Input STRINGs 'YYYY-MMDD HH:MM:SS'
      // Output STRING 'SSSSSSSSSS' i.e number of seconds difference between the two datetimes
      /////////////////////////////////////////////////////////////////////////////////////
      UNSIGNED8 SecondsSince1900(STRING20 dt) := FUNCTION

         ASSERT(REGEXFIND('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}',dt),'Passed Invalid datetime: '+dt,FAIL);
         RETURN  Date.dayssince1900((INTEGER2)dt[1..4],(INTEGER1)dt[6..7],(INTEGER1)dt[9..10])*86400
                +((UNSIGNED8)dt[12..13])*3600
                +((UNSIGNED8)dt[15..16])*60
                + (UNSIGNED8)dt[18..19];
      END;
      RETURN INTFORMAT(ABS(SecondsSince1900(sd2)-SecondsSince1900(sd1)),10,1);
END;

ds := DATASET('x', { string20 a, STRING20 b}, thor);

output(ds, { DateTimeDifference(a,b) });
