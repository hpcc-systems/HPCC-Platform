/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

export REAL RoundToPrecision( REAL num, INTEGER precision ) := FUNCTION

STRING StringVersion := (STRING) num;
unsigned1 CurPrecision := length(StringVersion) - stringlib.StringFind( StringVersion, '.', 1);

RETURN( MAP ( precision = 0  => TRUNCATE(num),
              CurPrecision <= precision         => num,
        ROUND( (num * POWER(10,precision)) ) / POWER(10,precision) ) );

END;


export Layout_Tabulate := { unsigned4 numActiveTrades, unsigned4 numReportedTrades };
export Attribute_Type := unsigned3;

export GetValue1( STRING16 InCommand = '',
                  Layout_Tabulate InData) := FUNCTION

      Attribute_Type val := IF( InData.numReportedTrades = 0,
                                    0.0,
                                    RoundToPrecision(InData.numActiveTrades/InData.numReportedTrades,2) * 100 );

      RETURN(val);
END;

export GetValue2( STRING16 InCommand = '',
                  Layout_Tabulate InData) := FUNCTION

      Attribute_Type val := IF( InData.numReportedTrades = 0,
                                    0.0,
                                    RoundToPrecision(InData.numActiveTrades/InData.numReportedTrades,2) * 100 + 0.5 );

      RETURN(val);
END;

export GetValue3( STRING16 InCommand = '',
                  Layout_Tabulate InData) := FUNCTION

      Attribute_Type val := IF( InData.numReportedTrades = 0,
                                    0.0,
                                    ROUND(RoundToPrecision(InData.numActiveTrades/InData.numReportedTrades,2) * 100));

      RETURN(val);
END;

export GetValue4( STRING16 InCommand = '',
                  Layout_Tabulate InData) := FUNCTION

      Attribute_Type val := IF( InData.numReportedTrades = 0,
                                    0.0,
                                    InData.numActiveTrades/InData.numReportedTrades * 10000);

      RETURN(val);
END;

ds := dataset([{4,7},{1,8},{2,7}], layout_Tabulate);

output(nofold(ds), {ds, GetValue1('', ds), GetValue2('', ds), GetValue3('', ds), GetValue4('', ds)});
