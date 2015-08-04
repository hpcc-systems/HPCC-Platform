/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
