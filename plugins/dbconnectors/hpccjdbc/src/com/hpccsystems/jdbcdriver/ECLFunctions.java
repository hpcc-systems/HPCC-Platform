package com.hpccsystems.jdbcdriver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ECLFunctions
{
    static Map<String, ECLFunction> functions;
    static
    {
        functions = new HashMap<String, ECLFunction>();

        functions.put("COUNT", new ECLFunction("COUNT", true, new HPCCColumnMetaData("countreturn", 0,
                java.sql.Types.NUMERIC), true));
        functions.put("MAX", new ECLFunction("MAX", true,
                new HPCCColumnMetaData("maxreturn", 0, java.sql.Types.NUMERIC), false));
        functions.put("MIN", new ECLFunction("MIN", true,
                new HPCCColumnMetaData("minreturn", 0, java.sql.Types.NUMERIC), false));
        functions.put("SUM", new ECLFunction("SUM", true,
                new HPCCColumnMetaData("sumreturn", 0, java.sql.Types.NUMERIC), false));
    }

    static ECLFunction getEclFunction(String funcname)
    {
        return functions.get(funcname);
    }

    static public boolean verifyEclFunction(String name, List<HPCCColumnMetaData> funccols)
    {
        String upperCaseName = name.toUpperCase();
        if (functions.containsKey(upperCaseName))
        {
            ECLFunction function = functions.get(upperCaseName);

            if (funccols.size() > 1 && !function.acceptsMultipleInputs())
                return false;

            for (HPCCColumnMetaData tmp : funccols)
            {
                if (!function.acceptsWilCard() && tmp.getColumnName().contains("*"))
                    return false;
            }
            return true;
        }
        else
            return false;
    }
}
