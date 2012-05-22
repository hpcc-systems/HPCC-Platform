package com.hpccsystems.ecljdbc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EclFunctions
{
	static Map<String, EclFunction> functions;
	static
	{
		functions = new HashMap<String, EclFunction>();
		
		functions.put("COUNT", new EclFunction("COUNT", true, new EclColumnMetaData("countreturn", 0, java.sql.Types.NUMERIC), true));
		functions.put("MAX", new EclFunction("MAX", true, new EclColumnMetaData("maxreturn", 0, java.sql.Types.NUMERIC), false));
		functions.put("MIN", new EclFunction("MIN", true, new EclColumnMetaData("minreturn", 0, java.sql.Types.NUMERIC), false));	
	}
	
	static EclFunction getEclFunction( String funcname)
	{
		return functions.get(funcname);
	}
	
	static public boolean verifyEclFunction(String name, List<EclColumnMetaData> funccols)
	{
		String upperCaseName = name.toUpperCase();
		if (functions.containsKey(upperCaseName))
		{
			EclFunction function = functions.get(upperCaseName);
			
			if (funccols.size() > 1 && !function.acceptsMultipleInputs())
				return false;
			
			for (EclColumnMetaData tmp: funccols)
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