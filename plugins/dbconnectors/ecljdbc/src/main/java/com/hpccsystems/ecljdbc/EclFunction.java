package com.hpccsystems.ecljdbc;


public class EclFunction 
{
	private String name;
	private boolean acceptsWilCard;
	private boolean acceptsMultipleInputs;
	private EclColumnMetaData returnType;
	

	public EclFunction (String thename, boolean acceptswild, EclColumnMetaData returntype, boolean multipleInput)
	{
		name = thename;
		acceptsWilCard = acceptswild;
		returnType = returntype;
		acceptsMultipleInputs = multipleInput;
	}
	
	public EclFunction (String thename, EclColumnMetaData returntype)
	{
		name = thename;
		acceptsWilCard = false;
		returnType = returntype;
	}
	
	public String getName() 
	{
		return name;
	}
	
	public boolean acceptsWilCard() 
	{
		return acceptsWilCard;
	}
		
	public EclColumnMetaData getReturnType() 
	{
		return returnType;
	}

	public boolean acceptsMultipleInputs() 
	{
		return acceptsMultipleInputs;
	}
}
