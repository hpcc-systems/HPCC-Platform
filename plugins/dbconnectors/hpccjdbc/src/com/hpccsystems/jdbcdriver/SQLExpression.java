package com.hpccsystems.jdbcdriver;

public class SQLExpression
{
	public static final int LOGICAL_OPERATOR_TYPE = 1;
	public static final int LOGICAL_EXPRESSION_TYPE = 2;

	private String name;
	private SQLOperator operator;
	private String value;
	private int type;
	private boolean isParametrized;

	public SQLExpression(String name, String operator, String value)
	{
		this.name = name;
		this.operator = new SQLOperator(operator);
		this.value = value;
		type = LOGICAL_EXPRESSION_TYPE;
	}

	public SQLExpression(String operator)
	{
		name = null;
		this.operator = new SQLOperator(operator);
		value = null;
		type = LOGICAL_OPERATOR_TYPE;
		isParametrized = false;
	}

	public SQLExpression(int type)
	{
		name = null;
		operator = null;
		value = null;
		this.type = type;
		isParametrized = false;
	}

	public int getType()
	{
		return type;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public SQLOperator getOperator() {
		return operator;
	}
	public void setOperator(SQLOperator operator) {
		this.operator = operator;
	}

	public void setOperator(String operator) {
		this.operator = new SQLOperator(operator);
	}

	public String getValue() {
		return value;
	}
	public void setValue(String value)
	{
		this.value = value;

		if (value != null && value.length() > 0 && (value.contains("${") || value.equals("?")))
			isParametrized = true;
		else
			isParametrized = false;
	}

	public boolean isParametrized(String param)
	{
		return isParametrized;
	}

	public boolean isParametrized()
	{
		return isParametrized;
	}

	@Override
	public String toString()
	{
		return (type == LOGICAL_EXPRESSION_TYPE ? name + " "  + operator.toString() + " " + value : " " + operator.toString() + " ");
	}
}
