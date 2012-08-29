package com.hpccsystems.jdbcdriver;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import com.hpccsystems.jdbcdriver.SQLFragment.FragmentType;

public class SQLExpression
{
    public enum ExpressionType
    {
        LOGICAL_OPERATOR_TYPE,
        LOGICAL_EXPRESSION_TYPE;
    }

    private SQLFragment        prefix;
    private SQLFragment        postfix;

    private SQLOperator        operator;
    private ExpressionType     type;

    public SQLExpression(String operator)
    {
        this.operator = new SQLOperator(operator);
        type = ExpressionType.LOGICAL_OPERATOR_TYPE;
        prefix = null;
        postfix = null;
    }

    public SQLExpression(ExpressionType type)
    {
        operator = null;
        this.type = type;
        prefix = new SQLFragment();
        postfix = new SQLFragment();
    }

    public void ParseExpression(String expression) throws SQLException
    {
        String trimmedExpression = expression.trim();
        String operator = null;

        // order matters here!
        if (trimmedExpression.indexOf(SQLOperator.gte) != -1)
            operator = SQLOperator.gte;
        else if (trimmedExpression.indexOf(SQLOperator.lte) != -1)
            operator = SQLOperator.lte;
        else if (trimmedExpression.indexOf(SQLOperator.neq) != -1)
            operator = SQLOperator.neq;
        else if (trimmedExpression.indexOf(SQLOperator.neq2) != -1)
            operator = SQLOperator.neq2;
        else if (trimmedExpression.indexOf(SQLOperator.eq) != -1)
            operator = SQLOperator.eq;
        else if (trimmedExpression.indexOf(SQLOperator.gt) != -1)
            operator = SQLOperator.gt;
        else if (trimmedExpression.indexOf(SQLOperator.lt) != -1)
            operator = SQLOperator.lt;
        else
            throw new SQLException("Invalid logical operator found: " + trimmedExpression);

        String splitedsqlexp[] = trimmedExpression.split(operator);

        if (splitedsqlexp.length != 2) // something went wrong, only the operator was found?
            throw new SQLException("Invalid SQL Where clause found around: " + expression);

        setPrefix(splitedsqlexp[0]);

        setOperator(operator);
        if (!isOperatorValid())
            throw new SQLException("Error: Invalid operator found: ");

        setPostfix(splitedsqlexp[1].trim());
    }

    public ExpressionType getExpressionType()
    {
        return type;
    }

    public String getPrefixParent()
    {
        return prefix.getParent();
    }

    public String getPrefixValue()
    {
        return prefix.getValue();
    }

    public String getFullPrefix()
    {
        return prefix.getFullColumnName();
    }

    public String getFullPostfix()
    {
        return postfix.getFullColumnName();
    }

    public void setPrefix(String prefix)
    {
        this.prefix.parseExpressionFragment(prefix);
    }

    public SQLOperator getOperator()
    {
        return operator;
    }

    public void setOperator(SQLOperator operator)
    {
        this.operator = operator;
    }

    public void setOperator(String operator)
    {
        this.operator = new SQLOperator(operator);
    }

    public boolean isOperatorValid()
    {
        return operator != null ? operator.isValid() : false;
    }

    public String getPostfixValue()
    {
        return postfix.getValue();
    }

    public String getPostfixParent()
    {
        return postfix.getParent();
    }

    public void setPostfix(String postfixstr)
    {
        postfix.parseExpressionFragment(postfixstr);
    }

    public boolean isPrefixParametrized()
    {
        return prefix.isParametrized();
    }

    public boolean isPostfixParametrized()
    {
        return postfix.isParametrized();
    }

    public FragmentType getPrefixType()
    {
        return prefix.getType();
    }

    public FragmentType getPostfixType()
    {
        return postfix.getType();
    }

    @Override
    public String toString()
    {
        if (type == ExpressionType.LOGICAL_EXPRESSION_TYPE)
            return prefix.getValue() + " " + operator.toString() + " " + postfix.getValue();
        else if (type == ExpressionType.LOGICAL_OPERATOR_TYPE)
            return " " + operator.toString() + " ";
        else
            return "";
    }

    public String fullToString()
    {
        if (type == ExpressionType.LOGICAL_EXPRESSION_TYPE)
            return getFullPrefix() + " " + operator.toString() + " " + getFullPostfix();
        else
            return this.toString();
    }

    public String toStringTranslateSource(HashMap<String, String> map)
    {
        if (type == ExpressionType.LOGICAL_EXPRESSION_TYPE)
        {
            StringBuffer tmpsb = new StringBuffer();

            String prefixtranslate = map.get(prefix.getParent());
            String postfixtranslate = map.get(postfix.getParent());

            if (prefixtranslate != null)
            {
                tmpsb.append(prefixtranslate);
                tmpsb.append(".");
            }
            else if (prefix.getParent() != null && prefix.getParent().length() > 0)
            {
                tmpsb.append(prefix.getParent());
                tmpsb.append(".");
            }

            tmpsb.append(prefix.getValue()).append(" ").append(operator.toString()).append(" ");
            if (postfixtranslate != null)
            {
                tmpsb.append(postfixtranslate);
                tmpsb.append(".");
            }
            else if (postfix.getParent() != null && postfix.getParent().length() > 0)
            {
                tmpsb.append(postfix.getParent());
                tmpsb.append(".");
            }
            tmpsb.append(postfix.getValue());

            return tmpsb.toString();
        }
        else
            return this.toString();
    }

    public void setPrefix(SQLFragment prefix)
    {
        this.prefix = prefix;
    }

    public void setPostfix(SQLFragment postfix)
    {
        this.postfix = postfix;
    }

    public void updateFragmentTables(List<SQLTable> sqlTables) throws Exception
    {
        if (type == ExpressionType.LOGICAL_EXPRESSION_TYPE)
        {
            if (postfix.getType() == FragmentType.FIELD_TYPE)
                postfix.updateFragmentColumParent(sqlTables);

            if (prefix.getType() == FragmentType.FIELD_TYPE)
                prefix.updateFragmentColumParent(sqlTables);
        }
    }
}
