package com.hpccsystems.jdbcdriver;

import java.util.HashMap;

public class SQLExpressionFragment
{
    public static final int    LOGICAL_OPERATOR_TYPE   = 1;
    public static final int    LOGICAL_EXPRESSION_TYPE = 2;

    private static final short PARENT_INDEX            = 0;
    private static final short NAME_INDEX              = 1;
    private String[]           prefix;
    private String[]           postfix;

    private SQLOperator        operator;
    private int                type;
    private boolean            isParametrized;

    public SQLExpressionFragment(String operator)
    {
        this.operator = new SQLOperator(operator);
        type = LOGICAL_OPERATOR_TYPE;
        isParametrized = false;
        prefix = new String[2];
        postfix = new String[2];
    }

    public SQLExpressionFragment(int type)
    {
        operator = null;
        this.type = type;
        isParametrized = false;
        prefix = new String[2];
        postfix = new String[2];
    }

    public int getType()
    {
        return type;
    }

    public String getPrefixParent()
    {
        return prefix[PARENT_INDEX];
    }

    public String getPrefixName()
    {
        return prefix[NAME_INDEX];
    }

    public String getFullPrefix()
    {
        return getPrefixParent() + "." + getPrefixName();
    }

    public String getFullPostfix()
    {
        return getPostfixParent() + "." + getPostfixName();
    }

    public void setPrefix(String prefix)
    {
        this.prefix = parseExpressionFragment(prefix);
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

    public String getPostfixName()
    {
        return postfix[NAME_INDEX];
    }

    public String getPostfixParent()
    {
        return postfix[PARENT_INDEX];
    }

    private String[] parseExpressionFragment(String fragment)
    {
        String[] parsedFrag = new String[2];

        if (!HPCCJDBCUtils.isLiteralString(fragment) && !HPCCJDBCUtils.isNumeric(fragment) )
        {

            String fragsplit[] = fragment.split("\\.", 2);
            if (fragsplit.length == 1)
            {
                parsedFrag[PARENT_INDEX] = "";
                parsedFrag[NAME_INDEX] = fragsplit[0].trim();
            }
            else
            {
                parsedFrag[PARENT_INDEX] = fragsplit[0].trim();
                parsedFrag[NAME_INDEX] = fragsplit[1].trim();
            }
        }
        else
        {
            parsedFrag[PARENT_INDEX] = "";
            parsedFrag[NAME_INDEX] = fragment;
        }

        return parsedFrag;
    }

    public void setPostfix(String postfix)
    {
        this.postfix = parseExpressionFragment(postfix);

        if ((this.postfix[1].contains("${") || this.postfix[1].equals("?")))
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
        if (type == LOGICAL_EXPRESSION_TYPE)
            return prefix[NAME_INDEX] + " " + operator.toString() + " " + postfix[NAME_INDEX];
        else if (type == LOGICAL_OPERATOR_TYPE)
            return " " + operator.toString() + " ";
        else
            return "";
    }

    public String fullToString()
    {
        if (type == LOGICAL_EXPRESSION_TYPE)
            return getFullPrefix() + " " + operator.toString() + " " + getFullPostfix();
        else
            return this.toString();
    }

    public String toStringTranslateSource(HashMap<String, String> map)
    {
        if (type == LOGICAL_EXPRESSION_TYPE)
        {
            StringBuffer tmpsb = new StringBuffer();
            String prefixtranslate = map.get(prefix[PARENT_INDEX]);
            String postfixtranslate = map.get(postfix[PARENT_INDEX]);

            if (prefixtranslate != null)
                tmpsb.append(prefixtranslate);
            else
                tmpsb.append(prefix[PARENT_INDEX]);
            tmpsb.append(".").append(prefix[NAME_INDEX]).append(" ").append(operator.toString()).append(" ");
            if (postfixtranslate != null)
                tmpsb.append(postfixtranslate);
            else
                tmpsb.append(postfix[PARENT_INDEX]);
            tmpsb.append(".").append(postfix[NAME_INDEX]);

            return tmpsb.toString();
        }
        else
            return this.toString();
    }

    public void setPrefix(String[] prefix)
    {
        this.prefix = prefix;
    }

    public void setPostfix(String[] postfix)
    {
        this.postfix = postfix;

        if ((this.postfix[1].contains("${") || this.postfix[1].equals("?")))
            isParametrized = true;
        else
            isParametrized = false;
    }
}
