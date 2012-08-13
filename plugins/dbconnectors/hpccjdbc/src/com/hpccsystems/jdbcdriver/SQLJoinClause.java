package com.hpccsystems.jdbcdriver;

import java.sql.SQLException;
import java.util.List;

public class SQLJoinClause
{
    public enum JoinType
    {
        INNER_JOIN, OUTER_JOIN;

        public String toSQLString()
        {
            switch (this)
            {
                case INNER_JOIN:
                    return "INNER JOIN";
                case OUTER_JOIN:
                    return "OUTER JOIN";
                default:
                    return "";
            }
        }

        public String toECLString()
        {
            switch (this)
            {
                case OUTER_JOIN:
                    return "FULL OUTER";
                case INNER_JOIN:
                    return "INNER";
                default:
                    return "";
            }
        }
    }

    private static final JoinType defaultType = JoinType.INNER_JOIN;

    private JoinType              type;
    private SQLWhereClause        OnClause;
    private SQLTable              joinTable   = null;

    public SQLJoinClause()
    {
        this.type = defaultType;
        this.OnClause = new SQLWhereClause();
    }

    public SQLJoinClause(JoinType type)
    {
        this.type = type;
        this.OnClause = new SQLWhereClause();
    }

    public void parse(String joinclausestr) throws SQLException
    {
        this.OnClause = new SQLWhereClause();

        if (joinclausestr.length() > 0)
        {
            String joinSplit[] = joinclausestr.split("\\s+(?i)join\\s+");
            if (joinSplit.length > 1)
            {
                if (joinSplit[0].trim().length() == 0)
                {
                    this.type = SQLJoinClause.defaultType;
                }
                else if (joinSplit[0].trim().equalsIgnoreCase("INNER"))
                {
                    this.type = JoinType.INNER_JOIN;
                }
                else if (joinSplit[0].trim().equalsIgnoreCase("OUTER"))
                {
                    this.type = JoinType.OUTER_JOIN;
                }
                else
                    throw new SQLException("Error: Invalid join clause found: " + type);

            }
            else
                throw new SQLException("Error: No valid join clause found: " + joinclausestr);


            String clausesplit[] = joinSplit[1].split("\\s+(?i)on\\s+", 2);

            if (clausesplit.length > 1)
            {
                String splittablefromalias[] = clausesplit[0].trim().split("\\s+(?i)as(\\s+|$)");
                if (splittablefromalias.length == 1)
                {
                    String splittablebyblank[] = splittablefromalias[0].trim().split("\\s+");
                    this.joinTable = new SQLTable(splittablebyblank[0]);
                    if (splittablebyblank.length == 2)
                        this.joinTable.setAlias(splittablebyblank[1].trim());
                    else if (splittablebyblank.length > 2)
                        throw new SQLException("Error: " + splittablefromalias[0]);
                }
                else if (splittablefromalias.length == 2)
                {
                    this.joinTable = new SQLTable(splittablefromalias[0].trim());
                    this.joinTable.setAlias(splittablefromalias[1].trim());
                }
                else
                    throw new SQLException("Error: Invalid SQL: " + clausesplit[0]);

                parseOnClause(clausesplit[1]);

            }
            else
                throw new SQLException("Error: 'Join' clause does not contain 'On' clause.");
        }
    }

    public void addOnClauseExpression(SQLExpression expression)
    {
        OnClause.addExpression(expression);
    }

    public SQLWhereClause getOnClause()
    {
        return this.OnClause;
    }

    public JoinType getType()
    {
        return this.type;
    }

    public String getSQLTypeStr()
    {
        return type.toSQLString();
    }

    public String getECLTypeStr()
    {
        return type.toECLString();
    }

    public SQLTable getJoinTable()
    {
        return joinTable;
    }

    public void setJoinTable(SQLTable joinTable)
    {
        this.joinTable = joinTable;
    }

    public String getJoinTableName()
    {
        return joinTable.getName();
    }

    public String getJoinTableAlias()
    {
        return joinTable.getAlias();
    }

    public void updateFragments(List<SQLTable> sqlTables) throws Exception
    {
        OnClause.updateFragmentColumnsParent(sqlTables);
    }
    @Override
    public String toString()
    {
        return getSQLTypeStr() + " " + getJoinTableName() + " ON " + OnClause.fullToString();
    }

    public void parseOnClause(String clause) throws SQLException
    {
        OnClause.parseWhereClause(clause);
    }
}
