EXPORT Functions := MODULE

    EXPORT fn_AccountStatus_sort_order(STRING status) := FUNCTION
           fn_AccountStatus_sort_order :=   CASE(status,
                                                 'OVERDUE'         => 1,
                                                 'CURRENT'          => 2,
                                                 'CLOSED'  => 3,
                                                 999);
        RETURN fn_AccountStatus_sort_order;
    END;

    EXPORT fn_AccountStatus_sort_order2(STRING status) := FUNCTION
        RETURN fn_AccountStatus_sort_order;
    END;

END; // module.

