package com.yq.srdb.backend.parser.statement;

/**
 * where 表达式 如：where age>1 and name = 'aa'
 */
public class Where {
    public SingleExpression singleExp1;
    public String logicOp;
    public SingleExpression singleExp2;
}
