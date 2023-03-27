package com.yq.srdb.backend.parser.statement;

/**
 * 单一表达式，如 age>10
 */
public class SingleExpression {
    public String field;
    public String compareOp;
    public String value;
}
