package com.yq.srdb.backend.parser;

import org.junit.Test;

public class ParserTest {
    @Test
    public void test() throws Exception {
        Object parse = Parser.Parse("select id from test where id=5".getBytes());
        Object parse1 = Parser.Parse("insert into test values 1 aaa 3".getBytes());
        Object parse2 = Parser.Parse("create table test id int32,name string, age int32 (index id)".getBytes());
    }
}
