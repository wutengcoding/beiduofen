package com.wuteng.pig_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created by wuteng1 on 2016/10/26.
 */
public class Sample_Eval extends EvalFunc<String>{
    public String exec(Tuple tuple) throws IOException {
        if(tuple == null || tuple.size() == 0)
            return null;
        String input = (String) tuple.get(0);
        return input.toLowerCase();
    }
}
