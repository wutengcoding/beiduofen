package com.wuteng.example;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by wuteng1 on 2016/10/20.
 */
public class SimpleUDFExample extends UDF {
    public Text evaluate(Text input){
        return new Text("Hello " + input.toString());
    }
}
