package com.learning;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Created by wuteng1 on 2016/11/17.
 */
public class JavaDemo {
    public static void main(String[] args) {
//        char[] NON_EXECUTE_PREFIX = ")]}\'\n".toCharArray();
//        for(char c: NON_EXECUTE_PREFIX){
//            System.out.println(c);
//        }

        JsonParser parser = new JsonParser();
        JsonObject obj = parser.parse("{\"srclist\":[1_2600,1_222], \"pageid\":1}").getAsJsonObject();
    }
}
