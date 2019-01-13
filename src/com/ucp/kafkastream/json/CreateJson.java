package com.ucp.kafkastream.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.Random;

public class CreateJson {

    public static JsonObject createJsonObject() {
        //创建JsonObject对象
        Random random = new Random() ;
        int s = random.nextInt(10) ;
        System.out.println(s);
        JsonObject result = new JsonObject();
        result.addProperty( "sucess", true );
        result.addProperty( "totalCount",  30);

        JsonObject user1 = new JsonObject();
        user1.addProperty( "id", "12" );
        user1.addProperty( "name", "张三" );
        user1.addProperty( "createTime", System.currentTimeMillis());

        JsonObject user2 = new JsonObject();
        user2.addProperty( "id", "13" );
        user2.addProperty( "name", "李四" );
        user2.addProperty( "createTime", System.currentTimeMillis());

        JsonObject department1 = new JsonObject();
        department1.addProperty( "id", 1 );
        department1.addProperty( "name", "技术部" );


        JsonObject department2 = new JsonObject();
        department2.addProperty( "id", s );
        department2.addProperty( "name", "财务部" );


        user1.add( "department", department1 );
        user2.add( "department", department2 );

        //返回一个jsonarray对象
        JsonArray jsonArray = new JsonArray();
        jsonArray.add( user1 );
        jsonArray.add( user2 );
        result.add( "data", jsonArray );
        return result;
    }

}