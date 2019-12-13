package org.bdcourse.depricated;

import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class JsonCreator {

    @SuppressWarnings("unchecked")
    public static void main( String[] args ) throws JSONException, IOException {
        //First Employee
        List<String> output = new ArrayList<>();

        for(int i = 0; i < 10; i++){
            JSONObject h1t1 = new JSONObject();
            JSONObject h1t2 = new JSONObject();
            JSONObject h1t3 = new JSONObject();
            h1t1.put("text", "china");
            h1t2.put("text", "notchina");
            h1t3.put("text", "notchina2");

            List<JSONObject> hashtags = new ArrayList<>();
            hashtags.add(h1t1);
            hashtags.add(h1t2);
            hashtags.add(h1t3);

            JSONObject h1 = new JSONObject();
            h1.put("hashtags", hashtags);

            JSONObject entities = new JSONObject();
            entities.put("entities", h1);


            entities.put("favorite_count", getRandomNumberInRange(0, 40));
            entities.put("favorited", true);

            JSONObject retw = new JSONObject();
            retw.put("retweet_count", getRandomNumberInRange(0, 40));
            entities.put("retweeted_status", retw);

            String tmp = "";
            for (int j=0; j < getRandomNumberInRange(0, 40); j++){
                String txt = getAlphaNumericString(getRandomNumberInRange(0, 20));

                tmp = tmp.concat(txt);
                tmp = tmp.concat(" ");
            }
            entities.put("text", tmp);


            output.add(entities.toString());
        }
        for(int i = 0; i < 10; i++){
            JSONObject h1t1 = new JSONObject();
            JSONObject h1t2 = new JSONObject();
            h1t1.put("text", "usa");
            h1t2.put("text", "notchina");

            List<JSONObject> hashtags = new ArrayList<>();
            hashtags.add(h1t1);
            hashtags.add(h1t2);

            JSONObject h1 = new JSONObject();
            h1.put("hashtags", hashtags);

            JSONObject entities = new JSONObject();
            entities.put("entities", h1);
            entities.put("favorite_count", getRandomNumberInRange(0, 40));

            entities.put("favorited", true);

            JSONObject retw = new JSONObject();
            retw.put("retweet_count", getRandomNumberInRange(0, 40));
            entities.put("retweeted_status", retw);

            String tmp = "";
            for (int j=0; j < getRandomNumberInRange(0, 40); j++){
                String txt = getAlphaNumericString(getRandomNumberInRange(0, 20));

                tmp = tmp.concat(txt);
                tmp = tmp.concat(" ");
            }
            entities.put("text", tmp);

            output.add(entities.toString());
        }
        for(int i = 0; i < 10; i++){
            JSONObject h1t1 = new JSONObject();
            JSONObject h1t2 = new JSONObject();
            JSONObject h1t3 = new JSONObject();
            JSONObject h1t4 = new JSONObject();
            h1t1.put("text", "russia");
            h1t2.put("text", "putin");
            h1t3.put("text", "strong");
            h1t4.put("text", "verystrong");


            List<JSONObject> hashtags = new ArrayList<>();
            hashtags.add(h1t1);
            hashtags.add(h1t2);
            hashtags.add(h1t3);
            hashtags.add(h1t4);

            JSONObject h1 = new JSONObject();
            h1.put("hashtags", hashtags);

            JSONObject entities = new JSONObject();
            entities.put("entities", h1);
            entities.put("favorite_count", getRandomNumberInRange(0, 40));
            entities.put("favorited", true);

            JSONObject retw = new JSONObject();
            retw.put("retweet_count", getRandomNumberInRange(0, 40));
            entities.put("retweeted_status", retw);

            String tmp = "";
            for (int j=0; j < getRandomNumberInRange(0, 40); j++){
                String txt = getAlphaNumericString(getRandomNumberInRange(0, 20));

                tmp = tmp.concat(txt);
                tmp = tmp.concat(" ");
            }
            entities.put("text", tmp);

            output.add(entities.toString());
        }
        for(int i = 0; i < 10; i++){
            JSONObject h1t1 = new JSONObject();
            h1t1.put("text", "germany");


            List<JSONObject> hashtags = new ArrayList<>();
            hashtags.add(h1t1);

            JSONObject h1 = new JSONObject();
            h1.put("hashtags", hashtags);

            JSONObject entities = new JSONObject();
            entities.put("entities", h1);
            entities.put("favorite_count", getRandomNumberInRange(0, 40));
            entities.put("favorited", true);

            JSONObject retw = new JSONObject();
            retw.put("retweet_count", getRandomNumberInRange(0, 40));
            entities.put("retweeted_status", retw);

            String tmp = "";
            for (int j=0; j < getRandomNumberInRange(0, 40); j++){
                String txt = getAlphaNumericString(getRandomNumberInRange(0, 20));

                tmp = tmp.concat(txt);
                tmp = tmp.concat(" ");
            }
            entities.put("text", tmp);

            output.add(entities.toString());
        }
        for(int i = 0; i < 10; i++){
            JSONObject h1t1 = new JSONObject();
            JSONObject h1t2 = new JSONObject();
            JSONObject h1t3 = new JSONObject();
            h1t1.put("text", "ukraine");
            h1t2.put("text", "putin");
            h1t3.put("text", "strong");


            List<JSONObject> hashtags = new ArrayList<>();
            hashtags.add(h1t1);
            hashtags.add(h1t2);
            hashtags.add(h1t3);

            JSONObject h1 = new JSONObject();
            h1.put("hashtags", hashtags);

            JSONObject entities = new JSONObject();
            entities.put("entities", h1);
            entities.put("favorite_count", getRandomNumberInRange(0, 40));
            entities.put("favorited", true);

            JSONObject retw = new JSONObject();
            retw.put("retweet_count", getRandomNumberInRange(0, 40));
            entities.put("retweeted_status", retw);

            String tmp = "";
            for (int j=0; j < getRandomNumberInRange(0, 40); j++){
                String txt = getAlphaNumericString(getRandomNumberInRange(0, 20));

                tmp = tmp.concat(txt);
                tmp = tmp.concat(" ");
            }
            entities.put("text", tmp);

            output.add(entities.toString());
        }
        FileWriter writer = new FileWriter("./data/testData");
        for(String str: output) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }
    private static int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

    private static String getAlphaNumericString(int n)
    {

        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                    = (int)(AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }

}
