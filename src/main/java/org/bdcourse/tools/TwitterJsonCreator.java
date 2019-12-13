package org.bdcourse.tools;

import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TwitterJsonCreator {

    public static void main(String[] args) throws JSONException, IOException {
        List<String> output = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            output.add(getRandomTweetWithHashtag("china").toString());
        }
        for (int i = 0; i < 100; i++){
            output.add(getRandomTweetWithHashtag("russia").toString());
        }
        for (int i = 0; i < 100; i++){
            output.add(getRandomTweetWithHashtag("germany").toString());
        }
        for (int i = 0; i < 100; i++){
            output.add(getRandomTweetWithHashtag("usa").toString());
        }
        for (int i = 0; i < 100; i++){
            output.add(getRandomTweetWithHashtag("ukraine").toString());
        }

        FileWriter writer = new FileWriter("./data/testData");
        for(String str: output) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }

    private static JSONObject getRandomTweetWithHashtag(String hashtag) throws JSONException {
        JSONObject hashtags = new JSONObject();
        hashtags.put("hashtags", generateRandomHashtagList(hashtag));
        JSONObject tweet = new JSONObject();
        tweet.put("entities", hashtags);
        tweet.put("favorite_count", getRandomNumberInRange(0, 40));
        tweet.put("favorited", true);

        JSONObject retw = new JSONObject();
        retw.put("retweet_count", getRandomNumberInRange(0, 40));
        tweet.put("retweeted_status", retw);

        tweet.put("text", generateRandomText());
        return tweet;
    }
    private static List<JSONObject> generateRandomHashtagList(String neededHashtag) throws JSONException {
        JSONObject hash = new JSONObject();
        hash.put("text", neededHashtag);
        List<JSONObject> hashtags = new ArrayList<>();
        hashtags.add(hash);
        for (int i = 0; i < getRandomNumberInRange(0,7); i++){
            JSONObject tmpHash = new JSONObject();
            tmpHash.put("text", getAlphaNumericString(getRandomNumberInRange(0,10)));
            hashtags.add(tmpHash);
        }
        return hashtags;
    }

    private static String generateRandomText(){
        String tmp = "";
        for (int j=0; j < getRandomNumberInRange(0, 40); j++){
            String txt = getAlphaNumericString(getRandomNumberInRange(0, 20));

            tmp = tmp.concat(txt);
            tmp = tmp.concat(" ");
        }
        return tmp;
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
