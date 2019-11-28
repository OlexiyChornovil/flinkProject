package org.bdcourse.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.security.InvalidParameterException;

public class TwitterSourceDelivery {
    public static TwitterSource getTwitterConnection() throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("src/main/resources/twitter.properties");
        if (parameterTool.has(TwitterSource.CONSUMER_KEY) &&
                parameterTool.has(TwitterSource.CONSUMER_SECRET) &&
                parameterTool.has(TwitterSource.TOKEN) &&
                parameterTool.has(TwitterSource.TOKEN_SECRET)
        ) {
            return new TwitterSource(parameterTool.getProperties());
        } else throw new InvalidParameterException("missing parameters");
    }

}



