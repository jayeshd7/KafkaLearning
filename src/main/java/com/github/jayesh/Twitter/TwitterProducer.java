package com.github.jayesh.Twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private  String consumerKey = "iaDdENNKRv7ghVv9b5dVlqGYK";
    private String consumerSecret = "1OpFVcWGFK5k0EhGbXPL22Z6tiu4vQWXdbjxpNiE5FmJLdieHA";
    private String token = "419524683-tlYgdKnqKBWKfrNtTHKam8ODOfAAMGQHIMOfthyF";
    private String secret = "mOJQEIGjfUY6DhNdgvNvMQ4SIrVgWAdidlibSoW7cWj6E";

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();

    }
        public void run(){
            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
            //BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);


            // create a twitter client

            Client client = createTwitterClient(msgQueue);
            client.connect();

            //create a producer

            //send a messages


            // on a different thread, or multiple different threads....
            while (!client.isDone()) {
                String msg = null;
                try {
                    msg = msgQueue.poll(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    client.stop();
                }
                if(msg != null ){
                    logger.info(msg);

                }

            }

            logger.info("End of Application...");

        }



        public Client createTwitterClient(BlockingQueue<String> msgQueue) {


            /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
            List<String> terms = Lists.newArrayList("kafka");
            hosebirdEndpoint.trackTerms(terms);

            Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


            //client Builders

            ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01")                              // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));
            // optional: use this if you want to process client events

            Client hosebirdClient = builder.build();
            // Attempts to establish a connection.

            return hosebirdClient;


        }

}
