import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import org.bson.Document;
import twitter4j.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class TweetSemantics {
    public static void main (String[] args) throws IOException {
        /* Establishing Connection With MongoDB */
        ConnectionString connectionString = new ConnectionString(ConfigConstants.MONGO_URI);
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
        MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        System.out.println("Database connected successfully!");

        /* Selecting Database In MongoDB */
        MongoDatabase mongoDatabase =mongoClient.getDatabase(ConfigConstants.DATABASE);
        System.out.println("Database selected successfully!");

        String[] collections = new String[]{"cold", "snow", "flu", "immune", "mask", "vaccine"};

        ArrayList<String> tweetList = new ArrayList<>();

        for (int i = 0; i < collections.length; i++) {
            MongoCollection<Document> collection = mongoDatabase.getCollection(collections[i]);
            try (MongoCursor<Document> cursor = collection.find().iterator()) {
                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    JSONObject jsonObject = new JSONObject(document);
                    String tweet = getProcessedTweet(jsonObject.getString("text").trim());
                    tweetList.add(tweet);
                }
            }
        }

        int totalDocuments = tweetList.size();
        HashMap<String, Integer> wordMap = new HashMap<>(){{
            put(ConfigConstants.WEATHER, 0);
            put(ConfigConstants.CONDITION, 0);
            put(ConfigConstants.PEOPLE, 0);
        }};

        TreeMap<Integer, Integer> weatherFrequency = new TreeMap<>();
        TreeMap<Integer, Integer> wordCount = new TreeMap<>();
        double maxRelativeFrequency = 0d;
        String tweetWithMaxRelativeFrequency = "";

        for (int i = 0; i < tweetList.size(); i++) {
            int weatherCount = 0;
            String tweet = tweetList.get(i);
            String[] wordsInTweet = tweet.split(" ");
            if (tweet.toLowerCase().contains(ConfigConstants.WEATHER)) {
                wordMap.put(ConfigConstants.WEATHER, wordMap.get(ConfigConstants.WEATHER) + 1);
            } else if (tweet.toLowerCase().contains(ConfigConstants.PEOPLE)) {
                wordMap.put(ConfigConstants.PEOPLE, wordMap.get(ConfigConstants.PEOPLE) + 1);
            } else if (tweet.toLowerCase().contains(ConfigConstants.CONDITION)) {
                wordMap.put(ConfigConstants.CONDITION, wordMap.get(ConfigConstants.CONDITION) + 1);
            }

            for (String word : wordsInTweet) {
                if (word.toLowerCase().contains(ConfigConstants.WEATHER)) {
                    weatherCount++;
                }
            }
            if (weatherCount > 0) {
                weatherFrequency.put(i, weatherCount);
                wordCount.put(i, wordsInTweet.length);
                if (maxRelativeFrequency < (double) weatherCount / wordsInTweet.length) {
                    maxRelativeFrequency = (double) weatherCount / wordsInTweet.length;
                    tweetWithMaxRelativeFrequency = tweet;
                }
            }
        }

        Formatter formatterOne = new Formatter();
        formatterOne.format("%30s %15s\n", "Total Documents", tweetList.size());
        formatterOne.format("%27s %42s %60s %25s\n", "Search Query", "Document Containing Term(df)", "Total Document(N)/number of documents term appeared(df)", "Log10(N/df)");

        for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();
            int logValue = totalDocuments / value;
            formatterOne.format("%27s %30s %45s %60s\n", key, value, totalDocuments + "/" + value, Math.log10(logValue));
        }

        System.out.println();
        System.out.println(formatterOne);

        Formatter formatter_two = new Formatter();
        formatter_two.format("%19s %50s\n", "Term", "weather");
        formatter_two.format("%46s %31s %30s\n", "Canada appeared in " + wordMap.get(ConfigConstants.WEATHER) + " documents", "Total Words (m)", "Frequency (f)");

        for (Map.Entry<Integer, Integer> entry : weatherFrequency.entrySet()) {
            Integer key = entry.getKey();
            Integer value = entry.getValue();

            formatter_two.format("%34s %35s %32s\n", "tweet#" + key, wordCount.get(key), value);
        }

        System.out.println();
        System.out.println(formatter_two);
        System.out.println();
        System.out.println("Tweet with maximum relative frequency is: " + tweetWithMaxRelativeFrequency);
        System.out.println("Highest relative frequency is: " + maxRelativeFrequency);
    }

    public static String getProcessedTweet(String tweet) {
        return tweet.replaceAll("<[^>]*>", "").replaceAll("[^\\u0000-\\u05C0\\u2100-\\u214F]+", "").replaceAll("http[s]?://[a-zA-Z0-9@./_-]+", "");
    }
}
