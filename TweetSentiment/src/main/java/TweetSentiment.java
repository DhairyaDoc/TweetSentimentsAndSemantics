import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import org.bson.Document;
import twitter4j.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

public class TweetSentiment {
    public static void main(String[] args) throws IOException {
        /* Establishing Connection With MongoDB */
        ConnectionString connectionString = new ConnectionString(ConfigConstants.MONGO_URI);
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
        MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        System.out.println("Mongo Database connected successfully!");

        /* Selecting Database In MongoDB */
        MongoDatabase mongoDatabase = mongoClient.getDatabase(ConfigConstants.DATABASE);
        System.out.println("Database selected!");

        String[] collections = new String[]{"cold", "flu", "immune", "mask", "vaccine"};

        File file = new File("src/main/java/Sentiment_Analysis_Report.txt");
        FileWriter fileWriter = new FileWriter(file);

        int tableIndex = 0, maxStringSize = 10;

        Formatter tableFormat = new Formatter();
        tableFormat.format("%10s %50s %50s %35s\n", "Index", "Tweet", "Match", "Polarity");
        fileWriter.write(String.format("%10s %50s %50s %35s\r\n", "Index", "Tweet", "Match", "Polarity"));

        for (int i = 0; i < collections.length; i++) {
            MongoCollection<Document> collection = mongoDatabase.getCollection(collections[i]);

            try (MongoCursor<Document> cursor = collection.find().iterator()) {
                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    JSONObject object = new JSONObject(document);
                    String tweet = getProcessedTweet(object.getString("full_text").trim());
                    HashMap<String, Integer> wordsBag = new HashMap<>();

                    String[] wordsInTweet = tweet.split(" ");

                    for (String word : wordsInTweet) {
                        if (wordsBag.containsKey(word)) {
                            wordsBag.put(word, wordsBag.get(word) + 1);
                        } else {
                            wordsBag.put(word, 1);
                        }
                    }

                    /* Getting Positive Words */
                    File positiveWordsFile = new File("src/main/java/positive-words.txt");
                    Path positiveFilePath = Paths.get(positiveWordsFile.getAbsolutePath());
                    List<String> positiveWordsList = Files.readAllLines(positiveFilePath);

                    /* Getting Negative Words */
                    File negativeWordsFile = new File("src/main/java/negative-words.txt");
                    Path negativeFilePath = Paths.get(negativeWordsFile.getAbsolutePath());
                    List<String> negativeWordsList = Files.readAllLines(negativeFilePath);

                    ArrayList<String> matchedWordsInTweet = new ArrayList<>();

                    int totalScore = 0;

                    for (String word : wordsInTweet) {
                        if (negativeWordsList.contains(word)) {
                            totalScore--;
                            if (!matchedWordsInTweet.contains(word)) {
                                matchedWordsInTweet.add(word);
                            }
                        } else if (positiveWordsList.contains(word)) {
                            totalScore++;
                            if (!matchedWordsInTweet.contains(word)) {
                                matchedWordsInTweet.add(word);
                            }
                        }
                    }
                    tableIndex++;
                    if (matchedWordsInTweet.size() > 0) {
                        String tweetData = tweet.length() > 10 ? tweet.substring(0, maxStringSize) + "..." : tweet;
                        tableFormat.format("%10s %50s %50s %35s\n",
                                tableIndex,
                                tweetData,
                                matchedWordsInTweet,
                                totalScore > 0 ? "Positive" : totalScore < 0 ? "Negative" : "Neutral");

                        fileWriter.write(String.format("%10s %50s %50s %35s\n",
                                tableIndex,
                                tweetData,
                                matchedWordsInTweet,
                                totalScore > 0 ? "Positive" : totalScore < 0 ? "Negative" : "Neutral"));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println(tableFormat);
    }

    public static String getProcessedTweet(String tweet) {
        return tweet.replaceAll("<[^>]*>", "").replaceAll("[^\\u0000-\\u05C0\\u2100-\\u214F]+", "").replaceAll("http[s]?://[a-zA-Z0-9@./_-]+", "");
    }
}
