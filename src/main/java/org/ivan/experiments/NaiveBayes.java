package org.ivan.experiments;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class NaiveBayes {
    private static final String MAILS = "Mails";

    public static void main(String[] args) throws Exception {
        try (Ignite client = IgniteStarter.startClient()) {
            IgniteCache<Integer, Mail> cache = client.getOrCreateCache(new CacheConfiguration<>(MAILS));
            cache.removeAll();

            // Load data
            try (IgniteDataStreamer<Integer, Mail> dataStreamer = client.dataStreamer(MAILS);
                 BufferedReader reader = Files.newBufferedReader(Paths.get("messages.txt"))) {
                int i = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty()) {
                        dataStreamer.addData(i++, parseMessage(line));
                    }
                }
            }

            IgniteCallable<MailStatistics> callable = new IgniteCallable<MailStatistics>() {
                @IgniteInstanceResource
                private Ignite localIgnite;

                @Override
                public MailStatistics call() throws Exception {
                    HashMap<String, Integer> spamMap = new HashMap<>();
                    HashMap<String, Integer> nonSpamMap = new HashMap<>();

                    IgniteCache<Integer, Mail> cache = localIgnite.cache(MAILS);
                    int spamMessageCount = 0;
                    int nonSpamMessageCount = 0;

                    for (Cache.Entry<Integer, Mail> entry : cache.localEntries()) {
                        Mail mail = entry.getValue();
                        for (String word : mail.text.split("\\s+")) {
                            if (mail.isSpam) {
                                spamMap.merge(word, 1, Integer::sum);

                                spamMessageCount++;
                            }

                            if (!mail.isSpam) {
                                nonSpamMap.merge(word, 1, Integer::sum);

                                nonSpamMessageCount++;
                            }
                        }
                    }

                    return new MailStatistics(spamMap, nonSpamMap, spamMessageCount, nonSpamMessageCount);
                }
            };

            Collection<MailStatistics> pieces = client.compute().broadcast(callable);

            System.out.println("Reduce " + pieces.size() + " pieces");

            HashMap<String, Integer> spamMap = new HashMap<>();
            HashMap<String, Integer> nonSpamMap = new HashMap<>();
            int wordsInSpam = 0;
            int wordsInNonSpam = 0;
            int spamMessagesCount = 0;
            int nonSpamMessagesCount = 0;
            int totalMessagesCount;

            for (MailStatistics piece : pieces) {
                spamMessagesCount += piece.spamMessageCount;
                nonSpamMessagesCount += piece.nonSpamMessageCount;

                Map<String, Integer> partialSpamMap = piece.spamWordCount;;

                for (Map.Entry<String, Integer> entry : partialSpamMap.entrySet()) {
                    String word = entry.getKey();
                    Integer count = entry.getValue();

                    spamMap.merge(word, count, Integer::sum);

                    wordsInSpam += count;
                }

                Map<String, Integer> partialNonSpamMap = piece.nonSpamWordCount;

                for (Map.Entry<String, Integer> entry : partialNonSpamMap.entrySet()) {
                    String word = entry.getKey();
                    Integer count = entry.getValue();

                    nonSpamMap.merge(word, count, Integer::sum);

                    wordsInNonSpam += count;
                }
            }

            totalMessagesCount = spamMessagesCount + nonSpamMessagesCount;
        }
    }

    private static Mail parseMessage(String line) {
        return new Mail("spam", true);
    }

    public static class Mail {
        final String text;
        final boolean isSpam;

        public Mail(String text, boolean isSpam) {
            this.text = text;
            this.isSpam = isSpam;
        }
    }

    public static class MailStatistics {
        final Map<String, Integer> spamWordCount;
        final Map<String, Integer> nonSpamWordCount;
        final int spamMessageCount;
        final int nonSpamMessageCount;

        public MailStatistics(Map<String, Integer> spamWordCount, Map<String, Integer> nonSpamWordCount, int spamMessageCount, int nonSpamMessageCount) {
            this.spamWordCount = spamWordCount;
            this.nonSpamWordCount = nonSpamWordCount;
            this.spamMessageCount = spamMessageCount;
            this.nonSpamMessageCount = nonSpamMessageCount;
        }
    }
}
