package org.ivan.experiments;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class NaiveBayes {
    private static final String MAILS = "Mails";

    public static void main(String[] args) throws Exception {
        try (Ignite client = IgniteStarter.startClient()) {
            IgniteCache<Integer, Mail> cache = client.getOrCreateCache(new CacheConfiguration<>(MAILS));
            cache.removeAll();

            // Load data
            try (IgniteDataStreamer<Integer, Mail> dataStreamer = client.dataStreamer(MAILS);
                 BufferedReader reader = Files.newBufferedReader(Paths.get("iris.csv"))) {
                int i = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty()) {
                        dataStreamer.addData(i++, parseMessage(line));
                    }
                }
            }

            IgniteCallable<IgniteBiTuple<Map<String, Integer>, Map<String, Integer>>> callable = new IgniteCallable<IgniteBiTuple<Map<String, Integer>, Map<String, Integer>>>() {
                @IgniteInstanceResource
                private Ignite localIgnite;

                @Override
                public IgniteBiTuple<Map<String, Integer>, Map<String, Integer>> call() throws Exception {
                    HashMap<String, Integer> spamMap = new HashMap<>();
                    HashMap<String, Integer> nonSpamMap = new HashMap<>();

                    IgniteCache<Integer, Mail> cache = localIgnite.cache(MAILS);

                    for (Cache.Entry<Integer, Mail> entry : cache.localEntries()) {
                        Mail mail = entry.getValue();
                        for (String word : mail.text.split("\\s+")) {
                            HashSet<String> spamWords = new HashSet<>();
                            HashSet<String> nonSpamWords = new HashSet<>();

                            if (mail.isSpam && !spamWords.contains(word)) {
                                spamWords.add(word);

                                spamMap.merge(word, 1, Integer::sum);
                            }

                            if (!mail.isSpam && !nonSpamWords.contains(word)) {
                                nonSpamWords.add(word);

                                nonSpamMap.merge(word, 1, Integer::sum);
                            }
                        }
                    }

                    return new IgniteBiTuple<>(spamMap, nonSpamMap);
                }
            };

            Collection<IgniteBiTuple<Map<String, Integer>, Map<String, Integer>>> pieces = client.compute().broadcast(callable);

            System.out.println("Reduce " + pieces.size() + " pieces");

            HashMap<String, Integer> spamMap = new HashMap<>();
            HashMap<String, Integer> nonSpamMap = new HashMap<>();
            int spamSum = 0;
            int nonSpamSum = 0;

            for (IgniteBiTuple<Map<String, Integer>, Map<String, Integer>> piece : pieces) {
                Map<String, Integer> partialSpamMap = piece.get1();
                Map<String, Integer> partialNonSpamMap = piece.get2();

                for (Map.Entry<String, Integer> entry : partialSpamMap.entrySet()) {
                    String word = entry.getKey();
                    Integer documents = entry.getValue();

                    spamMap.merge(word, documents, Integer::sum);

                    spamSum += documents;
                }

                for (Map.Entry<String, Integer> entry : partialNonSpamMap.entrySet()) {
                    String word = entry.getKey();
                    Integer documents = entry.getValue();

                    nonSpamMap.merge(word, documents, Integer::sum);

                    nonSpamSum += documents;
                }
            }

            HashMap<String, Float> spamProbabilityMap = new HashMap<>();
            HashMap<String, Float> nonSpamProbabilityMap = new HashMap<>();
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
}
