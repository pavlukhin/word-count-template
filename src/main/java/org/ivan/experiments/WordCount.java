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

public class WordCount {
    private static final String TEXTS = "Texts";

    public static void main(String[] args) throws Exception {
        try (Ignite client = IgniteStarter.startClient()) {
            IgniteCache<Integer, String> cache = client.getOrCreateCache(new CacheConfiguration<>(TEXTS));
            cache.removeAll();

            // Load data
            try (IgniteDataStreamer<Object, Object> dataStreamer = client.dataStreamer(TEXTS);
                 BufferedReader reader = Files.newBufferedReader(Paths.get("alice-in-wonderland.txt"))) {
                int i = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty()) {
                        dataStreamer.addData(i++, line.toLowerCase());
                    }
                }
            }

            //  Count words

            // For run on each node
            IgniteCallable<Map<String, Integer>> callable = new IgniteCallable<Map<String, Integer>>() {
                @IgniteInstanceResource
                private Ignite localIgnite;

                @Override
                public Map<String, Integer> call() throws Exception {
                    HashMap<String, Integer> wordCount = new HashMap<>();

                    IgniteCache<Integer, String> cache = localIgnite.cache(TEXTS);
                    for (Cache.Entry<Integer, String> entry : cache.localEntries()) {
                        String line = entry.getValue();
                        for (String word : line.split("\\s+")) {
                            if (wordCount.containsKey(word)) {
                                Integer previousCount = wordCount.get(word);
                                wordCount.put(word, previousCount + 1);
                            }
                            else {
                                wordCount.put(word, 1);
                            }
                        }
                    }

                    return wordCount;
                }
            };

            Collection<Map<String, Integer>> pieces = client.compute().broadcast(callable);

            HashMap<String, Integer> wordCount = new HashMap<>();

            System.out.println("Reduce " + pieces.size() + " pieces");
            // Aggregate results from multiple nodes
            for (Map<String, Integer> piece : pieces) {
                for (Map.Entry<String, Integer> entry : piece.entrySet()) {
                    String word = entry.getKey();
                    Integer count = entry.getValue();

                    if (wordCount.containsKey(word)) {
                        Integer previousCount = wordCount.get(word);
                        wordCount.put(word, previousCount + count);
                    }
                    else {
                        wordCount.put(word, count);
                    }
                }
            }

            System.out.println("Number of word Alice: " + wordCount.get("alice"));
        }
    }
}
