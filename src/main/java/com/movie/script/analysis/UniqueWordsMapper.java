package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || !line.contains(":")) {
            return;  // Skip empty lines or invalid format
        }

        // Splitting character and dialogue (Assuming "CHARACTER_NAME: dialogue")
        String[] parts = line.split(":", 2);
        if (parts.length < 2) {
            return;
        }

        character.set(parts[0].trim());  // Extract character name
        String dialogue = parts[1].trim().toLowerCase();  // Convert dialogue to lowercase

        // Extract unique words
        HashSet<String> uniqueWords = new HashSet<>();
        StringTokenizer tokenizer = new StringTokenizer(dialogue, " .,!?\"'()[]{}:;");  // Tokenize words
        while (tokenizer.hasMoreTokens()) {
            uniqueWords.add(tokenizer.nextToken());
        }

        // Emit each unique word
        for (String uniqueWord : uniqueWords) {
            word.set(uniqueWord);
            context.write(character, word);
        }
    }
}