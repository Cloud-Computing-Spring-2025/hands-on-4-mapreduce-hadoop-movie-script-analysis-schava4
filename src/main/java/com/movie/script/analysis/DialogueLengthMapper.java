package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        // Ignore empty lines
        if (line.isEmpty()) {
            return;
        }

        // Split by first occurrence of ':' to separate character and dialogue
        int colonIndex = line.indexOf(":");
        if (colonIndex == -1) {
            return; // Ignore lines without a valid character name
        }

        String characterName = line.substring(0, colonIndex).trim();
        String dialogue = line.substring(colonIndex + 1).trim();

        // Count words in the dialogue
        StringTokenizer tokenizer = new StringTokenizer(dialogue);
        int count = tokenizer.countTokens();

        // Set values and write to context
        character.set(characterName);
        wordCount.set(count);
        context.write(character, wordCount);

    }
}