package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringJoiner;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueWordsSet = new HashSet<>();

        // Collect unique words
        for (Text word : values) {
            uniqueWordsSet.add(word.toString());
        }

        // Format unique words as a comma-separated string
        StringJoiner joiner = new StringJoiner(", ");
        for (String word : uniqueWordsSet) {
            joiner.add(word);
        }

        // Emit (Character, Unique_Words_List)
        context.write(key, new Text(joiner.toString()));
    }
}