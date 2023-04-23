import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.*;

public class CleanReducer extends Reducer<Object, Text, NullWritable, Text> {

    private Text outValue = new Text();

    public void reduce(Object key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {

            FileWriter writer = new FileWriter("med_incom_cleaned.csv");

            List<String> list = new ArrayList<String>();
            for (Text value : values) {
                list.add(value.toString());
                writer.write(value.toString() + "\n");
            }

            String cleanedCsv = String.join("\n", list);
            outValue.set(cleanedCsv);
            context.write(NullWritable.get(), outValue);
    }

}
