import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.opencsv.exceptions.CsvMalformedLineException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static IntWritable ONE = new IntWritable(1);
    private Text outKey = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVReader reader = new CSVReader(new StringReader(value.toString()));

        try {
            String[] record = reader.readNext();
            if (record != null) {
                context.write(new Text("Number of Records"), ONE);
            }
        } catch (CsvValidationException | CsvMalformedLineException e) {
            String errorMessage = e.getMessage();
            System.out.println(errorMessage);
        }
    }

}
