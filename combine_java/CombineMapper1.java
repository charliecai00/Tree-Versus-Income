import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;
import java.io.StringReader;

public class CombineMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] line = reader.readNext();
            String zip = line[0];
            String avg = line[1];

            context.write(new Text(zip), new Text(avg + ",D"));
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}