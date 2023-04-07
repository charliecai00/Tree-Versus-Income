import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;
import java.io.StringReader;

public class DBHMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try (CSVReader reader = new CSVReader(new StringReader(value.toString()));) {
            String[] line = reader.readNext();
            String zip = line[3];
            int dbh = Integer.parseInt(line[1]);
            context.write(new Text(zip), new IntWritable(dbh));
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
} 
