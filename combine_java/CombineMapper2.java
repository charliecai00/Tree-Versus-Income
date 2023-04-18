import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;
import java.io.StringReader;

public class CombineMapper2  extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] line = reader.readNext();
            String zip = line[0];
            String fair = line[1];
            String good = line[2];
            String poor = line[3];
            String output = fair + "," +  good + "," + poor;

            context.write(new Text(zip), new Text(output + ",H"));
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}