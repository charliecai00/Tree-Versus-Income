import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;

public class CombineReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String avg = "";
        String health = "";
        String amount = "";

        for (Text value : values) {
            String[] pair = value.toString().split(",");
            String data = pair[0];
            String type = pair[1];

            if (type.contains("D")) {
                avg += data;
            } 
            else if (type.contains("H")) {
                health += data;
            }
            else {
                amount += data;
            }
        }
        
        String output = avg + "," + health + "," + amount;
        context.write(key, new Text(output));
        
        
    }

}

