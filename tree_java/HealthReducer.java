import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HealthReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int fair = 0;
        int good = 0;
        int poor = 0;
        
        for (Text value : values) {
            if (value.toString().equals("Fair")) {
                fair += 1;
            }
            if (value.toString().equals("Good")) {
                good += 1;
            }
            if (value.toString().equals("Poor")) {
                poor += 1;
            }
        }

        String res = String.format("Fair: %d, Good: %d, Poor: %d", fair, good, poor);
        
        // Write the total count to reducer
        context.write(key, new Text(res));
    }
}
