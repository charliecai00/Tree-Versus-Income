import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DBHReducer extends Reducer<Text, Text, Text, FloatWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int size = 0;

        for (Text value : values) {
            sum += Float.parseFloat(value.toString());
            size++;
        }

        float avg = sum / size;
        
        // Write the total count to reducer 
        context.write(key, new FloatWritable(avg));
    }
}
