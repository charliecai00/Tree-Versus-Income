import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;

public class IncomeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> zipcode = new ArrayList<String>();
        ArrayList<String> amount = new ArrayList<String>();

        for (Text value : values) {
            String[] pair = value.toString().split(",");
            String data = pair[0];
            String type = pair[1];

            if (type.equals("Z")) {
                zipcode.add(data);
              } else {
                amount.add(data);
              }
        }

        for (String i : zipcode) {
            for (String j : amount) {
              context.write(new Text(i), new Text(j));
            }
        }

    }
}