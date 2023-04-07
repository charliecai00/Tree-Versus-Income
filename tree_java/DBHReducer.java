import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DBHReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
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

// from ArrayStack import ArrayStack;

// class queue:
//     def __init__():
//         self.stack1 = ArrayStack()
//         self.stack2 = ArrayStack()

//     def push(value):
//         self.stack1.push(value);

//     def pop():
//         number_of_pop = len(self.stack1) - 1
//         for i in range(number_of_pop):
//             self.stack2.push(self.stack1.pop())
//         res = self.stack1.pop()
//         for i in range(number_of_pop):
//             self.stack1.push(self.stack2.pop())
        
//         return res