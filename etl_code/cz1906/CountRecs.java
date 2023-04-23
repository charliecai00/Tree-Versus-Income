import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// This MapReduce program outputs the number of records in the
// target dataset.
//
// @author: Stephen Zhang
// @version: 04/01/2023

public class CountRecs {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count records");
        job.addFileToClassPath(new Path("opencsv-5.7.1.jar"));
        job.setJarByClass(CountRecs.class);
        job.setMapperClass(CountRecsMapper.class);
        job.setCombinerClass(CountRecsReducer.class);
        job.setReducerClass(CountRecsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1); // 1 Reduce task
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}
