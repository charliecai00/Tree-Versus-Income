import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// This MapReduce program cleans the dataset and outputs the
// cleaned dataset in a CSV format.
// We only keep the records that have the following attributes:
// Household Type = All Households
// TimeFrame = 2015
// And we only keep the following columns:
// Location, Data
//
// @author: Stephen Zhang
// @version: 04/01/2023

public class Clean {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "clean");
        job.addFileToClassPath(new Path("opencsv-5.7.1.jar"));
        job.setJarByClass(Clean.class);
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(CleanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1); // 1 Reduce task
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
