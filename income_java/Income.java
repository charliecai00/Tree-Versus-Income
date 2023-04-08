import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Income {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();

        job.setJarByClass(Income.class);
        job.setJobName("Income");

        FileInputFormat.addInputPath(job, new Path(args[0]), IncomeMapper1.class);
        FileInputFormat.addInputPath(job, new Path(args[1]), IncomeMapper2.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(IncomeMapper1.class);
        job.setMapperClass(IncomeMapper2.class);
        job.setReducerClass(IncomeReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // 1 Reduce task
        job.addFileToClassPath(new Path("opencsv-5.7.1.jar"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
