import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Health {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();

        job.setJarByClass(Health.class);
        job.setJobName("Health");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(HealthMapper.class);
        job.setReducerClass(HealthReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addFileToClassPath(new Path("opencsv-5.7.1.jar"));
	job.setNumReduceTasks(1); // 1 Reduce task

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
