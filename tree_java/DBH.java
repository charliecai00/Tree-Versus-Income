import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DBH {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();

        job.setJarByClass(DBH.class);
        job.setJobName("DBH");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DBHMapper.class);
        job.setReducerClass(DBHReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1); // 1 Reduce task
        job.addFileToClassPath(new Path("opencsv-5.7.1.jar"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
