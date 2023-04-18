import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class Combine {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();

        job.setJarByClass(Combine.class);
        job.setJobName("Combine");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CombineMapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, CombineMapper2.class);
        MultipleInputs.addInputPath(job,new Path(args[2]), TextInputFormat.class, CombineMapper3.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.setMapperClass(CombineMapper1.class);
        job.setMapperClass(CombineMapper2.class);
        job.setMapperClass(CombineMapper3.class);
        job.setReducerClass(CombineReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // 1 Reduce task
        job.addFileToClassPath(new Path("opencsv-5.7.1.jar"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
