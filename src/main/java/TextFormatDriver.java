import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Rajiv on 4/7/17.
 */
public class TextFormatDriver {

    public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException{

        if(args.length!=2){
            System.out.println("Not enough arguments");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        conf.set("textinputformat.record.delimiter", "$");

        Job job = new Job(conf);

        job.setJarByClass(TextFormatDriver.class);
        job.setJobName("Custom Delimiter Text Input format");

        job.setMapperClass(TextFormatMapper.class);
        job.setReducerClass(TextFormatReducer.class);

        // Define Mapper output classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(args[1])))
            hdfs.delete(new Path(args[1]), true);


        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }


}
