import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Rajiv on 4/7/17.
 */
public class TextFormatMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String formattedStr = value.toString().replace("\n", "");
        context.write(new Text(formattedStr), NullWritable.get());

    }

}
