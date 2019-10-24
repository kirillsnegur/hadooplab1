import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import  java.util.StringTokenizer;


public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        StringTokenizer tokens = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z0-9\\s+]", "").toLowerCase());
        while (tokens.hasMoreTokens()) {
            word.set(tokens.nextToken());
            context.write(word,one);
        }
    }
}