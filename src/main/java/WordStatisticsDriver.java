/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 9/11/14
 * Time: 1:23 AM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordStatisticsDriver {

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {

        //Create the configuration object which specifies the default configuration options.
        Configuration conf = new Configuration();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "WordStatisticsDriver");

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatisticsDriver.class);

        /**
         * Set the output key and value types for mapper and reducer.
         * Output Key: word
         * Output value:
         *      #email_in_which_word_has_occurred,
         *      word_count,
         *      square_word_count,
         *      Mean,
         *      Variance,
         */

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordStatisticsWritable.class);

        // Set the map, combiner and reduce classes.
        job.setMapperClass(WordStatisticsMapper.class);
        job.setCombinerClass(WordStatisticsCombiner.class);
        job.setReducerClass(WordStatisticsReducer.class);

        // Set the input and output file formats.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPath(job, new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

    }


}
