/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 9/11/14
 * Time: 1:24 AM
 * To change this template use File | Settings | File Templates.
 */
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WordStatisticsMapper extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

    //Class variables that is used to store the key and the text
    private Text word = new Text();
    private WordStatisticsWritable outputValue = new WordStatisticsWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //HashMap: a data structure to store the word and its frequencies(count of its occurrence) in the email
        HashMap<String, Long> wordFreq = new HashMap<String, Long>();

        //value: an entire email.
        String email = value.toString();

        //Tokenize the email into words.
        StringTokenizer tokenizer = new StringTokenizer(email);


        //For every word, count the number of times it has occurred in the email.
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (wordFreq.containsKey(token)) {
                long count = wordFreq.get(token);
                count++;
                wordFreq.put(token,count);
            }
            else {
                wordFreq.put(token,1L);
            }
        }

        //Iterate through the map and compute the square of frequency of every word
        //Note: Map has a list of unique words and its corresponding frequency.

        for (Map.Entry<String, Long> entry : wordFreq.entrySet()) {
            word = new Text(entry.getKey());
            long freq = entry.getValue();
            long freqSquare = freq * freq;

            //Pass on these values to the WordStatisticsWritable class's object
            outputValue.setDocCount(1L);
            outputValue.setWordCount(freq);
            outputValue.setSqWordCount(freqSquare);
            outputValue.setMean(0.0);
            outputValue.setVariance(0.0);

            //Output every unique word along with the above mentioned 5-valued output object
            context.write(word,outputValue);
        }

    }

}
