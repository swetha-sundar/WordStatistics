import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 9/11/14
 * Time: 1:35 AM
 * To change this template use File | Settings | File Templates.
 */

/**
 * Combiner class used to compute the sums from the mappers so as to reduce the data transfers to reducers
 */

public class WordStatisticsCombiner extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

    private WordStatisticsWritable outputValue = new WordStatisticsWritable();

    @Override
    public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
            throws IOException, InterruptedException {

        Long tempDocCountSum = 0L;
        Long tempWordCountSum = 0L;
        Long tempSqWordCountSum = 0L;

        // Sum up the counts for the current word, specified in object "key".
        for (WordStatisticsWritable value : values) {
            tempDocCountSum += value.getDocCount();
            tempWordCountSum += value.getWordCount();
            tempSqWordCountSum += value.getSqWordCount();
        }

        //compute the mean as sum of word-counts divided by the number of emails in which it occurred
        double mean = ((double) tempWordCountSum/ tempDocCountSum);

        double squareOfMeans = mean * mean;
        double meanOfSquares = ((double) tempSqWordCountSum / tempDocCountSum);

        //compute variance as the difference between the mean of squares and square of means.
        double variance = meanOfSquares - squareOfMeans;

        outputValue.setDocCount(tempDocCountSum);
        outputValue.setWordCount(tempWordCountSum);
        outputValue.setSqWordCount(tempSqWordCountSum);
        outputValue.setMean(mean);
        outputValue.setVariance(variance);

        // Emit the word, the total number of emails in which it appeared, the number times it has appeared totally
        // in these emails, the square of the word count and the mean and the variance
        context.write(key, outputValue);
    }
}
