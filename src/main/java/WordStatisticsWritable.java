import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: swetha
 * Date: 9/25/14
 * Time: 9:24 AM
 * To change this template use File | Settings | File Templates.
 */

/**
 * This is a Custom Writable Interface class that is used to arbitrarily read & write
 * 3 longs and 2 double variables
 *
 * Also a toString() method is written in order to output in the text format
 *
 */
public class WordStatisticsWritable implements Writable {

    //Class member variables
    private Long docCount;
    private Long wordCount;
    private Long sqWordCount;
    private Double mean;
    private Double variance;
    public static final String delimiter = ",";

    //Getters and Setters for every variable. Useful in accessing the private variables of the class
    public Long getDocCount() {
        return docCount;
    }

    public void setDocCount(Long docCount) {
        this.docCount = docCount;
    }

    public Long getWordCount() {
        return wordCount;
    }

    public void setWordCount(Long wordCount) {
        this.wordCount = wordCount;
    }

    public Long getSqWordCount() {
        return sqWordCount;
    }

    public void setSqWordCount(Long sqWordCount) {
        this.sqWordCount = sqWordCount;
    }

    public Double getMean() {
        return mean;
    }

    public void setMean(Double mean) {
        this.mean = mean;
    }

    public Double getVariance() {
        return variance;
    }

    public void setVariance(Double variance) {
        this.variance = variance;
    }

    //The default constructor of the class to initialize the variables
    public WordStatisticsWritable() {
        docCount = 0L;
        wordCount = 0L;
        sqWordCount = 0L;
        mean = 0.0;
        variance = 0.0;
    }

    //The write and readFields class that are overridden in order to suit the reads and writes corresponding
    //to the class variables and utility

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(docCount);
        out.writeLong(wordCount);
        out.writeLong(sqWordCount);
        out.writeDouble(mean);
        out.writeDouble(variance);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        docCount  = in.readLong();
        wordCount = in.readLong();
        sqWordCount = in.readLong();
        mean = in.readDouble();
        variance = in.readDouble();
    }

    // A toString() method that is called while printing out the output in the Text format.
    // Also ensures that the values are comma delimited/separated
    @Override
    public String toString() {
        String str = docCount.toString() + delimiter + wordCount.toString() + delimiter + sqWordCount.toString()
                + delimiter + mean.toString() + delimiter + variance.toString();
        return str;
    }

}
