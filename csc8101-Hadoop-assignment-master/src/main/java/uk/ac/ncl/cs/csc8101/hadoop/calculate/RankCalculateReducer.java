package uk.ac.ncl.cs.csc8101.hadoop.calculate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * The `reduce(...)` method is called for each <key, (Iterable of values)> pair in the grouped input.
     * Output values must be of the same type as input values and Input keys must not be altered.
     *
     * Specifically, this method should take the iterable of links to a page, along with their pagerank and number of links.
     * It should then use these to increase this page's rank by its share of the linking page's:
     *      thisPagerank +=  linkingPagerank> / count(linkingPageLinks)
     *
     * Note: remember pagerank's dampening factor.
     *
     * Note: Remember that the pagerank calculation MapReduce job will run multiple times, as the pagerank will get
     * more accurate with each iteration. You should preserve each page's list of links.
     *
     * @param page The individual page whose rank we are trying to capture
     * @param values The Iterable of other pages which link to this page, along with their pagerank and number of links
     * @param context The Reducer context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	//page  initialPageRank  degree
    	double d = 0.85;
    	String temp = "";
    	double pageRankValue = 1 - d;
    	double secondPartValue = 0;
    	String links = "";
    	//use Iterator to decide which is the last element
    	
    	for(Text value: values) {
    		temp = value.toString();
    		if (!temp.startsWith("!")) {
        		String[] content = temp.split("\t");
    	    	secondPartValue = secondPartValue + Double.valueOf(content[0])/Integer.valueOf(content[1]);    				
    		} else {
    			links = temp.substring(1, temp.length());
    		}
    	} 
    	pageRankValue = pageRankValue + d * secondPartValue;
    	String message = String.valueOf(pageRankValue) + "\t" + links;
    	context.write(page, new Text(message));
    }
}
