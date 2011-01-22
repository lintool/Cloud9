
package edu.umd.cloud9.example.translation;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
 
import junit.framework.TestCase;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mock.MockInputSplit;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import edu.umd.cloud9.io.PairOfStrings;

import edu.umd.cloud9.example.translation.TranslationProbability.*;
 
/**
 * Test cases for the inverted index mapper.
 */
public class TransProbMapperTest extends TestCase {
 
    private Mapper<LongWritable, Text, PairOfStrings, FloatWritable> mapper;
    private MapDriver<LongWritable, Text, PairOfStrings, FloatWritable> driver;
 
    /** We expect pathname@offset for the key from each of these */
    private final FloatWritable EXPECTED_COUNT = new FloatWritable(1);
 
    @Before
    public void setUp() {
        mapper = new TransProbMapper();
        driver = new MapDriver<LongWritable, Text, PairOfStrings, FloatWritable>
(mapper);
    }
 
    @Test
    public void testEmpty() {
        List<Pair<PairOfStrings, FloatWritable>> out = null;
 
        try {
            out = driver.withInput(new LongWritable(0), new Text("")).run();
        } catch (IOException ioe) {
            fail();
        }
 
        List<Pair<PairOfStrings, FloatWritable>> expected = new ArrayList<Pair<PairOfStrings, FloatWritable>>();
 
        assertListEquals(expected, out);
    }
 
    @Test
    public void testOneWord() {
        List<Pair<PairOfStrings, FloatWritable>> out = null;
 
        try {
            out = driver.withInput(new LongWritable(0), new Text("evil::mal")).run();
        } catch (IOException ioe) {
            fail();
        }
 
        List<Pair<PairOfStrings, FloatWritable>> expected = new ArrayList<Pair<PairOfStrings, FloatWritable>>();
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("evil", "mal"), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("evil", ""), EXPECTED_COUNT));
 
        assertListEquals(expected, out);
    }
 
    @Test
    public void testMultiWords() {
        List<Pair<PairOfStrings, FloatWritable>> out = null;
 
        try {
            out = driver.withInput(new LongWritable(0), new Text("banks::banques are::sont on::sur the::la bank::rive")).run();
        } catch (IOException ioe) {
            fail();
        }
 
        List<Pair<PairOfStrings, FloatWritable>> expected = new ArrayList<Pair<PairOfStrings, FloatWritable>>();
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("banks", "banques"), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("banks", ""), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("are", "sont"), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("are", ""), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("on", "sur"), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("on", ""), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("the", "la"), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("the", ""), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("bank", "rive"), EXPECTED_COUNT));
        expected.add(new Pair<PairOfStrings, FloatWritable>(new PairOfStrings("bank", ""), EXPECTED_COUNT));

        assertListEquals(expected, out);
    }
}