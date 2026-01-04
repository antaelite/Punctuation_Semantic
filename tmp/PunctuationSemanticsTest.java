import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.research.punctuation.core.PunctuatedRecord;
import org.apache.flink.research.punctuation.operators.PunctuatedCountAggregator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;

public class PunctuationSemanticsTest {

    @Test
    public void testKeepInvariantPurgesState() throws Exception {
        // 1. Setup the Test Harness
        // Wraps your operator to run without a cluster
        PunctuatedCountAggregator processFunction = new PunctuatedCountAggregator();
        KeyedProcessOperator<Integer, PunctuatedRecord<Integer, Integer>, String> operator = 
            new KeyedProcessOperator<>(processFunction);

        // KeySelector matches the one used in your main job
        KeyedOneInputStreamOperatorTestHarness<Integer, PunctuatedRecord<Integer, Integer>, String> testHarness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                (record) -> record.isData()? record.getData() : record.getPunctuationKey(),
                Types.INT
            );

        testHarness.open();

        // 2. Inject Data (Pass Invariant Check)
        // Key 1: Data arrives
        testHarness.processElement(new PunctuatedRecord<>(1, 1000L), 1000L); 
        testHarness.processElement(new PunctuatedRecord<>(1, 2000L), 2000L);
        
        // Assert no output yet (because it's a blocking operator)
        Assert.assertTrue(testHarness.getOutput().isEmpty());

        // 3. Inject Punctuation (Keep Invariant Check)
        // Key 1: Punctuation arrives
        testHarness.processElement(new PunctuatedRecord<>(1, 3000L), 3000L);

        // 4. Verify Output
        // We expect: "RESULT: Key 1...", "PUNCTUATION: 1"
        Object result = testHarness.getOutput().poll();
        Assert.assertNotNull(result); 
        
        // 5. Verify State Purge (The "Faithfulness" Test)
        // This is the most critical check. The harness allows checking the number of active keys.
        // In a real harness, you verify the specific State descriptor is empty.
        Assert.assertEquals(0, testHarness.numKeyedStateEntries()); 
        
        testHarness.close();
    }
}