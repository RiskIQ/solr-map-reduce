package org.apache.solr.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This partitioner will use Solr's {@link CompositeIdRouter} to ensure that each doc ends up in the proper shard.
 * @author Joe Linn
 * 02/07/2018
 */
public class SolrCloudCompositeIdRoutingPartitioner extends Partitioner<Text, SolrInputDocumentWritable> implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(SolrCloudCompositeIdRoutingPartitioner.class);

    public static final String SHARDS = SolrCloudPartitioner.class.getName() + ".shards";

    private final SolrParams emptySolrParams = new MapSolrParams(Collections.EMPTY_MAP);

    private Configuration conf;
    private int shards;
    private DocCollection docCollection;
    private Map<String, Integer> shardNumbers;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.shards = conf.getInt(SHARDS, -1);
        if (shards <= 0) {
            throw new IllegalArgumentException("Illegal shards: " + shards);
        }

        CompositeIdRouter router = new CompositeIdRouter();
        Map<String, Slice> slices = createSlices(router, shards);
        docCollection = new DocCollection("collection1", slices, new HashMap<>(), router);

        List<Slice> sortedSlices = getSortedSlices(docCollection.getSlices());
        if (sortedSlices.size() != shards) {
            throw new IllegalStateException("Incompatible sorted shards: + " + shards + " for docCollection: " + docCollection);
        }

        shardNumbers = new HashMap<>(10 * slices.size()); // sparse for performance
        for (int i = 0; i < slices.size(); i++) {
            shardNumbers.put(sortedSlices.get(i).getName(), i);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(Text key, SolrInputDocumentWritable value, int numPartitions) {
        DocRouter docRouter = docCollection.getRouter();
        SolrInputDocument doc = value.getSolrInputDocument();
        String keyStr = key.toString();

        // TODO: scalability: replace linear search in HashBasedRouter.hashToSlice() with binary search on sorted hash ranges
        Slice slice = docRouter.getTargetSlice(keyStr, doc, null, emptySolrParams, docCollection);

        if (slice == null) {
            throw new IllegalStateException("No matching slice found! The slice seems unavailable. docRouterClass: "
                    + docRouter.getClass().getName());
        }

        int rootShard = shardNumbers.get(slice.getName());
        if (rootShard < 0 || rootShard >= shards) {
            throw new IllegalStateException("Illegal shard number " + rootShard + " for slice: " + slice + ", docCollection: "
                    + docCollection);
        }

        // map doc to micro shard aka leaf shard, akin to HashBasedRouter.sliceHash()
        // taking into account mtree merge algorithm
        if (numPartitions % shards != 0) {
            // numPartitions == number of reducers
            throw new IllegalStateException("Number of partitions must be a multiple of number of shards. Partitions: " + numPartitions + " shards: " + shards);
        }
        int hashCode = Hash.murmurhash3_x86_32(keyStr, 0, keyStr.length(), 0);
        int offset = (hashCode & Integer.MAX_VALUE) % (numPartitions / shards);
        int microShard = (rootShard * (numPartitions / shards)) + offset;

        assert microShard >= 0 && microShard < numPartitions;
        return microShard;
    }


    public static List<Slice> getSortedSlices(Collection<Slice> slices) {
        List<Slice> sorted = new ArrayList<>(slices);
        Comparator<Object> c = new AlphaNumericComparator();
        sorted.sort((slice1, slice2) -> c.compare(slice1.getName(), slice2.getName()));
        return sorted;
    }


    public static Map<String, Slice> createSlices(CompositeIdRouter router, int numShards) {
        Map<String, Slice> slices = new HashMap<>(numShards);
        List<DocRouter.Range> ranges = router.partitionRange(numShards, new DocRouter.Range(Integer.MIN_VALUE, Integer.MAX_VALUE));
        for (int i = 0; i < numShards; i++) {
            String name = String.format("%05d", i);
            Map<String, Object> sliceProps = new HashMap<>(1);
            sliceProps.put(Slice.RANGE, ranges.get(i));
            slices.put(name, new Slice(name, new HashMap<>(), sliceProps));
        }
        return slices;
    }
}
