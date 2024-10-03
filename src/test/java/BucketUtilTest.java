import com.google.cloud.storage.Bucket;
import org.helper_utility.BucketUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BucketUtilTest {
    private static final String projectId = "gcp-data-engineering-426405";

    @Test
    public void BucketUtilsTest() throws Exception {
        String bucketName = projectId + "test_bucket_nyc";
        BucketUtil bucketUtil = new BucketUtil(projectId);
        bucketUtil.createBucket(bucketName);

        ArrayList<String> buckets = bucketUtil.listBuckets();
        assertTrue(
                buckets.contains(bucketName),
                "Bucket doesn't contain test_bucket");

        bucketUtil.deleteBucket(bucketName);
    }
}
