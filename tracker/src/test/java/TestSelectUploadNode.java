import com.jay.oss.common.registry.StorageNodeInfo;
import com.jay.oss.tracker.registry.StorageRegistry;
import com.jay.oss.tracker.track.ConsistentHashRing;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * <p>
 *  上传点选择单元测试
 * </p>
 *
 * @author Jay
 * @date 2022/02/24 14:03
 */
public class TestSelectUploadNode {

    /**
     * 测试一致性哈希选择主上传点
     * @throws Exception e
     */
    @Test
    public void testSelectUploadNode() throws Exception {
        ConsistentHashRing ring = new ConsistentHashRing();
        StorageRegistry storageRegistry = new StorageRegistry(ring);
        StorageNodeInfo node1 = StorageNodeInfo.builder().url("192.168.154.128:9999").space(10240).available(true).build();
        StorageNodeInfo node2 = StorageNodeInfo.builder().url("192.168.154.129:9999").space(10240).available(true).build();
        StorageNodeInfo node3 = StorageNodeInfo.builder().url("192.168.154.130:9999").space(10240).available(true).build();
        StorageNodeInfo node4 = StorageNodeInfo.builder().url("192.168.154.131:9999").space(10240).available(true).build();
        storageRegistry.addStorageNode(node1);
        storageRegistry.addStorageNode(node2);
        storageRegistry.addStorageNode(node3);
        storageRegistry.addStorageNode(node4);

        List<StorageNodeInfo> nodes = storageRegistry.selectUploadNode("object-1", 1024, 3);
        System.out.println(nodes);
        Assert.assertFalse(nodes == null || nodes.size() < 3);
    }

    /**
     * 测试容量平衡的节点选择
     * @throws Exception e
     */
    @Test
    public void testSpaceBalancedSelector() throws Exception {
        ConsistentHashRing ring = new ConsistentHashRing();
        StorageRegistry storageRegistry = new StorageRegistry(ring);
        StorageNodeInfo node1 = StorageNodeInfo.builder().url("192.168.154.128:9999").space(2048).available(true).build();
        StorageNodeInfo node2 = StorageNodeInfo.builder().url("192.168.154.129:9999").space(10240).available(true).build();
        StorageNodeInfo node3 = StorageNodeInfo.builder().url("192.168.154.130:9999").space(1024).available(true).build();
        StorageNodeInfo node4 = StorageNodeInfo.builder().url("192.168.154.131:9999").space(20240).available(true).build();
        storageRegistry.addStorageNode(node1);
        storageRegistry.addStorageNode(node2);
        storageRegistry.addStorageNode(node3);
        storageRegistry.addStorageNode(node4);

        List<StorageNodeInfo> nodes = storageRegistry.selectUploadNode("object-1", 1024, 3);
        Assert.assertEquals(node4.getSpace(), nodes.get(1).getSpace());
    }
}
