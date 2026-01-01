// Ideally classic hashing would be like server = hash(key) % n_servers but if we have a new server added/deleted
// then all the keys need to be remapped to different servers. Consistent hashing solves this problem by
// mapping both keys and servers to a circular hash space. When a server is added/removed, only a small fraction of keys need to be remapped.

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class ConsistentHashRing {

    public static final int TOTAL_HASH_SPACE = 360;
    public static final int TOTAL_SERVERS = 2;
    public static final int VIRTUAL_NODES = 2;
    public static Set<String> servers = new HashSet<>();
    public Map<Integer, String> serverPosMappings = new HashMap<>();

    public int getHash(String key) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        BigInteger bigInt = new BigInteger(1, md.digest(key.getBytes()));
        return bigInt.mod(BigInteger.valueOf(TOTAL_HASH_SPACE)).intValue() + 1;
    }

    public void addServer(String server) throws NoSuchAlgorithmException {
        if (servers.contains(server)) {
            System.out.println("Server already exists: " + server);
            return;
        }

        if (servers.size() >= TOTAL_SERVERS) {
            System.out.println("Cannot add more servers. Max limit reached.");
            return;
        }

        servers.add(server);
        for (int i = 1; i <= VIRTUAL_NODES; i++) {
            String virtualNode = server.concat(":").concat(String.valueOf(i));
            int nodePos = getHash(virtualNode);
            if (serverPosMappings.containsKey(nodePos)) {
                System.out.println("Collision detected for virtual node: " + virtualNode + " at position: " + nodePos);
                break;
            }
            serverPosMappings.put(nodePos, virtualNode);
            System.out.println("Added virtual node: " + virtualNode + " at position: " + nodePos);
        }
    }

    public void removeServer(String server) throws NoSuchAlgorithmException {
        if (!servers.contains(server)) {
            System.out.println("Server does not exist: " + server);
            return;
        }

        servers.remove(server);
        for (int i = 1; i <= VIRTUAL_NODES; i++) {
            String virtualNode = server.concat(":").concat(String.valueOf(i));
            int nodePos = getHash(virtualNode);
            serverPosMappings.remove(nodePos);
            System.out.println("Removed virtual node: " + virtualNode + " from position: " + nodePos);
        }
    }

    public String getServerForKey(String key) throws NoSuchAlgorithmException {
        int keyPos = getHash(key);
        Integer targetNodePos = null;

        List<Integer> sortedNodePositions = serverPosMappings.keySet().stream().sorted().collect(Collectors.toList());
        for (Integer nodePos : sortedNodePositions) {
            if (nodePos >= keyPos) {
                targetNodePos = nodePos;
                break;
            }
        }

        if (targetNodePos == null) {
            targetNodePos = sortedNodePositions.get(0);
        }
        System.out.println("Key: " + key + " (pos: " + keyPos + ") is mapped to node position: " + targetNodePos);
        return serverPosMappings.get(targetNodePos);
    }

}

class ConsistentHashing {

    public static void main(String[] args) throws NoSuchAlgorithmException {
        ConsistentHashRing consistentHashRing = new ConsistentHashRing();
        consistentHashRing.addServer("Server1");
        consistentHashRing.addServer("Server2");
        consistentHashRing.addServer("Server3");
        consistentHashRing.addServer("Server4");

        System.out.println("Server for key 'someKey':  " + consistentHashRing.getServerForKey("someKey"));

        consistentHashRing.removeServer("Server2");
        consistentHashRing.addServer("Server3");
        System.out.println("Server for key 'Apple':  " +  consistentHashRing.getServerForKey("Apple"));
        System.out.println("Server for key 'Banana':  " +  consistentHashRing.getServerForKey("Banana"));
    }

}