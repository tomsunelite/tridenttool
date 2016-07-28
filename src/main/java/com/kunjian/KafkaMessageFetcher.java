package com.kunjian;

import com.google.common.base.Objects;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import org.json.simple.JSONValue;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by sunkunjian on 2016/7/27.
 */
public class KafkaMessageFetcher {
    private String _rootPath = ""; // kafka zk path
    private ZkHelper _zkHelper = null;
    public class Broker implements Serializable, Comparable<Broker> {
        public String host;
        public int port;

        // for kryo compatibility
        private Broker() {

        }

        public Broker(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public Broker(String host) {
            this(host, 9092);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(host, port);
        }

        @Override
        public int compareTo(Broker o) {
            if (this.host.equals(o.host)) {
                return this.port - o.port;
            } else {
                return this.host.compareTo(o.host);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final Broker other = (Broker) obj;
            return Objects.equal(this.host, other.host) && Objects.equal(this.port, other.port);
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }
    public KafkaMessageFetcher(String zkString,String rootPath) {
        _rootPath = rootPath;
        _zkHelper = ZkHelper.getInstance(zkString);
    }

    public ByteBufferMessageSet fetchMessages(String topic, int partitionId, long offset, int fetchSizeBytes) {
        int fetchMaxWait = 10000;
        String clientId = "";
        int leader = getLeaderFor(partitionId,topic);
        String path = brokerPath() + "/" + leader;
        byte[] brokerData = _zkHelper.getData(path);
        if(brokerData != null) {
            Broker hp = getBrokerHost(brokerData);
            if(hp != null) {
                FetchRequestBuilder builder = new FetchRequestBuilder();
                FetchRequest fetchRequest = builder.addFetch(topic, partitionId, offset, fetchSizeBytes).
                        clientId(clientId).maxWait(fetchMaxWait).build();
                FetchResponse fetchResponse = null;
                SimpleConsumer consumer = new SimpleConsumer(hp.host, hp.port, 10000, 1024 * 1024, clientId);
                try {
                    fetchResponse = consumer.fetch(fetchRequest);
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    consumer.close();
                }
                if (fetchResponse != null && fetchResponse.hasError()) {
                    KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic, partitionId));
                    if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE)) {
                        System.out.println("Got fetch request with offset out of range: [" + offset + "]; ");

                    } else {
                        String message = "Error fetching data from [" + partitionId + "] for topic [" + topic + "]: [" + error + "]";
                        System.out.println(message);
                    }
                } else if (fetchResponse != null) {
                    return fetchResponse.messageSet(topic, partitionId);
                }
            }
        }
        return null;
    }
    private String partitionPath(String topic) {
        return _rootPath + "/brokers/topics/" + topic + "/partitions";
    }

    private String brokerPath() {
        return _rootPath + "/brokers/ids";
    }

    private int getLeaderFor(long partition,String topic) {
        try {
            String topicBrokersPath = partitionPath(topic);
            byte[] hostPortData = _zkHelper.getData(topicBrokersPath + "/" + partition + "/state");
            Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(hostPortData, "UTF-8"));
            Integer leader = ((Number) value.get("leader")).intValue();
            if (leader == -1) {
                throw new RuntimeException("No leader found for partition " + partition);
            }
            return leader;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
//    private int getNumPartitions(String topic) {
//        try {
//            String topicBrokersPath = partitionPath(topic);
//            List<String> children = _zkHelper.getChildrens(topicBrokersPath);
//            return children.size();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
    private Broker getBrokerHost(byte[] contents) {
        try {
            Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(contents, "UTF-8"));
            String host = (String) value.get("host");
            Integer port = ((Long) value.get("port")).intValue();
            return new Broker(host, port);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close() {
        _zkHelper.releaseConnection();
    }
}
