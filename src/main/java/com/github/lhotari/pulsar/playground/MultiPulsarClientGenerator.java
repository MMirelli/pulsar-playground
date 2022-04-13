package com.github.lhotari.pulsar.playground;

import static org.apache.pulsar.shade.com.yahoo.sketches.Util.bytesToInt;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.shade.io.netty.util.HashedWheelTimer;
import org.apache.pulsar.shade.io.netty.util.Timer;

@Slf4j
public class MultiPulsarClientGenerator {
    private static final String PULSAR_HOST = System.getenv().getOrDefault("PULSAR_HOST",
            // deployed by the commmand:
            // helm install  pulsar-testenv-deployment datastax-pulsar/pulsar --namespace pulsar-testenv --create-namespace --values ~/dev/MMirelli/pulsar-helm-chart/examples/dev-values.yaml --set fullnameOverride=pulsar-testenv-deployment --debug --wait --timeout=10m
            "pulsar-testenv-deployment-proxy.pulsar-testenv.svc.cluster.local");
    private static final String PULSAR_SERVICE_URL =
            System.getenv().getOrDefault("PULSAR_SERVICE_URL", "http://" + PULSAR_HOST + ":8080/");
    private static final String PULSAR_BROKER_URL =
            System.getenv().getOrDefault("PULSAR_BROKER_URL", "pulsar://" + PULSAR_HOST + ":6650/");

    private ExecutorProvider externalExecutorProvider =
            new ExecutorProvider(8, "shared-external-executor");
    private ExecutorProvider internalExecutorProvider =
            new ExecutorProvider(8, "shared-internal-executor");

    // shared thread pool related resources
    private static Timer sharedTimer = new HashedWheelTimer(1, TimeUnit.MILLISECONDS);;

    private static int maxMessages = 10000;
    private int reportingInterval = maxMessages / 10;
    private static int messageSize = 20;

    private int partitions = 3;


    static byte[] intToBytes(final int i, int messageSize) {
        return ByteBuffer.allocate(Math.max(4, messageSize)).putInt(i).array();
    }

    private static Consumer<byte[]> createConsumer(PulsarClient pulsarClient, String topicName) throws PulsarClientException {
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Shared)
                .topic(topicName)
                .subscribe();
        return consumer;
    }

    public static void main(String[] args) throws Throwable{
        MultiPulsarClientGenerator multiPulsarClientGenerator = new MultiPulsarClientGenerator();

        // setup namespace, tenant and topic
        String namespace = "pulsar-test";
        NamespaceName namespaceName = NamespaceName.get("public", namespace);
        String topicName = namespaceName.getPersistentTopicName("test-1");

        multiPulsarClientGenerator.createNamespaceAndTopic(namespaceName, topicName);

        // unsure creating a subscription beforehand is needed
//        PulsarClient pulsarClient = PulsarClient.builder()
//                .serviceUrl(PULSAR_BROKER_URL)
//                .build();

//        try (Consumer<byte[]> consumer = createConsumer(pulsarClient, topicName)) {
//             just to create the subscription
//        }

//        This causes error:
//        at org.apache.pulsar.shade.io.netty.channel.epoll.Epoll.<clinit>(Epoll.java:40)
//	... 13 more
//        Caused by: java.lang.IllegalStateException: Only supported on Linux
//        EventLoopGroup sharedEventLoopGroup = new EpollEventLoopGroup();
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(PULSAR_BROKER_URL);

        multiPulsarClientGenerator.spawnProducerPool(topicName, conf);
        multiPulsarClientGenerator.spawnConsumerPool(topicName, conf);
    }

    private void spawnProducerPool(String topicName, ClientConfigurationData conf) throws Throwable {
        try {
            // example of creating a client which uses the shared thread pools
            PulsarClientImpl client = PulsarClientImpl.builder().conf(conf)
                    .internalExecutorProvider(internalExecutorProvider)
                    .externalExecutorProvider(externalExecutorProvider)
                    .timer(sharedTimer)
//                    .eventLoopGroup(sharedEventLoopGroup)
                    .build();
            Policies policies = new Policies();
            Producer<byte[]> producer = client.newProducer()
                    .topic(topicName)
                    .create();
            AtomicReference<Throwable> sendFailure = new AtomicReference<>();
            for (int i = 0; i < maxMessages; i++) {
                // add a message to the topic
                producer.sendAsync(intToBytes(i, messageSize)).whenComplete((messageId, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to send message to topic {}", topicName, throwable);
                        sendFailure.set(throwable);
                    }
                });
                if ((i + 1) % 1000 == 0) {
                    log.info("Sent {} msgs", i + 1);
                }
                Throwable throwable = sendFailure.get();
                if (throwable != null) {
                    throw throwable;
                }
            }
            log.info("Flushing");
            producer.flush();
            producer.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    private void spawnConsumerPool(String topicName,
                                   ClientConfigurationData conf) throws PulsarClientException {
        try (PulsarClientImpl client = PulsarClientImpl.builder().conf(conf)
                .internalExecutorProvider(internalExecutorProvider)
                .externalExecutorProvider(externalExecutorProvider)
                .timer(sharedTimer)
//                    .eventLoopGroup(sharedEventLoopGroup)
                .build();
        ){
            // example of creating a client which uses the shared thread pools
        int remainingMessages = maxMessages;
        try (Consumer<byte[]> consumer = createConsumer(client, topicName)) {
            for (int i = 0; i < maxMessages; i++) {
                Message<byte[]> msg = consumer.receive();
                int msgNum = bytesToInt(msg.getData());
                consumer.acknowledge(msg);
                if ((i + 1) % reportingInterval == 0) {
                    log.info("Received {} msgs", i + 1);
                    log.info("Received {} remaining: {}", msgNum, --remainingMessages);
                }
            }
            consumer.close();
        }
        }
        log.info("Done receiving.");
    }

    private void createNamespaceAndTopic(NamespaceName namespaceName, String topicName) throws PulsarClientException, PulsarAdminException {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(PULSAR_SERVICE_URL).build();
        try {
            Policies policies = new Policies();
            // no retention
            policies.retention_policies = new RetentionPolicies(0, 0);
            pulsarAdmin.namespaces().createNamespace(namespaceName.toString(), policies);
            pulsarAdmin.topics().createPartitionedTopic(topicName, this.partitions);
            log.info(String.format("Topic {} created", topicName));
        } catch (PulsarAdminException.ConflictException e) {
            // topic exists, ignore
            log.info("Namespace or Topic exists {}", topicName);
        } finally {
            pulsarAdmin.close();
        }
    }
}
