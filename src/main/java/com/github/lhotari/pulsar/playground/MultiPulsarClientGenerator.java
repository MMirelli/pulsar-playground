package com.github.lhotari.pulsar.playground;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.shade.io.netty.channel.EventLoopGroup;
import org.apache.pulsar.shade.io.netty.util.HashedWheelTimer;
import org.apache.pulsar.shade.io.netty.util.Timer;
import org.apache.pulsar.shade.io.netty.util.concurrent.DefaultThreadFactory;

@Slf4j
public class MultiPulsarClientGenerator {
    private static boolean quickExecution = true;
    private static final String PULSAR_HOST = System.getenv().getOrDefault("PULSAR_HOST",
            // deployed by the commmand:
            // helm install  pulsar-testenv-deployment datastax-pulsar/pulsar --namespace pulsar-testenv --create-namespace --values many-connections-values.yaml --set fullnameOverride=pulsar-testenv-deployment --debug --wait --timeout=10m
            "pulsar-testenv-deployment-proxy.pulsar-testenv.svc.cluster.local");
    private static final String PULSAR_SERVICE_URL =
            System.getenv().getOrDefault("PULSAR_SERVICE_URL", "http://" + PULSAR_HOST + ":8080/");
    private static final String PULSAR_BROKER_URL =
            System.getenv().getOrDefault("PULSAR_BROKER_URL", "pulsar://" + PULSAR_HOST + ":6650/");
    public static final String tenantName = "public";

    private static final int connectionsPerBroker = 100;
    // shared thread pool related resources
    private static Timer sharedTimer = new HashedWheelTimer(1, TimeUnit.MILLISECONDS);;

    private static int maxMessages = 100000;

    private int reportingInterval = maxMessages / 10;
    private static int messageSize = 20;

    private int producerPoolSize = 10;
    private int partitions = 3;
    private final int nThreads = 8;
    private final EventLoopGroup sharedEventLoopGroup = EventLoopUtil.newEventLoopGroup(nThreads, false,
            new DefaultThreadFactory("pulsar-client-io"));
    private ExecutorProvider externalExecutorProvider =
            new ExecutorProvider(nThreads, "shared-external-executor");
    private ExecutorProvider internalExecutorProvider =
            new ExecutorProvider(nThreads, "shared-internal-executor");

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
        NamespaceName namespaceName = NamespaceName.get(tenantName, namespace);
        String topicName = namespaceName.getPersistentTopicName("test-1");

        multiPulsarClientGenerator.createNamespaceAndTopic(namespaceName, topicName, quickExecution);

        // unsure creating a subscription beforehand is needed
        // Note: no subscription is needed beforehand as long as the retention_policy is set to -1 -1
//        PulsarClient pulsarClient = PulsarClient.builder()
//                .serviceUrl(PULSAR_BROKER_URL)
//                .build();
//        try (Consumer<byte[]> consumer = createConsumer(pulsarClient, topicName)) {
//             just to create the subscription
//        }

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(PULSAR_BROKER_URL);
        conf.setConnectionsPerBroker(connectionsPerBroker);

        multiPulsarClientGenerator.spawnProducerPool(topicName, conf);
        multiPulsarClientGenerator.spawnConsumerPool(topicName, conf);
    }

    private void spawnProducerPool(String topicName, ClientConfigurationData conf) throws Throwable {
        try {
            List<Producer<byte[]>> producerPool = new ArrayList<>();
            for (int i = 0; i < producerPoolSize; i++) {
                PulsarClient curPulsarClient = PulsarClientImpl.builder().conf(conf)
                        .internalExecutorProvider(internalExecutorProvider)
                        .externalExecutorProvider(externalExecutorProvider)
                        .timer(sharedTimer)
                        .eventLoopGroup(sharedEventLoopGroup)
                        .build();

                producerPool.add(curPulsarClient.newProducer()
                        .topic(topicName)
                        .create());
            }
            // example of creating a client which uses the shared thread pools
            for (Producer<byte[]> pulsarProducer : producerPool) {
                AtomicReference<Throwable> sendFailure = new AtomicReference<>();
                for (int i = 0; i < maxMessages; i++) {
                    // add a message to the topic
                    pulsarProducer.sendAsync(intToBytes(i, messageSize)).whenComplete((messageId, throwable) -> {
                        if (throwable != null) {
                            log.error("Failed to send message to topic {}", topicName, throwable);
                            sendFailure.set(throwable);
                        }
                        int messageIdInt = Integer.parseInt(messageId.toString().split(":")[messageId.toString().split(":").length-1]);
                        if (messageIdInt % reportingInterval == 0) {
                            log.info("Msg {} sent by producer {}", messageIdInt, pulsarProducer.getProducerName());
                        }
                    });
                    Throwable throwable = sendFailure.get();
                    if (throwable != null) {
                        throw throwable;
                    }
                }
                log.info("Flushing and closing producer {}", pulsarProducer.getProducerName());
                pulsarProducer.flushAsync();
                pulsarProducer.closeAsync();
            }
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    private static int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    private void spawnConsumerPool(String topicName,
                                   ClientConfigurationData conf) throws PulsarClientException {
        try (PulsarClientImpl client = PulsarClientImpl.builder().conf(conf)
                .internalExecutorProvider(internalExecutorProvider)
                .externalExecutorProvider(externalExecutorProvider)
                .timer(sharedTimer)
                .eventLoopGroup(sharedEventLoopGroup)
                .build();
        ){
            // example of creating a client which uses the shared thread pools
        int remainingMessages = maxMessages * producerPoolSize;
        try (Consumer<byte[]> consumer = createConsumer(client, topicName)) {

            for (int i = 0; i < maxMessages * producerPoolSize; i++) {
                Message<byte[]> msg = consumer.receive();
                int msgNum = bytesToInt(msg.getData());
                consumer.acknowledge(msg);
                --remainingMessages;
                if ((i + 1) % reportingInterval == 0) {
                    log.info("Received {} msgs", i + 1);
                    log.info("Received {} remaining: {}", msgNum, remainingMessages);
                }
            }
            consumer.close();
        }
        }

        log.info("Done receiving.");
    }

    private void createNamespaceAndTopic(NamespaceName namespaceName,
                                         String topicName,
                                         boolean quickExecution) throws PulsarClientException, PulsarAdminException {
        boolean recreateNamespaceAndTopic = ! quickExecution;
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(PULSAR_SERVICE_URL).build();
        try {
            if (recreateNamespaceAndTopic) {
                for (String namespace : pulsarAdmin.namespaces().getNamespaces(tenantName)) {
                    if (namespace.equals(namespaceName.toString())) {
                        for (String curTopic : pulsarAdmin.topics().getPartitionedTopicList(namespace)) {
                            if (curTopic.equals(topicName)) {
                                log.info("Deleting topic {}", curTopic);
                                pulsarAdmin.topics().deletePartitionedTopic(curTopic);
                            }
                        }
                        log.info("Deleting namespace {}", namespace);
                        pulsarAdmin.namespaces().deleteNamespace(namespaceName.toString());
                    }
                }
            }
            Policies policies = new Policies();
            // no retention
            policies.retention_policies = new RetentionPolicies(-1, -1);
            log.info("Creating new namespace {} with retention_policy:\n\t{}", namespaceName, policies.retention_policies);
            pulsarAdmin.namespaces().createNamespace(namespaceName.toString(), policies);
            pulsarAdmin.topics().createPartitionedTopic(topicName, this.partitions);
            log.info("Creating {}-partitioned topic {}", partitions, topicName);
        } catch (PulsarAdminException.ConflictException e) {
            // topic exists, ignore
            log.info("Namespace or Topic exists {}", topicName);
        } finally {
            pulsarAdmin.close();
        }
    }
}
