package com.github.lhotari.pulsar.playground;

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.shade.io.netty.channel.EventLoopGroup;
import org.apache.pulsar.shade.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.pulsar.shade.io.netty.util.HashedWheelTimer;
import org.apache.pulsar.shade.io.netty.util.Timer;

public class MultiPulsarClientGenerator {

    private static final String PULSAR_HOST = System.getenv().getOrDefault("PULSAR_HOST",
            // deployed by the commmand:
            // helm install  pulsar-testenv-deployment datastax-pulsar/pulsar --namespace pulsar-testenv --create-namespace --values ~/dev/MMirelli/pulsar-helm-chart/examples/dev-values.yaml --set fullnameOverride=pulsar-testenv-deployment --debug --wait --timeout=10m
            "pulsar-testenv-deployment-broker.pulsar-testenv.svc.cluster.local");
    private static final String PULSAR_SERVICE_URL =
            System.getenv().getOrDefault("PULSAR_SERVICE_URL", "http://" + PULSAR_HOST + ":8080/");
    private static final String PULSAR_BROKER_URL =
            System.getenv().getOrDefault("PULSAR_BROKER_URL", "pulsar://" + PULSAR_HOST + ":6650/");

    public static void main(String[] args) throws Throwable{

        // setup namespace, tenant and topic
        String namespace = "default";
        NamespaceName namespaceName = NamespaceName.get("public", namespace);
        String topicName = namespaceName.getPersistentTopicName("test");

        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().build();

        System.out.println(pulsarAdmin.namespaces().getNamespaces("public"));

        // shared thread pool related resources
//        ExecutorProvider internalExecutorProvider = new ExecutorProvider(8, "shared-internal-executor");
//        ExecutorProvider externalExecutorProvider = new ExecutorProvider(8, "shared-external-executor");
//        Timer sharedTimer = new HashedWheelTimer(1, TimeUnit.MILLISECONDS);
//
//        EventLoopGroup sharedEventLoopGroup = new EpollEventLoopGroup();
//        ClientConfigurationData conf = new ClientConfigurationData();
//        conf.setServiceUrl(PULSAR_BROKER_URL);
//
//        try {
//            // example of creating a client which uses the shared thread pools
//            PulsarClientImpl client = PulsarClientImpl.builder().conf(conf)
//                    .internalExecutorProvider(internalExecutorProvider)
//                    .externalExecutorProvider(externalExecutorProvider)
//                    .timer(sharedTimer)
//                    .eventLoopGroup(sharedEventLoopGroup)
//                    .build();
//            Policies policies = new Policies();
//            client.newProducer()
//                    .topic(topicName)
//                    .;
//        } catch (PulsarClientException e) {
//            e.printStackTrace();
//        }
//    }

    }
}
