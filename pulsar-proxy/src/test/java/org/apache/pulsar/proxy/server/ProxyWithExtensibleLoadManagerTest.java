/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.proxy.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@Slf4j
public class ProxyWithExtensibleLoadManagerTest extends MultiBrokerBaseTest {

    @Override
    public int numberOfAdditionalBrokers() {
        return 1;
    }

    @Override
    public void doInitConf() throws Exception {
        super.doInitConf();
        configureExtensibleLoadManager(conf);
    }

    @Override
    protected ServiceConfiguration createConfForAdditionalBroker(int additionalBrokerIndex) {
        return configureExtensibleLoadManager(getDefaultConf());
    }

    private ServiceConfiguration configureExtensibleLoadManager(ServiceConfiguration config) {
        config.setLoadBalancerInFlightServiceUnitStateWaitingTimeInMillis(5 * 1000);
        config.setLoadBalancerServiceUnitStateMonitorIntervalInSeconds(1);
        config.setForceDeleteNamespaceAllowed(true);
        config.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        config.setAllowAutoTopicCreation(true);
        config.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        config.setLoadBalancerSheddingEnabled(false);
        config.setLoadBalancerDebugModeEnabled(true);
        config.setTopicLevelPoliciesEnabled(true);
        return config;
    }

    private ProxyConfiguration initializeProxyConfig() {
        var proxyConfig = new ProxyConfiguration();
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        return proxyConfig;
    }

    private LookupService spyLookupService(PulsarClient client) throws IllegalAccessException {
        LookupService svc = (LookupService) FieldUtils.readDeclaredField(client, "lookup", true);
        var lookup = spy(svc);
        FieldUtils.writeDeclaredField(client, "lookup", lookup, true);
        return lookup;
    }

    private PulsarClientImpl createClient(ProxyService proxyService) {
        try {
            return Mockito.spy((PulsarClientImpl) PulsarClient.builder().
                    serviceUrl(proxyService.getServiceUrl()).
                    operationTimeout(1, TimeUnit.HOURS).
                    build());
        } catch (PulsarClientException e) {
            throw new CompletionException(e);
        }
    }

    @Test(timeOut = 3600_000, invocationCount = 100, skipFailedInvocations = true)
    public void testProxyProduceConsume() throws Exception {
        var proxyConfig = initializeProxyConfig();

        @Cleanup
        var proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper))).when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();
        proxyService.start();

        var timeoutMs = 15_000;
        var namespaceName = NamespaceName.get("public", "default");
        var topicName = TopicName.get(TopicDomain.persistent.toString(), namespaceName,
                BrokerTestUtil.newUniqueName("testProxyProduceConsume"));

        @Cleanup("shutdownNow")
        var threadPool = Executors.newCachedThreadPool();

        var producerClientFuture = CompletableFuture.supplyAsync(() -> createClient(proxyService), threadPool);
        var consumerClientFuture = CompletableFuture.supplyAsync(() -> createClient(proxyService), threadPool);

        @Cleanup
        var producerClient = producerClientFuture.get();

        @Cleanup
        var producer = producerClient.newProducer().topic(topicName.toString()).create();
        var producerLookupServiceSpy = spyLookupService(producerClient);

        @Cleanup
        var consumerClient = consumerClientFuture.get();

        @Cleanup
        var consumer = consumerClient.newConsumer().topic(topicName.toString()).
                subscriptionName(BrokerTestUtil.newUniqueName("my-sub")).subscribe();
        var consumerLookupServiceSpy = spyLookupService(consumerClient);


        var bundleRange = admin.lookups().getBundleRange(topicName.toString());

        var cdl = new CountDownLatch(1);
        var semSend = new Semaphore(0);
        var messagesBeforeUnload = 100;
        var messagesAfterUnload = 100;

        var pendingMessageIds = Collections.synchronizedSet(new HashSet<MessageId>());
        var producerFuture = CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < messagesBeforeUnload + messagesAfterUnload; i++) {
                    semSend.acquire();
                    var messageId = producer.send(("test" + i).getBytes());
                    pendingMessageIds.add(messageId);
                }
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, threadPool).orTimeout(timeoutMs, TimeUnit.MILLISECONDS);

        var consumerFuture = CompletableFuture.runAsync(() -> {
            while (!producerFuture.isDone() || !pendingMessageIds.isEmpty()) {
                try {
                    var recvMessage = consumer.receive(1_000, TimeUnit.MILLISECONDS);
                    if (recvMessage != null) {
                        var recvMessageId = recvMessage.getMessageId();
                        pendingMessageIds.remove(recvMessageId);
                        consumer.acknowledge(recvMessage);
                    }
                } catch (PulsarClientException e) {
                    // Retry
                }
            }
        }, threadPool).orTimeout(timeoutMs, TimeUnit.MILLISECONDS);

        var unloadFuture = CompletableFuture.runAsync(() -> {
            try {
                cdl.await();
                var srcBrokerUrl = admin.lookups().lookupTopic(topicName.toString());
                var dstBrokerUrl = getAllBrokers().stream().
                        filter(pulsarService -> !Objects.equals(srcBrokerUrl, pulsarService.getBrokerServiceUrl())).
                        map(PulsarService::getLookupServiceAddress).
                        findAny().get();
                semSend.release(messagesBeforeUnload);
                admin.namespaces().unloadNamespaceBundle(namespaceName.toString(), bundleRange, dstBrokerUrl);
                semSend.release(messagesAfterUnload);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, threadPool).orTimeout(timeoutMs, TimeUnit.MILLISECONDS);

        cdl.countDown();

        // Verify all futures completed successfully.
        unloadFuture.get();
        producerFuture.get();
        consumerFuture.get();

        verify(producerClient, times(1)).getProxiedConnection(any(), anyInt());
        verify(producerLookupServiceSpy, never()).getBroker(topicName);

        verify(consumerClient, times(1)).getProxiedConnection(any(), anyInt());
        verify(consumerLookupServiceSpy, never()).getBroker(topicName);
    }
}
