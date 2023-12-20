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
package org.apache.pulsar.broker.loadbalance.extensions;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
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
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ExtensibleLoadManagerImplProxyTest extends MultiBrokerBaseTest {

    private ProxyService proxyService;

    @Override
    public int numberOfAdditionalBrokers() {
        return 1;
    }

    @Override
    public void doInitConf() throws Exception {
        super.doInitConf();
        configureExtensibleLoadManager(conf);
    }

    @BeforeClass(dependsOnMethods = "setup", alwaysRun = true)
    public void proxySetup() throws Exception {
        var proxyConfig = initializeProxyConfig();
        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper))).when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();
        proxyService.start();
        registerCloseable(proxyService);
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

    @Test(timeOut = 30_000)
    public void testProxyProduceConsume() throws Exception {
        var timeoutMs = 15_000;
        var namespaceName = NamespaceName.get("public", "default");
        var topicName = TopicName.get(TopicDomain.persistent.toString(), namespaceName, "testProxyProduceConsume");

        @Cleanup
        var producerClient =
                Mockito.spy((PulsarClientImpl) PulsarClient.builder().serviceUrl(proxyService.getServiceUrl()).build());

        @Cleanup
        var producer = producerClient.newProducer().topic(topicName.toString()).create();
        var producerLookupServiceSpy = spyLookupService(producerClient);

        @Cleanup
        var consumerClient =
                Mockito.spy((PulsarClientImpl) PulsarClient.builder().serviceUrl(proxyService.getServiceUrl()).build());
        @Cleanup
        var consumer = consumerClient.newConsumer().topic(topicName.toString()).subscriptionName("my-sub").subscribe();
        var consumerLookupServiceSpy = spyLookupService(consumerClient);

        @Cleanup("shutdown")
        var threadPool = Executors.newCachedThreadPool();

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

        var futures = List.of(producerFuture, consumerFuture, unloadFuture);
        FutureUtil.waitForAllAndSupportCancel(futures).get();
        assertTrue(futures.stream().allMatch(CompletableFuture::isDone));
        assertTrue(futures.stream().noneMatch(CompletableFuture::isCompletedExceptionally));

        verify(producerClient, times(1)).getProxiedConnection(any(), anyInt());
        verify(producerLookupServiceSpy, never()).getBroker(topicName);

        verify(consumerClient, times(1)).getProxiedConnection(any(), anyInt());
        verify(consumerLookupServiceSpy, never()).getBroker(topicName);
    }

    private LookupService spyLookupService(PulsarClient client) throws IllegalAccessException {
        LookupService svc = (LookupService) FieldUtils.readDeclaredField(client, "lookup", true);
        var lookup = spy(svc);
        FieldUtils.writeDeclaredField(client, "lookup", lookup, true);
        return lookup;
    }
}
