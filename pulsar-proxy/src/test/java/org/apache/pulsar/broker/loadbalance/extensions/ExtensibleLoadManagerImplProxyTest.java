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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.PortManager;
import org.apache.pulsar.proxy.server.ProxyTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ExtensibleLoadManagerImplProxyTest extends ProxyTest {

    private PulsarTestContext additionalTestContext;

    @Override
    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        super.setup();
        additionalTestContext = createAdditionalPulsarTestContext(configureExtensibleLoadManager(getDefaultConf()));
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        if (additionalTestContext != null) {
            closeTestContext(additionalTestContext);
        }
        super.cleanup();
    }

    @Override
    public void doInitConf() throws Exception {
        super.doInitConf();
        configureExtensibleLoadManager(conf);
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


    public void closeTestContext(PulsarTestContext pulsarTestContext) {
        PulsarService pulsarService = pulsarTestContext.getPulsarService();
        try {
            pulsarService.getConfiguration().setBrokerShutdownTimeoutMs(0L);
            pulsarTestContext.close();
            pulsarService.getConfiguration().getBrokerServicePort().ifPresent(PortManager::releaseLockedPort);
            pulsarService.getConfiguration().getWebServicePort().ifPresent(PortManager::releaseLockedPort);
            pulsarService.getConfiguration().getWebServicePortTls().ifPresent(PortManager::releaseLockedPort);
        } catch (Exception e) {
            log.warn("Failed to stop additional broker", e);
        }
    }

    @Test(timeOut = 30_000)
    public void testProxyProduceConsumer() throws Exception {
        var tenant = "public";
        var ns = "default";
        var namespaceName = NamespaceName.get(tenant, ns);
        var topicName = TopicName.get(TopicDomain.persistent.toString(), namespaceName, "topicA");

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl()).build();

        @Cleanup
        var producer = client.newProducer().topic(topicName.toString()).create();

        @Cleanup
        var consumer = client.newConsumer().topic(topicName.toString()).subscriptionName("my-subscription").subscribe();

        var bundleRange = admin.lookups().getBundleRange(topicName.toString());

        var lookupServiceSpy = spyLookupService(client);

        @Cleanup("shutdown")
        var threadPool = Executors.newCachedThreadPool();

        var cdl = new CountDownLatch(1);
        var semSend = new Semaphore(0);
        var messagesBeforeUnload = 100;
        var messagesAfterUnload = 100;

        var futures = new ArrayList<CompletableFuture<?>>();

        for (int i = 0; i < messagesBeforeUnload + messagesAfterUnload; i++) {
            final int id = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    semSend.acquire();
                    producer.send(("test" + id).getBytes());
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }, threadPool).orTimeout(15, TimeUnit.SECONDS));
        }

        futures.add(CompletableFuture.runAsync(() -> {
            try {
                cdl.await();
                var broker = admin.lookups().lookupTopic(topicName.toString());
                var dstBrokerUrl = Stream.of(pulsarTestContext, additionalTestContext).
                        filter(ptc -> !broker.equals(ptc.getPulsarService().getLookupServiceAddress())).
                        map(pulsarTestContext -> pulsarTestContext.getPulsarService().getLookupServiceAddress())
                        .findAny().get();
                semSend.release(messagesBeforeUnload);
                admin.namespaces().unloadNamespaceBundle(namespaceName.toString(), bundleRange, dstBrokerUrl);
                semSend.release(messagesAfterUnload);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, threadPool).orTimeout(15, TimeUnit.SECONDS));

        cdl.countDown();
        FutureUtil.waitForAllAndSupportCancel(futures).orTimeout(15, TimeUnit.SECONDS).get();

        assertTrue(futures.stream().allMatch(CompletableFuture::isDone));
        assertTrue(futures.stream().noneMatch(CompletableFuture::isCompletedExceptionally));

        verify(lookupServiceSpy, never()).getBroker(topicName);
    }

    private LookupService spyLookupService(PulsarClient client) throws IllegalAccessException {
        LookupService svc = (LookupService) FieldUtils.readDeclaredField(client, "lookup", true);
        var lookup = spy(svc);
        FieldUtils.writeDeclaredField(client, "lookup", lookup, true);
        return lookup;
    }
}
