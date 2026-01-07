/*
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.pnc.deliverablesanalyzer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MultiValuedMap;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.infinispan.commons.api.BasicCacheContainer;
import org.jboss.pnc.api.deliverablesanalyzer.dto.FinderResult;
import org.jboss.pnc.api.dto.exception.ReasonedException;
import org.jboss.pnc.api.enums.ResultStatus;
import org.jboss.pnc.build.finder.core.BuildConfig;
import org.jboss.pnc.build.finder.core.BuildFinder;
import org.jboss.pnc.build.finder.core.BuildFinderListener;
import org.jboss.pnc.build.finder.core.BuildSystemInteger;
import org.jboss.pnc.build.finder.core.ChecksumType;
import org.jboss.pnc.build.finder.core.DistributionAnalyzer;
import org.jboss.pnc.build.finder.core.DistributionAnalyzerListener;
import org.jboss.pnc.build.finder.core.LocalFile;
import org.jboss.pnc.build.finder.koji.ClientSession;
import org.jboss.pnc.build.finder.koji.KojiBuild;
import org.jboss.pnc.build.finder.pnc.client.PncClient;
import org.jboss.pnc.build.finder.pnc.client.PncClientImpl;
import org.jboss.pnc.deliverablesanalyzer.model.FinderResultCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.red.build.koji.KojiClientException;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Provider;

@ApplicationScoped
public class Finder {
    private static final Logger LOGGER = LoggerFactory.getLogger(Finder.class);

    private final Map<String, CancelWrapper> runningOperations = new ConcurrentHashMap<>();

    private BasicCacheContainer cacheManager;

    @Inject
    ManagedExecutor executor;

    @Inject
    BuildConfig config;

    @Inject
    Provider<BasicCacheContainer> cacheProvider;

    @Inject
    ClientSession kojiSession;

    @Inject
    Cleaner cleaner;

    @PostConstruct
    public void init() {
        if (Boolean.FALSE.equals(config.getDisableCache())) {
            cacheManager = cacheProvider.get();
            LOGGER.info("Initialized cache {}", cacheManager);
        } else {
            LOGGER.info("Cache disabled");
        }
    }

    public boolean cancel(String id) {
        CancelWrapper cancelWrapper = runningOperations.get(id);

        if (cancelWrapper != null) {
            cancelWrapper.cancel();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Executes analysis of the provided archives identified by URLs, which must be downloadable using "HTTP(S)". The
     * operation is executed synchronously, but the analysis itself runs several executors in parallel.
     *
     * @param id ID of the analysis
     * @param urls List of URLs
     * @param distributionAnalyzerListener A listener for events from DistributionAnalyzer
     * @param buildFinderListener A listener for events from Build Finder
     * @param config Configuration of the analysis
     * @return The results of the analysis, if the whole operation was successful, or the partially failed results
     *         otherwise
     */
    public List<FinderResult> find(
            String id,
            List<String> urls,
            DistributionAnalyzerListener distributionAnalyzerListener,
            BuildFinderListener buildFinderListener,
            BuildConfig config) {
        CancelWrapper cancelWrapper = new CancelWrapper();
        runningOperations.put(id, cancelWrapper);

        // Capture all the futures used in the find method. This is used for the cancel operation
        List<Future<FinderResult>> submittedTasks = urls.stream().map(url -> executor.submit(() -> {
            LOGGER.debug("Analysis of URL {} started.", url);

            try {
                FinderResult result = find(
                        id,
                        URI.create(url).normalize().toURL(),
                        distributionAnalyzerListener,
                        buildFinderListener,
                        config);

                LOGGER.debug("Analysis of URL {} finished.", url);

                return result;
            } catch (KojiClientException e) {
                throw new ReasonedException(ResultStatus.SYSTEM_ERROR, "Error in Build Finder", e);
            } catch (MalformedURLException e) {
                throw new ReasonedException(ResultStatus.FAILED, e.getMessage(), "Please check the URL.", e);
            }
        })).collect(Collectors.toList());
        List<Future<FinderResult>> allTasks = new ArrayList<>(submittedTasks);

        try {
            return awaitResults(submittedTasks, allTasks, cancelWrapper);
        } catch (CancellationException e) {
            LOGGER.debug("Analysis {} was cancelled", id, e);
            throw e;
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            LOGGER.debug("Analysis {} failed due to ", id, cause);
            throw new ReasonedException(
                    ResultStatus.SYSTEM_ERROR,
                    cause.getMessage() == null ? cause.toString() : cause.getMessage(),
                    cause);
        } finally {
            runningOperations.remove(id);
        }
    }

    private List<FinderResult> awaitResults(
            List<Future<FinderResult>> submittedTasks,
            List<Future<FinderResult>> allTasks,
            CancelWrapper cancelWrapper) throws CancellationException, ExecutionException {
        List<FinderResult> results = new ArrayList<>(submittedTasks.size());

        int total = submittedTasks.size();
        int done = 0;
        Iterator<Future<FinderResult>> it = submittedTasks.iterator();

        while (done < total) {
            while (it.hasNext()) {
                try {
                    Future<FinderResult> futureTask = it.next();

                    if (futureTask.isDone()) {
                        it.remove();
                        results.add(futureTask.get());
                        done++;
                    } else {
                        Thread.sleep(1000L); // FIXME
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("Sleeping while awaiting results was interrupted", e);
                }
            }

            it = submittedTasks.iterator();

            if (cancelWrapper.isCancelled()) {
                LOGGER.info("Cancelling all remaining tasks: {}", allTasks.size());
                // cancel all remaining running futures
                allTasks.stream().filter(future -> !future.isDone()).forEach(future -> future.cancel(true));
                LOGGER.info("All remaining tasks were cancelled");
                throw new CancellationException("Operation was cancelled manually");
            }
        }

        return results;
    }

    /**
     * @param id ID of the analysis
     * @param url url to analyze
     * @param distributionAnalyzerListener A listener for events from DistributionAnalyzer
     * @param buildFinderListener A listener for events from Build Finder
     * @param config Configuration of the analysis
     *
     * @return results of the analysis
     * @throws KojiClientException Thrown in case of exceptions with Koji communication
     */
    private FinderResult find(
            String id,
            URL url,
            DistributionAnalyzerListener distributionAnalyzerListener,
            BuildFinderListener buildFinderListener,
            BuildConfig config) throws KojiClientException {
        FinderResult result;

        List<String> files = Collections.singletonList(url.toExternalForm());

        LOGGER.info(
                "Starting distribution analysis for {} with config {} and cache manager {}",
                files,
                config,
                cacheManager != null ? cacheManager : "disabled");

        DistributionAnalyzer analyzer = new DistributionAnalyzer(files, config, cacheManager);
        analyzer.setListener(distributionAnalyzerListener);

        Map<ChecksumType, MultiValuedMap<String, LocalFile>> checksums;
        try {
            checksums = analyzer.call();
        } catch (FileSystemException e) {
            if (e.getCause() instanceof UnknownHostException) {
                throw new ReasonedException(ResultStatus.FAILED, "Invalid URL", "Please check the URL", e);
            } else {
                throw new ReasonedException(ResultStatus.SYSTEM_ERROR, "Failed to analyze checksums", e);
            }
        } catch (IOException e) {
            throw new ReasonedException(ResultStatus.SYSTEM_ERROR, "Failed to analyze checksums", e);
        }
        result = findBuilds(id, url, analyzer, checksums, buildFinderListener, config);

        LOGGER.info("Done finding builds for {}", url);
        return result;
    }

    /**
     *
     * @param id ID of the analysis
     * @param url url to analyze
     * @param analyzer DistributionAnalyzer object for checking checksum of files
     * @param checksums results of checksum values from analyzer
     * @param buildFinderListener A listener for events from Build Finder
     * @param forceConfig forced config for build finder
     * @return results of the analysis
     * @throws KojiClientException Thrown in case of exceptions with Koji communication
     */
    private FinderResult findBuilds(
            String id,
            URL url,
            DistributionAnalyzer analyzer,
            Map<ChecksumType, MultiValuedMap<String, LocalFile>> checksums,
            BuildFinderListener buildFinderListener,
            BuildConfig forceConfig) throws KojiClientException {
        BuildConfig usedConfig = forceConfig != null ? forceConfig : this.config;
        URL pncURL = usedConfig.getPncURL();

        try (PncClient pncClient = pncURL != null ? new PncClientImpl(usedConfig) : null) {
            BuildFinder buildFinder;

            if (pncClient == null) {
                LOGGER.warn("Initializing Build Finder with PNC support disabled because PNC URL is not set");
                buildFinder = new BuildFinder(kojiSession, usedConfig, analyzer, cacheManager);
            } else {
                LOGGER.info("Initializing Build Finder PNC client with URL {}", pncURL);
                buildFinder = new BuildFinder(kojiSession, usedConfig, analyzer, cacheManager, pncClient);
            }

            buildFinder.setListener(buildFinderListener);

            Map<BuildSystemInteger, KojiBuild> builds = buildFinder.call();

            if (LOGGER.isInfoEnabled()) {
                int size = builds.size();
                int numBuilds = size >= 1 ? size - 1 : 0;

                LOGGER.info("Got {} checksum types and {} builds", checksums.size(), numBuilds);
            }

            FinderResult result = FinderResultCreator.createFinderResult(id, url, builds);

            LOGGER.info("Returning result for {}", url);

            return result;
        }
    }

    private static final class CancelWrapper {
        private boolean cancelled = false;

        public void cancel() {
            this.cancelled = true;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }
}
