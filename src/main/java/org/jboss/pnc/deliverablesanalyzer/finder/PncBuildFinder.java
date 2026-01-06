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
package org.jboss.pnc.deliverablesanalyzer.finder;

import com.redhat.red.build.koji.model.xmlrpc.KojiArchiveInfo;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.pnc.api.enums.ArtifactQuality;
import org.jboss.pnc.build.finder.core.BuildSystemInteger;
import org.jboss.pnc.build.finder.core.Checksum;
import org.jboss.pnc.build.finder.koji.KojiBuild;
import org.jboss.pnc.build.finder.koji.KojiLocalArchive;
import org.jboss.pnc.build.finder.pnc.EnhancedArtifact;
import org.jboss.pnc.build.finder.pnc.PncBuild;
import org.jboss.pnc.build.finder.pnc.client.PncClient;
import org.jboss.pnc.client.RemoteResourceException;
import org.jboss.pnc.client.RemoteResourceNotFoundException;
import org.jboss.pnc.dto.Artifact;
import org.jboss.pnc.dto.Build;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.jboss.pnc.build.finder.core.AnsiUtils.red;

@ApplicationScoped
public class PncBuildFinder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PncBuildFinder.class);

    private static final String BANG_SLASH = "!/";
    private static final String BUILD_ZERO_KEY = "0";
    private static final long CONCURRENT_MAP_PARALLELISM_THRESHOLD = 10L;

    private PncClient pncClient;

    public List<Object> find() {
        // TODO Tomas: implement

        // 1. Iterate over all checksums (from Blocking Queue)
        // 2. Process all checksums as needed
        // 3. Find builds in PNC
        // 4. Process licenses & archives
        // 5. Clean up build 0
        // 6. Print logs if enabled
        // 7. Format & return result

        return null;
    }

    private void process() {
        /*
        // Using a bounded ArrayBlockingQueue is usually faster than Linked due to CPU cache locality
            BlockingQueue<String> queue = new ArrayBlockingQueue<>(10000);

            Runnable consumer = () -> {
                List<String> batch = new ArrayList<>(100);
                while (true) {
                    try {
                        // Take one (blocking if empty) to keep the thread alive
                        String first = queue.take();
                        batch.add(first);

                        // Then drain up to 99 more instantly without blocking repeatedly
                        queue.drainTo(batch, 99);

                        // Process the whole batch
                        for (String s : batch) {
                            process(s);
                        }
                        batch.clear();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            };
         */
    }

    private Object findPncBuilds(Map<Checksum, Collection<String>> checksumTable) {
        if (checksumTable == null || checksumTable.isEmpty()) {
            LOGGER.warn("PNC Checksum Table is empty");
            // return new emptyresult;
        }

        Set<EnhancedArtifact> artifacts = findPncArtifacts(checksumTable);
        ConcurrentHashMap<String, PncBuild> pncBuilds = groupArtifactsAsPncBuilds(artifacts);

        populatePncBuildsMetadata(pncBuilds);

        // TODO Tomas: Map to Result object
        return null;
    }

    private void populatePncBuildsMetadata(ConcurrentHashMap<String, PncBuild> pncBuilds) {
        pncBuilds.forEach(CONCURRENT_MAP_PARALLELISM_THRESHOLD, (buildId, pncBuild) -> {
            Build build = pncBuild.getBuild();

            if (!this.isBuildZero(build.getId())) {
                try {
                  if (build.getProductMilestone() != null) {
                      pncBuild.setProductVersion(pncClient.getProductVersion(build.getProductMilestone().getId()));
                  }
                } catch (RemoteResourceException e) {
                    // TODO Tomas: Handle exceptions
                }
            }
        });

        // TODO Tomas: if exception -> throw it
    }

    private ConcurrentHashMap<String, PncBuild> groupArtifactsAsPncBuilds(Set<EnhancedArtifact> artifacts) {
        ConcurrentHashMap<String, PncBuild> pncBuilds = new ConcurrentHashMap<>();
        Build buildZero = Build.builder().id(BUILD_ZERO_KEY).build();

        // TODO Tomas: Enhanced Artifact -- needed? Or change Build to contain artifacts?
        artifacts.forEach(artifact -> {
            Build build = artifact.getArtifact().isPresent() && artifact.getArtifact().get().getBuild() != null
                ? artifact.getArtifact().get().getBuild()
                : buildZero;
            // Build Zero covers 2 cases:
            // 1) An Artifact stored in PNC DB, which was not built in PNC
            // Such artifacts are treated the same way as artifacts not found in PNC
            // 2) Artifact was not found in PNC
            // Such artifacts will be associated in a build with ID 0

            PncBuild pncBuild = pncBuilds.computeIfAbsent(build.getId(), k -> new PncBuild(build));

            synchronized (pncBuild) {
                pncBuild.getBuiltArtifacts().add(artifact);
            }
        });

        return pncBuilds;
    }

    private Set<EnhancedArtifact> findPncArtifacts(Map<Checksum, Collection<String>> checksumTable) {
        final ConcurrentHashMap<Checksum, Collection<String>> concurrentChecksumTable = new ConcurrentHashMap<>(checksumTable);
        final Set<EnhancedArtifact> artifacts = ConcurrentHashMap.newKeySet();

        concurrentChecksumTable.forEach(CONCURRENT_MAP_PARALLELISM_THRESHOLD, (checksum, filenames) -> {
            try {
                final Artifact pncArtifact = findPncArtifact(checksum, filenames).orElse(null);
                artifacts.add(new EnhancedArtifact(pncArtifact, checksum, filenames));
            } catch (RemoteResourceException e) {
                // TODO Tomas: Handle exceptions
            }

            // TODO Tomas: Handle listeners
            // if (listener != null && enhancedArtifact.getArtifact().isPresent()) {
            //     listener.buildChecked(new BuildCheckedEvent(checksum, BuildSystem.pnc));
            // }
        });

        // TODO Tomas: if exception -> throw it

        return artifacts;
    }

    private Optional<Artifact> findPncArtifact(Checksum checksum, Collection<String> filenames) throws RemoteResourceException {
        if (isEmptyFileDigest(checksum)) {
            return Optional.empty();
        }

        if (isEmptyZipDigest(checksum)) {
            return Optional.empty();
        }

        Collection<Artifact> artifacts = getPncArtifactsByChecksum(checksum);

        if (artifacts == null || artifacts.isEmpty()) {
            return Optional.empty();
        }
        return getBestPncArtifact(artifacts);
    }

    private Collection<Artifact> getPncArtifactsByChecksum(Checksum checksum) throws RemoteResourceException {
        return switch (checksum.getType()) {
            case md5 -> pncClient.getArtifactsByMd5(checksum.getValue()).getAll();
            case sha1 -> pncClient.getArtifactsBySha1(checksum.getValue()).getAll();
            case sha256 -> pncClient.getArtifactsBySha256(checksum.getValue()).getAll();
        };
    }

    private Optional<Artifact> getBestPncArtifact(Collection<Artifact> artifacts) {
        if (artifacts == null || artifacts.isEmpty()) {
            throw new IllegalArgumentException("No artifacts provided!");
        }

        if (artifacts.size() == 1) {
            return Optional.of(artifacts.iterator().next());
        } else {
            return Optional.of(
                artifacts.stream()
                    .sorted(Comparator.comparing(this::getArtifactQuality).reversed())
                    .filter(artifact -> artifact.getBuild() != null)
                    .findFirst()
                    .orElse(artifacts.iterator().next()));
        }
    }


    // TODO Tomas: Utils methods
    private boolean isEmptyFileDigest(Checksum checksum) {
        return false;
    }
    private boolean isEmptyZipDigest(Checksum checksum) {
        return false;
    }
    public boolean isBuildZero(String id) {
        return BUILD_ZERO_KEY.equals(id);
    }

    private int getArtifactQuality(Artifact artifact) {
        ArtifactQuality artifactQuality = artifact.getArtifactQuality(); // TODO Tomas: Make sure the new enum is used

        return switch (artifactQuality) {
            case NEW -> 1;
            case VERIFIED -> 2;
            case TESTED -> 3;
            case DEPRECATED -> -1;
            case BLACKLISTED -> -2;
            case TEMPORARY -> -3;
            case DELETED -> -4;
            default -> {
                LOGGER.warn("Unsupported ArtifactQuality! Got: {}", red(artifactQuality));
                yield -100;
            }
        };
    }


    /**
     * Cleans up Build Zero by attempting to relocate its files
     * to their true parent archives in other builds.
     */
    private void cleanUpBuildZero(Map<BuildSystemInteger, KojiBuild> builds) {
        KojiBuild buildZero = builds.get(BUILD_ZERO_KEY);
        if (buildZero == null) return;

        // Remove archives if they become empty after processing
        buildZero.getArchives().removeIf(archive -> {
            // Remove filenames if they are successfully relocated to a nested parent
            archive.getFilenames().removeIf(filename -> processFileRelocation(filename, builds));
            return archive.getFilenames().isEmpty();
        });
    }

    /**
     * Attempts to find a parent for the given filename.
     * If found, adds the file to the parent's unmatched list.
     * * @return true if the file was relocated to a nested parent (containing "!/"),
     * indicating it should be removed from the original list.
     */
    private boolean processFileRelocation(String filename, Map<BuildSystemInteger, KojiBuild> allBuilds) {
        LOGGER.debug("Processing relocation for file: {}", filename);

        // 1. Find the parent archive string
        Optional<String> parentFilenameOpt = findParentFilename(filename, allBuilds);

        // 2. Perform the side effect (Relocation)
        if (parentFilenameOpt.isPresent()) {
            String parentFilename = parentFilenameOpt.get();

            // This method finds the archive again to update it.
            // (See note below on performance optimization)
            attachFileToParent(filename, parentFilename, allBuilds);

            // 3. Return condition for removal
            return parentFilename.contains(BANG_SLASH);
        }

        return false;
    }

    /**
     * Scans up the path (stripping "!/") to find if a parent exists.
     */
    private Optional<String> findParentFilename(String filename, Map<BuildSystemInteger, KojiBuild> builds) {
        String candidate = filename;

        while (true) {
            int index = candidate.lastIndexOf(BANG_SLASH);

            String finalCandidate = candidate;
            if (index == -1) {
                // We are at the root, check one last time then exit
                return findArchiveContaining(candidate, builds).map(a -> finalCandidate);
            }

            candidate = candidate.substring(0, index);
            LOGGER.debug("Checking potential parent: {}", candidate);

            if (findArchiveContaining(candidate, builds).isPresent()) {
                return Optional.of(candidate);
            }
            // Loop continues to strip the next level
        }
    }

    /**
     * Helper to find an archive containing a specific filename.
     */
    private Optional<KojiLocalArchive> findArchiveContaining(String filename, Map<BuildSystemInteger, KojiBuild> builds) {
        return builds.values().stream()
            .flatMap(build -> build.getArchives().stream())
            .filter(archive -> archive.getFilenames().contains(filename))
            .findFirst();
    }

    /**
     * Handles the side effect of updating the target archive.
     */
    private void attachFileToParent(String childFilename, String parentFilename, Map<BuildSystemInteger, KojiBuild> builds) {
        findArchiveContaining(parentFilename, builds).ifPresent(matchedArchive -> {
            matchedArchive.getUnmatchedFilenames().add(childFilename);

            KojiArchiveInfo info = matchedArchive.getArchive();
            LOGGER.debug("Archive {} ({}) matches orphan file {} (Source: {})",
                info.getArchiveId(), info.getFilename(), childFilename, matchedArchive.isBuiltFromSource());
        });
    }

    // TODO Tomas: Possible speedup?
    /*
    // Create a reverse index: Filename -> Archive containing it
    Map<String, KojiLocalArchive> fileIndex = new HashMap<>();
    for (KojiBuild b : allBuilds.values()) {
        for (KojiLocalArchive a : b.getArchives()) {
            for (String f : a.getFilenames()) {
                fileIndex.put(f, a);
            }
        }
    }
     */
}
