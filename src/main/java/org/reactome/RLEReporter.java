package org.reactome;

import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.reactome.Utils.*;

/**
 * @author Joel Weiser (joel.weiser@oicr.on.ca)
 * Created 1/13/2025
 */
public class RLEReporter {
    private Path outputFilePath;

    public RLEReporter(Path outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    public void report(List<GKInstance> newReactionLikeEvents) throws Exception {
        Files.deleteIfExists(getOutputFilePath());

        outputInstanceReportHeader(getOutputFilePath());
        for (GKInstance newReactionLikeEvent : newReactionLikeEvents) {
            if (isManuallyCurated(newReactionLikeEvent)) {
                reportRLE(newReactionLikeEvent);
            }
        }
    }

    private boolean isManuallyCurated(GKInstance reactionLikeEvent) throws Exception {
        return reactionLikeEvent.getAttributeValue(ReactomeJavaConstants.inferredFrom) == null;
    }

    private void reportRLE(GKInstance reactionLikeEvent) throws Exception {
        String line = String.join("\t",
            reactionLikeEvent.getDBID().toString(),
            reactionLikeEvent.getDisplayName(),
            getCreatedDate(reactionLikeEvent) != null ?
                getCreatedDate(reactionLikeEvent).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) :
                "N/A"
            ,
            getCreatedAuthor(reactionLikeEvent),
            getReleaseDateForRLE(reactionLikeEvent).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
            getReleaseVersion(reactionLikeEvent).toString(),
            getDaysBetweenCreationAndReleaseForRLE(reactionLikeEvent) != null ?
                getDaysBetweenCreationAndReleaseForRLE(reactionLikeEvent).toString() :
                "N/A"
        ).concat(System.lineSeparator());
        Files.write(getOutputFilePath(), line.getBytes(), StandardOpenOption.APPEND);
    }

    private Long getDaysBetweenCreationAndReleaseForRLE(GKInstance instance) throws Exception {
        LocalDate creationDate = getCreatedDate(instance);
        LocalDate releaseDate = getReleaseDateForRLE(instance);

        if (creationDate == null || releaseDate == null) {
            return null;
        }

        return ChronoUnit.DAYS.between(creationDate, releaseDate);
    }

    private Path getOutputFilePath() {
        return this.outputFilePath;
    }
}
