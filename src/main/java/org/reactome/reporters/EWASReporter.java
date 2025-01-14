package org.reactome.reporters;

import org.gk.model.GKInstance;
import org.reactome.EWASReactionFetcher;
import org.reactome.Utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static org.reactome.Utils.*;

/**
 * @author Joel Weiser (joel.weiser@oicr.on.ca)
 * Created 1/13/2025
 */
public class EWASReporter implements InstanceReporter {
    private Path outputFilePath;

    public EWASReporter(Path outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    @Override
    public void report(List<GKInstance> newEWASs) throws Exception {
        Files.deleteIfExists(getOutputFilePath());

        outputInstanceReportHeader(getOutputFilePath());
        for (GKInstance newEWAS : newEWASs) {
            reportEWAS(newEWAS);
        }
    }

    private void reportEWAS(GKInstance ewas) throws Exception {
        String line = String.join("\t",
            ewas.getDBID().toString(),
            ewas.getDisplayName(),
            getCreatedDate(ewas) != null ?
                getCreatedDate(ewas).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) :
                "N/A"
            ,
            getCreatedAuthor(ewas),
            getReleaseDateForEWAS(ewas) != null ?
                getReleaseDateForEWAS(ewas).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) :
                "N/A"
            ,
            getReleaseVersion(ewas).toString(),
            getDaysBetweenCreationAndReleaseForEWAS(ewas) != null ?
                getDaysBetweenCreationAndReleaseForEWAS(ewas).toString() :
                "N/A"
        ).concat(System.lineSeparator());
        Files.write(getOutputFilePath(), line.getBytes(), StandardOpenOption.APPEND);
    }

    private LocalDate getReleaseDateForEWAS(GKInstance ewas) throws Exception {
        List<GKInstance> reactionLikeEvents = EWASReactionFetcher.getInstance().getReactionsFromEWAS(ewas);

        if (reactionLikeEvents.isEmpty()) {
            System.err.println(ewas + " has no RLE");
            return null;
        }

        List<LocalDate> releaseDates = reactionLikeEvents.stream().map(Utils::getReleaseDateForRLE).collect(Collectors.toList());
        if (!allDatesAreEqual(releaseDates)) {
            return Collections.min(releaseDates);
        }
        return releaseDates.get(0);
    }

    private Long getDaysBetweenCreationAndReleaseForEWAS(GKInstance ewas) throws Exception {
        LocalDate creationDate = getCreatedDate(ewas);
        LocalDate releaseDate = getReleaseDateForEWAS(ewas);

        if (creationDate == null || releaseDate == null) {
            return null;
        }

        return ChronoUnit.DAYS.between(creationDate, releaseDate);
    }

    private Path getOutputFilePath() {
        return this.outputFilePath;
    }
}
