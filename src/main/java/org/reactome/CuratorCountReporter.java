package org.reactome;

import org.gk.model.GKInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.reactome.Utils.getCreatedAuthor;

/**
 * @author Joel Weiser (joel.weiser@oicr.on.ca)
 * Created 1/13/2025
 */
public class CuratorCountReporter {
    private String reportType;
    private Path outputFilePath;

    public CuratorCountReporter(String reportType, Path outputFilePath) {
        this.reportType = reportType;
        this.outputFilePath = outputFilePath;
    }

    public void report(List<GKInstance> instances) throws Exception {
        Files.deleteIfExists(getOutputFilePath());

        Map<String, Integer> curatorToInstanceCount = new HashMap<>();
        for (GKInstance instance : instances) {
            String author = getCreatedAuthor(instance);
            curatorToInstanceCount.put(author, curatorToInstanceCount.computeIfAbsent(author, k -> 1) + 1);
        }

        outputCuratorTallyHeader();
        for (Map.Entry<String, Integer> curatorEntry : curatorToInstanceCount.entrySet()) {
            reportCurator(curatorEntry);
        }
    }

    private void outputCuratorTallyHeader() throws IOException {
        String curatorTallyHeader = String.join("\t",
            "Curator Name",
            getReportType() + " Count"
        ).concat(System.lineSeparator());
        Files.write(getOutputFilePath(), curatorTallyHeader.getBytes(), StandardOpenOption.CREATE);
    }

    private void reportCurator(Map.Entry<String, Integer> curatorEntry) throws IOException {
        String curatorEntryString = curatorEntry.getKey() + "\t" + curatorEntry.getValue() + System.lineSeparator();
        Files.write(getOutputFilePath(), curatorEntryString.getBytes(), StandardOpenOption.APPEND);
    }

    private String getReportType() {
        return this.reportType;
    }

    private Path getOutputFilePath() {
        return this.outputFilePath;
    }

}
