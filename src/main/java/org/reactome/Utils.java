package org.reactome;

import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @author Joel Weiser (joel.weiser@oicr.on.ca)
 * Created 1/13/2025
 */
public class Utils {

    public static void outputInstanceReportHeader(Path reportFilePath) throws IOException {
        String header = String.join("\t",
            "DB_ID",
            "Display Name",
            "Created date",
            "Created author",
            "Release date",
            "Release version",
            "Days between creation and release"
        ).concat(System.lineSeparator());
        Files.write(reportFilePath, header.getBytes(), StandardOpenOption.CREATE);
    }

    public static LocalDate getCreatedDate(GKInstance instance) throws Exception {
        String date = getCreatedDateAsString(instance);
        if (date.isEmpty()) {
            return null;
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
        return LocalDateTime.parse(date, formatter).toLocalDate();
    }

    public static String getCreatedAuthor(GKInstance instance) throws Exception {
        GKInstance createdInstance = (GKInstance) instance.getAttributeValue(ReactomeJavaConstants.created);
        if (createdInstance == null) {
            return "Unknown Author";
        }

        GKInstance authorInstance = (GKInstance) createdInstance.getAttributeValue(ReactomeJavaConstants.author);
        return authorInstance.getDisplayName();
    }

    public static LocalDate getReleaseDateForRLE(GKInstance reactionLikeEvent) {
        String date;
        try {
            date = (String) reactionLikeEvent.getAttributeValue(ReactomeJavaConstants.releaseDate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        return LocalDate.parse(date, formatter);
    }

    public static boolean allDatesAreEqual(List<LocalDate> dates) {
        if (dates == null || dates.isEmpty()) {
            return true;
        }

        LocalDate firstDate = dates.get(0);
        return dates.stream().allMatch(date -> date.equals(firstDate));
    }

    public static Integer getReleaseVersion(GKInstance instance) throws Exception {
        return ((MySQLAdaptor) instance.getDbAdaptor()).getReleaseNumber();
    }

    private static String getCreatedDateAsString(GKInstance instance) throws Exception {
        GKInstance createdInstance = (GKInstance) instance.getAttributeValue(ReactomeJavaConstants.created);
        if (createdInstance == null) {
            return "";
        }
        return (String) createdInstance.getAttributeValue(ReactomeJavaConstants.dateTime);
    }
}
