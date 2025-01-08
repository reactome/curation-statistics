package org.reactome;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.gk.model.ClassAttributeFollowingInstruction;
import org.gk.model.GKInstance;
import org.gk.model.InstanceUtilities;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Joel Weiser (joel.weiser@oicr.on.ca)
 * Created 1/6/2025
 */
public class Main {
    @Parameter(names ={"--user", "--u"}, required = true)
    private String userName;

    @Parameter(names ={"--password", "--p"}, required = true)
    private String password;

    @Parameter(names ={"--host", "--h"})
    private String host = "localhost";

    @Parameter(names ={"--currentDbName", "--cd"}, required = true)
    private String currentDatabaseName;

    @Parameter(names ={"--previousDbName", "--pd"}, required = true)
    private String previousDatabaseName;

    @Parameter(names ={"--port", "--P"})
    private int port = 3306;

    private Map<Long, List<GKInstance>> ewasDbIdToReactions;

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        JCommander.newBuilder()
            .addObject(main)
            .build()
            .parse(args);

        main.run();
    }

    public void run() throws Exception {
        MySQLAdaptor currentDba = getCurrentDba();
        MySQLAdaptor previousDba = getPreviousDba();

        this.ewasDbIdToReactions = fetchEWASDbIdToReactions(currentDba);

        reportRLEs(currentDba,previousDba);
        System.out.println();
        reportEWASs(currentDba, previousDba);
    }

    private void reportRLEs(MySQLAdaptor currentDba, MySQLAdaptor previousDba) throws Exception {
        outputHeader();
        List<GKInstance> newReactionLikeEvents = getNewRLEs(currentDba, previousDba);
        for (GKInstance newReactionLikeEvent : newReactionLikeEvents) {
            if (isManuallyCurated(newReactionLikeEvent)) {
                reportRLE(newReactionLikeEvent);
            }
        }
    }

    private void reportEWASs(MySQLAdaptor currentDba, MySQLAdaptor previousDba) throws Exception {
        outputHeader();
        List<GKInstance> newEWASs = getNewEWASs(currentDba, previousDba);
        for (GKInstance newEWAS : newEWASs) {
            reportEWAS(newEWAS);
        }
    }

    private Map<Long, List<GKInstance>> fetchEWASDbIdToReactions(MySQLAdaptor dba) throws Exception {
        Collection<GKInstance> reactionLikeEvents = dba.fetchInstancesByClass(ReactomeJavaConstants.ReactionlikeEvent);

        Map<Long, List<GKInstance>> ewasDbIdToReactions = new HashMap<>();
        for (GKInstance reactionLikeEvent : reactionLikeEvents) {
            String[] outClasses = new String[]{ReactomeJavaConstants.EntityWithAccessionedSequence};
            Set<GKInstance> ewass = InstanceUtilities.followInstanceAttributes(
                reactionLikeEvent, getClassAttributeInstructionsToFollow(), outClasses
            );

            for (GKInstance ewas : ewass) {
                ewasDbIdToReactions.computeIfAbsent(ewas.getDBID(), k -> new ArrayList<>()).add(reactionLikeEvent);
            }
        }
        return ewasDbIdToReactions;
    }

    private void outputHeader() {
        String header = String.join("\t",
            "DB_ID",
            "Created date",
            "Created author",
            "Release date",
            "Release version",
            "Days between creation and release"
        );
        System.out.println(header);
    }

    private boolean isManuallyCurated(GKInstance reactionLikeEvent) throws Exception {
        return reactionLikeEvent.getAttributeValue(ReactomeJavaConstants.inferredFrom) == null;
    }

    private void reportRLE(GKInstance reactionLikeEvent) throws Exception {
        String line = String.join("\t",
            reactionLikeEvent.getDBID().toString(),
            getCreatedDate(reactionLikeEvent).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
            getCreatedAuthor(reactionLikeEvent),
            getReleaseDate(reactionLikeEvent).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
            getReleaseVersion(reactionLikeEvent).toString(),
            getDaysBetweenCreationAndRelease(reactionLikeEvent).toString()
        );
        System.out.println(line);
    }

    private void reportEWAS(GKInstance ewas) throws Exception {
        String line = String.join("\t",
            ewas.getDBID().toString(),
            getCreatedDate(ewas).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
            getCreatedAuthor(ewas),
            getReleaseDateForEWAS(ewas) != null ?
                getReleaseDateForEWAS(ewas).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) :
                "N/A"
            ,
            getReleaseVersion(ewas).toString(),
            getDaysBetweenCreationAndReleaseForEWAS(ewas) != null ?
                getDaysBetweenCreationAndReleaseForEWAS(ewas).toString() :
                "N/A"
        );
        System.out.println(line);
    }

    private LocalDate getCreatedDate(GKInstance instance) throws Exception {
        String date = getCreatedDateAsString(instance);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
        return LocalDateTime.parse(date, formatter).toLocalDate();
    }

    private String getCreatedDateAsString(GKInstance instance) throws Exception {
        GKInstance createdInstance = (GKInstance) instance.getAttributeValue(ReactomeJavaConstants.created);
        return (String) createdInstance.getAttributeValue(ReactomeJavaConstants.dateTime);
    }

    private String getCreatedAuthor(GKInstance instance) throws Exception {
        GKInstance createdInstance = (GKInstance) instance.getAttributeValue(ReactomeJavaConstants.created);
        GKInstance authorInstance = (GKInstance) createdInstance.getAttributeValue(ReactomeJavaConstants.author);
        return authorInstance.getDisplayName();
    }

    private LocalDate getReleaseDate(GKInstance instance) {
        String date;
        try {
            date = (String) instance.getAttributeValue(ReactomeJavaConstants.releaseDate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        return LocalDate.parse(date, formatter);
    }

    private LocalDate getReleaseDateForEWAS(GKInstance ewas) throws Exception {
        List<GKInstance> reactionLikeEvents = getReactionsFromEWAS(ewas);

        if (reactionLikeEvents.isEmpty()) {
            System.err.println(ewas + " has no RLE");
            return null;
        }

        List<LocalDate> releaseDates = reactionLikeEvents.stream().map(this::getReleaseDate).collect(Collectors.toList());
        if (!allDatesAreEqual(releaseDates)) {
            System.err.println(ewas + " has multiple release dates");
        }
        return releaseDates.get(0);
    }

    private List<GKInstance> getReactionsFromEWAS(GKInstance ewas) {
        return getEWASDbIdToReactions().computeIfAbsent(ewas.getDBID(), k -> new ArrayList<>());
    }

    private Map<Long, List<GKInstance>> getEWASDbIdToReactions() {
        return this.ewasDbIdToReactions;
    }

    private List<ClassAttributeFollowingInstruction> getClassAttributeInstructionsToFollow() {
        List<ClassAttributeFollowingInstruction> classesToFollow = new ArrayList<>();

        Map<String, List<String>> classToAttributeMap = getClassToAttributeMap();
        for (String className : classToAttributeMap.keySet()) {
            List<String> classAttributes = classToAttributeMap.get(className);
            classesToFollow.add(getClassAttributeFollowingInstruction(className, classAttributes));
        }
        return classesToFollow;
    }

    private ClassAttributeFollowingInstruction getClassAttributeFollowingInstruction(
        String className, List<String> classAttributes
    ) {
        final List<String> reverseClassAttributes = Collections.emptyList();

        return new ClassAttributeFollowingInstruction(className, classAttributes, reverseClassAttributes);
    }

    private Map<String, List<String>> getClassToAttributeMap() {
        Map<String, List<String>> classToClassAttributesMapForObtainingProteins = new HashMap<>();

        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.ReactionlikeEvent,
            Arrays.asList(
                ReactomeJavaConstants.input,
                ReactomeJavaConstants.output,
                ReactomeJavaConstants.catalystActivity,
                ReactomeJavaConstants.regulatedBy
            )
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.Reaction,
            Arrays.asList(
                ReactomeJavaConstants.input,
                ReactomeJavaConstants.output,
                ReactomeJavaConstants.catalystActivity,
                ReactomeJavaConstants.regulatedBy
            )
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.Regulation,
            Arrays.asList(ReactomeJavaConstants.regulator)
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.CatalystActivity,
            Arrays.asList(ReactomeJavaConstants.physicalEntity)
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.Cell,
            Arrays.asList(ReactomeJavaConstants.RNAMarker, ReactomeJavaConstants.proteinMarker, "markerReference")
        );
        classToClassAttributesMapForObtainingProteins.put(
            "MarkerReference",
            Arrays.asList("marker")
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.Complex,
            Arrays.asList(ReactomeJavaConstants.hasComponent)
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.EntitySet,
            Arrays.asList(ReactomeJavaConstants.hasMember)
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.Polymer,
            Arrays.asList(ReactomeJavaConstants.repeatedUnit)
        );

        return classToClassAttributesMapForObtainingProteins;
    }

    private boolean allDatesAreEqual(List<LocalDate> dates) {
        if (dates == null || dates.isEmpty()) {
            return true;
        }

        LocalDate firstDate = dates.get(0);
        return dates.stream().allMatch(date -> date.equals(firstDate));
    }

    private Integer getReleaseVersion(GKInstance instance) throws Exception {
        return ((MySQLAdaptor) instance.getDbAdaptor()).getReleaseNumber();
    }

    private Long getDaysBetweenCreationAndRelease(GKInstance instance) throws Exception {
        LocalDate releaseDate = getReleaseDate(instance);
        LocalDate creationDate = getCreatedDate(instance);

        return ChronoUnit.DAYS.between(creationDate, releaseDate);
    }

    private Long getDaysBetweenCreationAndReleaseForEWAS(GKInstance ewas) throws Exception {
        LocalDate releaseDate = getReleaseDateForEWAS(ewas);
        LocalDate creationDate = getCreatedDate(ewas);

        if (releaseDate == null) {
            return null;
        }

        return ChronoUnit.DAYS.between(creationDate, releaseDate);
    }

    private List<GKInstance> getNewRLEs(MySQLAdaptor currentDba, MySQLAdaptor previousDba) throws Exception {
        return getNewInstances(currentDba, previousDba, ReactomeJavaConstants.ReactionlikeEvent);
    }

    private List<GKInstance> getNewEWASs(MySQLAdaptor currentDba, MySQLAdaptor previousDba) throws Exception {
        return getNewInstances(currentDba, previousDba, ReactomeJavaConstants.EntityWithAccessionedSequence);
    }

    private List<GKInstance> getNewInstances(MySQLAdaptor currentDba, MySQLAdaptor previousDba, String className)
        throws Exception {

        List<GKInstance> currentInstances = getInstances(currentDba, className);
        List<Long> previousInstanceDbIds = getInstanceDbIds(previousDba, className);

        List<GKInstance> newInstances = new ArrayList<>();
        for (GKInstance currentInstance : currentInstances) {
            if (!previousInstanceDbIds.contains(currentInstance.getDBID())) {
                newInstances.add(currentInstance);
            }
        }
        return newInstances;
    }

    private List<Long> getInstanceDbIds(MySQLAdaptor dba, String className) throws Exception {
        return getInstances(dba, className).stream().map(GKInstance::getDBID).collect(Collectors.toList());
    }

    private List<GKInstance> getInstances(MySQLAdaptor dba, String className) throws Exception {
        return new ArrayList<>(dba.fetchInstancesByClass(className));
    }

    private MySQLAdaptor getCurrentDba() throws SQLException {
        return getDba(this.currentDatabaseName);
    }

    private MySQLAdaptor getPreviousDba() throws SQLException {
        return getDba(this.previousDatabaseName);
    }

    private MySQLAdaptor getDba(String dbName) throws SQLException {
        return new MySQLAdaptor(
            this.host,
            dbName,
            this.userName,
            this.password,
            this.port
        );
    }
}