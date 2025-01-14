package org.reactome;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
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

    private MySQLAdaptor currentDba;
    private MySQLAdaptor previousDba;

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        JCommander.newBuilder()
            .addObject(main)
            .build()
            .parse(args);

        main.run();
    }

    public void run() throws Exception {
        this.currentDba = createCurrentDba();
        this.previousDba = createPreviousDba();

        List<GKInstance> newReactionLikeEvents = getNewRLEs();

        RLEReporter rleReporter = new RLEReporter(getRLEReportPath());
        rleReporter.report(newReactionLikeEvents);

        CuratorCountReporter rleCuratorCount = new CuratorCountReporter("RLE", getCurrentReleaseNumber());
        rleCuratorCount.report(newReactionLikeEvents);

        List<GKInstance> newEWASs = getNewEWASs();

        EWASReporter ewasReporter = new EWASReporter(getEWASReportPath());
        ewasReporter.report(newEWASs);

        CuratorCountReporter ewasCuratorCount = new CuratorCountReporter("EWAS", getCurrentReleaseNumber());
        ewasCuratorCount.report(newEWASs);
    }

    private List<GKInstance> getNewRLEs() throws Exception {
        return getNewInstances(ReactomeJavaConstants.ReactionlikeEvent);
    }

    private List<GKInstance> getNewEWASs() throws Exception {
        return getNewInstances(ReactomeJavaConstants.EntityWithAccessionedSequence);
    }

    private List<GKInstance> getNewInstances(String className)
        throws Exception {

        List<GKInstance> currentInstances = getInstances(getCurrentDba(), className);
        List<Long> previousInstanceDbIds = getInstanceDbIds(getPreviousDba(), className);

        List<GKInstance> newInstances = new ArrayList<>();
        for (GKInstance currentInstance : currentInstances) {
            if (!previousInstanceDbIds.contains(currentInstance.getDBID())) {
                newInstances.add(currentInstance);
            }
        }
        return newInstances;
    }

    private Path getRLEReportPath() throws Exception {
        return Paths.get("NewRLEsV" + getCurrentDba().getReleaseNumber() + ".txt");
    }

    private Path getEWASReportPath() throws Exception {
        return Paths.get("NewEWASsV" + getCurrentDba().getReleaseNumber() + ".txt");
    }

    private List<Long> getInstanceDbIds(MySQLAdaptor dba, String className) throws Exception {
        return getInstances(dba, className).stream().map(GKInstance::getDBID).collect(Collectors.toList());
    }

    private List<GKInstance> getInstances(MySQLAdaptor dba, String className) throws Exception {
        return new ArrayList<>(dba.fetchInstancesByClass(className));
    }

    private int getCurrentReleaseNumber() throws Exception {
        return getCurrentDba().getReleaseNumber();
    }

    private MySQLAdaptor getCurrentDba() {
        return this.currentDba;
    }

    private MySQLAdaptor getPreviousDba() {
        return this.previousDba;
    }

    private MySQLAdaptor createCurrentDba() throws SQLException {
        return createDba(this.currentDatabaseName);
    }

    private MySQLAdaptor createPreviousDba() throws SQLException {
        return createDba(this.previousDatabaseName);
    }

    private MySQLAdaptor createDba(String dbName) throws SQLException {
        return new MySQLAdaptor(
            this.host,
            dbName,
            this.userName,
            this.password,
            this.port
        );
    }
}