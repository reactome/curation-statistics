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

        List<GKInstance> newReactionLikeEvents = getNewRLEs(currentDba, previousDba);

        Path rleReportPath = Paths.get("NewRLEsV" + getCurrentDba().getReleaseNumber() + ".txt");
        RLEReporter rleReporter = new RLEReporter(rleReportPath);
        rleReporter.report(newReactionLikeEvents);

        Path curatorRLEReportPath = Paths.get("CuratorRLECountV" + getCurrentDba().getReleaseNumber() + ".txt");
        CuratorCountReporter rleCuratorCount = new CuratorCountReporter("RLE", curatorRLEReportPath);
        rleCuratorCount.report(newReactionLikeEvents);

        List<GKInstance> newEWASs = getNewEWASs(currentDba, previousDba);

        Path ewasReportPath = Paths.get("NewEWASsV" + getCurrentDba().getReleaseNumber() + ".txt");
        EWASReporter ewasReporter = new EWASReporter(ewasReportPath);
        ewasReporter.report(newEWASs);

        Path curatorEWASReportPath = Paths.get("CuratorEWASCountV" + getCurrentDba().getReleaseNumber() + ".txt");
        CuratorCountReporter ewasCuratorCount = new CuratorCountReporter("EWAS", curatorEWASReportPath);
        ewasCuratorCount.report(newEWASs);
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