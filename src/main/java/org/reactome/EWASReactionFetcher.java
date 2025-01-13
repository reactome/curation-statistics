package org.reactome;

import org.gk.model.ClassAttributeFollowingInstruction;
import org.gk.model.GKInstance;
import org.gk.model.InstanceUtilities;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;

import java.util.*;

/**
 * @author Joel Weiser (joel.weiser@oicr.on.ca)
 * Created 1/13/2025
 */
public class EWASReactionFetcher {
    private MySQLAdaptor dba;
    private Map<Long, List<GKInstance>> ewasDbIdToReactions;

    private EWASReactionFetcher() {}

    public static EWASReactionFetcher getInstance() {
        return new EWASReactionFetcher();
    }

    public List<GKInstance> getReactionsFromEWAS(GKInstance ewas) {
        return getEWASDbIdToReactions((MySQLAdaptor) ewas.getDbAdaptor())
            .computeIfAbsent(ewas.getDBID(), k -> new ArrayList<>());
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
            Arrays.asList(ReactomeJavaConstants.physicalEntity, ReactomeJavaConstants.activeUnit)
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
            ReactomeJavaConstants.CandidateSet,
            Arrays.asList(ReactomeJavaConstants.hasCandidate)
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.Polymer,
            Arrays.asList(ReactomeJavaConstants.repeatedUnit)
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.EntityWithAccessionedSequence,
            Arrays.asList(ReactomeJavaConstants.hasModifiedResidue)
        );
        classToClassAttributesMapForObtainingProteins.put(
            ReactomeJavaConstants.GroupModifiedResidue,
            Arrays.asList(ReactomeJavaConstants.modification)
        );


        return classToClassAttributesMapForObtainingProteins;
    }

    private Map<Long, List<GKInstance>> getEWASDbIdToReactions(MySQLAdaptor dba) {
        if (this.ewasDbIdToReactions == null) {
            try {
                this.ewasDbIdToReactions = fetchEWASDbIdToReactions(dba);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return this.ewasDbIdToReactions;
    }
}
