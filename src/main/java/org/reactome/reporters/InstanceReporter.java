package org.reactome.reporters;

import org.gk.model.GKInstance;

import java.util.List;

/**
 * @author Joel Weiser (joel.weiser@oicr.on.ca)
 * Created 1/13/2025
 */
public interface InstanceReporter {

    void report(List<GKInstance> instances) throws Exception;

}
