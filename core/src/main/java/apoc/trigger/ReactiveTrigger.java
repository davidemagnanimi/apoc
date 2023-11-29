package apoc.trigger;

import org.neo4j.graphdb.Entity;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class ReactiveTrigger {
    protected final String name;

    protected final Long stratum;

    protected final String statement;

    protected final String event;

    protected Map<String, Object> params;

    protected String phase;

    protected Boolean paused;

    protected Queue<Entity> entitiesToProcess;

    public ReactiveTrigger(String name, String statement, Map<String, Object> params, Map<String, Object> selector, Boolean paused) {
        this.name = name;
        this.statement = statement;
        this.params = params;
        this.phase = (String) selector.get("phase");
        this.stratum = (Long) selector.get("stratum");
        this.event = (String) selector.get("event");
        this.paused = paused;
        this.entitiesToProcess = new LinkedList<>();
    }

    public String getName() {
        return name;
    }

    public Long getStratum() {
        return stratum;
    }

    public String getStatement() {
        return statement;
    }

    public String getEvent() {
        return event;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public String getPhase() {
        return phase;
    }

    public Boolean getPaused() {
        return paused;
    }

    public Queue<Entity> getEntitiesToProcess() {
        return entitiesToProcess;
    }

    public void enqueueNewEntitiesToProcess(Set<Entity> entities) {
        this.entitiesToProcess.addAll(entities);
    }

    public void emptyEntitiesToProcess() {
        this.entitiesToProcess = new LinkedList<>();
    }

    @Override
    public String toString() {
        return "ReactiveTrigger{" +
                "name='" + name + '\'' +
                ", stratum=" + stratum +
                ", statement='" + statement + '\'' +
                ", event=" + event +
                ", params=" + params +
                ", phase='" + phase + '\'' +
                ", paused=" + paused +
                ", eventsToProcess=" + entitiesToProcess +
                '}';
    }
}
