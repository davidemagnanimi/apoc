package apoc.export.csv;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ImportCsv {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

    @Context
    public TerminationGuard terminationGuard;

    @Procedure(name = "apoc.import.csv", mode = Mode.SCHEMA)
    @Description("Imports nodes and relationships with the given labels and types from the provided CSV file.")
    public Stream<ProgressInfo> importCsv(
            @Name("nodes") List<Map<String, Object>> nodes,
            @Name("rels") List<Map<String, Object>> relationships,
            @Name("config") Map<String, Object> config
    ) {
        ProgressInfo result =
                Util.inThread(pools, () -> {

                    String file = "progress.csv";
                    String source = "file";
                    if (nodes.stream().anyMatch(node -> node.containsKey("data"))) {
                        file =  null;
                        source = "file/binary";
                    }
                    final CsvLoaderConfig clc = CsvLoaderConfig.from(config);
                    final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, source, "csv"));
                    final CsvEntityLoader loader = new CsvEntityLoader(clc, reporter, log, terminationGuard);

                    final Map<String, Map<String, String>> idMapping = new HashMap<>();
                    for (Map<String, Object> node : nodes) {
                        final Object data = node.getOrDefault("fileName", node.get("data"));
                        final List<String> labels = (List<String>) node.get("labels");
                        loader.loadNodes(data, labels, db, idMapping);
                    }

                    for (Map<String, Object> relationship : relationships) {
                        final Object fileName = relationship.getOrDefault("fileName", relationship.get("data"));
                        final String type = (String) relationship.get("type");
                        loader.loadRelationships(fileName, type, db, idMapping);
                    }

                    return reporter.getTotal();
                });
        return Stream.of(result);
    }


}
