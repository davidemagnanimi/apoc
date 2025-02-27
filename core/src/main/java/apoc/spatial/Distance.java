/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.spatial;

import org.neo4j.procedure.Description;
import apoc.result.DistancePathResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

public class Distance {

    private static final String LATITUDE = "latitude";
    private static final String LONGITUDE = "longitude";

    @Procedure("apoc.spatial.sortByDistance")
    @Description("Sorts the given collection of `PATH` values by the sum of their distance based on the latitude/longitude values in the `NODE` values.")
    public Stream<DistancePathResult> sortByDistance(@Name("paths")List<Path> paths) {
        return paths.size() > 0 ? sortPaths(paths).stream() : Stream.empty();
    }

    public SortedSet<DistancePathResult> sortPaths(List<Path> paths) {
        SortedSet<DistancePathResult> result = new TreeSet<DistancePathResult>();
        for (int i = 0; i <= paths.size()-1; ++i) {
            double d = getPathDistance(paths.get(i));
            result.add(new DistancePathResult(paths.get(i), d));
        }

        return result;
    }

    public double getPathDistance(Path path) {
        double distance = 0;
        List<Node> nodes = new ArrayList<>();
        for (Node node : path.nodes()) {
            checkNodeHasGeo(node);
            nodes.add(node);
        }

        for (int i = 1; i <= nodes.size()-1; ++i) {
            Node prev = nodes.get(i-1);
            Node curr = nodes.get(i);
            distance += getDistance(
                    (double) prev.getProperty(LATITUDE),
                    (double) prev.getProperty(LONGITUDE),
                    (double) curr.getProperty(LATITUDE),
                    (double) curr.getProperty(LONGITUDE)
            );
        }

        return distance;
    }

    public double getDistance(double lat1, double lon1, double lat2, double lon2) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515;

        return dist * 1.609344;
    }

    private double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
    }

    private void checkNodeHasGeo(Node node) {
        if (!node.hasProperty(LATITUDE) || !node.hasProperty(LONGITUDE)) {
            throw new IllegalArgumentException(String.format("Node with id %s has invalid geo properties", node.getElementId()));
        }
    }

}
