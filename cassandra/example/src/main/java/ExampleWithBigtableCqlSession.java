// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [START bigtable_create_bigtablecqlconfiguration]
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.bigtable.cassandra.BigtableCqlConfiguration;
import com.google.bigtable.cassandra.BigtableCqlSessionFactory;

/**
 * Example using Bigtable CQLSession
 */
public class ExampleWithBigtableCqlSession {

  public static void main(String[] args) {

    // Construct BigtableCqlConfiguration
    BigtableCqlConfiguration bigtableCqlConfiguration = BigtableCqlConfiguration.builder()
        .setProjectId("example-project-id")
        .setInstanceId("example-instance-id")
        .setDefaultColumnFamily("example-column-family")
        .setBigtableChannelPoolSize(4)
        .build();

    // Create CqlSession with BigtableCqlConfiguration
    BigtableCqlSessionFactory bigtableCqlSessionFactory = new BigtableCqlSessionFactory(bigtableCqlConfiguration);

    // Create CqlSession
    try (CqlSession session = bigtableCqlSessionFactory.newSession()) {

      // Create a table
      String createTableQuery = "CREATE TABLE <KEYSPACE>.<TABLE_NAME> (<COLUMN> <TYPE> PRIMARY KEY);";
      session.execute(createTableQuery);

      // Prepare an insert statement
      PreparedStatement preparedInsert = session.prepare(
          "INSERT INTO <KEYSPACE>.<TABLE_NAME> (<COLUMN>) VALUES (?)" // replace with your keyspace, table and columns
      );

      // Insert
      BoundStatement boundInsert = preparedInsert
          .bind()
          .setString("<COLUMN>", "<VALUE>");
      session.execute(boundInsert);

      // Query for all entries
      ResultSet resultSet = session.execute("SELECT <COLUMN> FROM <KEYSPACE>.<TABLE_NAME>;");
      // Print results
      for (Row row : resultSet) {
        System.out.println(row);
      }

    }

  }

}
// [END bigtable_create_bigtablecqlconfiguration]