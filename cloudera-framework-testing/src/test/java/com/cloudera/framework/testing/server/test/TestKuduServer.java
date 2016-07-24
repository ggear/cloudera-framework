package com.cloudera.framework.testing.server.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.CreateTableOptions;
import org.kududb.client.Insert;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduClient.KuduClientBuilder;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.PartialRow;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;

import com.cloudera.framework.testing.TestConstants;
import com.cloudera.framework.testing.TestRunner;
import com.cloudera.framework.testing.server.KuduServer;
import com.google.common.collect.ImmutableList;

@RunWith(TestRunner.class)
public class TestKuduServer implements TestConstants {

  @ClassRule
  public static KuduServer kuduServer = KuduServer.getInstance();

  @Test
  public void testKudu() throws Exception {
    KuduClient client = new KuduClientBuilder(kuduServer.getMasterAddresses()).build();
    String tableName = "mytable";
    try {
      List<ColumnSchema> columns = new ArrayList<>();
      columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());
      Schema schema = new Schema(columns);
      client.createTable(tableName, schema, new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key")));
      KuduTable table = client.openTable(tableName);
      KuduSession session = client.newSession();
      for (int i = 0; i < 3; i++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addInt(0, i);
        row.addString(1, "value " + i);
        session.apply(insert);
      }
      List<String> projectColumns = new ArrayList<>(1);
      projectColumns.add("value");
      KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();
      int i = 0;
      while (scanner.hasMoreRows()) {
        RowResultIterator results = scanner.nextRows();
        while (results.hasNext()) {
          RowResult result = results.next();
          assertEquals("value " + i++, result.getString(0));
        }
      }
    } finally {
      client.shutdown();
    }
  }

  @Test
  public void testKuduAgain() throws Exception {
    testKudu();
  }

}
