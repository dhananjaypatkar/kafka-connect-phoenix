package io.kafka.connect.phoenix.parser;

import io.kafka.connect.phoenix.PhoenixClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yubj
 */
public class PhoenixClientTest {

  private PhoenixClient phoenixClient;

  @Before
  public void setup() {
    phoenixClient = new PhoenixClient(null);
  }

  @Test
  public void testFormUpsert() {
    final Schema schema = SchemaBuilder.struct().name("test").version(1)
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.INT8_SCHEMA)
        .build();
    final String tableName = "hbase.test";
    final String result = phoenixClient.formUpsert(schema, tableName);
    Assert.assertEquals("upsert into \"hbase\".\"test\" (\"id\",\"name\",\"age\") values (?,?,?)",
        result);
  }

  @Test
  public void testFormDelete() {
    final String tableName = "hbase.test";
    String[] rowkeyColumns = new String[]{"id"};
    final String result = phoenixClient.formDelete(tableName, rowkeyColumns);
    Assert.assertEquals("delete from \"hbase\".\"test\" where \"id\" = ?",
        result);
  }

  @Test
  public void testFormDeleteCompositeKey() {
    final String tableName = "hbase.test2";
    String[] rowkeyColumns = new String[]{"name", "code"};
    final String result = phoenixClient.formDelete(tableName, rowkeyColumns);
    Assert.assertEquals("delete from \"hbase\".\"test2\" where \"name\" = ? and \"code\" = ?",
        result);
  }
}
