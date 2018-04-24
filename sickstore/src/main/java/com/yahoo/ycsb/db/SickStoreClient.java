package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import de.unihamburg.sickstore.backend.Version;
import de.unihamburg.sickstore.backend.timer.SystemTimeHandler;
import de.unihamburg.sickstore.database.ReadPreference;
import de.unihamburg.sickstore.database.WriteConcern;
import de.unihamburg.sickstore.database.messages.exception.DatabaseException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Steffen Friedrich on 12.11.2015.
 */
public class SickStoreClient extends DB {

  /**
   * status code indicating that an operation failed
   */
  private static final int STATUS_FAIL = -1;

  /**
   * status code indicating everything went fine
   */
  private static final int STATUS_OK = 0;

  /**
   * status code indicating that a value could not be retrieved, because it
   * was expected to be of type <code>String</code>, but wasn't
   */
  private static final int STATUS_WRONGTYPE_STRINGEXPECTED = -2;

  private static WriteConcern writeConcern;

  private static ReadPreference readPreference;


  private static de.unihamburg.sickstore.database.client.SickStoreClient client = null;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      try {
        if (curInitCount <= 0) {
          client.cleanup("");
          client.disconnect();
        }
        if (curInitCount < 0) {
          // This should never happen.
          throw new DBException(
              String.format("initCount is negative: %d", curInitCount));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      client.delete(table, key, writeConcern);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    synchronized (INIT_COUNT) {
      // Keep track of number of calls to init (for later cleanup)
      INIT_COUNT.incrementAndGet();

      // Check if the cluster has already been initialized
      if (client != null) {
        return;
      }

      try {
        // initialize SickStore driver
        Properties props = getProperties();
        String url = props.getProperty("sickstore.url", "localhost");
        int port = Integer.parseInt(props.getProperty("sickstore.port", "54000"));

        int maxconnections = Integer.parseInt(props.getProperty("sickstore.maxconnections", "64"));

        // configure write concern
        writeConcern = new WriteConcern();
        String ack = props.getProperty("sickstore.write_concern.ack", "1");
        try {
          writeConcern.setReplicaAcknowledgement(Integer.parseInt(ack));
        } catch (NumberFormatException e) {
          // no number given, assume it is a tag set
          writeConcern.setReplicaAcknowledgementTagSet(ack);
        }

        String journaling = props.getProperty("sickstore.write_concern.journaling", "false");
        if (journaling.equals("true")) {
          writeConcern.setJournaling(true);
        }

        String destinationNode = props.getProperty("sickstore.dest_node", "primary");

        String readPreferenceString = props.getProperty("sickstore.read_preference", ReadPreference.PRIMARY);
        readPreference = new ReadPreference(readPreferenceString);

        client = new de.unihamburg.sickstore.database.client.SickStoreClient(url, port, destinationNode, new SystemTimeHandler(), maxconnections);


        client.connect();

      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      Version version = new Version();
      for (String k : values.keySet()) {
        Object v = values.get(k).toString();
        version.put(k, (String) v);
      }
      client.insert(table, key, version, writeConcern);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      Version version = null;
      try {
        version = client.read(table, key, fields, readPreference);
      } catch (DatabaseException e) {
        e.printStackTrace();
        return Status.ERROR;
      }

      Object value = null;
      for (String k : version.getValues().keySet()) {
        value = version.get(k);
        if (value instanceof String) {
          result.put(k, new StringByteIterator((String) value));
        } else {
          return Status.ERROR;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      HashMap<String, ByteIterator> resultMap = null;
      List<Version> versions = null;
      Version version = null;
      Object value = null;

      try {
        versions = client.scan(table, startkey, recordcount, fields, true, readPreference);
      } catch (DatabaseException e) {
        e.printStackTrace();
        return Status.ERROR;
      }

      if (fields == null && versions.size() > 0) {
        // prevent NullPointerException
        fields = versions.get(0).getValues().keySet();
      }

      for (int i = 0; i < versions.size(); i++) {
        version = versions.get(i);
        resultMap = new HashMap<String, ByteIterator>();
        for (String k : fields) {
          value = version.get(k);
          if (value instanceof String) {
            resultMap
                .put(k, new StringByteIterator((String) value));
          } else {
            return Status.ERROR;
          }
        }

        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      Version version = new Version();
      for (String column : values.keySet()) {
        version.put(column, values.get(column).toString());
      }
      if (client.update(table, key, version, writeConcern)) {
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
}
