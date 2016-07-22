/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.Measurements;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.util.*;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class onlineShopDBWrapper extends onlineShopDB {
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY = "reportlatencyforeacherror";
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT = "false";
  private static final String LATENCY_TRACKED_ERRORS_PROPERTY = "latencytrackederrors";
  private final onlineShopDB _db;
  private final Measurements _measurements;


  private final Tracer _tracer;

  private final String SCOPE_STRING_INSERT_BOOK;
  private final String SCOPE_STRING_INSERT_AUTHOR;
  private final String SCOPE_STRING_INSERT_RECOMMENDATION;
  private final String SCOPE_STRING_INSERT_USER;
  private final String SCOPE_STRING_GET_LATEST_RECOMMENDATION;
  private final String SCOPE_STRING_GET_ALL_RECOMMENDATION;
  private final String SCOPE_STRING_GET_USERS_RECOMMENDATION;
  private final String SCOPE_STRING_GET_AUTHOR_BY_ID;
  private final String SCOPE_STRING_GET_BOOKS_BY_GENRE;
  private final String SCOPE_STRING_FIND_BOOKS_NAME;
  private final String SCOPE_STRING_FIND_AUTHOR_BY_BOOK;
  private final String SCOPE_STRING_UPDATE_AUTHOR;
  private final String SCOPE_STRING_UPDATE_BOOK;
  private final String SCOPE_STRING_UPDATE_RECOMMENDATION;
  private final String SCOPE_STRING_DELETE_BOOK;
  private final String SCOPE_STRING_DELETE_ALL_RECOMMENDATIONS_BELONG_TO_BOOK;
  private final String SCOPE_STRING_DELETE_AUTHOR;
  private final String SCOPE_STRING_INIT;

  private boolean reportLatencyForEachError = false;
  private HashSet<String> latencyTrackedErrors = new HashSet<String>();

  public onlineShopDBWrapper(final onlineShopDB db, final Tracer tracer) {
    _db = db;
    _measurements = Measurements.getMeasurements();
    _tracer = tracer;
    final String simple = db.getClass().getSimpleName();
    SCOPE_STRING_INIT = simple + "#init";
    SCOPE_STRING_INSERT_BOOK = simple + "#insertBook";
    SCOPE_STRING_INSERT_AUTHOR = simple + "#inserAuthor";
    SCOPE_STRING_INSERT_RECOMMENDATION = simple + "#insertRecommendation";
    SCOPE_STRING_INSERT_USER = simple + "#insertUser";
    SCOPE_STRING_GET_LATEST_RECOMMENDATION = simple + "#getLastRecommendation";
    SCOPE_STRING_GET_ALL_RECOMMENDATION = simple + "#getAllRecommendations";
    SCOPE_STRING_GET_USERS_RECOMMENDATION = simple + "#getUsersRecommendations";
    SCOPE_STRING_GET_AUTHOR_BY_ID = simple + "#getAuthorByID";
    SCOPE_STRING_GET_BOOKS_BY_GENRE = simple + "#getBooksByGenre";
    SCOPE_STRING_FIND_BOOKS_NAME = simple + "#findBooksName";
    SCOPE_STRING_FIND_AUTHOR_BY_BOOK = simple + "#findAuthorByBook";
    SCOPE_STRING_UPDATE_AUTHOR = simple + "#updateAuthor";
    SCOPE_STRING_UPDATE_BOOK = simple + "#updateBook";
    SCOPE_STRING_UPDATE_RECOMMENDATION = simple + "#updateRecommendation";
    SCOPE_STRING_DELETE_BOOK = simple + "#deleteBook";
    SCOPE_STRING_DELETE_ALL_RECOMMENDATIONS_BELONG_TO_BOOK = simple + "#deleteAllRecommendationsBelongToBook";
    SCOPE_STRING_DELETE_AUTHOR = simple + "#deleteAuthor";

  }

  /**
   * Get the set of properties for this DB.
   */
  public Properties getProperties() {
    return _db.getProperties();
  }

  /**
   * Set the properties for this DB.
   */
  public void setProperties(Properties p) {
    _db.setProperties(p);
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INIT)) {
      _db.init();
      this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
        REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

      if (!reportLatencyForEachError) {
        String latencyTrackedErrors = getProperties().getProperty(LATENCY_TRACKED_ERRORS_PROPERTY, null);
        if (latencyTrackedErrors != null) {
          this.latencyTrackedErrors = new HashSet<>(Arrays.asList(latencyTrackedErrors.split(",")));
        }
      }
      System.err.println("onlineShopDBWrapper: report latency for each error is "
        + this.reportLatencyForEachError
        + " and specific error codes to track"
        + " for latency are: "
        + this.latencyTrackedErrors.toString());
    }
  }


  private void measure(String op, Status result, long intendedStartTimeNanos, long startTimeNanos, long endTimeNanos) {
    String measurementName = op;
    if (result != Status.OK) {
      if (this.reportLatencyForEachError ||
        this.latencyTrackedErrors.contains(result.getName())) {
        measurementName = op + "-" + result.getName();
      } else {
        measurementName = op + "-FAILED";
      }
    }
    _measurements.measure(measurementName,
      (int) ((endTimeNanos - startTimeNanos) / 1000));
    _measurements.measureIntended(measurementName,
      (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
  }


  /*----------------------------------client wrapper methods----------------------------------------------------------*/
  @Override
  public Status insertUser(int userID, String userName, Date birthDate) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INSERT_USER)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.insertUser(userID, userName, birthDate);
        long en = System.nanoTime();
        measure("insertUser", res, ist, st, en);
        _measurements.reportStatus("insertUser", res);
        return res;
      }
    }
  }

  @Override
  public Status insertAuthor(int authorID, String authorFullName, String gender, Date birthDate, String resume) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INSERT_AUTHOR)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.insertAuthor(authorID, authorFullName, gender, birthDate, resume);
        long en = System.nanoTime();
        measure("insertAuthor", res, ist, st, en);
        _measurements.reportStatus("insertAuthor", res);
        return res;
      }
    }
  }

  @Override
  public Status insertBook(int bookID, String bookTitle, ArrayList<String> genres, String introductionText, String language, HashMap<Integer,String> authors) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INSERT_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.insertBook(bookID, bookTitle, genres, introductionText, language, authors);
        long en = System.nanoTime();
        measure("insertBook", res, ist, st, en);
        _measurements.reportStatus("insertBook", res);
        return res;
      }
    }
  }


  @Override
  public Status insertRecommendation(int bookID, int userID, int stars, int likes, String text, Date createTime) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_INSERT_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.insertRecommendation(bookID, userID, stars, likes, text, createTime);
        long en = System.nanoTime();
        measure("insertRecommendations", res, ist, st, en);
        _measurements.reportStatus("insertRecommendations", res);
        return res;
      }
    }
  }

  @Override
  public Status getLatestRecommendations(int bookID, int limit) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_LATEST_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.getLatestRecommendations(bookID, limit);
        long en = System.nanoTime();
        measure("getLatestRecommendations", res, ist, st, en);
        _measurements.reportStatus("getLatestRecommendations", res);
        return res;
      }
    }
  }

  @Override
  public Status getAllRecommendations(int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_ALL_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.getAllRecommendations(bookID);
        long en = System.nanoTime();
        measure("getAllRecommendations", res, ist, st, en);
        _measurements.reportStatus("getAllRecommendations", res);
        return res;
      }
    }
  }

  @Override
  public Status getUsersRecommendations(int userID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_USERS_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.getUsersRecommendations(userID);
        long en = System.nanoTime();
        measure("getUsersRecommendations", res, ist, st, en);
        _measurements.reportStatus("getUsersRecommendations", res);
        return res;
      }
    }
  }

  @Override
  public Status getAuthorByID(int authorID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_AUTHOR_BY_ID)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.getAuthorByID(authorID);
        long en = System.nanoTime();
        measure("getAuthorByID", res, ist, st, en);
        _measurements.reportStatus("getAuthorByID", res);
        return res;
      }
    }
  }

  @Override
  public Status findBooksByGenre(String genreList, int limit) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_GET_BOOKS_BY_GENRE)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.findBooksByGenre(genreList, limit);
        long en = System.nanoTime();
        measure("findBooksByGenre", res, ist, st, en);
        _measurements.reportStatus("findBooksByGenre", res);
        return res;
      }
    }
  }

  @Override
  public Status findBooksName(String bookName, int limit) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_FIND_BOOKS_NAME)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.findBooksName(bookName, limit);
        long en = System.nanoTime();
        measure("findBooksName", res, ist, st, en);
        _measurements.reportStatus("findBooksName", res);
        return res;
      }
    }
  }

  @Override
  public Status findAuthorByBookID(int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_FIND_AUTHOR_BY_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.findAuthorByBookID(bookID);
        long en = System.nanoTime();
        measure("findAuthorByBookID", res, ist, st, en);
        _measurements.reportStatus("findAuthorByBookID", res);
        return res;
      }
    }
  }

  @Override
  public Status updateAuthor(int authorID, String authorName, String gender, Date birthDate, String resume) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_UPDATE_AUTHOR)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.updateAuthor(authorID, authorName, gender, birthDate, resume);
        long en = System.nanoTime();
        measure("updateAuthor", res, ist, st, en);
        _measurements.reportStatus("updateAuthor", res);
        return res;
      }
    }
  }

  @Override
  public Status updateBook(int bookID, String title, String language, String introduction) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_UPDATE_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.updateBook(bookID, title, language, introduction);
        long en = System.nanoTime();
        measure("updateBook", res, ist, st, en);
        _measurements.reportStatus("updateBook", res);
        return res;
      }
    }
  }

  @Override
  public Status updateRecommendation(int bookID, int userID, int stars, String text) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_UPDATE_RECOMMENDATION)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.updateRecommendation(bookID, userID, stars, text);
        long en = System.nanoTime();
        measure("updateRecommendation", res, ist, st, en);
        _measurements.reportStatus("updateRecommendation", res);
        return res;
      }
    }
  }

  /*
  @Override
  public Status deleteBookReferenceFromAuthorList(int authorID, int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_DELETE)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.deleteBookReferenceFromAuthorList(authorID, bookID);
        long en = System.nanoTime();
        measure("DELETE", res, ist, st, en);
        _measurements.reportStatus("DELETE", res);
        return res;
      }
    }
  }
*/

  @Override
  public Status deleteBook(int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_DELETE_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.deleteBook(bookID);
        long en = System.nanoTime();
        measure("deleteBook", res, ist, st, en);
        _measurements.reportStatus("deleteBook", res);
        return res;
      }
    }
  }

/*
  public Status deleteBookReferenceFromAuthor(int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_DELETE)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.deleteBookReferenceFromAuthor(bookID);
        long en = System.nanoTime();
        measure("deleteBookReferenceFromAuthor", res, ist, st, en);
        _measurements.reportStatus("DELETE", res);
        return res;
      }
    }
  }
*/

  @Override
  public Status deleteAllRecommendationsBelongToBook(int bookID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_DELETE_ALL_RECOMMENDATIONS_BELONG_TO_BOOK)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.deleteAllRecommendationsBelongToBook(bookID);
        long en = System.nanoTime();
        measure("deleteAllRecommendationsBelongToBook", res, ist, st, en);
        _measurements.reportStatus("deleteAllRecommendationsBelongToBook", res);
        return res;
      }
    }
  }

  @Override
  public Status deleteAuthor(int authorID) {
    {
      try (final TraceScope span = _tracer.newScope(SCOPE_STRING_DELETE_AUTHOR)) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _db.deleteAuthor(authorID);
        long en = System.nanoTime();
        measure("deleteAuthor", res, ist, st, en);
        _measurements.reportStatus("deleteAuthor", res);
        return res;
      }
    }
  }


  /*-----------------------------------------------------Deprecated methods-------------------------------------------*/


  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return null;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }

}
