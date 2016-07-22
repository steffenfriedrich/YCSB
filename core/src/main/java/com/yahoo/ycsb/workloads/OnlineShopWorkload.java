/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class OnlineShopWorkload extends Workload {

  /*ID generators */
  NumberGenerator authorIDgenerator;
  NumberGenerator bookIDgenerator;
  NumberGenerator userIDgenerator;


  /* STRING generators */
  IncrementingPrintableStringGenerator text;


  /* PICK from fixed set generators */
  DiscreteGenerator language;
  DiscreteGenerator gender;
  DiscreteGenerator genres;
  DiscreteGenerator operationchooser;


  /* load phase */
  private int userStart;
  private int userCount;
  private int bookStart;
  private int bookCount;
  private int authorStart;
  private int authorCount;
  private int recCount;

  NumberGenerator authorIDchooser;
  NumberGenerator bookIDchooser;
  NumberGenerator userIDchooser;
  AtomicInteger recCounter;
  NumberGenerator LoadauthorIDchooser;
  NumberGenerator LoadbookIDchooser;
  NumberGenerator LoaduserIDchooser;


  /* transaction phase */
  int insertAuthorStart;
  int insertAuthorCount;
  int insertBookStart;
  int insertBookCount;
  int insertUserStart;
  int insertUserCount;

  String requestdistrib;
  Boolean dotransactions;

  AcknowledgedCounterGenerator transactioninsertkeysequenceUser;
  AcknowledgedCounterGenerator transactioninsertkeysequenceBook;
  AcknowledgedCounterGenerator transactioninsertkeysequenceAuthor;


  boolean orderedinserts;
  private Measurements _measurements = Measurements.getMeasurements();

  /* Text property */
  public static final int TEXT_LENGTH_PROPERTY = 50;


  /*----------- The default field length distribution."uniform", "zipfian","constant" and "histogram -----------------*/
  private static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";
  private static final String FIELD_LENGTH_PROPERTY = "fieldlength";
  private static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100"; //in bytes
  private static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
  private static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";
  private static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

  /*---------------------------------------------- OPERATION PROPERTIES NAMES----------------------------------------*/
  private static final String insertUser_PROPORTION_PROPERTY = "insertUser";
  private static final String insertAuthor_PROPORTION_PROPERTY = "insertAuthor";
  private static final String insertBook_PROPORTION_PROPERTY = "insertBook";
  private static final String insertRecommendation_PROPORTION_PROPERTY = "insertRecommendation";
  private static final String getLatestRecommendations_PROPORTION_PROPERTY = "getLatestRecommendations";
  private static final String getAllRecommendations_PROPORTION_PROPERTY = "getAllRecommendations";
  private static final String getUsersRecommendations_PROPORTION_PROPERTY = "getUsersRecommendations";
  private static final String getAuthorByID_PROPORTION_PROPERTY = "getAuthorByID";
  private static final String getAuthorByBookID_PROPORTION_PROPERTY = "getAuthorByBookID";
  private static final String findBooksByGenre_PROPORTION_PROPERTY = "findBooksByGenre";
  private static final String findBooksName_PROPORTION_PROPERTY = "findBooksName";
  private static final String updateAuthor_PROPORTION_PROPERTY = "updateAuthor";
  private static final String updateBook_PROPORTION_PROPERTY = "updateBook";
  private static final String updateRecommendation_PROPORTION_PROPERTY = "updateRecommendation";
  private static final String deleteBook_PROPORTION_PROPERTY = "deleteBook";
  private static final String deleteAllRecommendationsBelongToBook_PROPORTION_PROPERTY = "deleteAllRecommendationsBelongToBook";
  private static final String deleteAuthor_PROPORTION_PROPERTY = "deleteAuthor";

  /*---------------------------------------------- CRUID DEFAULT proportions ----------------------------------------*/
  private static final String insertUser_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String insertAuthor_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String insertBook_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String insertRecommendation_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String getLatestRecommendations_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String getAllRecommendations_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String getUsersRecommendations_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String getAuthorByID_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String getAuthorByBookID_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String findBooksByGenre_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String findBooksName_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String updateAuthor_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String updateBook_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String updateRecommendation_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String deleteBook_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String deleteAllRecommendationsBelongToBook_PROPORTION_PROPERTY_DEFAULT = "0";
  private static final String deleteAuthor_PROPORTION_PROPERTY_DEFAULT = "0";

  /*---------------------------------------------- DISTRIBUTION PROPERTIES----------------------------------------*/
  private static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
  private static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /*---------------------------------------------- HOTSPOT ------------------------------------------------------*/
  private static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
  private static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";
  private static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
  private static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

  private onlineShopDB db;
  private Object threadstate;


  //TODO in workload implementieren database.getCollection("author").createIndex(new Document("authorFullName", 1));database.getCollection("recommendations").createIndex(new Document("_id", 1)append("recommendations.likes",1));

  //  db.colB.insertIndex({_id: 1}{bookName: 1})
  // db.colA.insertIndex({fullName: 1})
  // db.colC.insertIndex({_id: 1}{recommendations.likes: 1})*/

  @Override
  public void init(Properties p) throws WorkloadException {

    operationchooser = createOperationGenerator(p);

    // doInsert start
    userStart = Integer.parseInt(p.getProperty("userStart"));
    bookStart = Integer.parseInt(p.getProperty("bookStart"));
    authorStart = Integer.parseInt(p.getProperty("authorStart"));

    // doInsert max
    userCount = Integer.parseInt(p.getProperty("userCount", insertUser_PROPORTION_PROPERTY_DEFAULT));
    bookCount = Integer.parseInt(p.getProperty("bookCount", insertBook_PROPORTION_PROPERTY_DEFAULT));
    recCount = Integer.parseInt(p.getProperty("recCount", insertRecommendation_PROPORTION_PROPERTY_DEFAULT));
    authorCount = Integer.parseInt(p.getProperty("authorCount", insertAuthor_PROPORTION_PROPERTY_DEFAULT));

    // init id insert couters
    transactioninsertkeysequenceUser = new AcknowledgedCounterGenerator(userCount + 1);
    transactioninsertkeysequenceBook = new AcknowledgedCounterGenerator(bookCount + 1);
    transactioninsertkeysequenceAuthor = new AcknowledgedCounterGenerator(authorCount + 1);

    // for book insertion
    LoadauthorIDchooser = new UniformIntegerGenerator(authorStart, authorStart + authorCount - 1);
    LoadbookIDchooser = new UniformIntegerGenerator(bookStart, bookStart + bookCount - 1);
    LoaduserIDchooser = new UniformIntegerGenerator(userStart, userStart + userCount - 1);

    // init id insert couters
    transactioninsertkeysequenceUser = new AcknowledgedCounterGenerator(userCount + 1);
    transactioninsertkeysequenceBook = new AcknowledgedCounterGenerator(bookCount + 1);
    transactioninsertkeysequenceAuthor = new AcknowledgedCounterGenerator(authorCount + 1);

    // transaction phase
    requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    dotransactions = Boolean.parseBoolean(p.getProperty("dotransactions"));
    insertAuthorStart = Integer.parseInt(p.getProperty("insertAuthorStart", INSERT_START_PROPERTY_DEFAULT));
    insertAuthorCount = Integer.parseInt(p.getProperty("insertAuthorCount", String.valueOf(authorCount - insertAuthorStart)));
    insertBookStart = Integer.parseInt(p.getProperty("insertBookStart", INSERT_START_PROPERTY_DEFAULT));
    insertBookCount = Integer.parseInt(p.getProperty("insertBookCount", String.valueOf(bookCount - insertAuthorStart)));
    insertUserStart = Integer.parseInt(p.getProperty("insertUserStart", INSERT_START_PROPERTY_DEFAULT));
    insertUserCount = Integer.parseInt(p.getProperty("insertUserCount", String.valueOf(userCount - insertAuthorStart)));


    if (requestdistrib.compareTo("uniform") == 0) {
      authorIDchooser = new UniformIntegerGenerator(authorStart, authorStart + insertAuthorCount - 1);
      bookIDchooser = new UniformIntegerGenerator(bookStart, bookStart + insertBookCount - 1);
      userIDchooser = new UniformIntegerGenerator(userStart, userStart + insertUserCount - 1);

    } else if (requestdistrib.compareTo("sequential") == 0) {
      authorIDchooser = new SequentialGenerator(authorStart, authorStart + insertAuthorCount - 1);
      bookIDchooser = new SequentialGenerator(bookStart, bookStart + insertBookCount - 1);
      userIDchooser = new SequentialGenerator(userStart, userStart + insertUserCount - 1);

    } else if (requestdistrib.compareTo("zipfian") == 0) {
      final double insertproportionUser = Double.parseDouble(p.getProperty(insertUser_PROPORTION_PROPERTY, insertUser_PROPORTION_PROPERTY_DEFAULT));
      final double insertproportionBook = Double.parseDouble(p.getProperty(insertAuthor_PROPORTION_PROPERTY, insertAuthor_PROPORTION_PROPERTY_DEFAULT));
      final double insertproportionAuthor = Double.parseDouble(p.getProperty(insertBook_PROPORTION_PROPERTY, insertBook_PROPORTION_PROPERTY_DEFAULT));
      int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));//TODO  muss immer als parameter mitgeliefert werden
      int expectednewkeysAuthor = (int) ((opcount) * (insertproportionAuthor) * 2.0);
      int expectednewkeysBook = (int) ((opcount) * insertproportionBook * 2.0);
      int expectednewkeysUser = (int) ((opcount) * insertproportionUser * 2.0);
      authorIDchooser = new ScrambledZipfianGenerator(authorStart, authorStart + insertAuthorCount + expectednewkeysAuthor);
      bookIDchooser = new ScrambledZipfianGenerator(bookStart, bookStart + insertBookCount + expectednewkeysBook);
      userIDchooser = new ScrambledZipfianGenerator(userStart, userStart + insertUserCount + expectednewkeysUser);

    } else if (requestdistrib.compareTo("latest") == 0) {
      authorIDchooser = new SkewedLatestGenerator(transactioninsertkeysequenceAuthor);
      bookIDchooser = new SkewedLatestGenerator(transactioninsertkeysequenceBook);
      userIDchooser = new SkewedLatestGenerator(transactioninsertkeysequenceUser);

    } else if (requestdistrib.equals("hotspot")) {
      double hotsetfraction = Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction = Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      authorIDchooser = new HotspotIntegerGenerator(authorStart, authorStart + insertAuthorCount - 1, hotsetfraction, hotopnfraction);
      bookIDchooser = new HotspotIntegerGenerator(bookStart, bookStart + insertBookCount - 1, hotsetfraction, hotopnfraction);
      userIDchooser = new HotspotIntegerGenerator(userStart, userStart + insertUserCount - 1, hotsetfraction, hotopnfraction);

    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY, ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY, ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      authorIDchooser = new ExponentialGenerator(percentile, insertAuthorCount * frac);
      bookIDchooser = new ExponentialGenerator(percentile, insertBookCount * frac);
      userIDchooser = new ExponentialGenerator(percentile, insertUserCount * frac);

    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
    }

    // init random text string
    text = new IncrementingPrintableStringGenerator(TEXT_LENGTH_PROPERTY);

    // init gender
    gender = new DiscreteGenerator();
    gender.addValue(0.6, "male");
    gender.addValue(0.4, "female");

    // init genres
    genres = new DiscreteGenerator();
    genres.addValue(0.20, "drama");
    genres.addValue(0.20, "novel");
    genres.addValue(0.20, "Comedy");
    genres.addValue(0.20, "romance");
    genres.addValue(0.20, "horror");

    // init languages
    language = new DiscreteGenerator();
    language.addValue(0.20, "english");
    language.addValue(0.20, "german");
    language.addValue(0.20, "russian");
    language.addValue(0.20, "polnish");
    language.addValue(0.20, "italian");



    // init id insert couters vor Recommendations
    recCounter = new AtomicInteger(0);

   // init key generators
    if (dotransactions) {
      bookIDgenerator = new CounterGenerator(insertBookStart);
      authorIDgenerator = new CounterGenerator(insertAuthorStart);
      userIDgenerator = new CounterGenerator(insertUserStart);
    } else {
      bookIDgenerator = new CounterGenerator(0);
      authorIDgenerator = new CounterGenerator(0);
      userIDgenerator = new CounterGenerator(0);
    }
  }


  public boolean doTransaction(DB db, Object threadstate) {
    switch (operationchooser.nextString()) {
      case "insertUser":
        doTransaction_InsertUser((onlineShopDB) db);
        break;
      case "insertAuthor":
        doTransaction_InsertAuthor((onlineShopDB) db);
        break;
      case "insertBook":
        doTransaction_InsertBook((onlineShopDB) db);
        break;
      case "insertRecommendation":
        doTransaction_InsertRecommendation((onlineShopDB) db);
        break;
      case "getLatestRecommendations":
        doTransaction_GetLatestRecommendations((onlineShopDB) db);
        break;
      case "getAllRecommendations":
        doTransaction_GetAllRecommendations((onlineShopDB) db);
        break;
      case "getUsersRecommendations":
        doTransaction_GetUsersRecommendations((onlineShopDB) db);
        break;
      case "getAuthorByID":
        doTransaction_GetAuthorByID((onlineShopDB) db);
        break;
      case "getAuthorByBookID":
        doTransaction_GetAuthorByBookID((onlineShopDB) db);
        break;
      case "findBooksByGenre":
        doTransaction_FindBooksByGenre((onlineShopDB) db);
        break;
      case "findBooksName":
        doTransaction_FindBooksName((onlineShopDB) db);
        break;
      case "updateAuthor":
        doTransaction_UpdateAuthor((onlineShopDB) db);
        break;
      case "updateBook":
        doTransaction_UpdateBook((onlineShopDB) db);
        break;
      case "updateRecommendation":
        doTransaction_UpdateRecommendation((onlineShopDB) db);
        break;
      case "deleteBook":
        doTransaction_DeleteBook((onlineShopDB) db);
        break;
      case "deleteAllRecommendationsBelongToBook":
        doTransaction_DeleteAllRecommendationsBelongToBook((onlineShopDB) db);
        break;
      case "deleteAuthor":
        doTransaction_DeleteAuthor((onlineShopDB) db);
        break;
    }
    return true;
  }


  /* -------------------------------------------transaction methods START---------------------------------------------------*/

  @Override
  public boolean doInsert(DB db, Object threadstate) {

    if (isStopRequested()) {
      return false;
    }
    int authorID;
    int bookID;
    int userID;
    int recID;

    do {
      doTransaction_InsertAuthor((onlineShopDB) db);
      authorID = authorIDgenerator.lastValue().intValue();
    } while (authorID < authorCount);

    do {
      doTransaction_InsertBook((onlineShopDB) db);
      bookID = bookIDgenerator.lastValue().intValue();
    } while (bookID < bookCount);

    do {
      doTransaction_InsertUser((onlineShopDB) db);
      userID = userIDgenerator.lastValue().intValue();
    } while (userID < userCount);

    do {
      doTransaction_InsertRecommendation((onlineShopDB) db);
      recID = recCounter.getAndIncrement();
    } while (recID < recCount);

    requestStop();
    return true;
  }


  public void doTransaction_InsertAuthor(onlineShopDB db) {
    int authorID;

    if (dotransactions) {
      authorID = transactioninsertkeysequenceAuthor.nextValue();
    } else {
      authorID = authorIDgenerator.nextValue().intValue();
    }
    try {
      String name = buildKeyName("author", authorID);
      String sex = gender.nextValue();
      Date birth = randomDate();
      String resume = text.nextValue().toString();
      db.insertAuthor(authorID, name, sex, birth, resume);
    } finally {
      if (dotransactions) transactioninsertkeysequenceAuthor.acknowledge(authorID);
    }
  }


  public void doTransaction_InsertBook(onlineShopDB db) {
    int bookID;
    NumberGenerator chooser;

    if (dotransactions) {
      bookID = transactioninsertkeysequenceBook.nextValue();
      chooser = authorIDchooser;
    } else {
      bookID = bookIDgenerator.nextValue().intValue();
      chooser = LoadauthorIDchooser;
    }

    Random random = new Random();// choose 1 to 4 genres for a book
    int quantityGenre;
    int quantityAuthor;
    HashSet<String> setGenres; //genres set to avoid duplications
    HashMap<Integer, String> authors;

    try {
      String bookTitle = buildKeyName("book", bookID);
      String lang = language.nextValue();
      String intro = text.nextValue();

      quantityGenre = random.nextInt(4 - 1 + 1) + 1;
      setGenres = new HashSet<>();

      quantityAuthor = random.nextInt(4 - 1 + 1) + 1;
      authors = new HashMap<>();

      for (int i = 0; i < quantityGenre; i++) {//build genres set
        setGenres.add(this.genres.nextValue());
      }
      ArrayList<String> genres = new ArrayList<String>(setGenres);

      for (int i = 0; i < quantityAuthor; i++) { //build authors HashMap
        int authorID = nextKeynum(chooser, transactioninsertkeysequenceAuthor.lastValue().intValue());
        authors.put(authorID, buildKeyName("author", authorID));
      }

      db.insertBook(bookID, bookTitle, genres, intro, lang, authors);
    } finally {
      if (dotransactions) transactioninsertkeysequenceBook.acknowledge(bookID);
    }
  }

  public void doTransaction_InsertUser(onlineShopDB db) {
    int userID;
    if (dotransactions) {
      userID = transactioninsertkeysequenceUser.nextValue();
    } else {
      userID = userIDgenerator.nextValue().intValue();
    }
    try {
      String name = buildKeyName("user", userID);
      Date birth = randomDate();
      db.insertUser(userID, name, birth);
    } finally {
      if (dotransactions) transactioninsertkeysequenceUser.acknowledge(userID);
    }

  }

  public void doTransaction_InsertRecommendation(onlineShopDB db) {
    int userID;
    int bookID;
    if (dotransactions) {
      userID = nextKeynum(userIDchooser, transactioninsertkeysequenceUser.lastValue().intValue());
      bookID = nextKeynum(bookIDchooser, transactioninsertkeysequenceBook.lastValue().intValue());
    } else {
      userID = nextKeynum(LoaduserIDchooser, userCount);
      bookID = nextKeynum(LoadbookIDchooser, bookCount);
    }

    String comment = text.nextValue();
    Date createTime = randomDate();
    Random random = new Random();
    int stars = random.nextInt(5 - 1 + 1) + 1;
    int likes = random.nextInt(30 - 1 + 1) + 1;
    try {
      db.insertRecommendation(bookID, userID, stars, likes, comment, createTime);
    } finally {
    }
  }

  private void doTransaction_GetLatestRecommendations(onlineShopDB db) {
    int bookID = nextKeynum(bookIDchooser, transactioninsertkeysequenceBook.lastValue().intValue());
    Random random = new Random();
    int latestCount = random.nextInt(10 - 1 + 1) + 1;
    db.getLatestRecommendations(bookID, latestCount);
  }

  private void doTransaction_GetAllRecommendations(onlineShopDB db) {
    int bookID = nextKeynum(bookIDchooser, transactioninsertkeysequenceBook.lastValue().intValue());
    db.getAllRecommendations(bookID);
  }

  private void doTransaction_GetUsersRecommendations(onlineShopDB db) {
    int userID = nextKeynum(userIDchooser, transactioninsertkeysequenceUser.lastValue().intValue());
    db.getUsersRecommendations(userID);
  }

  private Status doTransaction_GetAuthorByID(onlineShopDB db) {
    int authorID = nextKeynum(authorIDchooser, transactioninsertkeysequenceAuthor.lastValue().intValue());
    return db.getAuthorByID(authorID);

  }

  private Status doTransaction_GetAuthorByBookID(onlineShopDB db) {
    int bookID = nextKeynum(bookIDchooser, transactioninsertkeysequenceBook.lastValue().intValue());
    return db.getAuthorByID(bookID);
  }

  private void doTransaction_FindBooksName(onlineShopDB db) {
    int bookID = nextKeynum(bookIDchooser, transactioninsertkeysequenceBook.lastValue().intValue());
    String bookName1 = buildKeyName("book", bookID);
    db.findBooksName(bookName1, 5);
  }

  private void doTransaction_FindBooksByGenre(onlineShopDB db) {
    String genre = genres.nextValue();
    db.findBooksByGenre(genre, 5);
  }

  private void doTransaction_UpdateBook(onlineShopDB db) {
    int bookID = nextKeynum(bookIDchooser, transactioninsertkeysequenceBook.lastValue().intValue());
    String intro = text.nextValue();
    String title = buildKeyName("book", bookID);
    String lang = language.nextValue();

    db.updateBook(bookID, title, lang, intro);
  }

  private void doTransaction_UpdateAuthor(onlineShopDB db) {
    int authorID = nextKeynum(authorIDchooser, transactioninsertkeysequenceAuthor.lastValue().intValue());
    String resume = text.nextValue();
    String authorName = buildKeyName("author", authorID);
    String sex = gender.nextValue();
    Date bDay = randomDate();

    db.updateAuthor(authorID, authorName, sex, bDay, resume);
  }

  private void doTransaction_UpdateRecommendation(onlineShopDB db) {
    int bookID = nextKeynum(bookIDchooser, transactioninsertkeysequenceBook.lastValue().intValue());
    int userID = nextKeynum(userIDchooser, transactioninsertkeysequenceUser.lastValue().intValue());
    String textNew = text.nextValue();
    Random random = new Random();
    int stars = random.nextInt(5 - 1 + 1) + 1;

    db.updateRecommendation(bookID, userID, stars, textNew);
  }

  private void doTransaction_DeleteBook(onlineShopDB db) {
    int bookID = nextKeynum(bookIDchooser, transactioninsertkeysequenceBook.lastValue().intValue());

    db.deleteBook(bookID);

  }

  private void doTransaction_DeleteAllRecommendationsBelongToBook(onlineShopDB db) {
    int bookID = nextKeynum(bookIDchooser, transactioninsertkeysequenceBook.lastValue().intValue());

    db.deleteAllRecommendationsBelongToBook(bookID);
  }

  private void doTransaction_DeleteAuthor(onlineShopDB db) {
    int authorID = nextKeynum(authorIDchooser, transactioninsertkeysequenceAuthor.lastValue().intValue());

    db.deleteAuthor(authorID);
  }


   /* -------------------------------------------transaction methods END---------------------------------------------------*/

  public static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
//TODO proportions in eine property packen und 2 mal drüber iterieren -> insert,und nochmal hinzufügen zum operationchooser
    final double insertUser_proportion = Double.parseDouble(p.getProperty(insertUser_PROPORTION_PROPERTY, insertUser_PROPORTION_PROPERTY_DEFAULT));
    final double insertAuthor_proportion = Double.parseDouble(p.getProperty(insertAuthor_PROPORTION_PROPERTY, insertAuthor_PROPORTION_PROPERTY_DEFAULT));
    final double insertBook_proportion = Double.parseDouble(p.getProperty(insertBook_PROPORTION_PROPERTY, insertBook_PROPORTION_PROPERTY_DEFAULT));
    final double insertRecommendation_proportion = Double.parseDouble(p.getProperty(insertRecommendation_PROPORTION_PROPERTY, insertRecommendation_PROPORTION_PROPERTY_DEFAULT));
    final double getLatestRecommendations_proportion = Double.parseDouble(p.getProperty(getLatestRecommendations_PROPORTION_PROPERTY, getLatestRecommendations_PROPORTION_PROPERTY_DEFAULT));
    final double getAllRecommendations_proportion = Double.parseDouble(p.getProperty(getAllRecommendations_PROPORTION_PROPERTY, getAllRecommendations_PROPORTION_PROPERTY_DEFAULT));
    final double getUsersRecommendations_proportion = Double.parseDouble(p.getProperty(getUsersRecommendations_PROPORTION_PROPERTY, getUsersRecommendations_PROPORTION_PROPERTY_DEFAULT));
    final double getAuthorByID_proportion = Double.parseDouble(p.getProperty(getAuthorByID_PROPORTION_PROPERTY, getAuthorByID_PROPORTION_PROPERTY_DEFAULT));
    final double getAuthorByBookID_proportion = Double.parseDouble(p.getProperty(getAuthorByBookID_PROPORTION_PROPERTY, getAuthorByBookID_PROPORTION_PROPERTY_DEFAULT));
    final double findBooksByGenre_proportion = Double.parseDouble(p.getProperty(findBooksByGenre_PROPORTION_PROPERTY, findBooksByGenre_PROPORTION_PROPERTY_DEFAULT));
    final double findBooksName_proportion = Double.parseDouble(p.getProperty(findBooksName_PROPORTION_PROPERTY, findBooksName_PROPORTION_PROPERTY_DEFAULT));
    final double updateAuthor_proportion = Double.parseDouble(p.getProperty(updateAuthor_PROPORTION_PROPERTY, updateAuthor_PROPORTION_PROPERTY_DEFAULT));
    final double updateBook_proportion = Double.parseDouble(p.getProperty(updateBook_PROPORTION_PROPERTY, updateBook_PROPORTION_PROPERTY_DEFAULT));
    final double updateRecommendation_proportion = Double.parseDouble(p.getProperty(updateRecommendation_PROPORTION_PROPERTY, updateRecommendation_PROPORTION_PROPERTY_DEFAULT));
    final double deleteBook_proportion = Double.parseDouble(p.getProperty(deleteBook_PROPORTION_PROPERTY, deleteBook_PROPORTION_PROPERTY_DEFAULT));
    final double deleteAllRecommendationsBelongToBook_proportion = Double.parseDouble(p.getProperty(deleteAllRecommendationsBelongToBook_PROPORTION_PROPERTY, deleteAllRecommendationsBelongToBook_PROPORTION_PROPERTY_DEFAULT));
    final double deleteAuthor_proportion = Double.parseDouble(p.getProperty(deleteAuthor_PROPORTION_PROPERTY, deleteAuthor_PROPORTION_PROPERTY_DEFAULT));


    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (insertUser_proportion > 0) {
      operationchooser.addValue(insertUser_proportion, insertUser_PROPORTION_PROPERTY);
    }

    if (insertAuthor_proportion > 0) {
      operationchooser.addValue(insertAuthor_proportion, insertAuthor_PROPORTION_PROPERTY);
    }

    if (insertBook_proportion > 0) {
      operationchooser.addValue(insertBook_proportion, insertBook_PROPORTION_PROPERTY);
    }

    if (insertRecommendation_proportion > 0) {
      operationchooser.addValue(insertRecommendation_proportion, insertRecommendation_PROPORTION_PROPERTY);
    }

    if (getLatestRecommendations_proportion > 0) {
      operationchooser.addValue(getLatestRecommendations_proportion, getLatestRecommendations_PROPORTION_PROPERTY);
    }
    if (getAllRecommendations_proportion > 0) {
      operationchooser.addValue(getAllRecommendations_proportion, getAllRecommendations_PROPORTION_PROPERTY);
    }
    if (getUsersRecommendations_proportion > 0) {
      operationchooser.addValue(getUsersRecommendations_proportion, getUsersRecommendations_PROPORTION_PROPERTY);
    }
    if (getAuthorByID_proportion > 0) {
      operationchooser.addValue(getAuthorByID_proportion, getAuthorByID_PROPORTION_PROPERTY);
    }
    if (getAuthorByBookID_proportion > 0) {
      operationchooser.addValue(getAuthorByBookID_proportion, getAuthorByBookID_PROPORTION_PROPERTY);
    }
    if (findBooksByGenre_proportion > 0) {
      operationchooser.addValue(findBooksByGenre_proportion, findBooksByGenre_PROPORTION_PROPERTY);
    }
    if (findBooksName_proportion > 0) {
      operationchooser.addValue(findBooksName_proportion, findBooksName_PROPORTION_PROPERTY);
    }
    if (updateAuthor_proportion > 0) {
      operationchooser.addValue(updateAuthor_proportion, updateAuthor_PROPORTION_PROPERTY);
    }
    if (updateBook_proportion > 0) {
      operationchooser.addValue(updateBook_proportion, updateBook_PROPORTION_PROPERTY);
    }
    if (updateRecommendation_proportion > 0) {
      operationchooser.addValue(updateRecommendation_proportion, updateRecommendation_PROPORTION_PROPERTY);
    }
    if (deleteBook_proportion > 0) {
      operationchooser.addValue(deleteBook_proportion, deleteBook_PROPORTION_PROPERTY);
    }
    if (deleteAllRecommendationsBelongToBook_proportion > 0) {
      operationchooser.addValue(deleteAllRecommendationsBelongToBook_proportion, deleteAllRecommendationsBelongToBook_PROPORTION_PROPERTY);
    }
    if (deleteAuthor_proportion > 0) {
      operationchooser.addValue(deleteAuthor_proportion, deleteAuthor_PROPORTION_PROPERTY);
    }
    return operationchooser;
  }

  int nextKeynum(NumberGenerator keychooser, int recordcount) {
    int keynum;
    if (keychooser instanceof ExponentialGenerator) {
      do {
        keynum = recordcount - keychooser.nextValue().intValue();
      } while (keynum < 0);
    } else {
      do {
        keynum = keychooser.nextValue().intValue();
      } while (keynum > recordcount);
    }
    return keynum;
  }

  public String buildKeyName(String objectName, long keynum) {
    if (!orderedinserts) {
      keynum = Utils.hash(keynum);
    }
    String value = Long.toString(keynum);
    int fill = value.length();
    String prekey = objectName;
    for (int i = 0; i < fill; i++) {
      prekey += '0';
    }
    return prekey + value;
  }

  protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
    NumberGenerator fieldlengthgenerator;
    String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
    int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
    String fieldlengthhistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);

    if (fieldlengthdistribution.compareTo("constant") == 0) {
      fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
    } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
      fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
    } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
      fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
    } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
      try {
        fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
      } catch (IOException e) {
        throw new WorkloadException(
          "Couldn't read field length histogram file: " + fieldlengthhistogram, e);
      }
    } else {
      throw new WorkloadException(
        "Unknown field length distribution \"" + fieldlengthdistribution + "\"");
    }
    return fieldlengthgenerator;
  }

/* -------------------------------------------random date ---------------------------------------------------*/

  public Date randomDate() {
    GregorianCalendar gc = new GregorianCalendar();

    int year = randBetween(1900, 2010);

    gc.set(Calendar.YEAR, year);

    int dayOfYear = randBetween(1, gc.getActualMaximum(Calendar.DAY_OF_YEAR));

    gc.set(Calendar.DAY_OF_YEAR, dayOfYear);

    String dateInString = gc.get(Calendar.DAY_OF_MONTH) + "/" + (gc.get(Calendar.MONTH) + 1) + "/" + gc.get(Calendar.YEAR);
    SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
    SimpleDateFormat formatter2 = new SimpleDateFormat("dd/M/yyyy");
    Date date = null;

    try {
      date = formatter.parse(dateInString);
    } catch (ParseException e) {
      try {
        date = formatter2.parse(dateInString);
      } catch (ParseException e1) {
        e1.printStackTrace();
      }
    }
    return date;
  }

  public static int randBetween(int start, int end) {

    return start + (int) Math.round(Math.random() * (end - start));
  }


}
