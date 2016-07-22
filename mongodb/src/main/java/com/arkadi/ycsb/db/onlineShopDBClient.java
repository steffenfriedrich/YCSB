package com.arkadi.ycsb.db;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.db.OptionsSupport;
import com.yahoo.ycsb.onlineShopDB;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class onlineShopDBClient extends onlineShopDB {


  protected static final Integer INCLUDE = 1;
  protected static final InsertManyOptions INSERT_UNORDERED = new InsertManyOptions().ordered(false);
  protected static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  protected static String databaseName;
  protected static MongoDatabase database;
  protected static MongoClient mongoClient;
  protected static ReadPreference readPreference;
  protected static WriteConcern writeConcern;
  protected static int batchSize;
  protected static boolean useUpsert;
  protected List<Document> BULKINSERT_B = new ArrayList<>();
  protected List<Document> BULKINSERT_A = new ArrayList<>();


  public void init() {

    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {

      if (mongoClient != null) return;
      Properties props = getProperties();
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));
      useUpsert = Boolean.parseBoolean(props.getProperty("mongodb.upsert", "false"));
      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }
      url = OptionsSupport.updateUrl(url, props);
      if (!url.startsWith("mongodb://")) {
        System.err.println("ERROR: Invalid URL: '" + url);
        System.exit(1);
      }

      try {
        MongoClientURI uri = new MongoClientURI(url);
        String uriDb = uri.getDatabase();
        databaseName = "bookStore";
        if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty() && !"admin".equals(uriDb)) {
          databaseName = uriDb;
        }
        readPreference = uri.getOptions().getReadPreference();
        writeConcern = uri.getOptions().getWriteConcern();
        mongoClient = new MongoClient(uri);
        database = mongoClient.getDatabase(databaseName).withReadPreference(readPreference).withWriteConcern(writeConcern);
        System.out.println("mongo client connection created with " + url + "\n readPreference" + readPreference + "\n writeConcern" + writeConcern);

      } catch (Exception e1) {
        System.err.println("Could not initialize MongoDB connection pool for Loader: " + e1.toString());
        e1.printStackTrace();
      }
    }
  }

 /*----------------------------------------------insert operations----------------------------------------------------*/

  @Override
  public Status insertUser(int userID, String userName, Date birthDate) {


    try {
      MongoCollection<Document> collectionU = database.getCollection("users");

      Document toInsertUser = new Document("_id", userID)
        .append("userName", userName)
        .append("birthDate", birthDate);

      if (batchSize == 1) {
        collectionU.insertOne(toInsertUser);
      } else {
        BULKINSERT_A.add(toInsertUser);
        if (BULKINSERT_A.size() >= batchSize || BULKINSERT_B.size() >= batchSize) {
          collectionU.insertMany(BULKINSERT_A, INSERT_UNORDERED);
        }
      }


    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
        + BULKINSERT_A.size() + BULKINSERT_B.size());
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * db.author.insertOne(toInsertAuthor)
   */
  @Override
  public Status insertAuthor(int authorID, String authorFullName, String gender, Date birthDate, String resume) {

    try {
      //MongoCollection<Document> collectionA = database.getCollection("authors");

      Document toInsertAuthor = new Document("_id", authorID)
        .append("authorFullName", authorFullName)
        .append("gender", gender)
        .append("birthDate", birthDate)
        .append("resume", resume);


      if (batchSize == 1) {
        database.getCollection("authors").insertOne(toInsertAuthor);
      } else {
        BULKINSERT_A.add(toInsertAuthor);
        if (BULKINSERT_A.size() >= batchSize || BULKINSERT_B.size() >= batchSize) {
          database.getCollection("authors").insertMany(BULKINSERT_A, INSERT_UNORDERED);
        }
      }


    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
        + BULKINSERT_A.size() + BULKINSERT_B.size());
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * db.book.insertOne(toInsertBook)
   */
  @Override
  public Status insertBook(int bookID, String bookTitle, ArrayList<String> genres, String introductionText, String language, HashMap<Integer, String> authors) {
    try {
      List<Document> array = new ArrayList<>();
      for (Map.Entry<Integer, String> entry : authors.entrySet()) { // author injection
        array.add(new Document("_id", entry.getKey()).append("authorFullName", entry.getValue()));
      }

      Document toInsertBook = new Document("_id", bookID)
        .append("title", bookTitle)
        .append("genres", genres)
        .append("language", language)
        .append("introductionText", introductionText)
        .append("authors", array);


      //Keine möglichkeit buch aus author zu löschen ohne das buch zu löschen und auch anders rum beides soll gleichzeitig passieren also keine extra methode

      //System.out.println(toInsertBook);
      database.getCollection("books").insertOne(toInsertBook);
      insertRecommendationBundle(bookID, bookTitle);
      Document update = new Document("$push", new Document("booksPublished", new Document("_id", bookID).append("title", bookTitle)));
      for (Map.Entry<Integer, String> entry : authors.entrySet()) {
        database.getCollection("authors").updateOne(new Document("_id", entry.getKey()), update);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  //wird nur vom client benutzt und kann nicht von ausen angewand werden

  /**
   * db.recommendations.insertOne(toInsertRecSlot)
   */
  private Status insertRecommendationBundle(int bookID, String bookTitle) {

    try {
      MongoCollection<Document> collectionRecommendation = database.getCollection("recommendations");

      Document toInsertRecommendBundle = new Document("_id", bookID)
        .append("recommendCount", 0)
        .append("ratingAverage", 0)
        .append("bookTitle", bookTitle);

      collectionRecommendation.insertOne(toInsertRecommendBundle);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  /**
   * db.recommendations.updateOne({_id: recommendationBundleID},{$push:{recommendations:{values}})
   */
  @Override
  public Status insertRecommendation(int bookID, int userID, int stars, int likes, String text, Date createTime) {

    Document query = new Document("_id", bookID);
    Document query2 = new Document("_id", userID);
    Document toInsertRecommendation = new Document("_id", userID)
      .append("createTime", createTime)
      .append("stars", stars)
      .append("likes", likes)
      .append("text", text);
    database.getCollection("recommendations").updateOne(query, new Document("$push", new Document("recommendations", toInsertRecommendation)));
    database.getCollection("users").updateOne(query2, new Document("$push", new Document("bookRecommended", bookID)));
    return Status.OK;
  }


/**
 db.authors.updateOne({_id: authorID},{$push:{bookPublished:{_id: bookID,bookName:bookName}}})

 public Status insertBookReferenceInAuthor(int authorID, int bookID, String bookName) {
 Document query = new Document("_id", authorID);
 Document update = new Document("$push", new Document("bookPublished", new Document("_id", bookID).append("bookName", bookName)));
 UpdateResult result = database.getCollection("authors").updateOne(query, update);

 return Status.OK;
 }
 */

  /*----------------------------------------------get operations -----------------------------------------------------*/

  /**
   * db.recommendations.find({"_id":recommendationBundleID}).sort({"recommendations._id",-1}).limit(amount)
   */
  @Override
  public Status getLatestRecommendations(int bookID, int limit) {
    Document query = new Document("_id", bookID);
    Document sortKrit = new Document("recommends._id", -1);
    database.getCollection("recommendations").find(query).sort(sortKrit).limit(limit);

    return Status.OK;
  }

  /**
   * db.recommendations.find({_id: recommendationBundleID}).first()
   */
  @Override
  public Status getAllRecommendations(int bookID) {
    Document query = new Document("_id", bookID);
    database.getCollection("recommendations").find(query).first();

    return Status.OK;
  }

  /**
   * db.authors.find({_id:authorID}).first()
   */
  @Override
  public Status getAuthorByID(int authorID) {
    Document query = new Document("_id", authorID);
    database.getCollection("authors").find(query).first();
    return Status.OK;
  }

  /**
   * db.books.find({genres:{ $all: [genreList[0],genreList[n]]}}).limit(max)
   */
  @Override
  public Status findBooksByGenre(String genreList, int limit) {
    String[] genres = genreList.split(",");
    Document query = new Document("genre", new Document("$all", genres));
    database.getCollection("books").find(query).limit(limit);
    return Status.OK;
  }


  /**
   * db.users.find({_id: userID},{booksRecommended: 1})
   */
  @Override
  public Status getUsersRecommendations(int userID) {
    Document query = new Document("_id", userID);
    Document booksID = database.getCollection("users").find(query).first();
    int[] booksIDs = (int[]) booksID.get("booksRecommended");
    final List<Document> userRecommends = new LinkedList<>();

    if (booksIDs != null) {
      for (int book : booksIDs) {
        userRecommends.add(database.getCollection("recommendations").find(new Document("_id", book).append("recommends.userID", userID)).first());
      }
      return Status.OK;
    }
    return Status.OK;
  }

  /*----------------------------------------------find operations ----------------------------------------------------*/

  /**
   * db.books.find({"name": bName}.limit(max)
   */
  @Override
  public Status findBooksName(String bookName, int limit) {
    database.getCollection("books").find(new Document("name", bookName)).limit(limit);
    return Status.OK;
  }

  /**
   * Resultauthor  = db.books.find({_id:bookID}{author:1}
   * db.authors.find(Resultauthor)
   */
  @Override
  public Status findAuthorByBookID(int bookID) {
    Document query = new Document("_id", bookID);
    Document projection = new Document("author", 1).append("_id", 0);
    Document resultAuthor = (Document) database.getCollection("books").find(query).projection(projection);
    database.getCollection("authors").find(resultAuthor).first();
    return Status.OK;

  }
/*----------------------------------------------update operations --------------------------------------------------*/

  /**
   * db.books.updateOne({_id: bookID},{$set:bookValues})
   */
  @Override
  public Status updateBook(int bookID, String title, String language, String introduction) {
    Document query = new Document("_id", bookID);
    Document update = new Document("_id", bookID)
      .append("title", title)
      .append("language", language)
      .append("introduction", introduction);

    database.getCollection("books").updateOne(query, new Document("$set", update));


    return Status.OK;
  }

  /**
   * db.recommendations.updateOne({_id: recommendationBundleID,"recommendations._id": userID},
   * {$set: {"recommendations.stars": stars,"recommendations.text":text}}
   */
  @Override
  public Status updateRecommendation(int bookID, int userID, int stars, String text) {
    Document query = new Document("_id", bookID).append("recommendations._id", userID);
    Document update = new Document("recommendations.stars", stars).append("recommendations.text", text);
    database.getCollection("recommendations").updateOne(query, (new Document("$set", update)));
    //database.getCollection("recommendations").updateOne(query, (new Document("$set", new Document("recommendations.text", text))));

    return Status.OK;
  }

  @Override
  public Status updateAuthor(int authorID, String authorName, String gender, Date birthDate, String resume) {
    Document query = new Document("_id", authorID).append("authorName", authorName);
    Document update = new Document("gender", gender).append("birthDate", birthDate).append("resume", resume);
    database.getCollection("authors").updateOne(query, new Document("$set", update));

    return Status.OK;
  }



  /*----------------------------------------------delete Operations ---------------------------------------------------*/


  /**
   * db.books.findOne({_id: _bookID},{author:1})
   * db.colA.updateOne({_id: _authorID,},{$pull:{bookPublished:{_id: bookID}})
   */
  public Status deleteBookReferenceFromAuthor(int bookID) {
    Document queryBook = new Document("_id", bookID);
    Document projection = new Document("author", 1);
    Document foundAuthor = database.getCollection("books").find(queryBook).projection(projection).first();
    Document pullBook = new Document("$pull", new Document("bookPublished", queryBook));
    database.getCollection("authors").updateOne(foundAuthor, pullBook);

    return Status.OK;
  }


  /**
   * db.books.deleteOne({_id: bookID}}
   */
  @Override
  public Status deleteBook(int bookID) {
    Document queryBook = new Document("_id", bookID);
    database.getCollection("books").deleteOne(queryBook);
    deleteBookReferenceFromAuthorList(bookID);
    return Status.OK;
  }

  /**
   * db.authors.updateOne({_id: _authorID,},{$pull:{bookPublished:{_id: bookID}})
   */

  public Status deleteBookReferenceFromAuthorList(int bookID) {
    Document query = new Document("_id", bookID);
    Document project = new Document("authors._id", 1).append("_id", 0);
    Document authors = database.getCollection("books").find(query).projection(project).first();

    Document update = new Document("$pull", new Document("bookPublished", new Document("_id", bookID)));
    UpdateResult result = database.getCollection("authors").updateMany(query, update);

    return Status.OK;
  }

  /**
   * db.recommendations.deleteOne({_id: bookID})
   */
  @Override
  public Status deleteAllRecommendationsBelongToBook(int bookID) {
    Document query = new Document("_id", bookID);
    database.getCollection("recommendations").deleteOne(query);

    return Status.OK;
  }

  /**
   * db.authors.deleteOne({_id: authorID})
   */
  @Override
  public Status deleteAuthor(int authorID) {
    Document query = new Document("_id", authorID);
    database.getCollection("authors").deleteOne(query);

    return Status.OK;
  }



  /*----------------------------------------------Deprecated operations-----------------------------------------------*/


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

