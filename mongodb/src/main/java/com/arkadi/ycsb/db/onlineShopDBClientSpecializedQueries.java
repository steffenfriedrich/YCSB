package com.arkadi.ycsb.db;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.yahoo.ycsb.Status;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;


public class onlineShopDBClientSpecializedQueries extends onlineShopDBClient {


  /**
   * db.colR.aggregate([{"$match":{"_id": recommendationBundleID }},
   * {"$unwind": "$recommendations"},
   * {"$match": {"recommendations.likes": {"$gt":recRating}}},
   * {"$project": {"_id":0,"recommendations":1}},
   * {"$sort": {"recommendations.likes":1}},
   * {"$limit": 20}
   * ])
   */
  public Status findRecommendationsByRating(int bookID, double recRating) {
    Bson queryDoc = new Document("$match", new Document("_id", bookID));
    Bson queryEmbedDoc = new Document("$match", new Document("recommendations.likes", new Document("$gt", recRating)));
    Bson unwind = new Document("$unwind", "$recommendations");
    Bson project = new Document("_id", 0).append("recommendations", 1)
      .append("$sort", new Document("recommendations.likes", 1))
      .append("$limit", 20);

    Bson[] array = {queryDoc, unwind, queryEmbedDoc, project};
    MongoCollection collection = database.getCollection("recommendations");
    collection.aggregate(new ArrayList<>(Arrays.asList(array)));

    return Status.OK;
  }

  /**
   * db.books.ensureIndex({introduction:"text"})
   * db.books.find({$text:{$search:searchText}})
   */
  public Status findBooksByTextMatch(String searchText) {
    database.getCollection("books").createIndex(new Document("introduction", "text"));
    database.getCollection("books").find(new Document("$text", new Document("$search", searchText)));

    return Status.OK;
  }


  /**
   * db.authors.find({fullName: authorName,booksPublished:{$exists:true},booksPublished: {$size:{$gt:3}}})
   */
  public Status findHDAuthor(String authorName, int bookWritten) {
    FindIterable<Document> results = database.getCollection("authors")
      .find(new Document("fullName", authorName)
        .append("booksPublished", new Document("$exists", true))
        .append("booksPublished", new Document("$size", new Document("$gt", 3))));

    return Status.OK;
  }
}
