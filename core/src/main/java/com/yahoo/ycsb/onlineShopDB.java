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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public abstract class onlineShopDB extends DB {



  public abstract Status insertUser(int userID, String userName, Date birthDate);


  public abstract Status insertAuthor(int authorID, String authorFullName, String gender, Date birthDate, String resume);


  public abstract Status insertBook(int bookID, String bookTitle, ArrayList<String> genres, String introductionText, String language, HashMap<Integer, String> authors) ;


 // public abstract Status insertRecommendationBundle(int bookID, int authorID, int recommendCount, String bookTitle, String authorName);


  public abstract Status insertRecommendation(int bookID, int userID, int stars, int likes, String text, Date createTime);


  public abstract Status getLatestRecommendations(int bookID, int limit);


  public abstract Status getAllRecommendations(int bookID);


  public abstract Status getUsersRecommendations(int userID);


  public abstract Status getAuthorByID(int authorID);


  public abstract Status findBooksByGenre(String genreList, int limit);


  public abstract Status findBooksName(String bookName, int limit);


  public abstract Status findAuthorByBookID(int bookID);


  public abstract Status updateBook(int bookID, String title, String language, String introduction);


  public abstract Status updateRecommendation(int bookID, int userID, int stars, String text);


  public  abstract Status updateAuthor(int authorID, String authorName, String gender, Date birthDate, String resume);

  //public abstract Status deleteBookReferenceFromAuthorList(int authorID, int bookID);


  public abstract Status deleteBook(int bookID);


  //public abstract Status deleteBookReferenceFromAuthor(int bookID);


  public abstract Status deleteAllRecommendationsBelongToBook(int bookID);


  public abstract Status deleteAuthor(int authorID);



  /**
   * db.colB.find({releaseDate: {$gte: relDate},rating: {$gt:minRating}})
   * @param colB collection book
   * @param relDate start date
   * @param minRating minimum stars
   * @param limit limit found books
   * @return return Books which have minimum rating and are published after date
   *//*
  public abstract Status findBooksByRatingbyReleaseDate(String releaseDate,
                                                        double minRating,
                                                        int limit);
   */

}