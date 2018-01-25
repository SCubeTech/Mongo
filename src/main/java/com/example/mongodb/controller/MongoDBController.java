package com.example.mongodb.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;

@Controller
public class MongoDBController {

	@RequestMapping(value = "/mongo", method = RequestMethod.GET)
	@ResponseBody
	public String mongoDBConnection(ModelMap model) {
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase database = mongoClient.getDatabase("test");

		MongoCollection people = database.getCollection("names");
		people.drop();

		Document document = new Document().append("str", "MongoDB Hello").append("int", 42).append("L", 1L)
				.append("embeddedDoc", new Document("x", 0)).append("list", Arrays.asList(1, 2, 3));

		people.insertOne(document);

		FindIterable<Document> iterable = people.find();
		iterable.forEach((com.mongodb.Block<? super Document>) document1 -> System.out.println(document1));

		document.remove("_id");
		people.insertOne(document);
		iterable = people.find();
		iterable.forEach((com.mongodb.Block<? super Document>) document1 -> System.out.println(document1));

		document = new Document().append("str", "MongoDB Hello111").append("int", 42).append("L", 1L)
				.append("embeddedDoc", new Document("x", 0)).append("list", Arrays.asList(1, 2, 3, 5, 6));

		people.insertOne(document);

		String s = null;
		iterable = people.find();
		iterable.forEach((com.mongodb.Block<? super Document>) document1 -> {
			String json = com.mongodb.util.JSON.serialize(document1);
			System.out.println(json);
		});

		return "Success \n" + s;
	}

	@RequestMapping(value = "/mongo/findexamples", method = RequestMethod.GET)
	@ResponseBody
	public String mongoDBFind(ModelMap model) {
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase database = mongoClient.getDatabase("test");
		MongoCollection sample = database.getCollection("names");
		sample.drop();
		List<Integer> listNum = new ArrayList<Integer>(10);
		for (int i = 0; i < 10; i++) {
			int j = i;
			sample.insertOne(new Document("x", i).append("y", j + 5).append("z", j + 10));
		}

		String s = "Success";

		System.out.println("Find and Print one");
		Document doc = (Document) sample.find().first();
		printJson s1 = doc1 -> com.mongodb.util.JSON.serialize(doc1);
		System.out.println(s1.docToString(doc));
		s = s1.docToString(doc);

		Bson filter = Filters.and(Filters.gt("x", 5), Filters.lte("x", 9));

		Bson projection = Projections.fields(Projections.include("x", "y"), Projections.excludeId());
		System.out.println("Find all with Into and Print all");
		List<Document> allDoc = (List<Document>) sample.find(filter).projection(projection)
				.into(new ArrayList<Document>());
		allDoc.forEach(doc1 -> System.out.println(com.mongodb.util.JSON.serialize(doc1)));
		s = s + s1.docToString(doc);

		System.out.println("Find with Iterable");
		MongoCursor<Document> cursor = sample.find().iterator();
		try {
			while (cursor.hasNext()) {
				Document cur = cursor.next();
				printJson s2 = doc1 -> com.mongodb.util.JSON.serialize(doc1);
				System.out.println(s2.docToString(cur));
			}
		} finally {
			cursor.close();
		}

		System.out.println("Find the count");
		long count = sample.count();
		System.out.println(count);

		return s;

	}

	@RequestMapping(value = "/momngo/updateexamples", method = RequestMethod.GET)
	@ResponseBody
	public String mongoUpdate(ModelMap model) {

		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase database = mongoClient.getDatabase("test");
		MongoCollection sample = database.getCollection("names");

		sample.drop();

		for (int i = 0; i < 10; i++) {
			sample.insertOne(new Document().append("x", i).append("y", i).append("z", true));
		}
		StringBuffer s = new StringBuffer();
		s.append("S");
		List<Document> allDoc = (List<Document>) sample.find().limit(1000).into(new ArrayList<Document>());
		allDoc.forEach(doc1 -> {
			s.append(com.mongodb.util.JSON.serialize(doc1) + "/n");
			// System.out.println(s);
		});
		System.out.println(s);
		sample.updateOne(Filters.eq("x", 1),
				Updates.combine(Updates.set("z", false), Updates.set("abc", "inserted new")));

		allDoc = (List<Document>) sample.find().limit(1000).into(new ArrayList<Document>());
		allDoc.forEach(doc1 -> {
			s.append(com.mongodb.util.JSON.serialize(doc1) + "/n");

		});
		System.out.println(s);
		return s.toString();

	}

	@RequestMapping(value = "/mongo/delteexample", method = RequestMethod.GET)
	@ResponseBody
	public String delteExample(ModelMap model) {
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase database = mongoClient.getDatabase("students");
		MongoCollection sample = database.getCollection("grades");
		for (int i = 0; i < 200; i++) {
			Document doc = (Document) sample
					.find(Filters.and(Filters.eq("student_id", i), Filters.eq("type", "homework")))
					.sort(Sorts.orderBy(Sorts.ascending("student_id"), Sorts.ascending("score"))).first();
			System.out.println(doc.toString());
			sample.deleteOne(doc);
		}

		return "";
	}

	@RequestMapping(value = "/mongo/deleteHomeWork", method = RequestMethod.GET)
	@ResponseBody
	public String delteExampleWeek2(ModelMap model) {
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase database = mongoClient.getDatabase("school");
		MongoCollection<Document> sample = database.getCollection("students");
		for (int i = 1; i <= 200; i++) {
			Bson proj = Projections.fields(Projections.excludeId(), Projections.include("scores"));
			Bson filt = Filters.eq("_id", i);
			FindIterable<Document> iterable = (FindIterable) sample.find(filt).projection(proj);
			iterable.forEach((com.mongodb.Block<? super Document>) document1 -> {
				System.out.println(document1.toString());
				Document docBefore = document1;
				Document after = document1;
				List<Document> scoresList = (ArrayList) document1.get("scores");
				System.out.println("ScoresList before sorting"+scoresList.toString());
				 Double lowestScore = new Double(0);
				 Document dbObject = null;
				for (Object doc : scoresList) {
	                Document basicDBObject = (Document) doc;
	                if (basicDBObject.get("type").equals("homework")) {
	                    Double latestScore = (Double) basicDBObject
	                            .get("score");
	                    if (lowestScore.compareTo(Double.valueOf(0)) == 0) {
	                        lowestScore = latestScore;
	                        dbObject = basicDBObject;

	                    } else if (lowestScore.compareTo(latestScore) > 0) {
	                        lowestScore = latestScore;
	                        dbObject = basicDBObject;
	                    }
	                }
	            }
				System.out.println("object to be removed : " + dbObject + ":"
	                    + scoresList.remove(dbObject));
				document1.remove("scores");
				document1.put("scores", scoresList);
				
				System.out.println("ScoresList after sorting"+scoresList.toString());
				
				sample.updateOne(filt, Updates.set("scores", scoresList));
			});
			
			iterable = (FindIterable) sample.find(filt).projection(proj);
			iterable.forEach((com.mongodb.Block<? super Document>) document1 -> 
				System.out.println(document1.toString()));
		}
		return "";
	}

	@FunctionalInterface
	interface printJson {
		String docToString(Document doc);
	}

}
