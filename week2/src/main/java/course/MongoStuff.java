package course;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class MongoStuff {

    public static void main(String[] args) {
        final MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost"));

        final MongoDatabase blogDatabase = mongoClient.getDatabase("students");
        final MongoCollection<Document> usersCollection;
        usersCollection = blogDatabase.getCollection("grades");

        Document query = new Document("type", "homework");

        List<Document> documents = usersCollection.find(query).sort(new Document("student_id", 1).append("score", 1))
                .into(new ArrayList<>());

        List<Document> remove = new ArrayList<>();
        int student = -1;
        for(Document document: documents) {
            if ((int)document.get("student_id") != student) {
                remove.add(document);
                student = (int)document.get("student_id");
            }
        }
        System.out.println(remove.size());
        for(Document document: remove) {
            usersCollection.deleteOne(document);
        }

    }

    public static void json(Document document) {
        JsonWriter jsonWriter = new JsonWriter(new StringWriter(), new JsonWriterSettings(JsonMode.SHELL, true));
        new DocumentCodec().encode(jsonWriter, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        System.out.println(jsonWriter.getWriter());
        System.out.flush();
    }

}
