package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.BasicDBObject;
import org.bson.Document;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;
public class TimelineServlet extends HttpServlet {

    // HBASE Setting
    private static String zkAddr = "172.31.10.41";
    private static String tableName1 = "task2";
    private static String tableName2 = "task4";
    private static HTableInterface q2;
    private static HTableInterface q4;
    private static HConnection conn;
    private static byte[] bColFamily = Bytes.toBytes("followers");

    // MySQL Setting
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://xinaoccp33.cgzqmgiuvsjf.us-east-1.rds.amazonaws.com:3306/xinaop33";
    static final String USER = "root";
    static final String PASSWORD = "xinaoroot";
    static Connection conn1 = null;
    static{
        try{
            Class.forName("com.mysql.jdbc.Driver");
            conn1 = DriverManager.getConnection(DB_URL, USER, PASSWORD);
        }catch (Exception e){
            System.out.println("DB Connection Error");
        }
    }

    // Mongo Settting
    static MongoClient mongoClient = new MongoClient("ec2-54-165-84-71.compute-1.amazonaws.com");

    public TimelineServlet() throws Exception {
        /*
            Your initialization code goes here
        */
    }

    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        String id = request.getParameter("id");

        Statement s1 = null;
        Statement s2 = null;
        String url = null;
        String name = null;

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }


        /*
            Task 4 (1):
            Get the name and profile of the user as you did in Task 1
            Put them as fields in the result JSON object
        */
        try{
            s2 = conn1.createStatement();
            String q2 = "SELECT * FROM userinfo WHERE id="+id+";";
            ResultSet rs2 = s2.executeQuery(q2);
            while(rs2.next()){
                name = rs2.getString("name");
                url = rs2.getString("url");
            }
            s2.close();  
        } catch(Exception e) {
            System.out.println("Error Get user data");
        } 

        result.put("name", name);
        result.put("profile", url);

        /*
            Task 4 (2);
            Get the follower name and profiles as you did in Task 2
            Put them in the result JSON object as one array
        */

        conn = HConnectionManager.createConnection(conf);
        q2 = conn.getTable(Bytes.toBytes(tableName1));
        Get getReq = new Get(Bytes.toBytes(id));
        Result result1 = q2.get(getReq);
        byte[] bCol = Bytes.toBytes("follower_ids");
        String ids = Bytes.toString(result1.getValue(bColFamily, bCol));

        JSONArray followers = new JSONArray();
        try{
            s1 = conn1.createStatement();
            String q1 = "SELECT * FROM userinfo WHERE id IN ("+ ids.replace(" ", ", ") +") ORDER BY BINARY name ASC, BINARY url ASC;";
            System.out.println(q1);
            ResultSet rs1 = s1.executeQuery(q1);
            while(rs1.next()){
                JSONObject follow = new JSONObject();
                follow.put("name", rs1.getString("name"));
                follow.put("profile", rs1.getString("url"));
                followers.put(follow);
            }
            s1.close();

        } catch(Exception e) {
            System.out.println("Error Get user data");
        } 

        result.put("followers", followers);

        /*
            Task 4 (3):
            Get the 30 LATEST followee posts and put them in the
            result JSON object as one array.

            The posts should be sorted:
            First in ascending timestamp order
            Then numerically in ascending order by their PID (PostID)
        if there is a tie on timestamp
        */

        conn = HConnectionManager.createConnection(conf);
        q4 = conn.getTable(Bytes.toBytes(tableName2));
        Get getReq1 = new Get(Bytes.toBytes(id));
        Result result2 = q4.get(getReq1);
        byte[] bCol1 = Bytes.toBytes("ids");
        String ids1 = Bytes.toString(result2.getValue(Bytes.toBytes("followees"), bCol1));

        MongoDatabase db = mongoClient.getDatabase("posts");

        String[] uid_str = ids1.split(" ");

        int[] uid_list= new int[uid_str.length];
        for (int i = 0; i < uid_str.length; i++) {
            try {
                uid_list[i] = Integer.parseInt(uid_str[i]);
            } catch (NumberFormatException nfe) {

            };
        }

        List<JSONObject> posts = new ArrayList<JSONObject>();

        for (Document cur : db.getCollection("post").find(new BasicDBObject("uid", new BasicDBObject("$in", uid_list))).sort(descending("timestamp", "pid")).limit(30)){
            posts.add(new JSONObject(cur.toJson()));
        }

        Collections.reverse(posts);

        result.put("posts", posts);

        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

}
