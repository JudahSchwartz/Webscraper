import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;


import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

 class main {


     public static void main(String[] args) throws IOException, InterruptedException {
        WebScraper ws = new WebScraper("http://www.touro.edu/","[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+");
        Set<String> set = ws.getReg(10_000);

        addToDB(set);
        System.exit(0);
     }

     private static void addToDB(Set<String> set) throws InterruptedException {
         try {
             Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
             System.out.println("Driver Successfully Loaded!");
             String driver = "jdbc:sqlserver:";

             String url = "mco364.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com";
             String port = "1433";
             String username = "un";
             String password ="pw";
             String database = "SCHWARTZ";
             String connection = String.format(
                     "%s//%s:%s;databaseName=%s;user=%s;password=%s;",
                     driver, url, port, database, username, password);
             try (Connection connect = DriverManager.getConnection(connection))
             {
                 System.out.println("Connected to Database!");
                 //createEmailsTable(connect);
                 insertEmails(set, connect);
             }
             System.out.println("Database Closed!");
         } catch (ClassNotFoundException ex) {
             System.out.println("Error: Driver Class not found.");
             ex.printStackTrace();
         } catch (SQLException sqlex) {
             System.out.println("Error: SQL Error");
             sqlex.printStackTrace();

         }
     }
     private static void insertEmails(Set<String> emails, Connection connect) throws SQLException, InterruptedException {
         final String sql = "insert into Emails(Email) values (?)";
         //maybe could be faster if spliced the set and used threads?
         PreparedStatement query = connect.prepareStatement(sql);
         for (String s : emails) {
             query.setString(1, s);
             query.addBatch();

         }

         query.executeBatch();
     }
     private static void createEmailsTable(Connection connect) throws SQLException {
         PreparedStatement query = connect.prepareStatement(
                 "CREATE TABLE Emails (\n" +
                         "    Email VARCHAR(100) ,\n" +
                         "    id int IDENTITY);"

         );
         query.executeUpdate();
     }
 }
public class WebScraper {

    private final Queue<String> links = new ConcurrentLinkedQueue<>();
    private final Set<String> visitedLinks = ConcurrentHashMap.newKeySet();
    private final Set<String> emails = ConcurrentHashMap.newKeySet();

    private final Pattern emailRegex;


    WebScraper(String StartingURL, String regex) {

        emailRegex = Pattern.compile(regex);
        links.add(StartingURL);
    }

    public Set<String> getReg(int num) throws InterruptedException {
        ExecutorService threadPool = Executors.newFixedThreadPool(1000);

        while (emails.size() < num) {

//            System.out.println("links: " + links.size() + '\n' +
//                    "visited: " + visitedLinks.size() + '\n' +
//                    "emails: " + emails.size());
            threadPool.submit(() -> {
                String url = links.remove();
                Document doc = null;
                try {
                    doc = Jsoup.connect(url).get();
                } catch (IOException e) {
                    //e.printStackTrace();
                }
                if (doc != null) {
                    //      threadPool.submit(() -> {
                    addLinks(doc);

                    //    });
                    //  threadPool.submit(() -> {
                    addEmails(doc,num);
                    //});

                }
            });

            int c = 0;
            while (links.size() == 0) {
                c++;
                Thread.sleep(10);
                if (c == 10000) {
                    System.exit(1);
                    throw new RuntimeException("Internet connection is out");

                }
            }
        }
        threadPool.shutdownNow();
        return emails;
    }

    private void addEmails(Document doc,int num) {


        Matcher matcher = emailRegex.matcher(doc.body().text());
        while (matcher.find()) {
            if(emails.add(matcher.group().toLowerCase()))
                System.out.printf("%%%.2f of %d emails collected.\n",emails.size()*1.0/num,num);
        }
    }

    private void addLinks(Document doc) {
        for (String link : doc.select("a[href]").stream().map(e -> e.attr("abs:href")).collect(Collectors.toList())) {
            synchronized (links) {
                if (visitedLinks.add(link))
                    links.add(link);

            }
        }

    }
}
