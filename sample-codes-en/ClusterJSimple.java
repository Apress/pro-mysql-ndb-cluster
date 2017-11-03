import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

import java.io.*;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class ClusterJSimple {

    public static void main (String[] args)
        throws java.io.FileNotFoundException,
               java.io.IOException,
               com.mysql.clusterj.ClusterJException {

        // Step 2. Load properties from file
        File propsFile = new File("clusterj.properties");
        InputStream inStream = new FileInputStream(propsFile);
        Properties props = new Properties();
        props.load(inStream);

        // Step 3. Get a session instance
        SessionFactory factory = ClusterJHelper.getSessionFactory(props);
        Session session = factory.getSession();

        // Step 4. Create a new City instance and add one row.
        City newCity = session.newInstance(City.class);
        newCity.setId(4080);
        newCity.setName("Tochigi");
        newCity.setDistrict("Tochigi");
        newCity.setCountryCode("JPN");
        newCity.setPopulation(140000);
        session.persist(newCity);
        System.out.println("Saved Tochigi-shi.");

        // Step 5. Find a row with ID = 1532
        City whatsThis = session.find(City.class, 1532);
        System.out.println("Name of city where ID = 1532 is "
                           + whatsThis.getName().trim() + ".");

        // Step 6. Find all rows with CountryCode = "JPN". Watch Step 9
        List<City> cities = findByCountryCode(session, "JPN");
        System.out.println("Cities in Japan.");
        int n = 1;
        for (City c: cities) {
            System.out.println((n++) + ":" + c.getName().trim());
        }

        // Step 7. Updating a row: increment population of Tokyo by 1000000
        City tokyo = whatsThis;
        tokyo.setPopulation(tokyo.getPopulation() + 1000000);
        session.updatePersistent(tokyo);

        // Step 8. Delete a row
        City tochigi = session.newInstance(City.class);
        tochigi.setId(4080);
        session.deletePersistent(tochigi);
        System.out.println("Deleted Tochigi-shi");
    }

    // Step 9. Scan example using a query builder
    static List<City> findByCountryCode(Session session, String cc)
        throws com.mysql.clusterj.ClusterJException {
        QueryBuilder builder = session.getQueryBuilder();
        QueryDomainType<City> domain =
            builder.createQueryDefinition(City.class);
        domain.where(domain.get("countryCode")
                     .equal(domain.param("cc")));
        Query<City> query = session.createQuery(domain);
        query.setParameter("cc", cc);
        printExplain(query);
        return query.getResultList();
    }

    // 10. Print execution plan
    static <T> void printExplain(Query<T> q) {
        Map<String, Object> explain = q.explain();
        for (String k: explain.keySet()) {
            System.err.println(k + ":" + explain.get(k).toString());
        }
    }
}
