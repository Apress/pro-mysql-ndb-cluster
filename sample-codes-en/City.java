import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

// Step 1. Define an interface
@PersistenceCapable(table="City")
public interface City {
    @PrimaryKey
    @Column(name="ID")
    int getId();
    void setId(int id);
 
    @Column(name="Name")
    String getName();
    void setName(String name);

    @Column(name="District")
    String getDistrict();
    void setDistrict(String district);
    
    @Column(name="CountryCode")
    @Index(name="CountryCode")
    String getCountryCode();
    void setCountryCode(String countryCode);

    @Column(name="Population")
    int getPopulation();
    void setPopulation(int population);
}
