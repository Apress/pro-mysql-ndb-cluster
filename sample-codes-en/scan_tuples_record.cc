#include <NdbApi.hpp>
#include <iostream>
#include <string>
#include <stddef.h>
#include <cstdint>
#include <cstring>

const char *connectstring = "127.0.0.1:1186";
const char *db = "world";

class NdbApiExample3 {
public:
  NdbApiExample3() : cluster_connection(NULL), myNdb(NULL),
                     myDict(NULL), myTable(NULL) {};
  ~NdbApiExample3();
  int doTest();
  
private:
  struct CityRow {
    Int32 ID;
    char  Name[35];
    char  CountryCode[3];
    char  District[20];
    Int32 Population;
  };
  
  std::string char_to_str(char *s, int max_len)
  {
    std::string str(s, max_len);
    return str.substr(0, str.find_last_not_of(" ") + 1);
  }
  
  void print_city(CityRow *city)
  {
    std::cout << "Id: " << city->ID
              << ", Name: " << char_to_str(city->Name, 35)
              << ", Code: " << char_to_str(city->CountryCode, 3)
              << ", District: " << char_to_str(city->District, 20)
              << ", Population: " << city->Population
              << std::endl;
  }
  
  int do_scan_read();
  int do_index_scan_read();
  int do_scan_update();
  
  void print_error(const NdbError &e, const char *msg)
  {
    std::cerr << msg << ": Error code (" << e.code << "): "
              << e.message << "." << std::endl;
  }

  Ndb_cluster_connection *cluster_connection;
  Ndb *myNdb;
  NdbDictionary::Dictionary* myDict;
  const NdbDictionary::Table *myTable;
  const NdbDictionary::Index *myIndex;
  const NdbDictionary::Column *myColumn;
  const NdbRecord *pkRecord, *valsRecord, *indexRecord;
};

int NdbApiExample3::doTest()
{
  // Step 1. Initialize NDB API
  ndb_init();

  // Step 2. Connecting to the cluster
  cluster_connection = new Ndb_cluster_connection(connectstring);
  if (cluster_connection->connect(4 /* retries               */,
                                  5 /* delay between retries */,
                                  1 /* verbose               */)) {
    std::cerr << "Could not connect to MGMD." << std::endl;
    return 1;
  }

  if (cluster_connection->wait_until_ready(30, 0) < 0) {
    std::cerr << "Could not connect to NDBD." << std::endl;
    return 2;
  }

  // Step 3. Connect to 'world' database
  myNdb = new Ndb(cluster_connection, db);
  if (myNdb->init()) {
    print_error(myNdb->getNdbError(),
                "Could not connect to the database object.");
    return 3;
  }

  // Step 4. Get table metadata
  myDict = myNdb->getDictionary();
  myTable = myDict->getTable("City");
  if (myTable == NULL) {
    print_error(myDict->getNdbError(), "Could not retrieve a table.");
    return 4;
  }
  
  myIndex = myDict->getIndex("Population", "City");
  if (myIndex == NULL) {
    print_error(myDict->getNdbError(), "Could not retrieve an index.");
    return 4;
  }
  
  myColumn = myTable->getColumn("CountryCode");
  if (myColumn == NULL) {
    print_error(myDict->getNdbError(), "Could not retrieve a column.");
    return 4;
  }
  
  // Step 5. Define NdbRecord's
  NdbDictionary::RecordSpecification recordSpec[5];
  std::memset(recordSpec, 0, sizeof recordSpec);
  int rsSize = sizeof(recordSpec[0]);
  
  // Id
  recordSpec[0].column = myTable->getColumn("ID");
  recordSpec[0].offset = offsetof(struct CityRow, ID);

  // Name
  recordSpec[1].column = myTable->getColumn("Name");
  recordSpec[1].offset = offsetof(struct CityRow, Name);

  // CountryCode
  recordSpec[2].column = myTable->getColumn("CountryCode");
  recordSpec[2].offset = offsetof(struct CityRow, CountryCode);
  
  // District
  recordSpec[3].column = myTable->getColumn("District");
  recordSpec[3].offset = offsetof(struct CityRow, District);
  
  // Population
  recordSpec[4].column = myTable->getColumn("Population");
  recordSpec[4].offset = offsetof(struct CityRow, Population);

  pkRecord = myDict->createRecord(myTable, recordSpec, 1, rsSize);
  valsRecord = myDict->createRecord(myTable, recordSpec, 5, rsSize);
  indexRecord = myDict->createRecord(myIndex, &recordSpec[4], 1, rsSize);
  
  if (pkRecord == NULL || valsRecord == NULL || indexRecord == NULL) {
    print_error(myDict->getNdbError(), "Failed to initialize NdbRecords'.");
    return 5;
  }

  // Call test routines
  int err = 0;
  if ((err = do_scan_read()) ||
      (err = do_index_scan_read()) ||
      (err = do_scan_update())) {
    std::cout << "Transaction failed due to error ("
              << err << ")." << std::endl;
  }
  return err;
}

int NdbApiExample3::do_scan_read()
{
  std::cout << "========== Scan test ==========" << std::endl;
  
  // Step 6. Start a new transaction
  NdbTransaction *myTransaction = myNdb->startTransaction();
  if (myTransaction == NULL) {
    print_error(myNdb->getNdbError(), "Could not start transaction.");
    return 6;
  }
  
  // Step 7. Prepare filter to be applied
  NdbInterpretedCode code(myTable);
  NdbScanFilter filter(&code);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_LIKE,
                 myColumn->getColumnNo(), "JPN", 3) < 0 ||
      filter.end() < 0) {
    print_error(myTransaction->getNdbError(), "Failed to get a filter.");
    myNdb->closeTransaction(myTransaction);
    return 7;
  }
  
  Uint32 scanFlags = NdbScanOperation::SF_TupScan;
  NdbScanOperation::ScanOptions options;
  options.optionsPresent =
    NdbScanOperation::ScanOptions::SO_SCANFLAGS |
    NdbScanOperation::ScanOptions::SO_INTERPRETED;
  options.scan_flags = scanFlags;
  options.interpretedCode= &code;

  // Step 8. Instruct NDB API to scan table
  NdbScanOperation *sop =
    myTransaction->scanTable(valsRecord,
                             NdbOperation::LM_CommittedRead,
                             NULL,
                             &options,
                             sizeof(NdbScanOperation::ScanOptions));
  if (sop == NULL) {
    print_error(myTransaction->getNdbError(),
                "Could not retrieve an operation.");
    myNdb->closeTransaction(myTransaction);
    return 8;
  }

  // Step 9. Send a request to data nodes
  if (myTransaction->execute( NdbTransaction::NoCommit ) == -1) {
    print_error(myTransaction->getNdbError(), "Failed to prepare a scan.");
    myNdb->closeTransaction(myTransaction);
    return 5;
  }

  // Step 10. Fetch rows in a loop
  int check = 0;
  bool needToFetch = true;
  CityRow *row;
  while((check = sop->nextResult((const char**) &row,
                                 needToFetch, false)) >= 0) {
    if (check == 0) {
      // Row available
      needToFetch = false;
      print_city(row);
    } else if (check == 2) {
      // Need to fetch
      myTransaction->execute(NdbTransaction::NoCommit);
      needToFetch = true;
    } else if (check == 1) {
      // No more rows
      break;
    }
  }
  
  // Step 11. End transaction and free it
  if (check == -1) {
    print_error(myTransaction->getNdbError(), "Error during scan.");
    myNdb->closeTransaction(myTransaction);
    return 11;
  } else {
    myTransaction->execute(NdbTransaction::Commit);
  }
  myNdb->closeTransaction(myTransaction);
  return 0;
}

int NdbApiExample3::do_index_scan_read()
{
  std::cout << "========== Index scan test ==========" << std::endl;

  // Step 12. Start a new transaction
  NdbTransaction *myTransaction = myNdb->startTransaction();
  if (myTransaction == NULL) {
    print_error(myNdb->getNdbError(), "Could not start transaction.");
    return 12;
  }
  
  // Step 13. Prepare filter to be applied
  NdbInterpretedCode code(myTable);
  NdbScanFilter filter(&code);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_LIKE,
                 myColumn->getColumnNo(), "JPN", 3) < 0 ||
      filter.end() < 0) {
    print_error(myTransaction->getNdbError(), "Failed to set a filter.");
    myNdb->closeTransaction(myTransaction);
    return 13;
  }
  
  Uint32 scanFlags =
      NdbScanOperation::SF_OrderBy | NdbScanOperation::SF_Descending;
  NdbScanOperation::ScanOptions options;
  options.optionsPresent =
      NdbScanOperation::ScanOptions::SO_SCANFLAGS |
      NdbScanOperation::ScanOptions::SO_INTERPRETED;
  options.scan_flags = scanFlags;
  options.interpretedCode= &code;
  
  // Step 14. Define index boundary
  CityRow low, high;
  low.Population = 1000000;
  high.Population = 2000000;
  NdbIndexScanOperation::IndexBound bound;
  bound.low_key= (char*)&low;
  bound.low_key_count = 1;
  bound.low_inclusive = true;
  bound.high_key = (char*)&high;
  bound.high_key_count =1 ;
  bound.high_inclusive = false;
  bound.range_no = 0;  

  // Step 15. Instruct NDB API to scan index
  NdbIndexScanOperation *isop =
    myTransaction->scanIndex(indexRecord,
                             valsRecord,
                             NdbOperation::LM_Read,
                             NULL,
                             &bound,
                             &options,
                             sizeof(NdbScanOperation::ScanOptions));
  if (isop == NULL) {
    print_error(myTransaction->getNdbError(),
                "Could not retrieve an operation.");
    myNdb->closeTransaction(myTransaction);
    return 15;
  }
  
  // Step 16. Send a request to data nodes
  if (myTransaction->execute( NdbTransaction::NoCommit ) == -1) {
    print_error(myTransaction->getNdbError(), "Failed to prepare a scan.");
    myNdb->closeTransaction(myTransaction);
    return 16;
  }
  
  // Step 17. Fetch rows in a loop
  int check = 0;
  bool needToFetch = true;
  CityRow *row;
  while((check = isop->nextResult((const char**) &row,
                                  needToFetch, false)) >= 0) {
    if (check == 0) {
      // Row available
      needToFetch = false;
      print_city(row);
    } else if (check == 2) {
      // Need to fetch rows
      myTransaction->execute(NdbTransaction::NoCommit);
      needToFetch = true;
    } else if (check == 1) {
      // No more rows
      break;
    }
  }
  
  // Step 18. End transaction and free it
  if (check == -1) {
    print_error(myTransaction->getNdbError(), "Error during index scan.");
    myNdb->closeTransaction(myTransaction);
    return 18;
  } else {
    myTransaction->execute(NdbTransaction::Commit);
  }
  myNdb->closeTransaction(myTransaction);
  return 0;
}

int NdbApiExample3::do_scan_update()
{
  std::cout << "========== Scan update test ==========" << std::endl;
  
  // Step 19. Start a new transaction
  NdbTransaction *myTransaction = myNdb->startTransaction();
  if (myTransaction == NULL) {
    print_error(myNdb->getNdbError(), "Could not start transaction.");
    return 19;
  }

  // Step 20. Prepare filter to be applied
  NdbInterpretedCode code(myTable);
  NdbScanFilter filter(&code);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_LIKE,
                 myColumn->getColumnNo(), "JPN", 3) < 0 ||
      filter.end() < 0) {
    print_error(myTransaction->getNdbError(), "Failed to set a filter.");
    myNdb->closeTransaction(myTransaction);
    return 20;
  }
  
  Uint32 scanFlags = NdbScanOperation::SF_KeyInfo;
  NdbScanOperation::ScanOptions options;
  options.optionsPresent =
    NdbScanOperation::ScanOptions::SO_SCANFLAGS |
    NdbScanOperation::ScanOptions::SO_INTERPRETED;
  options.scan_flags = scanFlags;
  options.interpretedCode= &code;

  // Step 21. Instruct NDB API to scan table
  NdbScanOperation *sop =
    myTransaction->scanTable(valsRecord,
                             NdbOperation::LM_Exclusive,
                             NULL,
                             &options,
                             sizeof(NdbScanOperation::ScanOptions));
  if (sop == NULL) {
    print_error(myTransaction->getNdbError(),
                "Could not retrieve an operation.");
    myNdb->closeTransaction(myTransaction);
    return 21;
  }

  // Step 22. Send a request to data nodes
  if (myTransaction->execute( NdbTransaction::NoCommit ) == -1) {
    print_error(myTransaction->getNdbError(), "Failed to prepare a scan.");
    myNdb->closeTransaction(myTransaction);
    return 22;
  }

  // Step 23. Update rows in a loop
  int check = 0;
  bool needToFetch = true;
  CityRow *row;
  while((check = sop->nextResult((const char**) &row,
                                 needToFetch, false)) >= 0) {
    if (check == 0) {
      // Row available
      needToFetch = false;
      CityRow newCity = *row;
      std::memcpy(&newCity.CountryCode, "ZPG", 3);
      const NdbOperation *uop =
          sop->updateCurrentTuple(myTransaction,
                                  valsRecord,
                                  (char*) &newCity);
      if (uop == NULL) {
        print_error(myTransaction->getNdbError(), "Failed update row.");
        myNdb->closeTransaction(myTransaction);
        return 23;
      }
    } else if (check == 2) {
      // Need to fetch rows
      myTransaction->execute(NdbTransaction::NoCommit);
      needToFetch = true;
    } else if (check == 1) {
      // No more rows
      break;
    }
  }
  
  // Step 24. End transaction and free it
  if (check == -1) {
    print_error(myTransaction->getNdbError(), "Error during scan update.");
    myNdb->closeTransaction(myTransaction);
    return 24;
  } else {
    myTransaction->execute(NdbTransaction::Commit);
  }
  myNdb->closeTransaction(myTransaction);
  return 0;
}

NdbApiExample3::~NdbApiExample3()
{
  // Step 25. Cleanup
  if (myNdb) delete myNdb;
  if (cluster_connection) delete cluster_connection;
  ndb_end(0);
}

int main(int argc, char *argv[])
{
  NdbApiExample3 ex;
  return ex.doTest();
}
