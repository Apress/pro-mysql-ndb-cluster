#include <NdbApi.hpp>
#include <iostream>
#include <string>
#include <stddef.h>
#include <cstdint>
#include <cstring>

const char *connectstring = "127.0.0.1:1186";
const char *db = "world";

class NdbApiExample2 {
public:
  NdbApiExample2() : cluster_connection(NULL), myNdb(NULL),
              myDict(NULL), myTable(NULL) {};
  ~NdbApiExample2();
  int doTest();
  
private:
  void print_error(const NdbError &e, const char *msg)
  {
    std::cerr << msg << ": Error code (" << e.code << "): "
              << e.message << "." << std::endl;
  }

  Ndb_cluster_connection *cluster_connection;
  Ndb *myNdb;
  NdbDictionary::Dictionary* myDict;
  const NdbDictionary::Table *myTable;
  NdbTransaction *myTransaction;
  NdbOperation *myOperation;
};

int NdbApiExample2::doTest()
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

  if (cluster_connection->wait_until_ready(30,0) < 0) {
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
  myDict= myNdb->getDictionary();
  myTable= myDict->getTable("Country");
    
  if (myTable == NULL) {
    print_error(myDict->getNdbError(), "Could not retrieve a table.");
    return 4;
  }
  
  // Step 5. Define NdbRecord's
  
  struct CountryRow {
    char   nullBits;
    char   Code[3];
    char   Name[52];
    Uint32 Capital;
  };
    
  NdbDictionary::RecordSpecification recordSpec[3];
  
  // Code
  recordSpec[0].column = myTable->getColumn("Code");
  recordSpec[0].offset = offsetof(struct CountryRow, Code);
  recordSpec[0].nullbit_byte_offset = 0;   // Not nullable
  recordSpec[0].nullbit_bit_in_byte = 0;

  // Name
  recordSpec[1].column = myTable->getColumn("Name");
  recordSpec[1].offset = offsetof(struct CountryRow, Name);
  recordSpec[1].nullbit_byte_offset = 0;   // Not nullable
  recordSpec[1].nullbit_bit_in_byte = 0;

  // Capital
  recordSpec[2].column = myTable->getColumn("Capital");
  recordSpec[2].offset = offsetof(struct CountryRow, Capital);
  recordSpec[2].nullbit_byte_offset =
    offsetof(struct CountryRow, nullBits);;   // Nullable
  recordSpec[2].nullbit_bit_in_byte = 0;
  
  const NdbRecord *pkRecord =
    myDict->createRecord(myTable, recordSpec, 1, sizeof(recordSpec[0]));
    
  const NdbRecord *valsRecord =
    myDict->createRecord(myTable, recordSpec, 3, sizeof(recordSpec[0]));
    
  // Step 6. Start transaction
  myTransaction= myNdb->startTransaction();
  if (myTransaction == NULL) {
    print_error(myNdb->getNdbError(), "Could not start transaction.");
    return 5;
  }
  
  // Step 7. Specify type of operation and search condition
  CountryRow rowData;
  std::memset(&rowData, 0, sizeof rowData);
  std::memcpy(&rowData.Code, "JPN", 3);
  const NdbOperation *pop=
    myTransaction->readTuple(pkRecord,
                             (char*) &rowData,
                             valsRecord,
                             (char*) &rowData);
  if (pop==NULL)
    print_error(myTransaction->getNdbError(),
                "Could not execute record based read operation");
  
  // Step 8. Send a request to data nodes
  if (myTransaction->execute( NdbTransaction::Commit ) == -1) {
    print_error(myTransaction->getNdbError(), "Transaction failed.");
    return 8;
  }

  // Step 9. Retrieve values
  std::string nameStr = std::string(rowData.Name, sizeof(rowData.Name));
  std::cout << " Name:         "
            << nameStr.substr(0, nameStr.find_last_not_of(' ') + 1)
            << std::endl;
  std::cout << " Capital Code: "
            << (rowData.nullBits & 0x01 ? std::string("NULL") :
                                          std::to_string(rowData.Capital))
            << std::endl;

  return 0;
}

NdbApiExample2::~NdbApiExample2()
{
  // Step 11. Cleanup
  if (myTransaction) myNdb->closeTransaction(myTransaction);
  if (myNdb) delete myNdb;
  if (cluster_connection) delete cluster_connection;
  ndb_end(0);
}

int main(int argc, char *argv[])
{
  NdbApiExample2 ex;
  return ex.doTest();
}
