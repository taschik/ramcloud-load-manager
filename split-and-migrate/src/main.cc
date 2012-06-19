#include "Connection.h"
#include "Split.h"
#include "Migrate.h"
#include "global.h"

using namespace std;
using namespace RAMCloud;

/**
 * GLOBALS
 */

uint32_t INSERTAMOUNT = 20;
string host = "192.168.30.187";
int port = 12246;
map<string, int (*)(vector<string>&) > commands;
Connection* connection = NULL;

void insertTestData(RamCloud* cloud, const uint64_t tableId, unsigned int amount){
	// write INSERTAMOUNT Values to RAMCloud
	for (unsigned int i=0; i < amount; i++)
	{
		char* key = Helper::itoa(i);
		char* value = new char[strlen(key)+strlen("_Hallo")];
		sprintf(value, "%d_Hallo", i);
		cloud->write(tableId, key, strlen(key), value);
	}
}

void insertTestData(RamCloud* cloud, const char* tableName, int amount){
	const uint64_t tableId = cloud->getTableId(tableName);
	insertTestData(cloud, tableId, amount);
}

void read(RamCloud* cloud, const char* tableName){
	//read INSERTAMOUNT Values from RAMCloud
	for (unsigned int i=0; i < INSERTAMOUNT; i++)
	{
		RAMCloud::Buffer buffer;

		char* key = Helper::itoa(i);
		const uint64_t tableId = cloud->getTableId(tableName);

		cloud->read(tableId, key, strlen(key), &buffer);

		uint16_t length = buffer.getTotalLength();

		char* str = new char[length + 1];
		str[length] = 0;

		buffer.copy(0, buffer.getTotalLength(), str);
		std::cout << "Key: " << key << " Value: " << str << std::endl;
	}
}

void splitString(std::vector<std::string> &result, const std::string &s, const string &delim)
{
	std::stringstream ss(s);
	std::string item;
	char cdelim = delim[0];

	while (std::getline(ss, item, cdelim))
	{
		result.push_back(item);
	}
}
void toLowerString(string &input) {
	transform(input.begin(), input.end(), input.begin(),
			std::bind2nd(std::ptr_fun(&std::tolower<char>), std::locale("")));
}

string readline() {
	string input;
	getline(cin, input);
	if(cin.eof()){
		_Exit(0);
	}
	return input;
}

void write(string line) {
	cout << line;
}

void writeline(string line) {
	cout << line << endl;
}

void writeResultLine(string line) {
	cout << "> " << line << endl;
}

void printStringVector(vector<string> v,  string indention) {
	for(std::vector<string>::iterator it = v.begin(); it != v.end(); ++it) {
		cout << indention << *it << endl;
	}
}

bool ensureConnection() {
	if (connection == NULL) {
		writeResultLine("Not connected! Use 'CONNECT <HOST> <PORT>' to connect to a RAMCloud Server.");
	}
	return connection != NULL;
}

bool ensureSetTable() {
	if (ensureConnection()) {
		if (connection->getTableId() != (unsigned int)-1) {
			return true;
		}
	}
	writeResultLine("No table selected! Use 'SET TABLE <TABLE_NAME>' to select a table.");

	return false;
}

bool validateArgs(vector<string> args, unsigned int min, unsigned int max) {
	if (args.size() < min || args.size() > max) {
		writeResultLine("Invalid number of arguments!");
	}
	return args.size() >= min && args.size() <= max;
}
int setTable(vector<string>& args) {

	if (!ensureConnection() || !validateArgs(args, 1, 1)) {
		return -1;
	}

	string tableName = args[0];
	connection->setTableName(tableName);
	writeResultLine("Current table is: " + tableName);
	return 0;
}

int createTable(vector<string>& args) {

	if (!ensureConnection() || !validateArgs(args, 1, 1)) {
		return -1;
	}

	string tableName = args[0];
	connection->getRamCloud()->createTable(tableName.c_str());

	return 0;
}
int dropTable(vector<string>& args) {

	if (!ensureConnection() || !validateArgs(args, 1, 1)) {
		return -1;
	}

	string tableName = args[0];
	connection->getRamCloud()->dropTable(tableName.c_str());

	return 0;
}

int readString(vector<string>& args) {

	if (!ensureSetTable() || !validateArgs(args, 1, 1)) {
		return -1;
	}

	const char* key = args[0].c_str();
	RAMCloud::Buffer result;
	connection->getRamCloud()->read(connection->getTableId(), key, strlen(key), &result);
	char* resultString = new char[result.getTotalLength()];
	result.copy(0, result.getTotalLength(), resultString);
	writeResultLine(resultString);
	return 0;
}

int writeString(vector<string>& args) {

	if (!ensureSetTable() || !validateArgs(args, 2, 2)) {
		return -1;
	}

	const char* key = args[0].c_str();
	string value = args[1];

	connection->getRamCloud()->write(connection->getTableId(), key, strlen(key), value.c_str());
	return 0;
}

int writeThousandStrings(vector<string>& args) {

	if (!ensureSetTable() || !validateArgs(args, 2, 2)) {
		return -1;
	}

	// const char* key = args[0].c_str();
	string value = args[1];
	int entries = 0;
	entries = atoi(args[0].c_str());

	for (int i =0; i < entries; i++){
			char* key = Helper::itoa(i);
			connection->getRamCloud()->write(connection->getTableId(), key, strlen(key), value.c_str());
			cout << "wrote key " << Helper::itoa(i) << " with value " << value << endl;
	}
	return 0;
}

int splitTable(vector<string>& args){
	if (!ensureConnection() || !validateArgs(args, 4, 4)) {
		return -1;
	}
	const char* tableName = args[0].c_str();
	uint64_t start = atoi(args[1].c_str());
	uint64_t end =  NULL;
	uint64_t splitPoint = NULL;

	if (args[2] == "~0UL"){
		end = ~0UL;
	} else {
		end = atoi(args[2].c_str());
	}
	if (args[3] == "~0UL/2"){
		splitPoint = ~0UL/2;
	} else {
		splitPoint = atoi(args[3].c_str());
	}

	cout << start << " " << end << " split point " << splitPoint <<endl;
	Split* split = new Split(connection->getRamCloud(), tableName, start, end, splitPoint);
	split->splitTable();

	return 0;
}
int migrateTablet(vector<string>& args){
	if (!ensureConnection() || !validateArgs(args, 4, 4)) {
		return -1;
	}
	connection->setTableName(args[0]);

	uint64_t tableId = connection->getTableId();
	uint64_t start = NULL;
	uint64_t end = NULL;

	if (args[1] == "~0UL"){
		start = ~0UL;
	} else if (args[1] == "~0UL/2") {
		start = ~0UL/2;
	} else {
		start = atoi(args[1].c_str());
	}
	if (args[2] == "~0UL"){
		end = ~0UL;
	} else if (args[2] == "~0UL/2") {
		end = ~0UL/2 - 1;
	} else {
		end = atoi(args[2].c_str());
	}
	uint64_t targetServer = atoi(args[3].c_str());

	Migrate* migrate = new Migrate(connection, tableId, start, end, targetServer);
	migrate->migrateTablet();

	return 0;
}

void initializeCommands() {
	commands["set table"] = &setTable;
	commands["create table"] = &createTable;
	commands["split table"] = &splitTable;
	commands["migrate tablet"] = &migrateTablet;
	commands["drop table"] = &dropTable;
	commands["read string"] = &readString;
	commands["write string"] = &writeString;
	commands["write strings"] = &writeThousandStrings;
}


void executeCommand(string input) {
	vector<string> parts;
	string cmd;

	splitString(parts, input, " ");

	for(std::vector<string>::iterator it = parts.begin(); it != parts.end(); ++it) {
		cmd += *it;
		toLowerString(cmd);

		map<string, int (*)(vector<string>&)>::iterator cmdIt = commands.find(cmd);

		if (cmdIt != commands.end()) {
			int (*cmdFctPtr)(vector<string>&) = cmdIt->second;
			vector<string> args(it + 1, parts.end());
			cmdFctPtr(args);
			return;
		}

		cmd += " ";
	}
	writeResultLine("Command not found!");
}

int main(int argc, char const *argv[])
{
	initializeCommands();
	connection =  new Connection(host, port);
	connection->connect();

	writeline("Welcome to the RAMCloud Console!");
	while (true) {
		write("# ");
		string input = readline();
		if (input == "exit") {
			break;
		}
		executeCommand(input);
	}

	writeline("");
	writeline("Terminating RAMCloud Console...");

	return 0;
}
