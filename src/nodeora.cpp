//---------------------------------------------------------------------------------------------------------------------------------
// File: nodeora.cpp
// Contents: Creates an occi connection and executes pl/sql queries async.
// 
// Copyright Rubik Consulting 
// http://www.rubik.com.tr
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//---------------------------------------------------------------------------------------------------------------------------------



#include <v8.h>
#include <uv.h>
#include <node.h>
#include <node_buffer.h>

#include "occi.h"


namespace nodeora
{

#define REQ_STRING_ARG(I, VAR)                                                                       \
  if (args.Length() <= (I) || !args[I]->IsString())                                                  \
    return ThrowException(Exception::TypeError(String::New("Argument " #I " must be a string")));    \
  Local<String> VAR = Local<String>::Cast(args[I]);

#define REQ_FUN_ARG(I, VAR)                                                                          \
  if (args.Length() <= (I) || !args[I]->IsFunction())                                                \
    return ThrowException(Exception::TypeError(String::New("Argument " #I " must be a function")));  \
  Local<Function> VAR = Local<Function>::Cast(args[I]);


	using namespace v8;
	using namespace std;

	enum {
		VALUE_TYPE_NULL = 1,
		VALUE_TYPE_OUTPUT = 2,
		VALUE_TYPE_STRING = 3,
		VALUE_TYPE_NUMBER = 4,
		VALUE_TYPE_DATE = 5,
		VALUE_TYPE_TIMESTAMP = 6,
		VALUE_TYPE_CLOB = 7,
		VALUE_TYPE_BLOB = 8
	};

	class Cell
	{


	public:		
		Cell()
		{

		}
		~Cell()
		{

		}

		virtual Handle<Value> ToValue()
		{
			HandleScope sope;
			return Undefined();
		}
	};

	class StringCell : public Cell
	{
	public:
		StringCell(string text): text(text) 
		{

		}			

		Handle<Value> ToValue()
		{
			HandleScope scope;
			return scope.Close(String::New(text.c_str()));				
		}      
	private:
		string text;  
	};

	class NullCell : public Cell
	{
	public:
		Handle<Value> ToValue()
		{
			HandleScope scope;
			return scope.Close(Null());
		}
	};

	class NumberCell : public Cell
	{
	public:
		NumberCell(oracle::occi::Number value) : value(value) {}
		Handle<Value> ToValue()
		{
			HandleScope scope;
			return scope.Close(Number::New(value));
		}
	private:
		double value;
	};

	class BlobCell : public Cell
	{
	public:
		BlobCell(char *buffer,int length) : buffer(buffer),length(length) {}

		~BlobCell()
		{
			delete buffer;
		}

		Handle<Value> ToValue()
		{
			HandleScope scope;
			node::Buffer *slowBuffer = node::Buffer::New(length);
			memcpy(node::Buffer::Data(slowBuffer), buffer, length);
			v8::Local<v8::Object> globalObj = v8::Context::GetCurrent()->Global();
			v8::Local<v8::Function> bufferConstructor = v8::Local<v8::Function>::Cast(globalObj->Get(v8::String::New("Buffer")));
			v8::Handle<v8::Value> constructorArgs[3] = { slowBuffer->handle_, v8::Integer::New(length), v8::Integer::New(0) };
			v8::Local<v8::Object> actualBuffer = bufferConstructor->NewInstance(3, constructorArgs);
			return scope.Close(actualBuffer);
		}
	private:
		char *buffer ;
		int length;
	};
	
	class DateCell : public Cell
	{
	public:
		DateCell(oracle::occi::Date d)		
		{				
			d.getDate(year, month, day, hour, min, sec);
		}

		DateCell(oracle::occi::Timestamp d)
		{
			d.getDate(year, month, day);
			d.getTime(hour, min, sec, fs);
		}

		Handle<Value> ToValue()
		{
			HandleScope scope;
			Local<Date> date = Date::Cast(*Date::New(0.0));
			CallDateMethod(date, "setUTCMilliSeconds", fs);
			CallDateMethod(date, "setUTCSeconds", sec);
			CallDateMethod(date, "setUTCMinutes", min);
			CallDateMethod(date, "setUTCHours", hour);
			CallDateMethod(date, "setUTCDate", day);
			CallDateMethod(date, "setUTCMonth", month - 1);
			CallDateMethod(date, "setUTCFullYear", year);
			return scope.Close(date);
		}

	private:

		static void CallDateMethod(v8::Local<v8::Date> date, const char* methodName, int val) {
			Handle<Value> args[1];
			args[0] = Number::New(val);
			Local<v8::Function>::Cast(date->Get(String::New(methodName)))->Call(date, 1, args);
		}
		int year;
		unsigned int month, day, hour, min, sec,fs;
	};

	struct Column {
		int type;
		int charForm;
		string name;
	};

	class Row 
	{

	public:		
		
		Row()			
		{
			
		}

		~Row()
		{
			while(!cells.empty())
			{
				cells.front().reset();
				cells.pop_back();
			}			
		}

		Handle<Object> ToValue(vector<string> columnNames) {			
			Local<Object> item = Object::New();
			uint32_t colindex = 0;			
			for (vector<shared_ptr<Cell>>::iterator iterator = cells.begin(), end =cells.end(); iterator != end; ++iterator,colindex++) {				
				shared_ptr<Cell> cell = *iterator;	
				Handle<Value> value = cell->ToValue();
				item->Set(String::New(columnNames[colindex].c_str()),value);
			}
			return item;
		}

		vector<shared_ptr<Cell>> cells;

	};

	class Table
	{

	private:
		vector<Column> columns;
		
		vector<shared_ptr<Row>> Rows;

	public:
		Table(std::vector<oracle::occi::MetaData> metadata) 
		{
			for (std::vector<oracle::occi::MetaData>::iterator iterator = metadata.begin(), end = metadata.end(); iterator != end; ++iterator) {
				oracle::occi::MetaData metadata = *iterator;
				Column column;
				column.name = metadata.getString(oracle::occi::MetaData::ATTR_NAME);
				column.type = metadata.getInt(oracle::occi::MetaData::ATTR_DATA_TYPE);
				column.charForm = metadata.getInt(oracle::occi::MetaData::ATTR_CHARSET_FORM);
				switch(column.type) {
				case oracle::occi::OCCI_TYPECODE_NUMBER:
				case oracle::occi::OCCI_TYPECODE_FLOAT:
				case oracle::occi::OCCI_TYPECODE_DOUBLE:
				case oracle::occi::OCCI_TYPECODE_REAL:
				case oracle::occi::OCCI_TYPECODE_DECIMAL:
				case oracle::occi::OCCI_TYPECODE_INTEGER:
				case oracle::occi::OCCI_TYPECODE_SMALLINT:
					column.type = VALUE_TYPE_NUMBER;
					break;
				case oracle::occi::OCCI_TYPECODE_VARCHAR2:
				case oracle::occi::OCCI_TYPECODE_VARCHAR:
				case oracle::occi::OCCI_TYPECODE_CHAR:
					column.type = VALUE_TYPE_STRING;
					break;
				case oracle::occi::OCCI_TYPECODE_CLOB:
					column.type = VALUE_TYPE_CLOB;
					break;
				case oracle::occi::OCCI_TYPECODE_DATE:
					column.type = VALUE_TYPE_DATE;
					break;
				case OCI_TYPECODE_TIMESTAMP:
					column.type = VALUE_TYPE_TIMESTAMP;
					break;
				case oracle::occi::OCCI_TYPECODE_BLOB:
					column.type = VALUE_TYPE_BLOB;
					break;
				default:								
					ThrowException(String::New("CreateColumnsFromResultSet: Unhandled oracle data type: "));
					break;
				}
				columns.push_back(std::move(column)); 		
			}	

		}					
		
		~Table()
		{			
			while(!Rows.empty())
			{
				Rows.front().reset();
				Rows.pop_back();
			}
		}

		void ReadRow(oracle::occi::ResultSet* rs)
		{
			uint32_t colindex = 1;	
			shared_ptr<Row> row = make_shared<Row>();			
			for (std::vector<Column>::iterator iterator = columns.begin(), end =columns.end(); iterator != end; ++iterator, colindex++) {
				Column column = *iterator;			
				shared_ptr<Cell> cell;
				if(rs->isNull(colindex)) {					
					cell = make_shared<NullCell>();		
				} else {
					switch(column.type) {
					case VALUE_TYPE_STRING:
						{
							std::string v = rs->getString(colindex);							
							cell = make_shared<StringCell>(v);
						}
						break;
						case VALUE_TYPE_NUMBER:
						{
							oracle::occi::Number v = rs->getNumber(colindex);						
							cell = make_shared<NumberCell>(v);
						}
						break;
						case VALUE_TYPE_DATE:
						{
							oracle::occi::Date v = rs->getDate(colindex);							 
							cell = make_shared<DateCell>(v);
						}
						break;
						case VALUE_TYPE_TIMESTAMP:
						{
							oracle::occi::Timestamp v = rs->getTimestamp(colindex);							
							cell =  make_shared<DateCell>(v);
						}
						break;
						case VALUE_TYPE_CLOB:
							{
								oracle::occi::Clob v = rs->getClob(colindex);
								v.open(oracle::occi::OCCI_LOB_READONLY);

								switch(column.charForm) {
								case SQLCS_IMPLICIT:
									v.setCharSetForm(oracle::occi::OCCI_SQLCS_IMPLICIT);
									break;
								case SQLCS_NCHAR:
									v.setCharSetForm(oracle::occi::OCCI_SQLCS_NCHAR);
									break;
								case SQLCS_EXPLICIT:
									v.setCharSetForm(oracle::occi::OCCI_SQLCS_EXPLICIT);
									break;
								case SQLCS_FLEXIBLE:
									v.setCharSetForm(oracle::occi::OCCI_SQLCS_FLEXIBLE);
									break;
								}

								int cloblength = v.length();
								oracle::occi::Stream *instream = v.getStream(1,0);
								char *buffer = new char[cloblength];
								memset(buffer, 0, cloblength);
								instream->readBuffer(buffer, cloblength);
								v.closeStream(instream);
								v.close();								
								cell = make_shared<StringCell>(buffer);
								delete [] buffer;
							}
							break;
						case VALUE_TYPE_BLOB:
							{
								oracle::occi::Blob v = rs->getBlob(colindex);
								v.open(oracle::occi::OCCI_LOB_READONLY);
								int bloblength = v.length();
								oracle::occi::Stream *instream = v.getStream(1,0);
								char *buffer = new char[bloblength];
								memset(buffer, 0, bloblength);
								instream->readBuffer(buffer, bloblength);
								v.closeStream(instream);
								v.close();
								cell = make_shared<BlobCell>(buffer,bloblength);			
								break;
							}
							break;
					default:
						ThrowException(String::New("createv8objectfromrow: unhandled type: "));
						break;
					}	
					
					row->cells.push_back(cell);				
				}
			}
					
			Rows.push_back(make_shared<Row>(*row));
		}

		static vector<string> GetColumnNames(vector<Column> columns) 
		{
			vector<string> columnNames;
			for (vector<Column>::iterator iterator = columns.begin(), end =columns.end(); iterator != end; ++iterator) {
				Column column = *iterator;	
				columnNames.push_back(column.name);
			}
			return columnNames;
		}

		Handle<Array> ToValue() {
			uint32_t rowindex = 0;	
			Handle<Array> result = Array::New();
			vector<string> columnNames = GetColumnNames(columns);
			for (vector<shared_ptr<Row>>::iterator iterator = Rows.begin(), end =Rows.end(); iterator != end; ++iterator,rowindex++) {
				shared_ptr<Row> row = *iterator;	
				Handle<Object> item = row->ToValue(columnNames);
				result->Set(rowindex,item);
			}
			return result;
		}

	};

	struct QueryBaton 
	{

		Persistent<Function> callback;
		oracle::occi::Connection* connection;
		string query;
		uv_work_t request;
		Table* result;
	};

	class Connection:node::ObjectWrap {
	private:
		static Persistent<FunctionTemplate> constructor_template;		
		oracle::occi::Environment *env;
		oracle::occi::Connection *conn;

	public:


		static void Connection::Initialize(Handle<Object> target)
		{
			HandleScope scope;			

			Local<FunctionTemplate> t = FunctionTemplate::New(Connection::New);
			constructor_template = Persistent<FunctionTemplate>::New(t);
			constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
			constructor_template->SetClassName(String::NewSymbol("Connection"));

			NODE_SET_PROTOTYPE_METHOD(constructor_template, "open", Connection::Open);
			NODE_SET_PROTOTYPE_METHOD(constructor_template, "query", Connection::Query);
			NODE_SET_PROTOTYPE_METHOD(constructor_template, "close", Connection::Close);

			target->Set(String::NewSymbol("Connection"), constructor_template->GetFunction());
		}

		Connection(){
			env = oracle::occi::Environment::createEnvironment (oracle::occi::Environment::DEFAULT);
		}

		~Connection(){
			if(conn)
			{
				env->terminateConnection(conn);
				conn = NULL;
			}

			env=NULL;
		}

		static Handle<Value> Open(const Arguments& args){
			HandleScope scope;
			Connection* connection = ObjectWrap::Unwrap<Connection>(args.This());
			REQ_STRING_ARG(0,ora_user);	
			REQ_STRING_ARG(1,ora_password);	
			REQ_STRING_ARG(2,ora_tns);	
			try 
			{
				connection->conn = connection->env->createConnection(*String::Utf8Value(ora_user),*String::Utf8Value(ora_password),*String::Utf8Value(ora_tns));
			}
			catch(oracle::occi::SQLException ex)
			{
				ThrowException(String::New(ex.getMessage().c_str()));
			}

			return Undefined();
		}

		static Handle<Value> Query(const Arguments& args) {	
			HandleScope scope;
			Connection* connection = ObjectWrap::Unwrap<Connection>(args.This());
			REQ_STRING_ARG(0,ora_sql);
			REQ_FUN_ARG(1, callback);		  
			int count = 0;
			QueryBaton* baton = new QueryBaton();			
			baton->callback = Persistent<Function>::New(callback);
			baton->query = *String::Utf8Value(ora_sql);
			baton->connection = connection->conn;
			baton->request.data = baton;
			int r = uv_queue_work(uv_default_loop(), &baton->request, Read, Result);		
			return Undefined();
		}

		static void Read(uv_work_t* work)
		{
			QueryBaton* baton = static_cast<QueryBaton*>(work->data);				
			oracle::occi::Statement* stmt = baton->connection->createStatement(baton->query);
			oracle::occi::ResultSet* rs = stmt->executeQuery(baton->query);			
			std::vector<oracle::occi::MetaData> metadata =rs->getColumnListMetaData();
			baton->result = new Table(metadata);			

			while(rs->next()) {		
				baton->result->ReadRow(rs);
			}

			if(stmt && rs) {
				stmt->closeResultSet(rs);
				rs = NULL;
			}

			if(stmt)
			{
				baton->connection->terminateStatement(stmt);
				stmt = NULL;
			}
		}

		static void Result(uv_work_t* work,int status)
		{
			HandleScope scope;				
			Handle<Value> argv[1];	
			QueryBaton* baton = static_cast<QueryBaton*>(work->data);	
			argv[0] = baton->result->ToValue();
			baton->callback->Call(Context::GetCurrent()->Global(),1, argv);	
			baton->callback.Dispose();
			delete baton->result;
			delete baton;
		}

		static Handle<Value> Close(const Arguments& args) {
			HandleScope scope;
			Connection* connection = ObjectWrap::Unwrap<Connection>(args.This());
			try 
			{
				connection->env->terminateConnection(connection->conn);
				connection->env = NULL;
			}
			catch(oracle::occi::SQLException ex)
			{
				ThrowException(String::New(ex.getMessage().c_str()));
			}
			return Undefined();
		}

		static Handle<Value> Connection::New(const Arguments& args) 
		{
			HandleScope scope;

			if (!args.IsConstructCall()) {
				return Undefined();
			}

			Connection *c = new Connection();

			c->Wrap(args.This());

			return args.This();
		}    

	};

	Persistent<FunctionTemplate> Connection::constructor_template;

}


NODE_MODULE(nodeora, nodeora::Connection::Initialize);