/*
    @Author: Ayoub Serti
    @file private_func.cpp
    @brief ha_tsdb_engine private member function
*/

#include "PCHfile.h"
#include "ha_tsdb_engine.h"

#include "probes_mysql.h"
#include "sql_plugin.h"

int ha_tsdb_engine::CreateTSDBStructure(Field** inFields, tsdb::Structure* *outTSDBStruct)
{
    int error = 0;
        std::cerr << " Enter CreateTSDBStructure " << std::endl; 
	std::vector<tsdb::Field*> tsfields;

	/* Add the timestamp field */
	tsfields.push_back(new tsdb::TimestampField("_TSDB_timestamp"));
    
    if ( inFields  == NULL)
    {
        error = -1;
    }else
    {
      for (  Field** mfield = inFields; *mfield; mfield++)
        {
		Field* myfield = *mfield;
		if (myfield->is_null() )
		{
			return -1;
		}
            tsdb::Field* dbField = NULL;
	    std::cerr << "[DEBUG] " << myfield->field_name << std::endl;
           switch(myfield->type())
           {
               case MYSQL_TYPE_DECIMAL:
               case MYSQL_TYPE_FLOAT :
               case MYSQL_TYPE_DOUBLE:
               case MYSQL_TYPE_NEWDECIMAL:
                 dbField = new tsdb::DoubleField(myfield->field_name);
                 tsfields.push_back(dbField);
                 break;
               case MYSQL_TYPE_TINY :
               case MYSQL_TYPE_SHORT:
               case MYSQL_TYPE_YEAR :
               case MYSQL_TYPE_LONG :
                 dbField = new tsdb::Int32Field(myfield->field_name);
                 tsfields.push_back(dbField);
                 break;
               case MYSQL_TYPE_BIT :
                 dbField = new tsdb::CharField(myfield->field_name);
                 tsfields.push_back(dbField);
                 break;
               case MYSQL_TYPE_DATE :
               case MYSQL_TYPE_TIME :
               case MYSQL_TYPE_NEWDATE :
                 dbField = new tsdb::DateField(myfield->field_name);
                 tsfields.push_back(dbField);
                 break;
               case MYSQL_TYPE_TIMESTAMP:
               case MYSQL_TYPE_DATETIME:
                 dbField = new tsdb::TimestampField(myfield->field_name);
                 tsfields.push_back(dbField);
                 break;
               case MYSQL_TYPE_VARCHAR :
               case MYSQL_TYPE_ENUM :
               case MYSQL_TYPE_SET:
               case MYSQL_TYPE_VAR_STRING:
               case MYSQL_TYPE_STRING:
                 dbField = new tsdb::StringField(myfield->field_name,255);
                 tsfields.push_back(dbField);
                 break;
               case MYSQL_TYPE_LONGLONG :
                 dbField = new tsdb::RecordField(myfield->field_name);
                 tsfields.push_back(dbField);
       
               default:
                 break;
           }
        }
    }
    
    tsdb::Structure* TSDBStruct = new tsdb::Structure(tsfields,false);
    *outTSDBStruct = TSDBStruct;
    return error;
}


