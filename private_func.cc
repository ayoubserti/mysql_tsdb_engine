/*
    @Author: Ayoub Serti
    @file private_func.cpp
    @brief ha_tsdb_engine private member function
*/

#include "PCHfile.h"
#include "ha_tsdb_engine.h"

#include "probes_mysql.h"
#include "sql_plugin.h"

int ha_tsdb_engine::CreateTSDBStructure(Field** inFields, tsdb::Structure& *outTSDBStruct)
{
    int error = 0;
    
	std::vector<tsdb::Field*> tsfields;

	/* Add the timestamp field */
	tsfields.push_back(new TimestampField("_TSDB_timestamp"));
    
    if ( inFields  == NULL)
    {
        error = -1;
    }else
    {
        Field* myfield = *inFields;
        while (myfield != NULL)
        {
            tsdb::Field* dbField = NULL;
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
                 dbField = new Int32Field(myfield->field_name);
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
                 dbField = new tsdb::StringField(myfield->field_name,myfield->size_of());
                 tsfields.push_back(dbField);
                 break;
               case MYSQL_TYPE_LONGLONG :
                 dbField = new tsdb::RecordField(myfield->field_name);
                 tsfields.push_back(dbField);
               case 
               default:
                 break;
           }
            myfield++;
        }
    }
    
    outTSDBStruct = new tsdb::Structure(tsfields,false);
    
    return error;
}


