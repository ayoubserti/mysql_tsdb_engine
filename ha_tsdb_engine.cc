/*
    @Author: Ayoub Serti
    @file ha_tsdb_engine.cc
    
    @brief ha_tsdb_engine implementation
    
*/
//include precompiled headers
#include "PCHfile.h"


#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_tsdb_engine.h"
#include "probes_mysql.h"
#include "sql_plugin.h"

//internal use
#include <sys/stat.h>



//gobal variables:
const char* ha_tsdb_engine_system_database= NULL;

//file extensions
static const char *ha_tsdb_engine_exts[] = {
  ".tsdb"
};


static handler *tsdb_engine_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);

handlerton *tsdb_engine_hton;

/* Interface to mysqld, to check system tables supported by SE */
static const char* tsdb_engine_system_database();
static bool tsdb_engine_is_supported_system_table(const char *db,
                                      const char *table_name,
                                      bool is_sql_layer_system_table);
   
   
//tsdb_engine_share impl 

//ctor
tsdb_engine_share::tsdb_engine_share()
{
  thr_lock_init(&lock);
  use_count=0;
}



//init func 
static int tsdb_engine_init_func(void *p)
{
  DBUG_ENTER("tsdb_engine_init_func");

  tsdb_engine_hton= (handlerton *)p;
  tsdb_engine_hton->state=                     SHOW_OPTION_YES;
  tsdb_engine_hton->create=                    tsdb_engine_create_handler;
  tsdb_engine_hton->flags=                     HTON_CAN_RECREATE;
  tsdb_engine_hton->system_database=   tsdb_engine_system_database;
  tsdb_engine_hton->is_supported_system_table= tsdb_engine_is_supported_system_table;

  DBUG_RETURN(0);
}


//ha_tsdb_engine impl
tsdb_engine_share *ha_tsdb_engine::get_share()
{
  tsdb_engine_share *tmp_share;

  DBUG_ENTER("ha_tsdb_engine::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= static_cast<tsdb_engine_share*>(get_ha_share_ptr())))
  {
    tmp_share= new tsdb_engine_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}

//static function tsdb_engine_create_handler
/*
 @brief this function creates a handler of ha_tsdb_engine handler
 TODO: ?? explain more this machin
*/
static handler* tsdb_engine_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_tsdb_engine(hton, table);
}


//ctor
//call super ctor
ha_tsdb_engine::ha_tsdb_engine(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{
  fTMSeries = NULL;
}



//ha_tsdb_engine::bas_ext() return our extension
const char **ha_tsdb_engine::bas_ext() const
{
  return ha_tsdb_engine_exts;
}

const char* tsdb_engine_system_database()
{
  return ha_tsdb_engine_system_database;
}

//list of all systems tables specific to our engine
//we do not need to implemnt this because it's generic
//until we do not have system table we just put NULL
//TODO: remove this stuff
static st_system_tablename ha_tsdb_engine_system_tables[]= {
  {(const char*)NULL, (const char*)NULL}
};


/**
  @brief Check if the given db.tablename is a system table for this SE.
  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.
  @return
    @retval TRUE   Given db.table_name is supported system table.
    @retval FALSE  Given db.table_name is not a supported system table.
*/
static bool tsdb_engine_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table)
{
  st_system_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab= ha_tsdb_engine_system_tables;
  while (systab && systab->db)
  {
    if (systab->db == db &&
        strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}

/*
    @function ha_tsdb_engine:open()
    @brief open a table
    @params
        name table file name
        mode O_RDONLY  or O_RDWR
        test_if_locked self-segnificant :)
    @return mysql error code 
    
*/

int ha_tsdb_engine::open(const char *name, int mode, uint test_if_locked)
{
    //need to be changed to open our table
  DBUG_ENTER("ha_tsdb_engine::open");
  

  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);
  
  std::string filename(name);
  filename+=bas_ext()[0]; //add ".tsdb"
  
  hid_t ofh = H5Fopen(filename.c_str(), H5F_ACC_RDWR, H5P_DEFAULT);
  if(ofh < 0) 
  {
			std::cerr << "Error opening TSDB file: '" << filename << "'." << std::endl;
			return 0;
	}
	try{
	fTMSeries = new tsdb::Timeseries(ofh,"tsdb");
	}catch(...)
	{
	  return -1;
	}
  H5Fclose(ofh);
  DBUG_RETURN(0);
}


/*
    @function ha_tsdb_engine::close
    @brief close table
    @params void
    @return mysql error code
*/
int ha_tsdb_engine::close(void)
{
  DBUG_ENTER("ha_tsdb_engine::close");
  
  if (NULL != fTMSeries )
    delete fTMSeries;
  
  DBUG_RETURN(0);
}


/*
    @function ha_tsdb_engine::write_row
    @brief insert row
    @params buf is an uchar* that we could cast to any struct
    @return mysql error code
*/

int ha_tsdb_engine::write_row(uchar *buf)
{
  DBUG_ENTER("ha_tsdb_engine::write_row");
 
 
 size_t recordsize = fTMSeries->structure()->getSizeOf();
 uchar* recordPtr = (uchar*)sql_alloc(recordsize + 8 + 1); //8bytes for time stamps, 1 dummy byte
 
 struct timespec tms;
 if (clock_gettime(CLOCK_REALTIME,&tms)) 
 {
        return -1;
 }
  int64_t micros = tms.tv_sec * 1000000;
  /* Add full microseconds */
  micros += tms.tv_nsec/1000;
  /* round up if necessary */
  if (tms.tv_nsec % 1000 >= 500) {
      ++micros;
  }
  
  std::cerr << "[NOTE]: micros = " << micros << std::endl;
  
  memcpy(recordPtr,&micros,8);
  uchar* urecord = recordPtr;
  recordPtr+=8;
 for (Field **field = table->field ; *field ; field++)
 {
   
   if ( !((*field)->is_null()) )
   {
     //(*field)>pack()
     //uchar* to = (uchar*)sql_alloc((*field)->data_length());
     recordPtr=  (*field)->pack(recordPtr,buf,(*field)->data_length(),(*field)->offset(table->record[0]));
	 std::cerr << "[NOTE] data length "<< (*field)->data_length() <<std::endl;
     std::cerr << "[NOTE] field offset:" << (*field)->offset(table->record[0]) << " " << buf + (*field)->offset(table->record[0])<< std::endl;
    
   }
 }

 //must remove exception to enhance performance
  try{
  fTMSeries->appendRecords(1,urecord,true);
  fTMSeries->flushAppendBuffer();
  }
  catch (tsdb::TimeseriesException& e)
  {
    std::cerr << "COULD NOT SAVE ROW " << e.what() << std::endl;
  }

  
  DBUG_RETURN(0);
}



/*
    @function ha_tsdb_engine::update_row
    @brief update row
    @params old_data and new_data are unsigned char ptrs
    @return mysql error code
*/

int ha_tsdb_engine::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_tsdb_engine::update_row");
  //prealably must not update time series
  DBUG_RETURN(1);
}


/*
    @function ha_tsdb_engine::delete_row
    @brief update row
    @params buf is const uchar ptr
    @return mysql error code
*/

int ha_tsdb_engine::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_tsdb_engine::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/*
 @function ha_tsdb_engine::index_read_map
*/
int ha_tsdb_engine::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
{
  int rc;
  DBUG_ENTER("ha_tsdb_engine::index_read");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

/**
 * @function ha_tsdb_engine::index_next
  @brief
  Used to read forward through the index.
*/

int ha_tsdb_engine::index_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_tsdb_engine::index_next");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_tsdb_engine::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_tsdb_engine::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

/*
@function ha_tsdb_engine::index_first
*/
int ha_tsdb_engine::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_tsdb_engine::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}



/**
  @brief
  index_last() asks for the last key in the index.
  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.
  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_tsdb_engine::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_tsdb_engine::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the tsdb_engine in the introduction at the top of this file to see when
  rnd_init() is called.
  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.
  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_tsdb_engine::rnd_init(bool scan)
{
  DBUG_ENTER("ha_tsdb_engine::rnd_init");
  //initialize random access

  
  fRecordIndx=0;
  fRecordNbr = fTMSeries->getNRecords();

  std::cerr << "[NOTE]: scan value " << scan << std::endl;
  std::cerr << "[NOTE]: record Nbr " << fRecordNbr << std::endl;

  DBUG_RETURN(0);
}

int ha_tsdb_engine::rnd_end()
{
  DBUG_ENTER("ha_tsdb_engine::rnd_end");

  std::cerr << "[NOTE] :  random access end " << std::endl; 
  DBUG_RETURN(0);
}


/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.
  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.
  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_tsdb_engine::rnd_next(uchar *buf)
{
  int rc=0;
  DBUG_ENTER("ha_tsdb_engine::rnd_next");
  /*MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);*/
  if( fRecordIndx < fRecordNbr )
  {
	  tsdb::RecordSet  rcrdlist = fTMSeries->recordSet(fRecordIndx,fRecordIndx);

	  if ( rcrdlist.size() )
	  {
		  //my_bitmap_map *old_map = dbug_tmp_use_all_columns(table,table->write_set );
		  tsdb::MemoryBlockPtr memptr =  rcrdlist[0].memoryBlockPtr();
		  size_t mmlen = memptr.size();
		  char* val = memptr.raw();
		  std::cerr << "[NOTE] : record length " <<  mmlen  << std::endl;
		  for ( size_t i =0; i< mmlen ; i++ )
		  {
			  std::cerr << (unsigned char)val[i] << " ";
			  buf[i] = val[i];
		  }
		  std::cerr << " " << std::endl;

	  }
	  else 
	  {
		  std::cerr << "[NOTE]: empty record"  << std::endl;
	  }
	  fRecordIndx++;
	  table->status = 0;
  }
  else
  {
	  MYSQL_READ_ROW_DONE(rc);
  }
  
  //MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode
  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.
  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.
  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_tsdb_engine::position(const uchar *record)
{
  DBUG_ENTER("ha_tsdb_engine::position");
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.
  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.
  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_tsdb_engine::rnd_pos(uchar *buf, uchar *pos)
{
  int rc=0;
  DBUG_ENTER("ha_tsdb_engine::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);

  std::cerr << "[NOTE]: ha_tsdb_engine::rnd_pos"  << std::endl;

  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.
  @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.
  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.
  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.
  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_tsdb_engine::info(uint flag)
{
  DBUG_ENTER("ha_tsdb_engine::info");
  DBUG_RETURN(0);
}


/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.
    @see
  ha_innodb.cc
*/
int ha_tsdb_engine::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_tsdb_engine::extra");
  DBUG_RETURN(0);
}


/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.
  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().
  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_tsdb_engine::delete_all_rows()
{
  DBUG_ENTER("ha_tsdb_engine::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Used for handler specific truncate table.  The table is locked in
  exclusive mode and handler is responsible for reseting the auto-
  increment counter.
  @details
  Called from Truncate_statement::handler_truncate.
  Not used if the handlerton supports HTON_CAN_RECREATE, unless this
  engine can be used as a partition. In this case, it is invoked when
  a particular partition is to be truncated.
  @see
  Truncate_statement in sql_truncate.cc
  Remarks in handler::truncate.
*/
int ha_tsdb_engine::truncate()
{
  DBUG_ENTER("ha_tsdb_engine::truncate");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.
  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().
  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_tsdb_engine::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_tsdb_engine::external_lock");
  DBUG_RETURN(0);
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.
  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).
  Berkeley DB, for example, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).
  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.
  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.
  Called from lock.cc by get_lock_data().
  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)
  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_tsdb_engine::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}


/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.
  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().
  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.
  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_tsdb_engine::delete_table(const char *name)
{
  DBUG_ENTER("ha_tsdb_engine::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  DBUG_RETURN(0);
}


/**
  @brief
  Renames a table from one name to another via an alter table call.
  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().
  Called from sql_table.cc by mysql_rename_table().
  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_tsdb_engine::rename_table(const char * from, const char * to)
{
  DBUG_ENTER("ha_tsdb_engine::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.
  @details
  end_key may be empty, in which case determine if start_key matches any rows.
  Called from opt_range.cc by check_quick_keys().
  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_tsdb_engine::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_tsdb_engine::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.
  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.
  Called from handle.cc by ha_create_table().
  @see
  ha_create_table() in handle.cc
*/

int ha_tsdb_engine::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{

  DBUG_ENTER("ha_tsdb_engine::create");
mysql_mutex_lock(&fMutex);
 if ( share == NULL )share = get_share();
/* if (share->count == 0 )
 {
	thr_lock_init(&share->lock)
 }*/
//  thr_lock_data_init(&share->lock,&lock,NULL);

  /*
    retrieve table name
  */
  LEX_STRING filePath = table_arg->s->path;
  if ( filePath.length == 0 )
  {
    DBUG_RETURN(1);
  }
  
  std::string strTableName(name) , strFilePath;
  strFilePath.copy(filePath.str,filePath.length);
  
  //append file extension
  strTableName+=".tsdb";
  
  //check file exists
  struct stat finfo;
	int intstat;
	intstat = stat(strFilePath.c_str(),&finfo);
	hid_t ofh;
	
	if(intstat != 0) 
	{
	  // Try to create the file
		ofh = H5Fcreate(strTableName.c_str(),H5F_ACC_EXCL,H5P_DEFAULT,H5P_DEFAULT);
		if(ofh < 0) {
		  std::cerr << "[INFO]: name:" << name << std::endl;
			std::cerr << "Error creating TSDB file: '" << strFilePath << "'." << std::endl;
			DBUG_RETURN(-1);
		}
	}
	else
	{
	  //exception
	  std::cerr << "Error reading file" << strFilePath << std::endl;
	  DBUG_RETURN(-5);
	}

  tsdb::Structure* intStructure=NULL;
  int err = CreateTSDBStructure(table_arg->field,&intStructure);
  if ( err != 0)
  {
    std::cerr << "Error when creating internal structure " << err << std::endl;  ;
    return -6;
  }
  try{
    tsdb::Timeseries ts =  tsdb::Timeseries(ofh,"tsdb","",boost::make_shared<tsdb::Structure>(*intStructure));
  }catch(...)
  {
    std::cerr << "[ERROR]: exception" << std::endl;
    return -7;
  }
  
  //close hdf5 handle
  H5Fclose(ofh);
  
  
  fflush(stderr); 
  mysql_mutex_unlock(&fMutex);
  DBUG_RETURN(0);
}


struct st_mysql_storage_engine tsdb_engine_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;
static double srv_double_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1,
  "enum_var_typelib",
  enum_var_names,
  NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static MYSQL_SYSVAR_DOUBLE(
  double_var,
  srv_double_var,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);                             // reserved always 0

static MYSQL_THDVAR_DOUBLE(
  double_thdvar,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);

static struct st_mysql_sys_var* tsdb_engine_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  MYSQL_SYSVAR(double_var),
  MYSQL_SYSVAR(double_thdvar),
  NULL
};

// this is an tsdb_engine of SHOW_FUNC and of my_snprintf() service
static int show_func_tsdb_engine(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, "
              "double_var is %f, %.6b", // %b is a MySQL extension
              srv_enum_var, srv_ulong_var, srv_double_var, "really");
  return 0;
}

struct tsdb_engine_vars_t
{
	ulong  var1;
	double var2;
	char   var3[64];
  bool   var4;
  bool   var5;
  ulong  var6;
};

tsdb_engine_vars_t tsdb_engine_vars= {100, 20.01, "three hundred", true, 0, 8250};

static st_mysql_show_var show_status_tsdb_engine[]=
{
  {"var1", (char *)&tsdb_engine_vars.var1, SHOW_LONG, SHOW_SCOPE_GLOBAL},
  {"var2", (char *)&tsdb_engine_vars.var2, SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF} // null terminator required
};

static struct st_mysql_show_var show_array_tsdb_engine[]=
{
  {"array", (char *)show_status_tsdb_engine, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
  {"var3", (char *)&tsdb_engine_vars.var3, SHOW_CHAR, SHOW_SCOPE_GLOBAL},
  {"var4", (char *)&tsdb_engine_vars.var4, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF}
};

static struct st_mysql_show_var func_status[]=
{
  {"tsdb_engine_func_tsdb_engine", (char *)show_func_tsdb_engine, SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"tsdb_engine_status_var5", (char *)&tsdb_engine_vars.var5, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
  {"tsdb_engine_status_var6", (char *)&tsdb_engine_vars.var6, SHOW_LONG, SHOW_SCOPE_GLOBAL},
  {"tsdb_engine_status",  (char *)show_array_tsdb_engine, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF}
};

mysql_declare_plugin(tsdb_engine)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &tsdb_engine_storage_engine,
  "tsdb_engine",
  "Ayoub Serti ayb.serti@gmail.com",
  "time series storage engine",
  PLUGIN_LICENSE_GPL,
  tsdb_engine_init_func,                            /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  tsdb_engine_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
