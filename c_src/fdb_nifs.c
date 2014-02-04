#include <string.h>
#include "erl_nif.h"
#include "nif_resources.h"
#include "fdb_c.h"

static enif_network_t *network = NULL;

static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_not_found;
static ERL_NIF_TERM atom_undefined;
static ERL_NIF_TERM atom_true;
static ERL_NIF_TERM atom_false;
static ERL_NIF_TERM error_not_implemented;

// FDBStreamingMode enum
static ERL_NIF_TERM atom_iterator;
static ERL_NIF_TERM atom_want_all;
static ERL_NIF_TERM atom_small;
static ERL_NIF_TERM atom_medium;
static ERL_NIF_TERM atom_large;
static ERL_NIF_TERM atom_serial;
static ERL_NIF_TERM atom_exact;

// FDBDatabaseOption enum
static ERL_NIF_TERM atom_location_cache_size;
static ERL_NIF_TERM atom_max_watches;
static ERL_NIF_TERM atom_machine_id;
static ERL_NIF_TERM atom_datacenter_id;

static ERL_NIF_TERM mk_and_release_resource(ErlNifEnv* env,void *resptr)
{
    ERL_NIF_TERM res;
    res = enif_make_resource(env,resptr);
    enif_release_resource(resptr);
    return res;
}

static ERL_NIF_TERM mk_error(ErlNifEnv* env,const char* error)
{
    return enif_make_tuple2(env,atom_error,enif_make_atom(env,error));
}

static ERL_NIF_TERM mk_result(ErlNifEnv* env, fdb_error_t errcode, ERL_NIF_TERM result)
{
    return enif_make_tuple2(env,enif_make_int(env,errcode),result);
}

static fdb_bool_t get_boolean(ErlNifEnv* env, ERL_NIF_TERM atom, fdb_bool_t* val)
{
    if (enif_compare( atom, atom_true) == 0)
    {
       (*val) = 1;
       return 1;
    }
    else if (enif_compare( atom, atom_false) == 0) 
    {
       (*val) = 0;
       return 1;
    }
    else return 0;
}

static int get_FDBStreamingMode(ErlNifEnv* env, ERL_NIF_TERM atom, FDBStreamingMode* mode) 
{
    if (enif_compare( atom, atom_iterator) == 0) {
      (*mode) = FDB_STREAMING_MODE_ITERATOR; 
      return 1;
    }
    if (enif_compare( atom, atom_want_all) == 0) {
      (*mode) = FDB_STREAMING_MODE_WANT_ALL;
      return 1;
    }
    if (enif_compare( atom, atom_small) == 0) {
      (*mode) = FDB_STREAMING_MODE_SMALL;
      return 1;
    }
    if (enif_compare( atom, atom_medium) == 0) {
      (*mode) = FDB_STREAMING_MODE_MEDIUM;
      return 1;
    }
    if (enif_compare( atom, atom_large) == 0) {
      (*mode) = FDB_STREAMING_MODE_LARGE;
      return 1;
    }
    if (enif_compare( atom, atom_serial) == 0) {
      (*mode) =FDB_STREAMING_MODE_SERIAL;
      return 1;
    }
    if (enif_compare( atom, atom_exact) == 0) {
      (*mode) =FDB_STREAMING_MODE_EXACT;
      return 1;
    }
    return 0;
}

static int get_FDBDatabaseOption(ErlNifEnv* env, ERL_NIF_TERM atom, FDBDatabaseOption* mode) 
{
    if (enif_compare( atom, atom_location_cache_size) == 0) {
      (*mode) = FDB_DB_OPTION_LOCATION_CACHE_SIZE; 
      return 1;
    }
    if (enif_compare( atom, atom_max_watches) == 0) {
      (*mode) = FDB_DB_OPTION_MAX_WATCHES; 
      return 1;
    }
    if (enif_compare( atom, atom_machine_id) == 0) {
      (*mode) = FDB_DB_OPTION_MACHINE_ID; 
      return 1;
    }
    if (enif_compare( atom, atom_datacenter_id) == 0) {
      (*mode) = FDB_DB_OPTION_DATACENTER_ID; 
      return 1;
    }
    return 0;
}

static ERL_NIF_TERM make_binary(ErlNifEnv *env, const uint8_t *data, int size)
{

        ERL_NIF_TERM result;
        uint8_t *chars = enif_make_new_binary(env,size,&result);
        memcpy(chars,data,size);
        return result;
}

static int get_binary(ErlNifEnv *env, ERL_NIF_TERM term,ErlNifBinary *Key)
{
    if (  enif_inspect_binary(env, term, Key) == 0
       && enif_inspect_iolist_as_binary(env, term, Key) == 0)
    {
       Key = NULL;
       return 0;
    }
    return 1;
}

/*
 * This function is called when loading the nif.
 *
 * it registers the different resource types that are being used by the library.
 */
static int nif_on_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    atom_ok = enif_make_atom(env, "ok");
    atom_error = enif_make_atom(env, "error");
    atom_true = enif_make_atom(env, "true");
    atom_false = enif_make_atom(env, "false");
    atom_not_found = enif_make_atom(env,"not_found");
    atom_undefined = enif_make_atom(env,"undefined");
    error_not_implemented = mk_error(env,"not_implemented");
    // FDBStreamingMode enum
    atom_iterator = enif_make_atom(env, "iterator");
    atom_want_all = enif_make_atom(env, "want_all");
    atom_small = enif_make_atom(env, "small");
    atom_medium = enif_make_atom(env, "medium");
    atom_large = enif_make_atom(env, "large");
    atom_serial = enif_make_atom(env, "serial");
    atom_exact = enif_make_atom(env, "exact");
    // FDBDatabaseOption enum
    atom_location_cache_size = enif_make_atom(env, "location_cache_size");
    atom_max_watches = enif_make_atom(env, "max_watches");
    atom_machine_id = enif_make_atom(env, "machine_id");
    atom_datacenter_id = enif_make_atom(env, "datacenter_id");

    if (register_fdb_resources(env)!=0)
        return -1;

    network = wrap_network();

    enif_make_resource(env,(void*)network);

    return 0;
}

/*
 * Start FDB API wrapper.
 */

static ERL_NIF_TERM nif_fdb_cluster_create_database(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_cluster_t *cluster;
    char *db_name = "DB";
    int db_name_length = 2;
    ErlNifBinary name;
    if (  argc < 1 
       || argc > 2  
       || get_cluster(env,argv[0],&cluster)==0
       ) return enif_make_badarg(env);
    if (argc == 2)
    { 
       if (get_binary(env, argv[1], &name) == 0) 
       { 
          return enif_make_badarg(env);
       } 
       else
       {
          db_name = (char*)name.data;
          db_name_length = name.size;
       }
    }  

    FDBFuture* future  =
        fdb_cluster_create_database(cluster->handle,(const uint8_t*)db_name,db_name_length);

    return mk_and_release_resource(env, wrap_future(future));
}

static ERL_NIF_TERM nif_fdb_cluster_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    // invoked by GC
    return enif_make_int(env,0);
}

static ERL_NIF_TERM nif_fdb_cluster_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
// from /usr/include/foundationdb/fdb_c_opts.g.h:
//
// typedef enum {
//   // This option is only a placeholder for C compatibility and should not be used
//   // Parameter: Option takes no parameter
//   FDB_CLUSTER_OPTION_DUMMY_DO_NOT_USE=-1
//} FDBClusterOption;
    return enif_make_int(env, 0);
}

static ERL_NIF_TERM nif_fdb_create_cluster(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary fp;
    char* cluster_file_path = NULL;
    FDBFuture *F;
    if (argc>1) return enif_make_badarg(env);
    if (argc==1) 
    {
       if (get_binary(env, argv[0],&fp) == 0)
          return enif_make_badarg(env);
       cluster_file_path = enif_alloc(fp.size+1);
       memcpy((void*)cluster_file_path, (const void*)fp.data, fp.size);
       *(cluster_file_path+fp.size) = 0;
    }
    
    F = fdb_create_cluster(cluster_file_path);
    
    if (cluster_file_path != NULL)
    {
      enif_free(cluster_file_path);
    }

    return mk_and_release_resource(env,wrap_future(F));
}

static ERL_NIF_TERM nif_fdb_database_create_transaction(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_database_t *DB;
    enif_transaction_t *Tx = wrap_transaction(NULL);

    if (  argc!=1 
       || get_database(env, argv[0],&DB) == 0) 
      return enif_make_badarg(env);

    fdb_error_t err = fdb_database_create_transaction(DB->handle, &Tx->handle);

    return mk_result(env, err,mk_and_release_resource(env,Tx));
}

static ERL_NIF_TERM nif_fdb_database_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    // invoked by GC
    return enif_make_int(env,0);
}

static ERL_NIF_TERM nif_fdb_database_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_database_t *DB;
    FDBDatabaseOption opt;
    ErlNifBinary value;
    if (  argc!=3
       || get_database(env, argv[0], &DB) == 0
       || get_FDBDatabaseOption(env, argv[1], &opt) == 0
       || get_binary(env, argv[2], &value) == 0)
      return enif_make_badarg(env);
 
    fdb_error_t err = fdb_database_set_option(DB->handle, opt, value.data, value.size);


    return enif_make_int(env, err);
}

static ERL_NIF_TERM nif_fdb_future_block_until_ready(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    //FDBFuture* f;
    enif_future_t *f;
    if (  argc!=1 
       || get_future(env,argv[0],&f) == 0)
      return enif_make_badarg(env);

    return enif_make_int(env,fdb_future_block_until_ready(f->handle));
}

static ERL_NIF_TERM nif_fdb_future_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *f;
    if (  argc!=1 
       || get_future(env,argv[0],&f) == 0)
      return enif_make_badarg(env);
    
    fdb_future_cancel(f->handle);

    return enif_make_int(env,0);
}

static ERL_NIF_TERM nif_fdb_future_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    // invoked by GC
    return enif_make_int(env,0);
}

static ERL_NIF_TERM nif_fdb_future_get_cluster(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    //FDBFuture* f;
    //FDBCluster** out_cluster;
    enif_future_t *f;
    enif_cluster_t *out_cluster = wrap_cluster(NULL);
    if (argc!=1|| get_future(env,argv[0],&f) == 0)
        return enif_make_badarg(env);

    int err =fdb_future_get_cluster(f->handle,&out_cluster->handle);

    return mk_result(env,err,mk_and_release_resource(env,out_cluster));
}

static ERL_NIF_TERM nif_fdb_future_get_database(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *f;
    enif_database_t *out_database=wrap_database(NULL);

    if (  argc!=1
       || get_future(env,argv[0],&f) == 0 )
      return enif_make_badarg(env);

    int err =fdb_future_get_database(f->handle,&out_database->handle);

    return mk_result(env,err,mk_and_release_resource(env,out_database));
}

static ERL_NIF_TERM nif_fdb_future_get_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *Future;
    if (  argc!=1
       || get_future(env,argv[0],&Future) == 0) 
      return enif_make_badarg(env);

    return enif_make_int(env,fdb_future_get_error(Future->handle));
}

static ERL_NIF_TERM nif_fdb_future_get_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
   enif_future_t *Future;
   const uint8_t *data;
   int size;

   if (  argc!=1
      || get_future(env,argv[0],&Future) == 0) 
      return enif_make_badarg(env);

   fdb_error_t err = fdb_future_get_key(Future->handle, &data, &size);

   if (err != 0)
   {
      return mk_result(env,err,atom_undefined);
   }
   else
   {
        ERL_NIF_TERM result;

        uint8_t *chars = enif_make_new_binary(env,size,&result);
        memcpy(chars,data,size);

        return mk_result(env,err,result);
    }
}

static ERL_NIF_TERM nif_fdb_future_get_keyvalue_array(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *f;
    const FDBKeyValue * out_kv=NULL;
    int out_count = 0;
    fdb_bool_t out_more = 0;

    if (  argc!=1
       || get_future(env, argv[0], &f) == 0 )
      return enif_make_badarg(env);
    
    fdb_error_t err = fdb_future_get_keyvalue_array(f->handle, &out_kv, &out_count, &out_more);

    if (err != 0)
    {
        return mk_result(env,err,atom_undefined);
    }

    ERL_NIF_TERM result = enif_make_list(env, 0);

    while (out_count>0)
    {
      out_count = out_count - 1;
      const FDBKeyValue *kv = &(out_kv[out_count]);
      ERL_NIF_TERM k = make_binary(env, kv->key, kv->key_length);
      ERL_NIF_TERM v = make_binary(env, kv->value, kv->value_length);
      ERL_NIF_TERM elem = enif_make_tuple2(env, k, v);
      result = enif_make_list_cell(env, elem, result);
    }
    return mk_result(env,err,result);
}

static ERL_NIF_TERM nif_fdb_future_get_string_array(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *Future;
    const char **out_strings;
    int out_count;
    fdb_error_t err;

    if (  argc != 1
       || get_future(env, argv[0], &Future) == 0)
      return enif_make_badarg(env);
    
    err = fdb_future_get_string_array(Future->handle, &out_strings, &out_count);

    if (err!=0)
    {
      return mk_result(env,err,atom_undefined);
    }

    ERL_NIF_TERM result = enif_make_list(env, 0);

    while (out_count>0)
    {
      out_count = out_count - 1;
      const char * n = out_strings[out_count];
      ERL_NIF_TERM elem = make_binary(env, (const uint8_t *)n, strlen(n));
      result = enif_make_list_cell(env, elem, result);
    }
    return mk_result(env,err,result);
}

static ERL_NIF_TERM nif_fdb_future_get_value(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *Future;
    fdb_bool_t out_present;
    const uint8_t *data;
    int size;

    if (  argc!=1
       || get_future(env,argv[0],&Future) == 0) 
      return enif_make_badarg(env);

    fdb_error_t err = fdb_future_get_value(Future->handle, &out_present,&data, &size);

    if (err != 0)
    {
        return mk_result(env,err,atom_undefined);
    }
    if (out_present == 0)
    {
        return mk_result(env,err,atom_not_found);
    }
    else
    {
        ERL_NIF_TERM result;

        uint8_t *chars = enif_make_new_binary(env,size,&result);
        memcpy(chars,data,size);

        return mk_result(env,err,result);
    }
}

static ERL_NIF_TERM nif_fdb_future_get_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *Future;
    int64_t version;

    if (  argc!=1
       || get_future(env,argv[0],&Future) == 0) 
      return enif_make_badarg(env);
   
    fdb_error_t err = fdb_future_get_version(Future->handle, &version);
    return mk_result(env, err, enif_make_int(env, version));
}

static ERL_NIF_TERM nif_fdb_future_is_ready(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *f; 
    if (  argc!=1
       || get_future(env,argv[0],&f) == 0) 
      return enif_make_badarg(env);

    if(fdb_future_is_ready(f->handle))
    {
       return atom_true;
    }
    else
    {
       return atom_false;
    }
}

static ERL_NIF_TERM nif_fdb_future_release_memory(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *f; 
    if (  argc!=1
       || get_future(env,argv[0],&f) == 0) 
       return enif_make_badarg(env);

    fdb_future_release_memory(f->handle);

    return enif_make_int(env, 0);
}

static ERL_NIF_TERM nif_fdb_get_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    fdb_error_t code;
    if (  argc!=1 
       || enif_get_int(env, argv[0],&code)== 0)
      return enif_make_badarg(env);

    return enif_make_atom(env,fdb_get_error(code));
}

static ERL_NIF_TERM nif_fdb_network_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=3) return enif_make_badarg(env);

    //  FDBNetworkOption option;
    //  uint8_t const* value;
    //  int value_length;

    return error_not_implemented;
}

static void* thr_event_loop(void* obj)
{
    fdb_run_network();
    return NULL;
}

static ERL_NIF_TERM nif_fdb_run_network(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    if (network->is_running) return mk_error(env,"network_already_running");

    if (enif_thread_create("", &(network->tid), thr_event_loop, NULL,NULL)!=0) {
        return mk_error(env,"error launching network thread");
    }
 
    enif_make_resource(env,network);

    network->is_running = 1;

    return atom_ok;
}

static ERL_NIF_TERM nif_fdb_select_api_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int runtime_version;
    if (  argc!=1 
       || enif_get_int(env,argv[0],&runtime_version)==0)
      return enif_make_badarg(env);

    return enif_make_int(env,fdb_select_api_version(runtime_version));
}

static ERL_NIF_TERM nif_fdb_setup_network(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    return enif_make_int(env,fdb_setup_network());
}

static ERL_NIF_TERM nif_fdb_stop_network(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    if (network->is_running==0) return enif_make_int(env,-1);

    enif_release_resource(network);

    return enif_make_int(env,0);
}

static ERL_NIF_TERM nif_fdb_transaction_add_conflict_range(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=6) return enif_make_badarg(env);

    // FDBTransaction *tr;
    //  uint8_t const* begin_key_name;
    //  int begin_key_name_length;
    //  uint8_t const* end_key_name;
    //  int end_key_name_length;
    //  FDBConflictRangeType type)

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_atomic_op(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=6) return enif_make_badarg(env);

    //  FDBTransaction* tr;
    //  uint8_t const* key_name;
    //  int key_name_length;
    //  uint8_t const* param;
    //  int param_length;
    //  FDBMutationType operation_type;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;

    if (  argc!=1
       || get_transaction(env,argv[0],&Tx) == 0)
      return enif_make_badarg(env);

    fdb_transaction_cancel(Tx->handle);

    return enif_make_int(env, 0);
}

static ERL_NIF_TERM nif_fdb_transaction_clear(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary Key;

    if (  argc!=2
       || get_transaction(env,argv[0],&Tx) == 0
       || get_binary(env,argv[1],&Key) == 0) 
      return enif_make_badarg(env);

    fdb_transaction_clear(Tx->handle,Key.data,Key.size);

    return enif_make_int(env, 0);
}

static ERL_NIF_TERM nif_fdb_transaction_clear_range(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary Begin, End;

    if (  argc!=3
       || get_transaction(env,argv[0],&Tx) == 0
       || get_binary(env,argv[1],&Begin) == 0
       || get_binary(env,argv[2],&End) == 0) 
      return enif_make_badarg(env);

    fdb_transaction_clear_range(Tx->handle, Begin.data, Begin.size, End.data, End.size);

    return enif_make_int(env, 0);
}

static ERL_NIF_TERM nif_fdb_transaction_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;

    if (  argc!=1 
       || get_transaction(env,argv[0],&Tx)==0) 
      return enif_make_badarg(env);

    return mk_and_release_resource(env,wrap_future(fdb_transaction_commit(Tx->handle)));
}

static ERL_NIF_TERM nif_fdb_transaction_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    // invoked by GC
    return enif_make_int(env,0);
}

static ERL_NIF_TERM nif_fdb_transaction_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary Key;
    enif_future_t *Future = wrap_future(NULL);

    if (  argc!=2
       || get_transaction(env,argv[0],&Tx) == 0
       || get_binary(env,argv[1],&Key) == 0) 
      return enif_make_badarg(env);

    Future->handle = fdb_transaction_get(Tx->handle,Key.data,Key.size,0);

    return mk_and_release_resource(env,Future);

}

static ERL_NIF_TERM nif_fdb_transaction_get_addresses_for_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary Key;
    enif_future_t *Future = wrap_future(NULL);

    if (  argc!=2
       || get_transaction(env,argv[0],&Tx) == 0
       || get_binary(env,argv[1],&Key) == 0) 
      return enif_make_badarg(env);

    Future->handle = fdb_transaction_get_addresses_for_key(Tx->handle,Key.data,Key.size);

    return mk_and_release_resource(env,Future);
}

static ERL_NIF_TERM nif_fdb_transaction_get_committed_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    int64_t out_version;

    if (  argc!=1
       || get_transaction(env, argv[0], &Tx) == 0) 
      return enif_make_badarg(env);

    fdb_error_t err = fdb_transaction_get_committed_version(Tx->handle,&out_version);

    return mk_result(env, err, enif_make_int(env, out_version));
}

static ERL_NIF_TERM nif_fdb_transaction_get_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=6) return enif_make_badarg(env);

    //  FDBTransaction* tr;
    //  uint8_t const* key_name;
    //  int key_name_length;
    //  fdb_bool_t or_equal;
    //  int offset;
    //  fdb_bool_t snapshot;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_get_range(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary begin_key;
    fdb_bool_t begin_or_equal;
    int begin_offset;
    ErlNifBinary end_key;
    fdb_bool_t end_or_equal;
    int end_offset;
    int limit;
    int target_bytes;
    FDBStreamingMode mode;
    int iteration;
    fdb_bool_t snapshot;
    fdb_bool_t reverse;
    enif_future_t *Future = wrap_future(NULL);

    if (  argc!=13
       || get_transaction(env,argv[0],&Tx) == 0
       || get_binary(env,argv[1],&begin_key) == 0
       || get_boolean(env, argv[2], &begin_or_equal) == 0
       || enif_get_int(env, argv[3], &begin_offset) == 0
       || get_binary(env,argv[4],&end_key) == 0
       || get_boolean(env, argv[5], &end_or_equal) == 0
       || enif_get_int(env, argv[6], &end_offset) == 0 
       || enif_get_int(env, argv[7], &limit) == 0 
       || enif_get_int(env, argv[8], &target_bytes) == 0
       || get_FDBStreamingMode(env, argv[9], &mode) == 0
       || enif_get_int(env, argv[10], &iteration) == 0
       || get_boolean(env, argv[11], &snapshot) == 0
       || get_boolean(env, argv[12], &reverse) == 0)
      return enif_make_badarg(env);

    Future->handle = fdb_transaction_get_range(Tx->handle,
       begin_key.data, begin_key.size, begin_or_equal, begin_offset,
       end_key.data, end_key.size, end_or_equal, end_offset,
       limit, target_bytes, mode, iteration, snapshot, reverse
       );

    return mk_and_release_resource(env,Future);
}

static ERL_NIF_TERM nif_fdb_transaction_get_range_selector(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=10) return mk_error(env,"expected_10_parameters");

    //  FDBTransaction* tr;
    //  uint8_t const* begin_key_name;
    //  int begin_key_name_length;
    //  fdb_bool_t begin_or_equal;
    //  int begin_offset;
    //  uint8_t const* end_key_name;
    //  int end_key_name_length;
    //  fdb_bool_t end_or_equal;
    //  int end_offset;
    //  int limit;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_get_read_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    enif_future_t *Future = wrap_future(NULL);

    if (  argc!=1
       || get_transaction(env,argv[0],&Tx) == 0)
      return enif_make_badarg(env);

    Future->handle = fdb_transaction_get_read_version(Tx->handle);

    return mk_and_release_resource(env,Future);
}

static ERL_NIF_TERM nif_fdb_transaction_on_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

    enif_transaction_t* Tx;
    fdb_error_t error;
    if (  argc!=2
       || get_transaction(env,argv[0],&Tx) == 0
       || enif_get_int(env,argv[1],&error) == 0) 
      return enif_make_badarg(env);
    
    return mk_and_release_resource(env,wrap_future(fdb_transaction_on_error(Tx->handle,error)));
}

static ERL_NIF_TERM nif_fdb_transaction_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary Key;

    if (  argc!=2
       || get_transaction(env,argv[0],&Tx) == 0
       || get_binary(env,argv[1],&Key) == 0) 
      return enif_make_badarg(env);

    fdb_transaction_clear(Tx->handle,Key.data,Key.size);

    return enif_make_int(env, 0);
}

static ERL_NIF_TERM nif_fdb_transaction_set(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    ErlNifBinary Key,Value;
    if (  argc!=3
       || get_transaction(env,argv[0], &Tx) == 0
       || get_binary(env,argv[1],&Key) == 0
       || get_binary(env,argv[2],&Value) == 0)
      return enif_make_badarg(env);

    fdb_transaction_set(Tx->handle,Key.data, Key.size, Value.data, Value.size);

    return enif_make_int(env, 0);
}

static ERL_NIF_TERM nif_fdb_transaction_set_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=4) return enif_make_badarg(env);

    //  FDBTransaction* tr;
    //  FDBTransactionOption option;
    //  uint8_t const* value;
    //  int value_length;

    return error_not_implemented;
}

static ERL_NIF_TERM nif_fdb_transaction_set_read_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_transaction_t *Tx;
    int64_t version;

    if (  argc!=2
       || get_transaction(env,argv[0],&Tx) == 0
       || enif_get_int64(env,argv[1],&version) == 0)
      return enif_make_badarg(env);

    fdb_transaction_set_read_version(Tx->handle,version);

    return enif_make_int(env,0);
}

static ERL_NIF_TERM nif_fdb_transaction_watch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=3) return enif_make_badarg(env);

    //  FDBTransaction *tr;
    //  uint8_t const* key_name;
    //  int key_name_length)

    return error_not_implemented;
}

static ERL_NIF_TERM nif_new_cluster(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    return mk_and_release_resource(env,wrap_cluster(NULL));
}

static ERL_NIF_TERM nif_new_database(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    return mk_and_release_resource(env,wrap_database(NULL));
}

static ERL_NIF_TERM nif_new_transaction(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc!=0) return enif_make_badarg(env);

    return mk_and_release_resource(env,wrap_transaction(NULL));
}

void send_message_callback(FDBFuture* f,void* nothing);


static ERL_NIF_TERM send_on_complete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    enif_future_t *f;
    int errcode = 0;
    
    if (  argc!=3
       || get_future(env,argv[0],&f) == 0
       || enif_get_local_pid(env, argv[1], &(f->callback_pid))==0) 
      return enif_make_badarg(env);

    f->callback_env = enif_alloc_env();
    f->callback_msg = enif_make_copy(f->callback_env, argv[2]);
    enif_keep_resource(f);
    errcode = fdb_future_set_callback(f->handle, send_message_callback, f);
    return enif_make_int(env,errcode);
}

void send_message_callback(FDBFuture* f,void* enifF)
{
    enif_future_t* ctx = (enif_future_t*)enifF;
    if (ctx->lock == NULL) return;
    enif_mutex_lock(ctx->lock);
    enif_send(NULL, &(ctx->callback_pid),ctx->callback_env,ctx->callback_msg);  

    ctx->callback_env = NULL;
    enif_mutex_unlock(ctx->lock);
    enif_release_resource(ctx);
}

static ErlNifFunc nifs[] =
{
    {"fdb_cluster_create_database", 1, nif_fdb_cluster_create_database},
    {"fdb_cluster_create_database", 2, nif_fdb_cluster_create_database},
    {"fdb_cluster_destroy", 1, nif_fdb_cluster_destroy},
    {"fdb_cluster_set_option", 3, nif_fdb_cluster_set_option},
    {"fdb_create_cluster", 0, nif_fdb_create_cluster},
    {"fdb_create_cluster", 1, nif_fdb_create_cluster},
    {"fdb_database_create_transaction", 1, nif_fdb_database_create_transaction},
    {"fdb_database_destroy", 1, nif_fdb_database_destroy},
    {"fdb_database_set_option", 3, nif_fdb_database_set_option},
    {"fdb_future_block_until_ready", 1, nif_fdb_future_block_until_ready},
    {"fdb_future_cancel", 1, nif_fdb_future_cancel},
    {"fdb_future_destroy", 1, nif_fdb_future_destroy},
    {"fdb_future_get_cluster", 1, nif_fdb_future_get_cluster},
    {"fdb_future_get_database", 1, nif_fdb_future_get_database},
    {"fdb_future_get_error", 1, nif_fdb_future_get_error},
    {"fdb_future_get_key", 1, nif_fdb_future_get_key},
    {"fdb_future_get_keyvalue_array", 1, nif_fdb_future_get_keyvalue_array},
    {"fdb_future_get_string_array", 1, nif_fdb_future_get_string_array},
    {"fdb_future_get_value", 1, nif_fdb_future_get_value},
    {"fdb_future_get_version", 1, nif_fdb_future_get_version},
    {"fdb_future_is_ready", 1, nif_fdb_future_is_ready},
    {"fdb_future_release_memory", 1, nif_fdb_future_release_memory},
    {"fdb_get_error", 1, nif_fdb_get_error},
    {"fdb_network_set_option", 3, nif_fdb_network_set_option},
    {"fdb_run_network", 0, nif_fdb_run_network},
    {"fdb_select_api_version", 1, nif_fdb_select_api_version},
    {"fdb_setup_network", 0, nif_fdb_setup_network},
    {"fdb_stop_network", 0, nif_fdb_stop_network},
    {"fdb_transaction_add_conflict_range", 6, nif_fdb_transaction_add_conflict_range},
    {"fdb_transaction_atomic_op", 6, nif_fdb_transaction_atomic_op},
    {"fdb_transaction_cancel", 1, nif_fdb_transaction_cancel},
    {"fdb_transaction_clear", 2, nif_fdb_transaction_clear},
    {"fdb_transaction_clear_range", 3, nif_fdb_transaction_clear_range},
    {"fdb_transaction_commit", 1, nif_fdb_transaction_commit},
    {"fdb_transaction_destroy", 1, nif_fdb_transaction_destroy},
    {"fdb_transaction_get", 2, nif_fdb_transaction_get},
    {"fdb_transaction_get_addresses_for_key", 2, nif_fdb_transaction_get_addresses_for_key},
    {"fdb_transaction_get_committed_version", 2, nif_fdb_transaction_get_committed_version},
    {"fdb_transaction_get_key", 6, nif_fdb_transaction_get_key},
    {"fdb_transaction_get_range", 13, nif_fdb_transaction_get_range},
    {"fdb_transaction_get_range_selector", 10, nif_fdb_transaction_get_range_selector},
    {"fdb_transaction_get_read_version", 1, nif_fdb_transaction_get_read_version},
    {"fdb_transaction_on_error", 2, nif_fdb_transaction_on_error},
    {"fdb_transaction_reset", 1, nif_fdb_transaction_reset},
    {"fdb_transaction_set", 3, nif_fdb_transaction_set},
    {"fdb_transaction_set_option", 4, nif_fdb_transaction_set_option},
    {"fdb_transaction_set_read_version", 2, nif_fdb_transaction_set_read_version},
    {"fdb_transaction_watch", 3, nif_fdb_transaction_watch},
    {"new_cluster", 0, nif_new_cluster},
    {"new_database", 0, nif_new_database},
    {"new_transaction", 0, nif_new_transaction},
    {"send_on_complete",3, send_on_complete}
};


ERL_NIF_INIT(fdb_nif,nifs,nif_on_load,NULL,NULL,NULL)

