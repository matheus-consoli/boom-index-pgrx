mod foo;

use std::ffi::{c_int, c_void};
use std::ptr::null_mut;

use pgrx::{
    pg_sys::{self, *},
    prelude::*,
};

pgrx::pg_module_magic!();


/// Index AM handler function
/// returns IndexAmRoutine with access method parameters and callbacks
#[pgrx::pg_extern(sql = "
    CREATE OR REPLACE FUNCTION fulon_filter(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
    CREATE ACCESS METHOD bloom_fulon TYPE INDEX HANDLER fulon_filter;
    CREATE OPERATOR CLASS int4_ops DEFAULT FOR TYPE int4 USING bloom_fulon AS OPERATOR 1= (int4, int4), FUNCTION 1 hashint4(int4);
")]
fn fulon_filter(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut am_routine =
        unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(NodeTag::T_IndexAmRoutine) };

    am_routine.amstrategies = 0;
    am_routine.amsupport = 1;
    am_routine.amoptsprocnum = 0;

    am_routine.amcanorder = false;
    am_routine.amcanorderbyop = false;
    am_routine.amcanbackward = false;
    am_routine.amcanunique = false;
    am_routine.amcanmulticol = false;
    am_routine.amoptionalkey = false;
    am_routine.amsearcharray = false;
    am_routine.amsearchnulls = false;
    am_routine.amstorage = false;
    am_routine.amclusterable = false;
    am_routine.ampredlocks = false;
    am_routine.amcanparallel = false;
    am_routine.amcaninclude = false;
    am_routine.amusemaintenanceworkmem = false;
    am_routine.amkeytype = pg_sys::InvalidOid;
    am_routine.amparallelvacuumoptions = 0;

    am_routine.ambuild = Some(dibuild);
    am_routine.ambuildempty = Some(dibuildempty);
    am_routine.aminsert = Some(diinsert);
    am_routine.ambulkdelete = Some(dibulkdelete);
    am_routine.amvacuumcleanup = Some(divacuumcleanup);

    am_routine.amcanreturn = None;

    am_routine.amcostestimate = Some(dicostestimate);
    am_routine.amoptions = Some(dioptions);

    am_routine.amproperty = None;
    am_routine.ambuildphasename = None;

    am_routine.amvalidate = Some(divalidate);
    am_routine.ambeginscan = Some(dibeginscan);
    am_routine.amrescan = Some(direscan);

    am_routine.amgettuple = None;
    am_routine.amgetbitmap = None;

    am_routine.amendscan = Some(diendscan);

    am_routine.ammarkpos = None;
    am_routine.amrestrpos = None;
    am_routine.amestimateparallelscan = None;
    am_routine.aminitparallelscan = None;
    am_routine.amparallelrescan = None;

    am_routine.into_pg_boxed()
}

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    info!("_PG_init");
}

// build a new index
#[pg_guard]
pub unsafe extern "C" fn dibuild(
    _heap: pg_sys::Relation,
    _index: pg_sys::Relation,
    _index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    info!("dibuild");
    let mut index_build_result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    index_build_result.heap_tuples = 0.0;
    index_build_result.index_tuples = 0.0;
    index_build_result.into_pg()
}

// build an empty index for the initialization fork
#[pg_guard]
pub unsafe extern "C" fn dibuildempty(_relation: pg_sys::Relation) {
    info!("dibuildempty");
}

// insert into ...
// insert new tuple to index AM
#[pg_guard]
pub unsafe extern "C" fn diinsert(
    _index_relation: Relation,
    _values: *mut Datum,
    _isnull: *mut bool,
    _heap_tid: ItemPointer,
    _heap_relation: Relation,
    _check_unique: IndexUniqueCheck,
    _index_info: *mut IndexInfo,
) -> bool {
    info!("diinsert");
    false
}

/// Bulk deletion of all index entries pointing to a set of table tuples
#[pg_guard]
pub unsafe extern "C" fn dibulkdelete(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    _callback_state: *mut c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    info!("dibulkdelete");
    // there's nothing to delete
    // return null as there is nothing to pass to `amvacuumcleanup`
    null_mut()
}

/// Post-VACUUM cleanup for index AM
#[pg_guard]
pub unsafe extern "C" fn divacuumcleanup(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    info!("divacuumcleanup");
    // index has not been modified, so returning NULL is fine
    null_mut()
}

// delete from dummy where i=1;
/// Estimate cost of index AM
#[pg_guard]
pub unsafe extern "C" fn dicostestimate(
    _root: *mut PlannerInfo,
    _path: *mut IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut Cost,
    index_total_cost: *mut Cost,
    index_selectivity: *mut Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    info!("dicostestimate");
    *index_startup_cost = 1e10;
    *index_total_cost = 1e10;
    *index_selectivity = 1.0;
    *index_correlation = 1.0;
    *index_pages = 1.0;
}

#[repr(C)]
struct DummyIndexOptions {
    vl_len: c_int, // varlena header (do not touch directly!!!)
    size: c_int,
}

// Kind of relation options for index
static mut RELOPT_KIND: pg_sys::relopt_kind = 0;


// this function creates a full set of relation option types, with various patterns
unsafe fn create_reloptions_table() -> [pg_sys::relopt_parse_elt; 1] {
    unsafe {
        RELOPT_KIND = add_reloption_kind();
        add_int_reloption(
            RELOPT_KIND,
            "size".as_pg_cstr(),
            "Integer option for fulon_filter".as_pg_cstr(),
            10,
            -10,
            100,
            AccessExclusiveLock as pg_sys::LOCKMODE,
        );
        let tab: [pg_sys::relopt_parse_elt; 1] = [pg_sys::relopt_parse_elt {
            optname: "size".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: core::mem::offset_of!(DummyIndexOptions, size) as i32,
        }];
        tab
    }
}

/// Parse relation options for index AM, returning a DummyIndexOptions structure filled with option values
#[pg_guard]
pub unsafe extern "C" fn dioptions(reloptions: Datum, validate: bool) -> *mut bytea {
    info!("dioptions validate={validate}");
    let tab = create_reloptions_table();
    // parse the user-given reloptions
    build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND,
        std::mem::size_of::<DummyIndexOptions>(),
        tab.as_ptr(),
        tab.len() as i32,
    ) as *mut bytea
}


/// Validator for Index AM
#[pg_guard]
pub unsafe extern "C" fn divalidate(_opclassoid: Oid) -> bool {
    info!("divalidate");
    // index is dummy, so we are happy with any opclass
    true
}

// Begin scan of index AM
#[pg_guard]
pub unsafe extern "C" fn dibeginscan(
    index_relation: Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> IndexScanDesc {
    info!("dibeginscan");
    // pretend we're doing something
    let scan: PgBox<IndexScanDescData> =
        unsafe { PgBox::from_pg(RelationGetIndexScan(index_relation, nkeys, norderbys)) };
    scan.into_pg()
}

/// Rescan of index AM
#[pg_guard]
pub unsafe extern "C" fn direscan(
    _scan: IndexScanDesc,
    _keys: ScanKey,
    _nkeys: c_int,
    _orderbys: ScanKey,
    _norderbys: c_int,
) {
    // nothing to do
    info!("direscan");
}

/// End scan of index AM
#[pg_guard]
pub unsafe extern "C" fn diendscan(_scan: IndexScanDesc) {
    // nothing to do
    info!("diendscan");
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
