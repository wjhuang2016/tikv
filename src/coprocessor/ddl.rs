// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;
use std::borrow::BorrowMut;
use std::sync::Arc;
use tipb::{self, DdlRequest};
use tidb_query_datatype::codec::data_type::{BATCH_MAX_SIZE, ChunkRef, LogicalRows, VectorValue};
use tidb_query_datatype::expr::{EvalConfig, EvalContext};
use tidb_query_executors::BatchTableScanExecutor;
use tidb_query_executors::util::scan_executor::*;
use tidb_query_executors::interface::BatchExecutor;
use tikv_alloc::MemoryTraceGuard;
use protobuf::Message;
use crate::coprocessor::dag::TikvStorage;
use crate::coprocessor::{ReqContext, RequestHandler, *};
use crate::storage::{SnapshotStore, Snapshot};
use kvproto::coprocessor::{KeyRange, Response};
use tidb_query_datatype::codec::batch::LazyBatchColumnVec;
use tidb_query_datatype::codec::datum_codec::EvaluableDatumEncoder;
use tikv_util::codec::number::NumberEncoder;

// `DDLContext` is used to handle `AnalyzeReq`
pub struct DDLContext<S: Snapshot> {
    req: DdlRequest,
    ranges: Vec<KeyRange>,
    storage: Option<TikvStorage<SnapshotStore<S>>>,
}

impl<S: Snapshot> DDLContext<S> {
    pub fn new(
        req: DdlRequest,
        ranges: Vec<KeyRange>,
        start_ts: u64,
        snap: S,
        req_ctx: &ReqContext,
    ) -> Result<Self> {
        let store = SnapshotStore::new(
            snap,
            start_ts.into(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
            req_ctx.bypass_locks.clone(),
            req_ctx.access_locks.clone(),
            false,
        );
        Ok(Self{
            req,
            ranges,
            storage: Some(TikvStorage::new(store, false)),
        })
    }
}

#[async_trait]
impl<S: Snapshot> RequestHandler for DDLContext<S> {
    async fn handle_request(&mut self) -> Result<MemoryTraceGuard<Response>> {
        let columns_info: Vec<_> = self.req.take_columns().into();
        let idx_columns_info: tipb::IndexInfo = self.req.take_index_info();
        let table_info: tipb::TableInfo = self.req.take_table_info();
        if columns_info.is_empty() {
            return Err(box_err!("empty columns_info"));
        }
        let common_handle_ids = self.req.take_primary_column_ids();
        let mut table_scanner = BatchTableScanExecutor::new(
            self.storage.take().unwrap(),
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            self.ranges.clone(),
            common_handle_ids.clone(),
            false,
            false, // Streaming mode is not supported in Analyze request, always false here
            self.req.take_primary_prefix_column_ids(),
        )?;

        let mut total_idx_key:Vec<Vec<u8>> = vec![];
        let mut total_idx_value:Vec<Vec<u8>> = vec![];

        let mut is_drained = false;
        while !is_drained {
            let mut result = table_scanner.next_batch(BATCH_MAX_SIZE).await;
            is_drained = result.is_drained?;

            let columns_slice = result.physical_columns.as_mut_slice();
            let columns_info = &columns_info[..];
            let int_handle_offset = columns_info.len()-1;

            for idx in 0..columns_info.len() {
                columns_slice[idx].ensure_decoded(EvalContext::default().borrow_mut(), &field_type_from_column_info(&columns_info[idx]), LogicalRows::from_slice(result.logical_rows.as_slice())).unwrap();
            }


            let columns_slice = result.physical_columns.as_slice();

            let handles = columns_slice[int_handle_offset].decoded();

            for logical_row_offset in &result.logical_rows {
                // let mut index_values = vec![];
                let mut idx_key = vec![];
                let idx_value: Vec<u8> = vec!['0' as u8];

                let i: i64;
                match &handles {
                    VectorValue::Int(v)  => {
                        i = *v.get_option_ref(*logical_row_offset).unwrap();
                    }
                    _ => {
                        panic!("panic")
                    }
                }

                gen_index_key(&result.physical_columns, *logical_row_offset, &table_info, &idx_columns_info,   i, &mut idx_key).unwrap();
                total_idx_key.push(idx_key);
                total_idx_value.push(idx_value);
            }
        }
        // while let Some((k, v)) = self.scanner.next()? {
        //
        // }

        let resp = DdlResult::new(total_idx_key, total_idx_value).into_proto();

        let data = box_try!(resp.write_to_bytes());

        let mut resp = Response::default();
        resp.set_data(data);


        Ok(resp.into())
    }
}

/// `DdlResult` collect the result of DDL request.
#[derive(Default)]
struct DdlResult {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
}

impl DdlResult {
    fn new(index_key: Vec<Vec<u8>>, index_value: Vec<Vec<u8>>) -> DdlResult {
        DdlResult {
            keys: index_key,
            values: index_value,
        }
    }

    fn into_proto(self) -> tipb::DdlResponse {
        let mut res = tipb::DdlResponse::default();
        res.set_keys(protobuf::RepeatedField::from_vec(self.keys));
        res.set_values(protobuf::RepeatedField::from_vec(self.values));
        res
    }
}

fn gen_index_key(indexed_values: &LazyBatchColumnVec, row_index: usize, table_info: &tipb::TableInfo, index_info: &tipb::IndexInfo, handle: i64, index_buf: &mut Vec<u8>) -> Result<()> {
    // if index_info.has_unique() {
    //
    // }
    index_buf.extend_from_slice("t".as_bytes());
    index_buf.encode_i64(table_info.get_table_id()).unwrap();
    index_buf.extend_from_slice("_i".as_bytes());
    index_buf.encode_i64(index_info.get_index_id()).unwrap();

    // Don't consider handle here.
    for  idx in 0..indexed_values.columns_len()-1 {
        let idx_value = indexed_values[idx].decoded();
        idx_value.encode_sort_key(row_index, &field_type_from_column_info(&index_info.get_columns()[idx]), EvalContext::default().borrow_mut(), index_buf).unwrap();
    }

    index_buf.write_evaluable_datum_int( handle, false).unwrap();
    Ok(())
}