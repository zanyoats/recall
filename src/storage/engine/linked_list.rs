pub mod page;
mod pager;

use std::collections::HashMap;

use crate::storage::freelist;
use crate::storage::engine::Engine;
use crate::storage::engine::RecordValue;
use crate::storage::engine::EngineError;
use crate::storage::engine::ArgType;
use crate::storage::engine::SchemaArg;
use crate::storage::engine::SchemaType;
use pager::Pager;

pub struct Predicate {
    base_dir: String,
    name: String,
    pager: Pager,
}

trait DirPageOps {
    fn get_head_page_num(&mut self) -> Option<usize>;
    fn set_head_page_num(&mut self, page_num: usize);
    fn get_tail_page_num(&mut self) -> Option<usize>;
    fn set_tail_page_num(&mut self, page_num: usize);
    fn get_num_pages(&mut self) -> usize;
    fn inc_num_pages(&mut self, up: bool);
    fn get_num_cells(&mut self) -> usize;
    fn inc_num_cells(&mut self, up: bool);
    fn get_arity(&mut self) -> usize;
    fn get_stride(&mut self) -> usize;
    fn get_data_page_capacity(&mut self) -> usize;
    fn get_record_info(&mut self, arg_num: usize) -> RecordInfo;
    fn get_key_offsets(&mut self) -> Vec<usize>;
    fn get_next_data_page_num(&mut self) -> usize;
    fn print_debug_info(&mut self);
}

struct RecordInfo {
    type_name: String,
    record_offset: usize,
}

trait DataPageOps {
    fn get_pred_page_num(&mut self, page_num: usize) -> Option<usize>;
    fn set_pred_page_num(&mut self, page_num: usize, new_value: usize);
    fn get_succ_page_num(&mut self, page_num: usize) -> Option<usize>;
    fn set_succ_page_num(&mut self, page_num: usize, new_value: usize);
    fn get_data_num_cells(&mut self, page_num: usize) -> usize;
    fn inc_data_num_cells(&mut self, page_num: usize, up: bool);
    fn get_arg(
        &mut self,
        page_num: usize,
        cell_num: usize,
        arg_num: usize,
    ) -> ArgType;
    fn put_arg(
        &mut self,
        page_num: usize,
        cell_num: usize,
        arg_num: usize,
        value: &ArgType,
    );
    fn get(
        &mut self,
        page_num: usize,
        cell_num: usize,
    ) -> Vec<ArgType>;
    fn put(
        &mut self,
        page_num: usize,
        cell_num: usize,
        value: &Vec<ArgType>,
    );
    fn delete(
        &mut self,
        page_num: usize,
        cell_num: usize,
    );
    fn repair_linkage_after_delete(
        &mut self,
        page_num: usize,
    );
    fn print_page_debug_info(&mut self, page_num: usize);
}

impl Predicate {
    pub fn print_dir_page_info(&mut self) {
        self.print_debug_info();
    }

    pub fn print_data_page_info(&mut self, page_num: usize) {
        self.print_page_debug_info(page_num);
    }
}

impl Engine for Predicate {
    // type Item = ArgType;
    // type RecordItem = RecordResult;
    type Iter<'a> = PredicateIterator<'a>;

    fn create(
        base_dir: &str,
        name: &str,
        schema: Vec<SchemaArg>,
        keys: Vec<usize>,
    ) -> Self {
        let mut pager = Pager::create(base_dir, name);
        let page = pager.get_mut(page::DIR_PAGE_NUM);
        page::init_dir_page(page, schema, keys);

        Predicate {
            base_dir: base_dir.to_string(),
            name: name.to_string(),
            pager,
        }
    }

    fn open(base_dir: &str, name: &str) -> Self {
        let pager = Pager::open(base_dir, name);
        Predicate {
            base_dir: base_dir.to_string(),
            name: name.to_string(),
            pager,
         }
    }

    fn artiy(&mut self) -> usize {
        self.get_arity()
    }

    fn iter<'a>(&'a mut self) -> PredicateIterator<'a> {
        match self.get_head_page_num() {
            Some(head_page_num) => {
                let tail_page_num = self.get_tail_page_num().unwrap();
                let head_num_cells = self.get_data_num_cells(head_page_num);
                assert!(head_num_cells > 0);
                let tail_num_cells = self.get_data_num_cells(tail_page_num);
                assert!(tail_num_cells > 0);
                let head_cell_num = 0usize;
                let tail_cell_num = tail_num_cells - 1;
                let front = RecordLocator(head_page_num, head_cell_num);
                let back = RecordLocator(tail_page_num, tail_cell_num);
                PredicateIterator {
                    predicate: self, locs: Some((front, back))
                }
            }
            None => {
                PredicateIterator {
                    predicate: self, locs: None
                }
            }
        }
    }

    fn assertz(&mut self, value: &Vec<ArgType>) {
        let (page_num, cell_num) =
            match self.get_tail_page_num() {
                Some(tail_page_num) => {
                    let tail_page_num_cells = self.get_data_num_cells(tail_page_num);
                    let data_cap = self.get_data_page_capacity();

                    if tail_page_num_cells == data_cap { /* need to grow a new page */
                        let new_tail_page_num = self.get_next_data_page_num();
                        self.set_succ_page_num(tail_page_num, new_tail_page_num);
                        self.set_pred_page_num(new_tail_page_num, tail_page_num);
                        self.set_tail_page_num(new_tail_page_num);
                        (new_tail_page_num, 0)
                    } else { /* have more room for new records */
                        (tail_page_num, tail_page_num_cells)
                    }
                }
                None => {
                    let first_page_num = self.get_next_data_page_num();
                    self.set_head_page_num(first_page_num);
                    self.set_tail_page_num(first_page_num);
                    (first_page_num, 0)
                }
            };

        self.put(page_num, cell_num, value);
        self.inc_num_cells(true);
        self.inc_data_num_cells(page_num, true);
    }

    fn retract(
        &mut self,
        filters: &HashMap<usize, ArgType>,
    ) -> Result<(), EngineError> {
        self
        .iter()
        .find_map(|result| {
            if record_matches(&result.value, filters) {
                Some(result.locator)
            } else {
                None
            }
        })
        .map_or_else(
            || Err(EngineError::RecordNotFound),
            |RecordLocator(page_num, cell_num )| {
                self.delete(page_num, cell_num);
                Ok(())
            },
        )
    }

    fn save(&mut self) {
        self.pager.flush_all_pages();
    }
}

pub struct PredicateIterator<'a> {
    predicate: &'a mut Predicate,
    locs: Option<(RecordLocator, RecordLocator)>,
}

#[derive(Debug, Copy, Clone)]
struct RecordLocator(usize, usize);

#[derive(Debug)]
pub struct RecordResult {
    locator: RecordLocator,
    pub value: Vec<ArgType>,
}

impl RecordValue for RecordResult {
    fn value(&self) -> &Vec<ArgType> {
        &self.value
    }
}

impl<'a> Iterator for PredicateIterator<'a> {
    type Item = RecordResult;

    fn next(&mut self) -> Option<Self::Item> {
        self
        .locs
        .take()
        .map(|(front, back)| {
            let result = self.predicate.get(front.0, front.1);
            self.step_forward(front, back);
            RecordResult { locator: front, value: result }
        })
    }
}

impl<'a> DoubleEndedIterator for PredicateIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self
        .locs
        .take()
        .map(|(front, back)| {
            let result = self.predicate.get(back.0, back.1);
            self.step_backward(front, back);
            RecordResult { locator: back, value: result }
        })
    }
}

impl<'a> PredicateIterator<'a> {
    fn front_back_overlap(front: &RecordLocator, back: &RecordLocator) -> bool {
           front.0 == back.0
        && front.1 == back.1
    }

    fn step_forward(&mut self, front: RecordLocator, back: RecordLocator) {
        if Self::front_back_overlap(&front, &back) {
            self.locs = None;
            return;
        }

        let num_cells = self.predicate.get_data_num_cells(front.0);
        let succ_cell_num = front.1 + 1;

        if succ_cell_num == num_cells {
            let succ_page_num =
                self
                .predicate
                .get_succ_page_num(front.0)
                .unwrap(); // since no overlap, should always succeed
            let new_front = RecordLocator(succ_page_num, 0);
            self.locs = Some((new_front, back))
        } else {
            let new_front = RecordLocator(front.0, succ_cell_num);
            self.locs = Some((new_front, back));
        }
    }

    fn step_backward(&mut self, front: RecordLocator, back: RecordLocator) {
        if Self::front_back_overlap(&front, &back) {
            self.locs = None;
            return;
        }

        if back.1 == 0 {
            let num_cells = self.predicate.get_data_num_cells(back.0);
            let pred_page_num =
                self
                .predicate
                .get_pred_page_num(back.0)
                .unwrap(); // since no overlap, should always succeed
            let new_back = RecordLocator(pred_page_num, num_cells - 1);
            self.locs = Some((front, new_back))
        } else {
            let new_back = RecordLocator(back.0, back.1 - 1);
            self.locs = Some((front, new_back));
        }
    }
}

impl DirPageOps for Predicate {
    fn get_head_page_num(&mut self) -> Option<usize> {
        let page = self.pager.get(page::DIR_PAGE_NUM);
        let page_num = page::read_usize_offset(page, page::HEAD_PAGE_OFFSET);
        if page_num == page::NULL_PAGE_PTR {
            None
        } else {
            Some(page_num)
        }
    }

    fn set_head_page_num(&mut self, page_num: usize) {
        let page = self.pager.get_mut(page::DIR_PAGE_NUM);
        page::write_usize_offset(page, page::HEAD_PAGE_OFFSET, page_num)
    }

    fn get_tail_page_num(&mut self) -> Option<usize> {
        let page = self.pager.get(page::DIR_PAGE_NUM);
        let page_num = page::read_usize_offset(page, page::TAIL_PAGE_OFFSET);
        if page_num == page::NULL_PAGE_PTR {
            None
        } else {
            Some(page_num)
        }
    }

    fn set_tail_page_num(&mut self, page_num: usize) {
        let page = self.pager.get_mut(page::DIR_PAGE_NUM);
        page::write_usize_offset(page, page::TAIL_PAGE_OFFSET, page_num)
    }

    fn get_num_pages(&mut self) -> usize {
        let page = self.pager.get(page::DIR_PAGE_NUM);
        page::read_usize_offset(page, page::NUM_PAGES_OFFSET)
    }

    fn inc_num_pages(&mut self, up: bool) {
        let num_pages = self.get_num_pages();
        let page = self.pager.get_mut(page::DIR_PAGE_NUM);
        let value = if up { num_pages + 1 } else { num_pages - 1 };
        page::write_usize_offset(page, page::NUM_PAGES_OFFSET, value)
    }

    fn get_num_cells(&mut self) -> usize {
        let page = self.pager.get(page::DIR_PAGE_NUM);
        page::read_usize_offset(page, page::NUM_CELLS_OFFSET)
    }

    fn inc_num_cells(&mut self, up: bool) {
        let num_cells = self.get_num_cells();
        let page = self.pager.get_mut(page::DIR_PAGE_NUM);
        let value = if up { num_cells + 1 } else { num_cells - 1 };
        page::write_usize_offset(page, page::NUM_CELLS_OFFSET, value)
    }

    fn get_arity(&mut self) -> usize {
        let page = self.pager.get(page::DIR_PAGE_NUM);
        page::read_usize_offset(page, page::ARITY_OFFSET)
    }

    fn get_stride(&mut self) -> usize {
        let page = self.pager.get(page::DIR_PAGE_NUM);
        page::read_usize_offset(page, page::STRIDE_OFFSET)
    }

    fn get_data_page_capacity(&mut self) -> usize {
        let stride = self.get_stride();
        if stride == 0 { 0 } // zero for 0-arity predicates
        else { page::DATA_CELL_SPACE / stride }
    }

    fn get_record_info(&mut self, arg_num: usize) -> RecordInfo {
        let arity = self.get_arity();
        assert!(
            arg_num < arity,
            "Error: arg num out of range",
        );

        let page = self.pager.get(page::DIR_PAGE_NUM);

        let arg_cell_offset =
              page::DIR_HEADER_SIZE
            + arg_num * page::ARG_CELL_SIZE;

        let type_name = page::read_string_offset(
            page,
            arg_cell_offset,
        );
        let record_offset = page::read_usize_offset(
            page,
            arg_cell_offset + page::ARG_TYPE_SIZE + page::ARG_FLAGS_SIZE,
        );

        RecordInfo { type_name, record_offset }
    }

    fn get_key_offsets(&mut self) -> Vec<usize> {
        let page = self.pager.get(page::DIR_PAGE_NUM);
        let stringfied_offset = page::read_string_offset(&*page, page::KEYS_OFFSET_OFFSET);
        page::parse_stringified_keys(&stringfied_offset).unwrap()
    }

    fn get_next_data_page_num(&mut self) -> usize {
        self.inc_num_pages(true);

        freelist::get(&self.base_dir, &self.name)
        .unwrap_or_else(|| {
            self.get_num_pages()
        })
    }

    fn print_debug_info(&mut self) {
        println!("Directory page data:");
        println!("\tnum pages {}", self.get_num_pages());
        println!("\tnum cells {}", self.get_num_cells());
        println!("\thead page num {}", self.get_head_page_num().unwrap_or(page::NULL_PAGE_PTR));
        println!("\ttail page num {}", self.get_tail_page_num().unwrap_or(page::NULL_PAGE_PTR));
        println!("\tarity {}", self.get_arity());
        println!("\tarity cap {}", page::ARG_CELL_CAPACITY);
        println!("\tstride {}", self.get_stride());
        println!("\tkey offsets {:?}", self.get_key_offsets());
        println!("\tdata cap {}", self.get_data_page_capacity());
        println!("\tschema:");

        let arity = self.get_arity();
        let page = self.pager.get(page::DIR_PAGE_NUM);

        for arg_num in 0..arity {
            let arg_cell_offset = page::DIR_HEADER_SIZE + arg_num * page::ARG_CELL_SIZE;
            let arg_type = page::read_string_offset(page, arg_cell_offset);
            let flags = page::read_u32_offset(page, arg_cell_offset + page::ARG_TYPE_SIZE);
            let offset = page::read_usize_offset(page, arg_cell_offset + page::ARG_TYPE_SIZE + page::ARG_FLAGS_SIZE);

            println!("\t\targ num {} type {} flags {} offset {}",
                arg_num, arg_type, flags, offset);
        }
    }
}

impl DataPageOps for Predicate {
    fn get_pred_page_num(&mut self, page_num: usize) -> Option<usize> {
        let page = self.pager.get(page_num);
        let page_num = page::read_usize_offset(page, page::PRED_OFFSET);
        if page_num == page::NULL_PAGE_PTR {
            None
        } else {
            Some(page_num)
        }
    }

    fn set_pred_page_num(&mut self, page_num: usize, new_value: usize) {
        let page = self.pager.get_mut(page_num);
        page::write_usize_offset(page, page::PRED_OFFSET, new_value)
    }

    fn get_succ_page_num(&mut self, page_num: usize) -> Option<usize> {
        let page = self.pager.get(page_num);
        let page_num = page::read_usize_offset(page, page::SUCC_OFFSET);
        if page_num == page::NULL_PAGE_PTR {
            None
        } else {
            Some(page_num)
        }
    }

    fn set_succ_page_num(&mut self, page_num: usize, new_value: usize) {
        let page = self.pager.get_mut(page_num);
        page::write_usize_offset(page, page::SUCC_OFFSET, new_value)
    }

    fn get_data_num_cells(&mut self, page_num: usize) -> usize {
        let page = self.pager.get(page_num);
        page::read_usize_offset(page, page::DATA_NUM_CELLS_OFFSET)
    }

    fn inc_data_num_cells(&mut self, page_num: usize, up: bool) {
        let num_cells = self.get_data_num_cells(page_num);
        let page = self.pager.get_mut(page_num);
        let value = if up { num_cells + 1 } else { num_cells - 1 };
        page::write_usize_offset(page, page::DATA_NUM_CELLS_OFFSET, value)
    }

    fn get_arg(
        &mut self,
        page_num: usize,
        cell_num: usize,
        arg_num: usize,
    ) -> ArgType {
        let RecordInfo {
            type_name, record_offset
        } = self.get_record_info(arg_num);
        let schema_type: SchemaType = type_name.parse().unwrap();
        let stride = self.get_stride();
        let page = self.pager.get_mut(page_num);
        let data_cell_offset =
              page::DATA_HEADER_SIZE
            + cell_num * stride;

        match schema_type {
            SchemaType::Int => {
                let value = page::read_i64_offset(
                    &*page,
                    data_cell_offset + record_offset,
                );
                ArgType::Int(value)
            }

            SchemaType::String(_) => {
                let value = page::read_string_offset(
                    &*page,
                    data_cell_offset + record_offset,
                );
                ArgType::String(value)
            }
        }
    }

    fn put_arg(
        &mut self,
        page_num: usize,
        cell_num: usize,
        arg_num: usize,
        value: &ArgType,
    ) {
        let RecordInfo {
            type_name, record_offset
        } = self.get_record_info(arg_num);
        let schema_type: SchemaType = type_name.parse().unwrap();
        let stride = self.get_stride();
        let page = self.pager.get_mut(page_num);
        let data_cell_offset =
              page::DATA_HEADER_SIZE
            + cell_num * stride;

        // Workaround for issue: https://github.com/rust-lang/rust/issues/86935
        type EngineItem = ArgType;

        match schema_type {
            SchemaType::Int => {
                if let EngineItem::Int(value) = value {
                    page::write_i64_offset(
                        page,
                        data_cell_offset + record_offset,
                        *value,
                    );
                } else {
                    panic!("Type Error: expected int");
                }
            }

            SchemaType::String(max_length) => {
                if let EngineItem::String(value) = value {
                    page::write_string_offset(
                        page,
                        data_cell_offset + record_offset,
                        value,
                        max_length,
                    );
                } else {
                    panic!("Type Error: expected string");
                }
            }
        }
    }

    fn get(
        &mut self,
        page_num: usize,
        cell_num: usize,
    ) -> Vec<ArgType> {
        let arity = self.get_arity();

        (0..arity)
        .map(|arg_num| {
            self.get_arg(page_num, cell_num, arg_num)
        })
        .collect()
    }

    fn put(
        &mut self,
        page_num: usize,
        cell_num: usize,
        value: &Vec<ArgType>,
    ) {
        let arity = self.get_arity();
        assert!(value.len() == arity);

        for arg_num in 0..arity {
            self.put_arg(
                page_num,
                cell_num,
                arg_num,
                &value[arg_num],
            );
        }
    }

    fn delete(
        &mut self,
        page_num: usize,
        cell_num: usize,
    ) {
        let num_cells = self.get_data_num_cells(page_num);
        assert!(num_cells > 0);
        assert!(cell_num < num_cells);
        let stride = self.get_stride();
        let page = self.pager.get_mut(page_num);

        // shift all cells to left by 1 after `cell_num`
        for i in cell_num + 1..num_cells {
            let src_offset = page::DATA_HEADER_SIZE + i * stride;
            let dest_offset = page::DATA_HEADER_SIZE + (i - 1) * stride;
            page.copy_within(
                src_offset..src_offset + stride,
                dest_offset,
            );
        }

        self.inc_num_cells(false);
        self.inc_data_num_cells(page_num, false);

        if num_cells == 1 { /* reach empty page condition */
            freelist::add(&self.base_dir, &self.name, page_num);
            self.inc_num_pages(false);
            self.repair_linkage_after_delete(page_num);
            // zero out links
            self.set_pred_page_num(page_num, page::NULL_PAGE_PTR);
            self.set_succ_page_num(page_num, page::NULL_PAGE_PTR);
        }
    }

    fn repair_linkage_after_delete(
        &mut self,
        page_num: usize,
    ) {
        match (
            self.get_pred_page_num(page_num),
            self.get_succ_page_num(page_num),
        ) {
            (Some(pred), Some(succ)) => {
                self.set_succ_page_num(pred, succ);
                self.set_pred_page_num(succ, pred);
            }
            (Some(pred), None) => {
                self.set_succ_page_num(pred, page::NULL_PAGE_PTR);
                self.set_tail_page_num(pred);
            }
            (None, Some(succ)) => {
                self.set_pred_page_num(succ, page::NULL_PAGE_PTR);
                self.set_head_page_num(succ);
            }
            (None, None) => {
                self.set_head_page_num(page::NULL_PAGE_PTR);
                self.set_tail_page_num(page::NULL_PAGE_PTR);
            }
        }
    }

    fn print_page_debug_info(&mut self, page_num: usize) {
        println!("Debug info for page {}", page_num);
        println!("\tpage size {}", page::PAGE_SIZE);
        println!("\theader size {}", page::DATA_HEADER_SIZE);
        println!("\tpred page num {}", self.get_pred_page_num(page_num).unwrap_or(page::NULL_PAGE_PTR));
        println!("\tsucc page num {}", self.get_succ_page_num(page_num).unwrap_or(page::NULL_PAGE_PTR));
        println!("\tnum cells {}", self.get_data_num_cells(page_num));
    }
}

pub fn record_matches(
    record: &Vec<ArgType>,
    filters: &HashMap<usize, ArgType>,
) -> bool {
    filters
    .keys()
    .all(|arg_num| {
        let filter_arg = filters.get(arg_num).unwrap();
        let stored_arg = &record[*arg_num];

        *filter_arg == *stored_arg
    })
}
