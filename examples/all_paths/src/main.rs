use std::env;
use std::io;
use std::io::Read;
use std::io::Write;
use std::fs;
use std::fs::OpenOptions;
use std::collections::HashMap;

use recall::eval::Eval;
use recall::eval::DbEngine;
use recall::eval::naive::Naive;
use recall::lang::Program;
use recall::lang::Term;
use recall::storage::engine::Engine;
use recall::storage::engine::linked_list;
use recall::storage::engine::SchemaType;
use recall::storage::engine::SchemaArg;
use recall::storage::engine::ArgType;
use recall::storage::engine::NO_FLAGS;
use recall::storage::engine::RecordValue;

fn main() {
    let args: Vec<String> = env::args().collect();
    let args: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let program_name = args[0];
    match &args[1..] {
        [] => {
            let default_base_dir = "./paths_db";
            run_repl(default_base_dir, None);
        }
        ["--create-db"] => {
            create_db("./paths_db");
        }
        [file_path] => {
            let default_base_dir = "./paths_db";
            let mut program_file =
                OpenOptions::new()
                .read(true)
                .open(file_path)
                .expect("Could not read program file");
            let mut program_string = String::new();
            program_file.read_to_string(&mut program_string).unwrap();
            let program = Program::from_string(&program_string).unwrap();
            run_repl(default_base_dir, Some(program));
        }
        _ => {
            panic!(
                "usage error: {} [--create-courses-db] [--create-paths-db]",
                program_name);
        }
    }
}

fn run_repl(base_dir: &str, additional: Option<Program>) {
    let mut db_engine = LinkedListEngine::<linked_list::Predicate>::open(base_dir).unwrap();
    loop {
        io::stdout()
        .write_all(b"repl> ")
        .expect("Failed to write to stdout");

        io::stdout()
        .flush()
        .expect("Failed to flush stdout");

        let mut input = String::new();

        io::stdin()
        .read_line(&mut input)
        .expect("Failed to read user input");

        if input.len() == 0 {
            eprintln!("Error: received no input.");
            continue
        }

        let query = match Program::from_string(&input) {
            Ok(Program{ facts, rules}) if facts.is_empty() && rules.is_empty() => continue,
            Ok(Program{ facts, rules}) => {
                let cond1 = facts.len() == 1 && rules.len() == 0;
                let cond2 = facts.len() == 0 && rules.len() == 1;
                assert!(cond1 || cond2);
                if cond1 {
                    Ok(facts.into_iter().next().unwrap())
                } else {
                    Err(rules.into_iter().next().unwrap())
                }
            },
            Err(e) => {
                eprintln!("{}", e);
                continue
            }
        };

        let results = match query {
            Ok(query) => {
                let context =
                    <Naive as Eval>::from_term(&query, additional.as_ref(), &mut db_engine);
                context.execute()
            }
            Err(query) => {
                let context =
                    <Naive as Eval>::from_rule(&query, additional.as_ref(), &mut db_engine);
                context.execute()
            }
        };

        for result in results {
            println!("=> {result}");
        }
    }
}

fn create_db(base_dir: &str) {
    let name = "link";
    let schema = vec![
        SchemaArg {
            schema_type: SchemaType::String(20),
            flags: NO_FLAGS,
        },
        SchemaArg {
            schema_type: SchemaType::String(20),
            flags: NO_FLAGS,
        },
    ];
    let keys = vec![];
    let mut pred = <linked_list::Predicate as Engine>::create(&base_dir, name, schema, keys);

    let links = vec![
        vec![ArgType::String("a".to_string()), ArgType::String("b".to_string())],
        vec![ArgType::String("b".to_string()), ArgType::String("c".to_string())],
        vec![ArgType::String("c".to_string()), ArgType::String("c".to_string())],
        vec![ArgType::String("c".to_string()), ArgType::String("d".to_string())],
    ];

    for elem in links {
        pred.assertz(&elem);
    }
    pred.save();
}

// Get stored terms from db engine
pub struct LinkedListEngine<T: Engine> {
    pub stored_predicates: HashMap<FunctorSpec, T>,
}

type FunctorSpec = (String, usize);

impl<T: Engine> DbEngine for LinkedListEngine<T> {
    fn get_stored_terms(&mut self) -> Vec<Term> {
        self
        .stored_predicates
        .iter_mut()
        .flat_map(|((name, _arity), stored_predicate)| {
            stored_predicate
            .iter()
            .map(|record| {
                record_2_functor(name, record)
            })
        })
        .collect()
    }
}

impl<T: Engine> LinkedListEngine<T> {
    pub fn open(base_dir: &str) -> Result<Self, io::Error> {
        let mut stored_predicates = HashMap::new();

        for result in fs::read_dir(base_dir)? {
            let dir_entry = result?;
            let is_pred_file =
                dir_entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map_or(false, |ext| ext == "db");

            if !is_pred_file { continue }

            let file_name = dir_entry.file_name();
            let name =
                file_name
                .to_str()
                .unwrap()
                .strip_suffix(".db")
                .unwrap();
            eprintln!("DEBUG: added stored predicate: '{name}'");
            let mut pred = T::open(base_dir, name);
            let arity = pred.artiy();
            let key = (name.to_string(), arity);
            stored_predicates.insert(key, pred);
        }

        Ok(LinkedListEngine { stored_predicates })
    }
}

fn record_2_functor<T: RecordValue>(name: &str, record: T) -> Term {
    let terms: Vec<Term> =
        record
        .value()
        .iter()
        .map(|arg_type| match arg_type {
            ArgType::String(v) => Term::String(v.to_string()),
            ArgType::Int(v) => Term::Int(*v),
        })
        .collect();

    Term::Functor(name.to_string(), terms)
}
