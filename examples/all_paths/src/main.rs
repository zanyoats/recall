use std::env;
use std::io;
use std::io::Read;
use std::io::Write;
use std::fs::OpenOptions;

use recall::eval::Eval;
use recall::eval::DbEngine;
use recall::eval::naive::Naive;
use recall::lang::Program;
use recall::lang::Term;
use recall::storage::db::DB;
use recall::storage::db::Predicate;
use recall::storage::engine::btree::format::ATOM_TYPE;
use recall::storage::engine::btree::format::UINT_TYPE;
use recall::storage::engine::btree::format::tuple::ParameterType;

fn main() {
    let args: Vec<String> = env::args().collect();
    let args: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let program_name = args[0];
    match &args[1..] {
        [] => {
            run_repl("./paths.db", None);
        }
        [file_path] => {
            let mut program_file =
                OpenOptions::new()
                .read(true)
                .open(file_path)
                .expect("Could not read program file");
            let mut program_string = String::new();
            program_file.read_to_string(&mut program_string).unwrap();
            let program = Program::from_string(&program_string).unwrap();
            run_repl("./paths.db", Some(program));
        }
        _ => {
            panic!(
                "usage error: {} [program_file]",
                program_name);
        }
    }
}

fn create_db(file_path: &str) -> DB {
    let mut db = DB::new(file_path);
    let links = db.create_predicate("links", &vec![UINT_TYPE], &vec![
        ATOM_TYPE,
        ATOM_TYPE,
    ]);
    links.assert(&vec![
        ParameterType::Atom("a".to_string()),
        ParameterType::Atom("b".to_string()),
    ]).unwrap();
    links.assert(&vec![
        ParameterType::Atom("b".to_string()),
        ParameterType::Atom("c".to_string()),
    ]).unwrap();
    links.assert(&vec![
        ParameterType::Atom("c".to_string()),
        ParameterType::Atom("c".to_string()),
    ]).unwrap();
    links.assert(&vec![
        ParameterType::Atom("c".to_string()),
        ParameterType::Atom("d".to_string()),
    ]).unwrap();
    db
}

fn run_repl(file_path: &str, additional: Option<Program>) {
    let mut db = create_db(file_path);
    let links = db.get_predicate("links");
    let mut dummy_engine = DummyEngine { predicate: links };
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
                    <Naive as Eval>::from_term(&query, additional.as_ref(), &mut dummy_engine);
                context.execute()
            }
            Err(query) => {
                let context =
                    <Naive as Eval>::from_rule(&query, additional.as_ref(), &mut dummy_engine);
                context.execute()
            }
        };

        for result in results {
            println!("=> {result}");
        }
    }
}

struct DummyEngine {
    predicate: Predicate,
}

impl DbEngine for DummyEngine {
    fn get_stored_terms(&mut self) -> Vec<Term> {
        self
        .predicate
        .iter_owned()
        .map(|tuple| {
            tuple_2_functor("link", &tuple)
        })
        .collect()
    }
}

fn tuple_2_functor(name: &str, tuple: &[ParameterType]) -> Term {
    let terms: Vec<Term> =
        tuple
        .iter()
        .map(|arg_type| match arg_type {
            ParameterType::Atom(val) => Term::Atom(val.to_string()),
            _ => { panic!("ahhh") }
        })
        .collect();

    Term::Functor(name.to_string(), terms)
}
