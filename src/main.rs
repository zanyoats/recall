use std::fs;
use std::path::PathBuf;

use recall::lang::scan::Scanner;
use recall::lang::parse::Parser;
use recall::lang::parse::parse_program;
use recall::eval;
use recall::storage::db::DB;
use recall::errors;

#[derive(Debug)]
struct Config {
    program: String,
    h: bool,
    f: Vec<PathBuf>,
    q: Option<String>,
    db: Option<PathBuf>,
}

fn main() {
    let result =
        parse_config(std::env::args().peekable())
        .and_then(run);

    if let Err(err) = result {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn parse_config(mut iter: impl Iterator<Item = String>) -> Result<Config, errors::RecallError> {
    let program =
        iter
        .next()
        .unwrap();

    let mut result = Config {
        program, h: false, f: vec![], q: None, db: None,
    };

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" => {
                result.h = true;
            },
            "-f" => {
                if let Some(s) = iter.next() {
                    result.f.push(PathBuf::from(s));
                } else {
                    return Err(errors::RecallError::CLIError(format!(
                        "expected file path to after '-f'",
                    )))
                }
            },
            "-q" => {
                if let Some(s) = iter.next() {
                    result.q = Some(s);
                }
            },
            other if other.starts_with('-') => {
                return Err(errors::RecallError::CLIError(format!(
                    "unknown flag '{}'", other,
                )))
            },
            positional => {
                if result.db.is_none() {
                    result.db = Some(PathBuf::from(positional));
                } else {
                    return Err(errors::RecallError::CLIError(format!(
                        "extra positional argument '{}'", positional,
                    )))
                }
            },
        }
    }

    Ok(result)
}

fn run(cfg: Config) -> Result<(), errors::RecallError> {
    if cfg.h {
        println!("{}", usage(&cfg.program));
        return Ok(())
    }

    let mut input = String::new();

    for path in cfg.f {
        let s = fs::read_to_string(path).unwrap();
        input.push_str(&s);
    }

    if let Some(q) = cfg.q  {
        input.push_str(&q);
    } else {
        // TODO: start REPL
        // TODO: if no db given create a in-memory db instance for this evaluation
    }

    let scanner = Scanner::new(&input);
    let mut parser = Parser::new(scanner);
    let program = parse_program(&mut parser)?;

    if let Some(file_path) = cfg.db {
        let db = DB::new(file_path)?;
        let results = eval::eval(program, db)?;

        results
        .into_iter()
        .for_each(|term| {
            println!("{}", term);
        });

        Ok(())
    } else {
        return Err(errors::RecallError::CLIError(format!(
           "db argument required (for now)",
        )))
    }
}

fn usage(program: &str) -> String {
    let mut result = String::new();
    result.push_str("usage: ");
    result.push_str(program);
    result.push_str(" [ options ] [ db ]\n\n");
    result.push_str("Options:\n");
    result.push_str("[-h] help\n");
    result.push_str("[-q] quit, if not supplied starts repl\n");
    result.push_str("[-f file] read in datalog program from file\n");
    result.push_str("[-s '...'] read in datalog program from string\n\n");
    result.push_str("Positional Argument:\n");
    result.push_str("[db] path to db, if not supplied creates an in memory database for this invocation");
    result
}
