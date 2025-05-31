use std::fs;
use std::path::PathBuf;

use recall::lang::program::DatalogAST;
use recall::eval;

#[derive(Debug)]
struct Config {
    h: bool,
    f: Vec<PathBuf>,
    q: Option<String>,
    db: Option<PathBuf>,
}

fn main() {
    let mut cfg = Config {
        h: false, f: vec![], q: None, db: None,
    };

    let mut iter = std::env::args().peekable();
    let program =
        iter
        .next()
        .unwrap();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" => {
                cfg.h = true;
            },
            "-f" => {
                if let Some(s) = iter.next() {
                    cfg.f.push(PathBuf::from(s));
                } else {
                    print_usage_and_exit(&program, Some("expected file path to after '-f'"))
                }
            },
            "-q" => {
                if let Some(s) = iter.next() {
                    cfg.q = Some(s);
                }
            },
            other if other.starts_with('-') => {
                let reason = format!("unknown flag '{}'", other);
                print_usage_and_exit(&program, Some(&reason))
            },
            positional => {
                if cfg.db.is_none() {
                    cfg.db = Some(PathBuf::from(positional));
                } else {
                    let reason = format!("extra positional argument '{}'", positional);
                    print_usage_and_exit(&program, Some(&reason))
                }
            },
        }
    }

    if cfg.h {
        print_usage_and_exit(&program, None)
    }

    let mut input = String::new();

    for path in cfg.f {
        let s = fs::read_to_string(path).unwrap();
        input.push_str(&s);
    }

    if let Some(q) = cfg.q  {
        input.push_str(&q);
    }

    let ast = DatalogAST::from_string(&input);
    let ast = ast.unwrap();
    let mut mem = eval::Memory::new();
    let results = eval::eval(ast, &mut mem);

    results
    .into_iter()
    .for_each(|term| {
        println!("{}", term);
    });
}

fn usage(program: &str) -> String {
    format!(
        "usage: {} [-h] [ [-f file1] ... ] [ -q ['... aditional datalog program'] ] [db]",
        program,
    )
}

fn print_usage_and_exit(program: &str, reason: Option<&str>) -> ! {
    if let Some(reason) = reason {
        eprintln!(
            "error: {reason}\n{}",
            usage(program),
        );
        std::process::exit(1)
    } else {
        eprintln!("{}", usage(program));
        std::process::exit(0)
    }
}
