use std::slice;
use std::hash::Hash;
use std::fmt;

use crate::errors;
use crate::lang::scan::DatalogToken;
use crate::lang::scan::Scanner;
use crate::lang::parsers::GrammarRule;
use crate::lang::parsers::SyntaxTree;
use crate::lang::parsers::SyntaxToken;
use crate::lang::parsers::TokenInputStream;
use crate::lang::parsers::LLGrammar;

#[derive(Debug)]
pub struct DatalogAST {
    pub program: Vec<Statement>,
}

#[derive(Debug)]
pub enum Statement {
    Asserted(Literal),
    Retracted(Literal),
    Query(Predicate),
}

impl DatalogAST {
    pub fn from_string(program: &str) -> Result<Self, errors::RecallError> {
        let scanner = Scanner::new(program);
        let input_stream = DatalogTokenStream::new(scanner);
        let rules = datalog_grammar_rules();
        let grammar = LLGrammar::new(&rules)?;
        let pn = DatalogSyntaxTree::parse(
            input_stream,
            &grammar.table,
            &grammar.terminals,
            "Program",
        )?;
        Ok(DatalogAST::from_program(&pn))
    }

    fn from_program(tree: &DatalogSyntaxTree) -> Self {
        let stmts = &tree.children[0];
        Self::from_stmts(stmts)
    }

    fn from_stmts(stmts: &DatalogSyntaxTree) -> Self {
        use DatalogToken::*;

        let mut program = vec![];
        let mut stmts = stmts;

        while stmts.children.len() > 0 {
            let stmt = &stmts.children[0];
            let literal = &stmt.children[0];
            let stmt_ = &stmt.children[1];
            if { /* statement with body? */
                match &stmt_.children[0].elem {
                    Some(If) => true,
                    _ => false
                }
            } {
                let body = &stmt_.children[1];
                let end_body = &stmt_.children[2];

                match &end_body.children[0].elem {
                    Some(Assertion) => {
                        program.push(Statement::Asserted(Self::get_literal(literal, body)));
                    }
                    Some(Retraction) => {
                        program.push(Statement::Retracted(Self::get_literal(literal, body)));
                    }
                    _ => panic!("unexpected end body"),
                }
            } else {
                match &stmt_.children[0].elem {
                    Some(Assertion) => {
                        program.push(Statement::Asserted(Literal::from_head(Self::get_predicate(literal))));
                    }
                    Some(Retraction) => {
                        program.push(Statement::Retracted(Literal::from_head(Self::get_predicate(literal))));
                    }
                    Some(Query) => {
                        program.push(Statement::Query(Self::get_predicate(literal)));
                    }
                    _ => panic!("unexpected begin to stmt_"),
                }
            }

            stmts = &stmts.children[1];
        }


        DatalogAST { program }
    }

    fn get_predicate(literal: &DatalogSyntaxTree) -> Predicate {
        use DatalogToken::*;

        let pred_sym = &literal.children[0];
        let literal_ = &literal.children[1];

        let predicate = match &pred_sym.children[0].elem {
            Some(Atom(val)) => val.clone(),
            Some(String(val)) => val.clone(),
            _ => panic!("unexpected type for pred_sym"),
        };
        let arguments =
            if literal_.children.len() > 0 {
                Self::collect_terms(&literal_.children[1])
            } else {
                vec![]
            };

        Predicate::new(predicate, arguments)
    }

    fn get_literal(literal: &DatalogSyntaxTree, body: &DatalogSyntaxTree) -> Literal {
        let head = Self::get_predicate(literal);
        let mut result = vec![];
        {
            let literal = &body.children[0];
            result.push(Self::get_predicate(literal));
        }

        let mut body_ = &body.children[1];

        while body_.children.len() > 0 {
            let literal = &body_.children[1];
            result.push(Self::get_predicate(literal));
            body_ = &body_.children[2];
        }

        Literal { head, body: result }
    }

    fn collect_terms(terms: &DatalogSyntaxTree) -> Vec<Term> {
        let mut result = vec![];

        if terms.children.len() == 0 {
            return result;
        }

        let mut term = &terms.children[0];
        result.push(Self::get_term(term));
        let mut terms_ = &terms.children[1];

        while terms_.children.len() > 0 {
            term = &terms_.children[1];
            result.push(Self::get_term(term));
            terms_ = &terms_.children[2];
        }

        result
    }

    fn get_term(term: &DatalogSyntaxTree) -> Term {
        use DatalogToken::*;

        match &term.children[0].elem {
            Some(Atom(val)) => Term::Atom(val.clone()),
            Some(String(val)) => Term::String(val.clone()),
            Some(Var(val)) => Term::Var(val.clone()),
            Some(Expr(sym)) if sym == "Numeric" => Self::get_numeric_term(&term.children[0], true),
            Some(Plus) => Self::get_numeric_term(&term.children[1], true),
            Some(Minus) => Self::get_numeric_term(&term.children[1], false),
            _ => panic!("unexpected type for term"),
        }
    }

    fn get_numeric_term(numeric: &DatalogSyntaxTree, is_pos: bool) -> Term {
        use DatalogToken::*;

        match &numeric.children[0].elem {
            Some(Integer(val)) => Term::Integer(if is_pos { *val } else { -*val }),
            _ => panic!("unexpected type for numeric"),
        }
    }
}

pub type Predicate = Term;

impl Predicate {
    fn new(name: String, args: Vec<Term>) -> Self {
        Term::Functor(name, args)
    }

    pub fn name(term: &Term) -> &String{
        if let Term::Functor(name, _) = term {
            name
        } else {
            panic!("predicate not functor term type")
        }
    }

    pub fn arguments(term: &Term) -> &Vec<Term> {
        if let Term::Functor(_, args) = term {
            args
        } else {
            panic!("predicate not functor term type")
        }
    }
}

#[derive(Debug, Clone)]
pub struct Literal {
    pub head: Predicate,
    pub body: Vec<Predicate>,
}

impl Literal {
    fn from_head(head: Predicate) -> Self {
        Literal { head, body: vec![] }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Term {
    Atom(String),
    String(String),
    Var(String),
    Integer(i32),
    Functor(String, Vec<Term>), // grammar does not allow this at the moment
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Atom(value) => write!(f, "{}", value),
            Term::String(value) => write!(f, "\"{}\"", value),
            Term::Var(value) => write!(f, "{}", value),
            Term::Integer(value) => write!(f, "{}", value),
            Term::Functor(name, terms) => {
                write!(f, "{}", name)?;
                if terms.len() > 0 {
                    write!(f, "(")?;

                    let formatted: Vec<String> = terms
                        .iter()
                        .map(|term| format!("{}", term))
                        .collect();

                    write!(f, "{}", formatted.join(", "))?;

                    write!(f, ")")?;
                }
                Ok(())
            }
        }
    }
}

fn datalog_grammar_rules() -> Vec<GrammarRule> {
    vec![
        GrammarRule::new("Program", &["Stmts", "$"]),
        GrammarRule::new("Stmts", &["Stmt", "Stmts"]),
        GrammarRule::new("Stmts", &[]),
        GrammarRule::new("Stmt", &["Literal", "Stmt_"]),
        GrammarRule::new("Stmt_", &["?"]),
        GrammarRule::new("Stmt_", &["."]),
        GrammarRule::new("Stmt_", &["!"]),
        GrammarRule::new("Stmt_", &[":-", "Body", "EndBody"]),
        GrammarRule::new("EndBody", &["."]),
        GrammarRule::new("EndBody", &["!"]),
        GrammarRule::new("Body", &["Literal", "Body_"]),
        GrammarRule::new("Body_", &[",", "Literal", "Body_"]),
        GrammarRule::new("Body_", &[]),
        GrammarRule::new("Literal", &["PredSym", "Literal_"]),
        GrammarRule::new("Literal_", &["(", "Terms", ")"]),
        GrammarRule::new("Literal_", &[]),
        GrammarRule::new("PredSym", &["atom"]),
        GrammarRule::new("PredSym", &["string"]),
        GrammarRule::new("Terms", &["Term", "Terms_"]),
        GrammarRule::new("Terms", &[]),
        GrammarRule::new("Terms_", &[",", "Term", "Terms_"]),
        GrammarRule::new("Terms_", &[]),
        GrammarRule::new("Term", &["atom"]),
        GrammarRule::new("Term", &["string"]),
        GrammarRule::new("Term", &["var"]),
        GrammarRule::new("Term", &["Numeric"]),
        GrammarRule::new("Term", &["+", "Numeric"]),
        GrammarRule::new("Term", &["-", "Numeric"]),
        GrammarRule::new("Numeric", &["integer"]),
        GrammarRule::new("Numeric", &["float"]),
    ]
}

impl SyntaxToken for DatalogToken {
    fn label(&self) -> String {
        match self {
            Self::Assertion => ".".to_string(),
            Self::Retraction => "!".to_string(),
            Self::Query => "?".to_string(),
            Self::If => ":-".to_string(),
            Self::Comma => ",".to_string(),
            Self::Plus => "+".to_string(),
            Self::Minus => "-".to_string(),
            Self::LeftParen => "(".to_string(),
            Self::RightParen => ")".to_string(),
            Self::End => "$".to_string(),
            Self::Atom(_) => "atom".to_string(),
            Self::String(_) => "string".to_string(),
            Self::Var(_) => "var".to_string(),
            Self::Integer(_) => "integer".to_string(),
            Self::Float(_) => "float".to_string(),
            Self::Expr(sym) => sym.to_string(),
        }
    }

    fn from_sym(sym: &str) -> Self {
        Self::Expr(sym.to_string())
    }
}

#[derive(Debug)]
struct DatalogSyntaxTree {
    elem: Option<DatalogToken>,
    children: Vec<DatalogSyntaxTree>,
}

impl SyntaxTree<DatalogToken> for DatalogSyntaxTree {
    fn new() -> Self {
        DatalogSyntaxTree { elem: None, children: vec![] }
    }

    fn update(&mut self, elem: DatalogToken) {
        self.elem = Some(elem);
    }

    fn add_child(&mut self, child: Self) {
        self.children.push(child);
    }

    fn children_mut(&mut self) -> slice::IterMut<Self> {
        self.children.iter_mut()
    }

    fn children(&self) -> slice::Iter<Self> {
        self.children.iter()
    }
}

struct DatalogTokenStream<'a> {
    scanner: Scanner<'a>,
    buf: Option<DatalogToken>,
    end_of_stream: bool,
}

impl<'a> DatalogTokenStream<'a> {
    fn new(scanner: Scanner<'a>) -> Self {
        DatalogTokenStream { scanner, buf: None, end_of_stream: false }
    }
}

impl<'a> TokenInputStream<DatalogToken> for DatalogTokenStream<'a> {
    fn peek_token(&mut self) -> Result<Option<&DatalogToken>, errors::RecallError> {
        if self.end_of_stream {
            return Ok(None)
        }

        if self.buf.is_none() {
            self.buf = Some(self.scanner.next_token()?);
        }

        Ok(self.buf.as_ref())
    }

    fn next_token(&mut self) -> Result<Option<DatalogToken>, errors::RecallError> {
        match self.buf.take() {
            Some(token) => {
                if let DatalogToken::End = token {
                    self.end_of_stream = true;
                }

                Ok(Some(token))
            },
            None => {
                if self.end_of_stream {
                    return Ok(None)
                }

                let token = self.scanner.next_token()?;

                if let DatalogToken::End = token {
                    self.end_of_stream = true;
                }

                Ok(Some(token))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_scans_and_parses_datalog() {
        let program = "
            bar(a). bar(z).
            score(42). score(+42). score(-42).
            foo(X) :- bar(X).
            foo(X)?
            bar(Z)!
        ";
        let ast = DatalogAST::from_string(program);
        ast.unwrap();
    }
}
