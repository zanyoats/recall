use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::zip;
use std::iter::Peekable;
use std::slice;
use std::fmt;

use crate::errors;

#[derive(Debug)]
struct LLGrammar<'a> {
    non_terminals: HashSet<&'a str>,
    terminals: HashSet<&'a str>,
    table: LLGrammarTable<'a>,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct GrammarRule {
    lhs: String,
    rhs: Vec<String>,
}

impl fmt::Display for GrammarRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {}", self.lhs, self.rhs.join(" "))?;
        Ok(())
    }
}

type LLGrammarTable<'a> = HashMap<(&'a str, &'a str), Vec<&'a GrammarRule>>;

type LLGrammarSets<'a> = (
    HashMap<&'a str, bool>,
    HashMap<&'a str, HashSet<&'a str>>,
    HashMap<&'a str, HashSet<&'a str>>,
);

impl GrammarRule {
    fn new(lhs: &str, rhs: &[&str]) -> Self {
        let rhs: Vec<String> =
            rhs
            .iter()
            .map(|x| x.to_string())
            .collect();

        GrammarRule { lhs: lhs.to_string(), rhs }
    }

}

impl<'a> LLGrammar<'a> {
    fn new(grammar: &'a [GrammarRule]) -> Result<Self, errors::RecallError> {
        let non_terminals: HashSet<&str> =
            grammar
            .iter()
            .map(|rule| rule.lhs.as_str())
            .collect();
        let terminals: HashSet<&str> =
            grammar
            .iter()
            .flat_map(|rule| { &rule.rhs })
            .filter_map(|elem| {
                let elem = elem.as_str();
                if non_terminals.contains(elem) { None }
                else { Some(elem) }
            })
            .collect();
        let table =
            Self::build_parsing_table(grammar, &non_terminals, &terminals);

        // check that grammar is unambiguous
        let mut error = String::new();
        for non_terminal in non_terminals.iter() {
            for terminal in terminals.iter() {
                if let Some(rules) =
                    table
                    .get(&(*non_terminal, *terminal))
                {
                    if rules.len() > 1 {
                        error.push_str(&format!("On entry ({}, {}) there are {} rules:\n", *non_terminal, *terminal, rules.len()));
                        for rule in rules {
                            error.push('\t');
                            error.push_str(&rule.to_string());
                            error.push('\n');
                        }
                    }
                }
            }
        }

        if error.len() > 0 {
            Err(errors::RecallError::NonLLGrammar(format!("Grammar not LL(1)\n{}", error)))
        } else {
            Ok(LLGrammar { non_terminals, terminals, table })
        }
    }

    fn terminals_for_rule(rule: &'a GrammarRule, sets: &LLGrammarSets<'a>) -> HashSet<&'a str> {
        let x = rule.lhs.as_str();
        let expr_iter = rule.rhs.iter().map(|x| x.as_str());
        let (
            nullable,
            first,
            follow,
        ) = sets;

        let mut expr_nullable = true;
        let mut result = HashSet::new();

        for elem in expr_iter {
            result.extend(
                first
                .get(elem)
                .unwrap()
                .iter()
            );

            if !nullable.get(elem).unwrap() {
                expr_nullable = false;
                break;
            }
        }

        if expr_nullable {
            result.extend(
                follow
                .get(x)
                .unwrap()
                .iter()
            );
        }

        result
    }

    fn build_parsing_table(
        grammar: &'a [GrammarRule],
        non_terminals: &HashSet<&'a str>,
        terminals: &HashSet<&'a str>,
    ) -> LLGrammarTable<'a> {
        let mut table: LLGrammarTable<'a> = HashMap::new();
        let sets = Self::compute_sets(grammar, &non_terminals, &terminals);

        for rule in grammar.iter() {
            let columns = Self::terminals_for_rule(rule, &sets);

            for column in columns {
                table
                .entry((rule.lhs.as_str(), column))
                .and_modify(|e: &mut Vec<&GrammarRule>| {
                    e.push(rule);
                })
                .or_insert(vec![rule]);
            }
        }

        table
    }

    fn print_table(&self) {
        for non_terminal in self.non_terminals.iter() {
            for terminal in self.terminals.iter() {
                let result =
                    self
                    .table
                    .get(&(*non_terminal, *terminal))
                    .map_or_else(|| "no entry".to_string(), |rules| {
                        rules
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                    });

                println!("[{}, {}] {}", non_terminal, terminal, result);
            }
            println!();
        }
    }

    fn compute_sets(
        grammar: &'a [GrammarRule],
        non_terminals: &HashSet<&'a str>,
        terminals: &HashSet<&'a str>,
    ) -> LLGrammarSets<'a> {
        let mut nullable = HashMap::new();
        let mut first = HashMap::new();
        let mut follow = HashMap::new();
        for &non_terminal in non_terminals.iter() {
            assert!(nullable.insert(non_terminal, false).is_none());
            assert!(first.insert(non_terminal, HashSet::new()).is_none());
            assert!(follow.insert(non_terminal, HashSet::new()).is_none());
        }
        for &terminal in terminals.iter() {
            assert!(nullable.insert(terminal, false).is_none());
            first.insert(
                terminal,
                HashSet::from_iter(vec![terminal].into_iter()),
            );
            follow.insert(terminal, HashSet::new());
        }

        // loop until fixed point is found
        loop {
            let mut no_change = true;

            for rule in grammar.iter() { /* for each X -> Y_0 Y_1 .. Y_k */
                let x = rule.lhs.as_str();
                let ys = &rule.rhs;
                let k = ys.len();

                if (0..k).all(|i| { /* if all Y_i are nullable for X -> X is nullable */
                    *nullable.get(ys[i].as_str()).unwrap()
                }) {
                    let x_nullable = nullable.get_mut(x).unwrap();
                    if !*x_nullable {
                        *x_nullable = true;
                        no_change = false;
                    }
                }

                for i in 0..k {
                    if (0..i).all(|i| { /* if Y0 .. Y_i-1 are nullable */
                        *nullable.get(ys[i].as_str()).unwrap()
                    }) { /* FIRST(X) = FIRST(X) union FIRST(Y_i) */
                        let new_elems_iter =
                            first
                            .get(ys[i].as_str())
                            .unwrap()
                            .clone()
                            .into_iter();

                        let x_elems = first.get_mut(x).unwrap();

                        for elem in new_elems_iter {
                            if x_elems.contains(elem) {
                                continue;
                            }

                            x_elems.insert(elem);
                            no_change = false;
                        }
                    }

                    for j in i + 1..k { /* if Y_i+1 .. Y_j are nullable */
                        if (i+1..j).all(|i| {
                            *nullable.get(ys[i].as_str()).unwrap()
                        }) { /* FOLLOW(Y_i) = FOLLOW(Y_i) union FIRST(Y_j) */
                            let new_elems_iter =
                                first
                                .get(ys[j].as_str())
                                .unwrap()
                                .clone()
                                .into_iter();

                            let yi_elems = follow.get_mut(ys[i].as_str()).unwrap();

                            for elem in new_elems_iter {
                                if yi_elems.contains(elem) {
                                    continue;
                                }

                                yi_elems.insert(elem);
                                no_change = false;
                            }
                        }
                    }

                    if (i+1..k).all(|i| { /* if Y_i+1 .. Y_k are nullable */
                        *nullable.get(ys[i].as_str()).unwrap()
                    }) { /* FOLLOW(Y_i) = FOLLOW(Y_i) union FOLLOW(X) */
                        let new_elems_iter =
                            follow
                            .get(x)
                            .unwrap()
                            .clone()
                            .into_iter();

                        let yi_elems = follow.get_mut(ys[i].as_str()).unwrap();

                        for elem in new_elems_iter {
                            if yi_elems.contains(elem) {
                                continue;
                            }

                            yi_elems.insert(elem);
                            no_change = false;
                        }
                    }
                }
            }
            if no_change {
                break;
            }
        }

        (nullable, first, follow)
    }
}

trait SyntaxToken {
    fn label(&self) -> String;
    fn from_sym(sym: &str) -> Self;
}

trait SyntaxTree<E>
    where Self: Sized,
          E: SyntaxToken + Clone + fmt::Display,
{
    fn new() -> Self;

    fn elem_as_string(&self) -> String;

    fn update(&mut self, elem: E);

    fn add_child(&mut self, child: Self);

    fn children_mut(&mut self) -> slice::IterMut<Self>;

    fn children(&self) -> slice::Iter<Self>;

    fn parse<'a>(
        mut input: Peekable<impl Iterator<Item = E>>,
        table: &LLGrammarTable<'a>,
        terminals: &HashSet<&'a str>,
        start_sym: &str,
    ) -> Result<Self, errors::RecallError> {
        let mut stack = vec![];
        let mut start = Self::new();
        start.update(E::from_sym(start_sym));
        stack.push((start_sym, &mut start));

        while let Some((sym, pn)) = stack.pop() {
            if let Some(token) = input.peek() {
                if terminals.contains(sym) {
                    if token.label() != sym {
                        return Err(errors::RecallError::ParserError(format!("wanted {:?} but got {:?} from input", sym, token.label())));
                    }

                    pn.update(token.clone());

                    input.next().unwrap(); // consume input
                } else {
                    if let Some(rules) = table.get(&(sym, &token.label())) {
                        let rule = rules[0];

                        pn.update(E::from_sym(sym));

                        rule
                        .rhs
                        .iter()
                        .for_each(|_| {
                            pn.add_child(Self::new());
                        });

                        for (sym, pn) in zip(
                            rule.rhs.iter().rev(),
                            pn.children_mut().rev(),
                        ) {
                            stack.push((sym, pn));
                        }
                    } else {
                        return Err(errors::RecallError::ParserError(format!("no way to parse input {:?} using rule {:?}", token.label(), sym)));
                    }
                }
            } else {
                return Err(errors::RecallError::ParserError(format!("input ended wanted {:?}", sym)));
            }
        }

        if input.peek().is_none() {
            Ok(start)
        } else {
            let rem =
                input
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Err(errors::RecallError::ParserError(format!("input parsed completely with remaining input tokens: {:?}", rem)))
        }
    }

    fn print_tree(&self, indent: usize, print_arrow: bool) {
        // Prepare the prefix: indent and arrow if needed.
        let indent_str = " ".repeat(indent);
        let arrow = if print_arrow { "-> " } else { "" };

        // Print the current node.
        println!("{}{}{}", indent_str, arrow, self.elem_as_string());
        // Recursively print each child with increased indentation.
        for child in self.children() {
            child.print_tree(indent + 2, true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod datalog_grammar {
        use super::*;

        fn datalog_grammar() -> Vec<GrammarRule> {
            vec![
                // Program -> Stmts $
                GrammarRule::new("Program", &["Stmts", "$"]),
                // Stmts -> Stmt Stmts
                // Stmts ->
                GrammarRule::new("Stmts", &["Stmt", "Stmts"]),
                GrammarRule::new("Stmts", &[]),
                // Stmt -> Literal Stmt'
                GrammarRule::new("Stmt", &["Literal", "Stmt'"]),
                // Stmt' -> ?
                // Stmt' -> .
                // Stmt' -> !
                // Stmt' -> :- Body EndBody
                GrammarRule::new("Stmt'", &["?"]),
                GrammarRule::new("Stmt'", &["."]),
                GrammarRule::new("Stmt'", &["!"]),
                GrammarRule::new("Stmt'", &[":-", "Body", "EndBody"]),
                // EndBody -> .
                // EndBody -> !
                GrammarRule::new("EndBody", &["."]),
                GrammarRule::new("EndBody", &["!"]),
                // Body -> Literal Body'
                GrammarRule::new("Body", &["Literal", "Body'"]),
                // Body' -> , Literal Body'
                // Body' ->
                GrammarRule::new("Body'", &[",", "Literal", "Body'"]),
                GrammarRule::new("Body'", &[]),
                // Literal -> PredSym Literal'
                GrammarRule::new("Literal", &["PredSym", "Literal'"]),
                // Literal' -> ( Terms )
                // Literal' ->
                GrammarRule::new("Literal'", &["(", "Terms", ")"]),
                GrammarRule::new("Literal'", &[]),
                // PredSym -> atom
                // PredSym -> string
                GrammarRule::new("PredSym", &["atom"]),
                GrammarRule::new("PredSym", &["string"]),
                // Terms -> Term Terms'
                // Terms ->
                GrammarRule::new("Terms", &["Term", "Terms'"]),
                GrammarRule::new("Terms", &[]),
                // Terms' -> , Term Terms'
                // Terms' ->
                GrammarRule::new("Terms'", &[",", "Term", "Terms'"]),
                GrammarRule::new("Terms'", &[]),
                // Term -> atom
                // Term -> string
                // Term -> var
                // Term -> number
                GrammarRule::new("Term", &["atom"]),
                GrammarRule::new("Term", &["string"]),
                GrammarRule::new("Term", &["var"]),
                GrammarRule::new("Term", &["number"]),
            ]
        }

        #[derive(Debug, Clone)]
        pub enum DatalogToken {
            Assertion,
            Retraction,
            Query,
            If,
            Comma,
            LeftParen,
            RightParen,
            End,
            Atom(String),
            String(String),
            Var(String),
            Number(i32),
            Expr(String),
        }

        impl fmt::Display for DatalogToken {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", match self {
                    Self::Assertion => ".".to_string(),
                    Self::Retraction => "!".to_string(),
                    Self::Query => "?".to_string(),
                    Self::If => ":-".to_string(),
                    Self::Comma => ",".to_string(),
                    Self::LeftParen => "(".to_string(),
                    Self::RightParen => ")".to_string(),
                    Self::Atom(_) => "atom".to_string(),
                    Self::String(_) => "string".to_string(),
                    Self::Var(_) => "var".to_string(),
                    Self::Number(_) => "number".to_string(),
                    Self::Expr(sym) => sym.to_string(),
                    Self::End => "$".to_string(),
                })?;
                Ok(())
            }
        }

        impl SyntaxToken for DatalogToken {
            fn label(&self) -> String {
                match self {
                    Self::Assertion => ".".to_string(),
                    Self::Retraction => "!".to_string(),
                    Self::Query => "?".to_string(),
                    Self::If => ":-".to_string(),
                    Self::Comma => ",".to_string(),
                    Self::LeftParen => "(".to_string(),
                    Self::RightParen => ")".to_string(),
                    Self::Atom(_) => "atom".to_string(),
                    Self::String(_) => "string".to_string(),
                    Self::Var(_) => "var".to_string(),
                    Self::Number(_) => "number".to_string(),
                    Self::Expr(sym) => sym.to_string(),
                    Self::End => "$".to_string(),
                }
            }

            fn from_sym(sym: &str) -> Self {
                Self::Expr(sym.to_string())
            }
        }

        struct DatalogSyntaxTree<E: SyntaxToken> {
            elem: Option<E>,
            children: Vec<DatalogSyntaxTree<E>>,
        }

        impl<E: SyntaxToken + fmt::Debug + Clone + fmt::Display> SyntaxTree<E> for DatalogSyntaxTree<E> {
            fn new() -> Self {
                DatalogSyntaxTree { elem: None, children: vec![] }
            }

            fn elem_as_string(&self) -> String {
                self.elem.as_ref().unwrap().to_string()
            }

            fn update(&mut self, elem: E) {
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

        #[test]
        fn it_can_parse_datalog_grammar() {
            let input = vec![
                DatalogToken::Atom("foo".to_string()),
                DatalogToken::LeftParen,
                DatalogToken::Var("A".to_string()),
                DatalogToken::Comma,
                DatalogToken::Var("B".to_string()),
                DatalogToken::RightParen,
                DatalogToken::If,
                //
                DatalogToken::Atom("bar".to_string()),
                DatalogToken::LeftParen,
                DatalogToken::Var("A".to_string()),
                DatalogToken::Comma,
                DatalogToken::Var("B".to_string()),
                DatalogToken::RightParen,
                //
                DatalogToken::Assertion,
                //
                DatalogToken::Atom("foo".to_string()),
                DatalogToken::LeftParen,
                DatalogToken::RightParen,
                DatalogToken::Retraction,
                DatalogToken::End,
            ];
            let grammar = datalog_grammar();
            let result = LLGrammar::new(&grammar);
            assert!(result.is_ok());
            let ll = result.unwrap();
            let pn = DatalogSyntaxTree::parse(
                input.into_iter().peekable(),
                &ll.table,
                &ll.terminals,
                "Program",
            );
            assert!(pn.is_ok());
        }
    }

    mod simple_grammar {
        use super::*;

        fn simple_arithmetic_grammar() -> Vec<GrammarRule> {
            vec![
                // Grammar
                //
                // non-terminal symbols are simply keys
                // terminal symbols are those rhs elements that have no corresponding lhs (production)
                // S -> E$
                GrammarRule::new("S", &["E", "$"]),
                // E -> T E'
                GrammarRule::new("E", &["T", "E'"]),
                // E' -> + T E'
                // E' -> - T E'
                // E' ->
                GrammarRule::new("E'", &["+", "T", "E'"]),
                GrammarRule::new("E'", &["-", "T", "E'"]),
                GrammarRule::new("E'", &[]),
                // T -> F T'
                GrammarRule::new("T", &["F", "T'"]),
                // T' -> * F T'
                // T' -> / F T'
                // T' ->
                GrammarRule::new("T'", &["*", "F", "T'"]),
                GrammarRule::new("T'", &["/", "F", "T'"]),
                GrammarRule::new("T'", &[]),
                // F -> id
                // F -> num
                // F -> ( E )
                GrammarRule::new("F", &["id"]),
                GrammarRule::new("F", &["num"]),
                GrammarRule::new("F", &["(", "E", ")"]),
            ]
        }

        // Simple grammars
        #[derive(Debug, Clone)]
        pub enum SimpleToken {
            LeftParen,
            RightParen,
            Op(String),
            Ident(String),
            Num(i32),
            Expr(String),
            End,
        }

        impl fmt::Display for SimpleToken {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", match self {
                    Self::LeftParen => "(".to_string(),
                    Self::RightParen => ")".to_string(),
                    Self::Op(label) => label.clone(),
                    Self::Ident(_) => "id".to_string(),
                    Self::Num(_) => "num".to_string(),
                    Self::Expr(sym) => sym.to_string(),
                    Self::End => "$".to_string(),
                })?;
                Ok(())
            }
        }

        impl SyntaxToken for SimpleToken {
            fn label(&self) -> String {
                match self {
                    Self::LeftParen => "(".to_string(),
                    Self::RightParen => ")".to_string(),
                    Self::Op(label) => label.clone(),
                    Self::Ident(_) => "id".to_string(),
                    Self::Num(_) => "num".to_string(),
                    Self::Expr(sym) => sym.to_string(),
                    Self::End => "$".to_string(),
                }
            }

            fn from_sym(sym: &str) -> Self {
                Self::Expr(sym.to_string())
            }
        }

        struct SimpleSyntaxTree<E: SyntaxToken> {
            elem: Option<E>,
            children: Vec<SimpleSyntaxTree<E>>,
        }

        impl<E: SyntaxToken + fmt::Debug + Clone + fmt::Display> SyntaxTree<E> for SimpleSyntaxTree<E> {
            fn new() -> Self {
                SimpleSyntaxTree { elem: None, children: vec![] }
            }

            fn elem_as_string(&self) -> String {
                self.elem.as_ref().unwrap().to_string()
            }

            fn update(&mut self, elem: E) {
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

        #[test]
        fn it_can_parse_input_0() {
            let input = vec![
                SimpleToken::Ident("foo".to_string()),
                SimpleToken::End,
            ];
            let grammar = simple_arithmetic_grammar();
            let result = LLGrammar::new(&grammar);
            assert!(result.is_ok());
            let ll = result.unwrap();
            let pn = SimpleSyntaxTree::parse(
                input.into_iter().peekable(),
                &ll.table,
                &ll.terminals,
                "S",
            );
            assert!(pn.is_ok());
        }

        #[test]
        fn it_can_parse_input_1() {
            let input = vec![
                SimpleToken::Num(42),
                SimpleToken::Op("+".to_string()),
                SimpleToken::Num(100),
                SimpleToken::End,
            ];
            let grammar = simple_arithmetic_grammar();
            let result = LLGrammar::new(&grammar);
            assert!(result.is_ok());
            let ll = result.unwrap();
            let pn = SimpleSyntaxTree::parse(
                input.into_iter().peekable(),
                &ll.table,
                &ll.terminals,
                "S",
            );
            assert!(pn.is_ok());
        }

        #[test]
        fn it_can_parse_input_2() {
            let input = vec![
                SimpleToken::LeftParen,
                SimpleToken::Num(42),
                SimpleToken::Op("-".to_string()),
                SimpleToken::Num(100),
                SimpleToken::RightParen,
                SimpleToken::Op("/".to_string()),
                SimpleToken::Ident("n".to_string()),
                SimpleToken::End,
            ];
            let grammar = simple_arithmetic_grammar();
            let result = LLGrammar::new(&grammar);
            assert!(result.is_ok());
            let ll = result.unwrap();
            let pn = SimpleSyntaxTree::parse(
                input.into_iter().peekable(),
                &ll.table,
                &ll.terminals,
                "S",
            );
            assert!(pn.is_ok());
        }
    }

    mod non_ll {
        use super::*;

        fn non_ll_grammar_1() -> Vec<GrammarRule> {
            vec![
                // Grammar
                //
                // non-terminal symbols are simply keys
                // terminal symbols are those rhs elements that have no corresponding lhs (production)
                // S -> Z$
                GrammarRule::new("S", &["Z", "$"]),         //0
                // Z -> XYZ
                // Z -> d
                GrammarRule::new("Z", &["X", "Y", "Z"]),    //1
                GrammarRule::new("Z", &["d"]),              //2
                // Y ->
                // Y -> c
                GrammarRule::new("Y", &[]),                 //3
                GrammarRule::new("Y", &["c"]),              //4
                // X -> Y
                // X -> a
                GrammarRule::new("X", &["Y"]),              //5
                GrammarRule::new("X", &["a"]),              //6
            ]
        }

        #[test]
        fn it_works_on_non_ll_grammar() {
            let grammar = non_ll_grammar_1();
            let result = LLGrammar::new(&grammar);
            assert!(result.is_err());
            if let errors::RecallError::NonLLGrammar(m) = result.unwrap_err() {
                assert!(m.len() > 0);
            } else {
                assert!(false, "expected grammar to not be ll(1)");
            }
        }
    }
}
