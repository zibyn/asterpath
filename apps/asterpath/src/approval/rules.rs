//! Command rules (GLOSSARY「命令规则」, ADR-0055): `[approval]`'s `allow` /
//! `ask` / `deny` lists and the builtin read-only commands, matched against
//! one tool call without knowing the approval mode.
//!
//! A rule is `tool` or `bash(prefix)`. A shell command is parsed with
//! `brush-parser` and split into simple commands; a prefix matches a simple
//! command whose words start with the prefix's words. Deny and ask match any
//! recognizable simple command, also inside substitutions; allow needs every
//! simple command allowed and a command plain enough to judge.

use brush_parser::ast::{
    Command, CommandPrefixOrSuffixItem, CompoundCommand, CompoundList, IoFileRedirectKind,
    IoFileRedirectTarget, IoRedirect, Program, SimpleCommand,
};
use brush_parser::word::{WordPiece, WordPieceWithSource};
use brush_parser::{Parser, ParserOptions};

/// The one tool that takes a command prefix.
const BASH: &str = "bash";

/// Longer commands match no allow rule.
const MAX_ALLOWED_LEN: usize = 4096;

/// Builtin allow rules: commands that only read. Not `git`: a repository's
/// config can make it run programs.
const READ_ONLY: &[&str] = &[
    "cat", "echo", "find", "grep", "head", "ls", "pwd", "rg", "tail", "wc",
];

/// `find` arguments that write or run programs.
const FIND_UNSAFE: &[&str] = &[
    "-delete", "-exec", "-execdir", "-ok", "-okdir", "-fls", "-fprint", "-fprint0", "-fprintf",
];

/// Which list a call matched.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RuleKind {
    Allow,
    Ask,
    Deny,
}

#[derive(Clone, Debug)]
struct Rule {
    tool: String,
    /// `None` matches every call of `tool`.
    prefix: Option<Vec<String>>,
}

impl Rule {
    fn parse(raw: &str) -> Result<Self, &'static str> {
        let (tool, prefix) = match raw.split_once('(') {
            None => (raw, None),
            Some((tool, rest)) => {
                let spec = rest.strip_suffix(')').ok_or("missing `)`")?;
                let words: Vec<String> = spec.split_whitespace().map(str::to_owned).collect();
                if words.is_empty() {
                    return Err("empty command prefix");
                }
                (tool, Some(words))
            }
        };
        if tool.is_empty() || tool.contains([' ', '(', ')']) {
            return Err("expected `tool` or `bash(command prefix)`");
        }
        if prefix.is_some() && tool != BASH {
            return Err("only bash takes a command prefix");
        }
        Ok(Self {
            tool: tool.to_owned(),
            prefix,
        })
    }
}

/// The user's rules; the builtin read-only commands are always added.
#[derive(Clone, Debug, Default)]
pub struct CommandRules {
    allow: Vec<Rule>,
    ask: Vec<Rule>,
    deny: Vec<Rule>,
}

impl CommandRules {
    /// The lists of `[approval]`; an error names the bad entry.
    pub fn new(allow: &[String], ask: &[String], deny: &[String]) -> Result<Self, String> {
        let parse = |key: &str, list: &[String]| {
            list.iter()
                .enumerate()
                .map(|(index, raw)| {
                    Rule::parse(raw)
                        .map_err(|error| format!("approval.{key}[{index}] `{raw}`: {error}"))
                })
                .collect::<Result<Vec<_>, _>>()
        };
        Ok(Self {
            allow: parse("allow", allow)?,
            ask: parse("ask", ask)?,
            deny: parse("deny", deny)?,
        })
    }

    /// The strongest list matching a call of `tool` with JSON `args`, or `None`.
    pub fn matches(&self, tool: &str, args: &str) -> Option<RuleKind> {
        let parsed = (tool == BASH).then(|| command(args)).flatten();
        let parsed = parsed.map(|source| (parse(&source), source.len()));
        let commands = parsed
            .as_ref()
            .map_or(&[][..], |(parsed, _)| &parsed.commands);
        // Deny and ask: the whole tool, or any simple command by prefix.
        let hits = |rules: &[Rule]| {
            rules.iter().any(|rule| {
                rule.tool == tool
                    && rule.prefix.as_deref().is_none_or(|prefix| {
                        commands
                            .iter()
                            .any(|simple| simple.starts_with(prefix, true))
                    })
            })
        };
        if hits(&self.deny) {
            return Some(RuleKind::Deny);
        }
        if hits(&self.ask) {
            return Some(RuleKind::Ask);
        }
        let prefixes = || self.allow.iter().filter(|rule| rule.tool == tool);
        if prefixes().any(|rule| rule.prefix.is_none()) {
            return Some(RuleKind::Allow);
        }
        let (parsed, len) = parsed?;
        let allowed = parsed.plain
            && len <= MAX_ALLOWED_LEN
            && !parsed.commands.is_empty()
            && parsed.commands.iter().all(|simple| {
                simple.plain()
                    && (read_only(simple)
                        || prefixes()
                            .filter_map(|rule| rule.prefix.as_deref())
                            .any(|prefix| simple.starts_with(prefix, false)))
            });
        allowed.then_some(RuleKind::Allow)
    }

    /// Adds the allow rules of [`session_rules`] for a call of `tool` with
    /// JSON `args`; false, adding none, when there are none.
    pub fn allow_for_session(&mut self, tool: &str, args: &str) -> bool {
        let Some(rules) = session(tool, args) else {
            return false;
        };
        self.allow.extend(rules);
        true
    }
}

/// What "allow for session" allows for a call of `tool` with JSON `args`:
/// the tool, or for `bash` each simple command's words up to its first
/// non-option argument (`npm run test -- --watch` gives `npm run`), shown
/// as those words; `None` for a command not plain enough to give them.
pub fn session_rules(tool: &str, args: &str) -> Option<Vec<String>> {
    let rules = session(tool, args)?;
    Some(
        rules
            .into_iter()
            .map(|rule| rule.prefix.map_or(rule.tool, |words| words.join(" ")))
            .collect(),
    )
}

fn session(tool: &str, args: &str) -> Option<Vec<Rule>> {
    if tool != BASH {
        return Some(vec![Rule {
            tool: tool.to_owned(),
            prefix: None,
        }]);
    }
    let parsed = parse(&command(args)?);
    if !parsed.plain || parsed.commands.is_empty() {
        return None;
    }
    let mut rules: Vec<Rule> = Vec::new();
    for simple in &parsed.commands {
        let (name, rest) = simple.words.split_first()?;
        let mut prefix = vec![name.clone()?];
        for word in rest {
            let word = word.clone()?;
            let option = word.starts_with('-');
            prefix.push(word);
            if !option {
                break;
            }
        }
        if !rules
            .iter()
            .any(|rule| rule.prefix.as_ref() == Some(&prefix))
        {
            rules.push(Rule {
                tool: BASH.to_owned(),
                prefix: Some(prefix),
            });
        }
    }
    Some(rules)
}

/// `bash`'s `command` argument.
fn command(args: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(args).ok()?;
    value.get("command")?.as_str().map(str::to_owned)
}

/// One simple command as far as it can be read without running anything.
#[derive(Default)]
struct Simple {
    /// Each word unquoted; `None` for one that expands at run time.
    words: Vec<Option<String>>,
    /// A word holds an unquoted glob or brace pattern.
    glob: bool,
    /// A variable assignment (`PATH=…`) comes first.
    assigns: bool,
    /// A redirection writes a file other than `/dev/null`.
    writes: bool,
}

impl Simple {
    /// Whether the words start with `prefix`; `loose` compares the command
    /// word by its file name, so `/usr/bin/git` counts as `git`.
    fn starts_with(&self, prefix: &[String], loose: bool) -> bool {
        self.words.len() >= prefix.len()
            && self
                .words
                .iter()
                .zip(prefix)
                .enumerate()
                .all(|(index, (word, want))| {
                    word.as_deref().is_some_and(|word| {
                        let word = if loose && index == 0 {
                            word.rsplit('/').next().unwrap_or(word)
                        } else {
                            word
                        };
                        word == want
                    })
                })
    }

    fn plain(&self) -> bool {
        !self.assigns && !self.writes
    }
}

/// A builtin read-only command in a form that cannot run or write anything.
fn read_only(simple: &Simple) -> bool {
    let Some(words) = simple
        .words
        .iter()
        .map(Option::as_deref)
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let Some((name, args)) = words.split_first() else {
        return false;
    };
    !simple.glob
        && READ_ONLY.contains(name)
        && match *name {
            "find" => !args.iter().any(|arg| FIND_UNSAFE.contains(arg)),
            // `--pre` runs a program on each file, `-z` runs decompressors.
            "rg" => !args.iter().any(|arg| {
                *arg == "--pre"
                    || arg.starts_with("--pre=")
                    || *arg == "--search-zip"
                    || (arg.starts_with('-') && !arg.starts_with("--") && arg.contains('z'))
            }),
            _ => true,
        }
}

#[derive(Default)]
struct Parsed {
    commands: Vec<Simple>,
    /// Nothing ran unseen: no substitution, function or other construct the
    /// walk does not read. Unparsable source is not plain.
    plain: bool,
}

fn options() -> ParserOptions {
    ParserOptions::default()
}

fn parse(source: &str) -> Parsed {
    let mut parsed = Parsed {
        commands: Vec::new(),
        plain: true,
    };
    match Parser::new(source.as_bytes(), &options()).parse_program() {
        Ok(program) => program_commands(&program, &mut parsed),
        Err(_) => parsed.plain = false,
    }
    parsed
}

fn program_commands(program: &Program, parsed: &mut Parsed) {
    for complete in &program.complete_commands {
        list(complete, parsed);
    }
}

fn list(list: &CompoundList, parsed: &mut Parsed) {
    for item in &list.0 {
        let and_or = &item.0;
        for pipeline in
            std::iter::once(&and_or.first).chain(and_or.additional.iter().map(|next| match next {
                brush_parser::ast::AndOr::And(pipeline)
                | brush_parser::ast::AndOr::Or(pipeline) => pipeline,
            }))
        {
            for command in &pipeline.seq {
                self::command_node(command, parsed);
            }
        }
    }
}

fn command_node(command: &Command, parsed: &mut Parsed) {
    match command {
        Command::Simple(simple) => simple_command(simple, parsed),
        Command::Compound(compound, redirects) => {
            for redirect in redirects.iter().flat_map(|list| &list.0) {
                if redirect_writes(redirect, parsed) {
                    // Attributed to no simple command; enough to stop an allow.
                    parsed.plain = false;
                }
            }
            compound_command(compound, parsed);
        }
        Command::Function(function) => {
            parsed.plain = false;
            compound_command(&function.body.0, parsed);
        }
        Command::ExtendedTest(..) => parsed.plain = false,
    }
}

fn compound_command(compound: &CompoundCommand, parsed: &mut Parsed) {
    match compound {
        CompoundCommand::BraceGroup(group) => list(&group.list, parsed),
        CompoundCommand::Subshell(subshell) => list(&subshell.list, parsed),
        CompoundCommand::IfClause(clause) => {
            list(&clause.condition, parsed);
            list(&clause.then, parsed);
            for clause in clause.elses.iter().flatten() {
                if let Some(condition) = &clause.condition {
                    list(condition, parsed);
                }
                list(&clause.body, parsed);
            }
        }
        CompoundCommand::WhileClause(clause) | CompoundCommand::UntilClause(clause) => {
            list(&clause.0, parsed);
            list(&clause.1.list, parsed);
        }
        // Their words (loop values, case patterns) are not read.
        CompoundCommand::ForClause(clause) => {
            parsed.plain = false;
            list(&clause.body.list, parsed);
        }
        CompoundCommand::ArithmeticForClause(clause) => {
            parsed.plain = false;
            list(&clause.body.list, parsed);
        }
        CompoundCommand::CaseClause(clause) => {
            parsed.plain = false;
            for item in &clause.cases {
                if let Some(cmd) = &item.cmd {
                    list(cmd, parsed);
                }
            }
        }
        CompoundCommand::Coprocess(coprocess) => {
            parsed.plain = false;
            command_node(&coprocess.body, parsed);
        }
        CompoundCommand::Arithmetic(_) => parsed.plain = false,
    }
}

fn simple_command(command: &SimpleCommand, parsed: &mut Parsed) {
    let mut simple = Simple::default();
    let prefix = command.prefix.iter().flat_map(|prefix| &prefix.0);
    let suffix = command.suffix.iter().flat_map(|suffix| &suffix.0);
    for item in prefix {
        item_of(item, &mut simple, parsed);
    }
    if let Some(name) = &command.word_or_name {
        read(name, &mut simple, parsed);
    }
    for item in suffix {
        item_of(item, &mut simple, parsed);
    }
    parsed.commands.push(simple);
}

fn read(word: &brush_parser::ast::Word, simple: &mut Simple, parsed: &mut Parsed) {
    let (text, glob) = unquote(&word.value, parsed);
    simple.glob |= glob;
    simple.words.push(text);
}

fn item_of(item: &CommandPrefixOrSuffixItem, simple: &mut Simple, parsed: &mut Parsed) {
    match item {
        CommandPrefixOrSuffixItem::Word(word) => read(word, simple, parsed),
        CommandPrefixOrSuffixItem::IoRedirect(redirect) => {
            simple.writes |= redirect_writes(redirect, parsed);
        }
        CommandPrefixOrSuffixItem::AssignmentWord(_, word) => {
            simple.assigns = true;
            // Read for its substitutions only.
            let _ = unquote(&word.value, parsed);
        }
        CommandPrefixOrSuffixItem::ProcessSubstitution(_, subshell) => {
            parsed.plain = false;
            list(&subshell.list, parsed);
        }
    }
}

/// Whether `redirect` writes a file other than `/dev/null`.
fn redirect_writes(redirect: &IoRedirect, parsed: &mut Parsed) -> bool {
    match redirect {
        IoRedirect::File(_, kind, target) => {
            let target = match target {
                IoFileRedirectTarget::Filename(word) | IoFileRedirectTarget::Duplicate(word) => {
                    unquote(&word.value, parsed).0
                }
                IoFileRedirectTarget::Fd(_) => return false,
                IoFileRedirectTarget::ProcessSubstitution(_, subshell) => {
                    parsed.plain = false;
                    list(&subshell.list, parsed);
                    return false;
                }
            };
            match kind {
                IoFileRedirectKind::Read => false,
                // `>&2` and the like; anything else is a file.
                IoFileRedirectKind::DuplicateInput | IoFileRedirectKind::DuplicateOutput => {
                    !target.is_some_and(|fd| fd.chars().all(|c| c.is_ascii_digit() || c == '-'))
                }
                _ => target.as_deref() != Some("/dev/null"),
            }
        }
        IoRedirect::OutputAndError(word, _) => {
            unquote(&word.value, parsed).0.as_deref() != Some("/dev/null")
        }
        // Bodies may hold substitutions; not read.
        IoRedirect::HereDocument(..) | IoRedirect::HereString(..) => {
            parsed.plain = false;
            false
        }
    }
}

/// `raw` unquoted, or `None` when it expands at run time, and whether it
/// holds an unquoted glob or brace. Substitutions are parsed for their
/// commands and make the source not plain.
fn unquote(raw: &str, parsed: &mut Parsed) -> (Option<String>, bool) {
    let Ok(pieces) = brush_parser::word::parse(raw, &options()) else {
        parsed.plain = false;
        return (None, false);
    };
    let mut text = Some(String::new());
    let mut glob = false;
    pieces_text(&pieces, &mut text, &mut glob, false, parsed);
    (text, glob)
}

fn pieces_text(
    pieces: &[WordPieceWithSource],
    text: &mut Option<String>,
    glob: &mut bool,
    quoted: bool,
    parsed: &mut Parsed,
) {
    let push = |text: &mut Option<String>, value: &str| {
        if let Some(text) = text {
            text.push_str(value);
        }
    };
    for piece in pieces {
        match &piece.piece {
            WordPiece::Text(value) => {
                *glob |= !quoted && value.contains(['*', '?', '[', '{']);
                push(text, value);
            }
            WordPiece::SingleQuotedText(value) | WordPiece::AnsiCQuotedText(value) => {
                push(text, value);
            }
            WordPiece::EscapeSequence(value) => {
                push(text, value.strip_prefix('\\').unwrap_or(value));
            }
            WordPiece::DoubleQuotedSequence(inner)
            | WordPiece::GettextDoubleQuotedSequence(inner) => {
                pieces_text(inner, text, glob, true, parsed)
            }
            // The home directory: a path, never an option.
            WordPiece::TildeExpansion(_) => push(text, "~"),
            WordPiece::CommandSubstitution(source)
            | WordPiece::BackquotedCommandSubstitution(source) => {
                let inner = self::parse(source);
                parsed.commands.extend(inner.commands);
                parsed.plain = false;
                *text = None;
            }
            WordPiece::ParameterExpansion(_) | WordPiece::ArithmeticExpression(_) => *text = None,
        }
    }
}
