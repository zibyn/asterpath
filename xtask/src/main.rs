mod deps;

fn main() {
    let command = std::env::args().nth(1);
    match command.as_deref() {
        Some("check-deps") => match deps::check_workspace() {
            Ok(()) => {}
            Err(violations) => {
                for violation in violations {
                    eprintln!("error: {violation}");
                }
                std::process::exit(1);
            }
        },
        _ => {
            eprintln!("usage: cargo xtask check-deps");
            std::process::exit(2);
        }
    }
}
