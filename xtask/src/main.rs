pub mod codegen;

use camino::Utf8PathBuf;

#[allow(unused_imports)]
use re_type_definitions;

fn main() {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("java") {
        eprintln!("usage: xtask java [--rerun-worktree <path>] <output-dir>");
        std::process::exit(2);
    }

    let mut rerun_worktree = None;
    let mut output_dir = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--rerun-worktree" => rerun_worktree = args.next().map(Utf8PathBuf::from),
            _ if arg.starts_with("--rerun-worktree=") => {
                rerun_worktree = Some(Utf8PathBuf::from(
                    arg.strip_prefix("--rerun-worktree=").unwrap(),
                ));
            }
            _ if arg.starts_with('-') || output_dir.is_some() => {
                eprintln!("usage: xtask java [--rerun-worktree <path>] <output-dir>");
                std::process::exit(2);
            }
            _ => output_dir = Some(Utf8PathBuf::from(arg)),
        }
    }

    let Some(output_dir) = output_dir else {
        eprintln!("usage: xtask java [--rerun-worktree <path>] <output-dir>");
        std::process::exit(2);
    };

    codegen::gen_java(rerun_worktree.as_deref(), &output_dir).unwrap();
}
