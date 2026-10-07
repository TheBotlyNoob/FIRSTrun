mod java;

use anyhow::Context;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use cargo_metadata::MetadataCommand;
use std::path::Path;

pub fn gen_java(
    rerun_worktree: Option<&Utf8Path>,
    output_dir: &Utf8Path,
) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(output_dir).ok();

    let def_path = match rerun_worktree {
        Some(path) => path.into(),
        None => fetch_rerun_definitions()?,
    };

    let (report, reporter) = re_types_builder::report::init();

    let (objects, type_registry) = re_types_builder::generate_lang_agnostic(&reporter, &def_path);

    println!("[+] Generating Java code to {output_dir}");

    java::generate(output_dir, &objects, &type_registry)?;
    report.finalize(false);

    Ok(())
}

pub fn fetch_rerun_definitions() -> Result<Utf8PathBuf, anyhow::Error> {
    let target = env!("TARGET");
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let metadata = MetadataCommand::new()
        .manifest_path(manifest)
        .other_options(["--filter-platform".into(), target.into()])
        .exec()
        .unwrap();

    let path = metadata
        .packages
        .iter()
        .find(|p| p.name == "re_type_definitions")
        .map(|p| p.manifest_path.as_ref())
        .and_then(|p: &Utf8Path| p.parent())
        .map(|p| p.into())
        .context("Failed to find re_type_definitions package")?;

    return Ok(path);
}
