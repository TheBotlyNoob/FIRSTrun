use std::path::Path;

use camino::Utf8Path;

static RE_DEFINITIONS_DIR_PATH: &str = "crates/store/re_types/definitions";

pub fn gen_components(re_worktree: &Utf8Path) -> Result<(), anyhow::Error> {
    let def_path = re_worktree.join(RE_DEFINITIONS_DIR_PATH);

    let (report, reporter) = re_types_builder::report::init();

    let (objects, type_registry) = re_types_builder::generate_lang_agnostic(
        &reporter,
        &def_path,
        def_path.join("entry_point.fbs"),
    );

    let mut generator = NetworkTablesCodeGenerator::new(output_path.as_ref());

    Ok(())
}
