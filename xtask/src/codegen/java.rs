use std::fmt::Write as _;
use std::fs;

use anyhow::{Context as _, Result};
use camino::{Utf8Path, Utf8PathBuf};
use re_types_builder::{AtomicDataType, Docs, Object, ObjectClass, Objects, Type, TypeRegistry};

const JAVA_PACKAGE_PREFIX: &str = "org.teamvan.moneyshift";

pub fn generate(
    output_dir: &Utf8Path,
    objects: &Objects,
    _type_registry: &TypeRegistry,
) -> Result<()> {
    for object in objects.values() {
        let path = output_path(output_dir, object);
        println!(
            "[+] Generating Java code for {} to {}",
            &object.fqname, &path
        );
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {parent}"))?;
        }
        fs::write(&path, render_object(object)).with_context(|| format!("writing {path}"))?;
    }
    Ok(())
}

fn output_path(output_dir: &Utf8Path, object: &Object) -> Utf8PathBuf {
    let package = java_package(&object.fqname);
    let relative_package = package.replace('.', "/");
    let relative_package = relative_package
        .strip_prefix('/')
        .unwrap_or(&relative_package);
    output_dir
        .join(relative_package)
        .join(format!("{}.java", object.name))
}

fn render_object(object: &Object) -> String {
    let package = java_package(&object.fqname);
    let mut code = String::new();
    writeln!(
        code,
        "// DO NOT EDIT: generated from Rerun type definitions."
    )
    .unwrap();
    writeln!(code, "package {package};\n").unwrap();
    write_docs(&mut code, &object.docs, "");

    match object.class {
        ObjectClass::Struct => {
            writeln!(code, "public record {}(", object.name).unwrap();
            for (index, field) in object.fields.iter().enumerate() {
                let comma = if index + 1 == object.fields.len() {
                    ""
                } else {
                    ","
                };
                write_docs(&mut code, &field.docs, "    ");
                writeln!(
                    code,
                    "    {} {}{}",
                    java_type(&field.typ),
                    field_name(&field.name),
                    comma
                )
                .unwrap();
            }
            code.push_str(") {}\n");
        }
        ObjectClass::Enum(_) => {
            writeln!(code, "public enum {} {{", object.name).unwrap();
            for (index, field) in object.fields.iter().enumerate() {
                let comma = if index + 1 == object.fields.len() {
                    ";"
                } else {
                    ","
                };
                write_docs(&mut code, &field.docs, "    ");
                writeln!(code, "    {}{}", field_name(&field.name), comma).unwrap();
            }
            code.push_str("}\n");
        }
        ObjectClass::Union => {
            writeln!(code, "public sealed interface {} permits", object.name).unwrap();
            for (index, field) in object.fields.iter().enumerate() {
                let separator = if index + 1 == object.fields.len() {
                    " {"
                } else {
                    ","
                };
                writeln!(
                    code,
                    "    {}.{}{}",
                    object.name,
                    field_name(&field.name),
                    separator
                )
                .unwrap();
            }
            for field in &object.fields {
                let variant = field_name(&field.name);
                write_docs(&mut code, &field.docs, "    ");
                if field.typ.is_unit() {
                    writeln!(
                        code,
                        "    record {variant}() implements {} {{}}",
                        object.name
                    )
                    .unwrap();
                } else {
                    writeln!(
                        code,
                        "    record {variant}({}) implements {} {{}}",
                        java_type(&field.typ),
                        object.name
                    )
                    .unwrap();
                }
            }
            code.push_str("}\n");
        }
    }

    code
}

fn write_docs(code: &mut String, docs: &Docs, indent: &str) {
    let lines = docs.only_lines_tagged("");
    if lines.is_empty() {
        return;
    }

    writeln!(code, "{indent}/**").unwrap();
    for line in lines {
        writeln!(code, "{indent} * {}", java_doc_line(line)).unwrap();
    }
    writeln!(code, "{indent} */").unwrap();
}

fn java_doc_line(line: &str) -> String {
    let mut output = String::new();
    let mut rest = line;

    while !rest.is_empty() {
        let code_start = rest.find('`');
        let rust_link_start = rest.find("[`");
        let markdown_link_start = rest.find('[');

        if rust_link_start
            .is_some_and(|link_start| code_start.is_none_or(|code_start| link_start < code_start))
        {
            let link_start = rust_link_start.unwrap();
            output.push_str(&rest[..link_start]);
            let after_start = &rest[link_start + 2..];
            if let Some(link_end) = after_start.find("`]") {
                let label = &after_start[..link_end];
                let after_label = &after_start[link_end + 2..];
                let (target, remainder) = if let Some(target) = after_label.strip_prefix('[') {
                    if let Some(target_end) = target.find(']') {
                        (&target[1..target_end], &target[target_end + 1..])
                    } else {
                        (label, after_label)
                    }
                } else {
                    (label, after_label)
                };
                output.push_str("{@link ");
                output.push_str(&java_doc_link(target));
                if target != label {
                    output.push(' ');
                    output.push_str(&escape_javadoc_text(label));
                }
                output.push('}');
                rest = remainder;
                continue;
            }
        }

        if markdown_link_start
            .is_some_and(|link_start| code_start.is_none_or(|code_start| link_start < code_start))
        {
            let link_start = markdown_link_start.unwrap();
            output.push_str(&rest[..link_start]);
            let after_start = &rest[link_start + 1..];
            if let Some(label_end) = after_start.find("](") {
                let label = &after_start[..label_end];
                let after_label = &after_start[label_end + 2..];
                if let Some(url_end) = after_label.find(')') {
                    output.push_str("<a href=\"");
                    output.push_str(&escape_javadoc_attribute(&after_label[..url_end]));
                    output.push_str("\">");
                    output.push_str(&escape_javadoc_text(label));
                    output.push_str("</a>");
                    rest = &after_label[url_end + 1..];
                    continue;
                }
            }
        }

        if let Some(code_start) = code_start {
            output.push_str(&rest[..code_start]);
            let after_start = &rest[code_start + 1..];
            if let Some(code_end) = after_start.find('`') {
                output.push_str("{@code ");
                output.push_str(&escape_javadoc_code(&after_start[..code_end]));
                output.push('}');
                rest = &after_start[code_end + 1..];
                continue;
            }
        }

        output.push_str(rest);
        break;
    }

    output.replace("*/", "*\\/")
}

fn java_doc_link(path: &str) -> String {
    let path = if let Some(path) = path.strip_prefix("crate::") {
        format!("{JAVA_PACKAGE_PREFIX}.{}", path.replace("::", "."))
    } else if let Some(path) = path.strip_prefix("rerun::") {
        format!("{JAVA_PACKAGE_PREFIX}.{}", path.replace("::", "."))
    } else {
        path.to_owned()
    };
    let path = path.replace("::", ".");
    let mut parts = path.rsplitn(2, '.');
    let last = parts.next().unwrap_or_default();
    let Some(prefix) = parts.next() else {
        return path;
    };
    if last
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_lowercase())
    {
        format!("{prefix}#{last}")
    } else {
        format!("{prefix}.{last}")
    }
}

fn escape_javadoc_code(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_javadoc_text(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_javadoc_attribute(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn java_package(fqname: &str) -> String {
    let parent = fqname.rsplit_once('.').map_or(fqname, |(parent, _)| parent);
    format!(
        "{JAVA_PACKAGE_PREFIX}.{}",
        parent.trim_start_matches("rerun.")
    )
}

fn java_type(typ: &Type) -> String {
    match typ {
        Type::Atomic(atomic) => match atomic {
            AtomicDataType::Null => "Void".to_owned(),
            AtomicDataType::Boolean => "boolean".to_owned(),
            AtomicDataType::Int8 => "byte".to_owned(),
            AtomicDataType::Int16 => "short".to_owned(),
            AtomicDataType::Int32 => "int".to_owned(),
            AtomicDataType::Int64 => "long".to_owned(),
            AtomicDataType::UInt8 => "short".to_owned(),
            AtomicDataType::UInt16 => "int".to_owned(),
            AtomicDataType::UInt32 => "long".to_owned(),
            AtomicDataType::UInt64 => "java.math.BigInteger".to_owned(),
            AtomicDataType::Float16 | AtomicDataType::Float32 => "float".to_owned(),
            AtomicDataType::Float64 => "double".to_owned(),
        },
        Type::Binary => "byte[]".to_owned(),
        Type::Utf8 => "String".to_owned(),
        Type::FixedSizeList { elem_type, length } => {
            format!("{}[] /* length {length} */", java_type(elem_type))
        }
        Type::List { elem_type } => format!("java.util.List<{}>", boxed_java_type(elem_type)),
        Type::Object { fqname } => format!(
            "{}.{name}",
            java_package(fqname),
            name = fqname.rsplit('.').next().unwrap()
        ),
    }
}

fn boxed_java_type(typ: &Type) -> String {
    match typ {
        Type::Atomic(AtomicDataType::Boolean) => "Boolean".to_owned(),
        Type::Atomic(AtomicDataType::Int8 | AtomicDataType::UInt8) => "Byte".to_owned(),
        Type::Atomic(AtomicDataType::Int16 | AtomicDataType::UInt16) => "Short".to_owned(),
        Type::Atomic(AtomicDataType::Int32 | AtomicDataType::UInt32) => "Integer".to_owned(),
        Type::Atomic(AtomicDataType::Int64 | AtomicDataType::UInt64) => "Long".to_owned(),
        Type::Atomic(AtomicDataType::Float16 | AtomicDataType::Float32) => "Float".to_owned(),
        Type::Atomic(AtomicDataType::Float64) => "Double".to_owned(),
        Type::Atomic(AtomicDataType::Null) => "Void".to_owned(),
        _ => java_type(typ),
    }
}

fn field_name(name: &str) -> String {
    let mut result = String::new();
    for (index, character) in name.chars().enumerate() {
        if character == '_' || character.is_ascii_alphanumeric() {
            if index == 0 && character.is_ascii_digit() {
                result.push('_');
            }
            result.push(character);
        } else {
            result.push('_');
        }
    }
    if is_java_keyword(&result) {
        format!("_{result}")
    } else {
        result
    }
}

fn is_java_keyword(name: &str) -> bool {
    matches!(
        name,
        "abstract"
            | "assert"
            | "boolean"
            | "break"
            | "byte"
            | "case"
            | "catch"
            | "char"
            | "class"
            | "const"
            | "continue"
            | "default"
            | "do"
            | "double"
            | "else"
            | "enum"
            | "extends"
            | "final"
            | "finally"
            | "float"
            | "for"
            | "goto"
            | "if"
            | "implements"
            | "import"
            | "instanceof"
            | "int"
            | "interface"
            | "long"
            | "native"
            | "new"
            | "package"
            | "private"
            | "protected"
            | "public"
            | "return"
            | "short"
            | "static"
            | "strictfp"
            | "super"
            | "switch"
            | "synchronized"
            | "this"
            | "throw"
            | "throws"
            | "transient"
            | "try"
            | "void"
            | "volatile"
            | "while"
            | "true"
            | "false"
            | "null"
    )
}

#[cfg(test)]
mod tests {
    use re_types_builder::Docs;

    use super::{field_name, java_doc_line, java_package, write_docs};

    #[test]
    fn maps_rerun_names_to_java_packages() {
        assert_eq!(
            java_package("rerun.components.Position2D"),
            "org.van.rerun.components"
        );
    }

    #[test]
    fn escapes_invalid_field_names() {
        assert_eq!(field_name("3d-value"), "_3d_value");
    }

    #[test]
    fn renders_docs_as_javadoc() {
        let docs = Docs::from_lines(
            &re_types_builder::report::init().1,
            "test",
            "Test",
            [" A description.", "", " A closing `*/`."].into_iter(),
        );
        let mut code = String::new();
        write_docs(&mut code, &docs, "    ");
        assert_eq!(
            code,
            "    /**\n     * A description.\n     * \n     * A closing {@code *\\/}.\n     */\n"
        );
    }

    #[test]
    fn converts_rustdoc_links_and_code() {
        assert_eq!(
            java_doc_line("See [`rerun::components::Position2D`] and [`crate::foo::draw`]."),
            "See {@link org.van.rerun.components.Position2D} and {@link org.van.rerun.foo#draw}."
        );
        assert_eq!(
            java_doc_line("Use `Vec<T>` or [the guide](https://example.com?a=1&b=2)."),
            "Use {@code Vec&lt;T&gt;} or <a href=\"https://example.com?a=1&amp;b=2\">the guide</a>."
        );
    }
}
