use std::collections::{HashMap, HashSet};

const ENUMS_RS: &str = include_str!("../src/enums.rs");
const QUERY_RS: &str = include_str!("../src/query.rs");
const TYPES_RS: &str = include_str!("../src/types.rs");
const UNIFFI_TOML: &str = include_str!("../uniffi.toml");

fn exported_u_types(source: &str) -> HashSet<String> {
    source
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if !(trimmed.starts_with("pub struct U") || trimmed.starts_with("pub enum U")) {
                return None;
            }
            trimmed
                .split_whitespace()
                .nth(2)
                .map(|name| name.trim_end_matches('{').to_string())
        })
        .collect()
}

fn python_rename_map(config: &str) -> HashMap<String, String> {
    let mut in_python_rename_section = false;
    let mut renames = HashMap::new();

    for line in config.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed.starts_with('[') {
            in_python_rename_section = trimmed == "[bindings.python.rename]";
            continue;
        }
        if !in_python_rename_section {
            continue;
        }
        let Some((from, to)) = trimmed.split_once('=') else {
            continue;
        };
        let from = from.trim().to_string();
        let to = to.trim().trim_matches('"').to_string();
        renames.insert(from, to);
    }

    renames
}

#[test]
fn python_rename_config_covers_all_exported_u_types() {
    let expected: HashSet<String> = [ENUMS_RS, QUERY_RS, TYPES_RS]
        .into_iter()
        .flat_map(exported_u_types)
        .collect();
    let renames = python_rename_map(UNIFFI_TOML);
    let actual: HashSet<String> = renames.keys().cloned().collect();

    assert_eq!(
        actual, expected,
        "uniffi.toml must rename every exported U-prefixed binding type"
    );

    for name in expected {
        assert_eq!(
            renames.get(&name).map(String::as_str),
            Some(&name[1..]),
            "expected {name} to be renamed to {}",
            &name[1..]
        );
    }
}
