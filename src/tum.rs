use oma_pm::apt::OmaOperation;
use oma_tum::TopicUpdateEntryRef;
use serde::Serialize;
use std::collections::BTreeMap;
use tracing::warn;

#[derive(Debug, Serialize)]
pub(crate) struct UpdatesListResponse {
    #[serde(flatten)]
    operation: OmaOperation,
    has_important_updates: bool,
    tum: Vec<TumEntry>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TumEntry {
    id: String,
    kind: &'static str,
    security: bool,
    important: bool,
    name: BTreeMap<String, String>,
    caution: Option<BTreeMap<String, String>>,
    packages: Vec<String>,
    topics: Vec<String>,
    package_count: usize,
}

fn tum_entry(id: &str, entry: TopicUpdateEntryRef<'_>) -> TumEntry {
    match entry {
        TopicUpdateEntryRef::Conventional {
            security,
            packages,
            packages_v2,
            name,
            caution,
        } => {
            let mut package_names = if packages_v2.is_empty() {
                packages.keys().cloned().collect::<Vec<_>>()
            } else {
                packages_v2.keys().cloned().collect::<Vec<_>>()
            };
            package_names.sort_unstable();

            TumEntry {
                id: id.to_string(),
                kind: "conventional",
                security,
                important: security,
                name: name.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                caution: caution
                    .map(|values| values.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                package_count: package_names.len(),
                packages: package_names,
                topics: Vec::new(),
            }
        }
        TopicUpdateEntryRef::Cumulative {
            security,
            name,
            caution,
            topics,
            count_packages_changed,
        } => TumEntry {
            id: id.to_string(),
            kind: "cumulative",
            security,
            important: security,
            name: name.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            caution: caution
                .map(|values| values.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            packages: Vec::new(),
            topics: topics.to_vec(),
            package_count: count_packages_changed,
        },
    }
}

pub(crate) fn updates_list_response(
    lists_dir: &str,
    operation: OmaOperation,
) -> UpdatesListResponse {
    let mut tum = match oma_tum::get_tum(lists_dir) {
        Ok(manifests) => oma_tum::get_matches_tum(&manifests, &operation)
            .into_iter()
            .map(|(id, entry)| tum_entry(id, entry))
            .collect::<Vec<_>>(),
        Err(e) => {
            warn!(
                error = e.to_string(),
                "Failed to load topic update manifests"
            );
            Vec::new()
        }
    };

    tum.sort_unstable_by(|a, b| b.security.cmp(&a.security).then_with(|| a.id.cmp(&b.id)));
    let has_important_updates = tum.iter().any(|entry| entry.security);

    UpdatesListResponse {
        operation,
        has_important_updates,
        tum,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oma_pm::apt::{InstallEntry, InstallOperation, PackageUrl};

    fn matched_topic(security: bool) -> TumEntry {
        let json = format!(
            r#"{{
                "test-topic": {{
                    "type": "conventional",
                    "security": {security},
                    "name": {{"default": "Test topic"}},
                    "caution": {{"default": "Test caution"}},
                    "packages": {{"test-package": "2.0"}}
                }}
            }}"#
        );
        let manifest = oma_tum::parse_single_tum(json.as_bytes()).unwrap();
        let manifests = vec![manifest];
        let operation = OmaOperation {
            install: vec![
                InstallEntry::builder()
                    .name("test-package".to_string())
                    .name_without_arch("test-package".to_string())
                    .old_version("1.0".to_string())
                    .new_version("2.0".to_string())
                    .new_size(1)
                    .pkg_urls(vec![PackageUrl {
                        download_url: String::new(),
                        index_url: String::new(),
                    }])
                    .arch("amd64".to_string())
                    .download_size(1)
                    .op(InstallOperation::Upgrade)
                    .index(0)
                    .build(),
            ],
            remove: Vec::new(),
            disk_size_delta: 0,
            autoremovable: (0, 0),
            total_download_size: 1,
            suggest: Vec::new(),
            recommend: Vec::new(),
        };
        let mut matches = oma_tum::get_matches_tum(&manifests, &operation);
        tum_entry("test-topic", matches.remove("test-topic").unwrap())
    }

    #[test]
    fn security_tum_is_important() {
        let topic = matched_topic(true);
        assert!(topic.security);
        assert!(topic.important);
    }

    #[test]
    fn non_security_tum_is_not_important() {
        let topic = matched_topic(false);
        assert!(!topic.security);
        assert!(!topic.important);
    }
}
